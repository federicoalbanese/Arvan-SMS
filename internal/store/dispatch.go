package store

import (
	"context"
	"maps"
	"slices"
	"time"

	"github.com/jackc/pgx/v5"

	"sms-gateway/internal/sms"
)

// DispatchPending is one step of the transactional-outbox relay.
//
// It locks up to limit pending messages of one lane, hands them to publish
// (which pushes them to Redis) and marks them queued, all inside one
// transaction. If publish fails, the transaction rolls back and the rows
// stay pending for the next attempt. If the process dies after publish but
// before commit, the rows are published again later; that duplicate is
// harmless because ClaimForSending only claims a message once.
//
// FOR UPDATE SKIP LOCKED lets several relays run in parallel without
// blocking each other or picking the same rows.
func (s *Store) DispatchPending(ctx context.Context, express bool, limit int,
	publish func(context.Context, []sms.QueueItem) error) (int, error) {
	var n int
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		rows, err := tx.Query(ctx, `
			SELECT id, customer_id FROM messages
			WHERE status = 'pending' AND express = $1 AND next_attempt_at <= now()
			ORDER BY next_attempt_at, id
			LIMIT $2
			FOR UPDATE SKIP LOCKED`, express, limit)
		if err != nil {
			return err
		}
		items, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (sms.QueueItem, error) {
			var it sms.QueueItem
			err := row.Scan(&it.ID, &it.CustomerID)
			return it, err
		})
		if err != nil || len(items) == 0 {
			return err
		}

		if err := publish(ctx, items); err != nil {
			return err
		}

		ids := make([]int64, len(items))
		for i, it := range items {
			ids[i] = it.ID
		}
		if _, err := tx.Exec(ctx, `
			UPDATE messages SET status = 'queued', updated_at = now()
			WHERE id = ANY($1)`, ids); err != nil {
			return err
		}
		n = len(items)
		return nil
	})
	return n, err
}

// ClaimForSending moves messages to sending and returns them. A message is
// claimed at most once, so an id that Redis delivered twice is sent once.
// attempts is incremented here and acts as a fencing token for FinishSending.
//
// Why "pending" is accepted too: the relay publishes to Redis *before* its
// transaction commits, so a fast worker can pop an id while the committed
// row still says pending. Matching pending rows makes Postgres wait for the
// relay's row lock and then re-check the row: after the commit it is
// queued and gets claimed. Without this, the id would be dropped and the
// message would stay queued forever. Pending rows waiting for a retry
// (next_attempt_at in the future) are not claimable, so backoff is kept.
func (s *Store) ClaimForSending(ctx context.Context, ids []int64) ([]sms.Message, error) {
	rows, err := s.pool.Query(ctx, `
		UPDATE messages SET status = 'sending', attempts = attempts + 1, updated_at = now()
		WHERE id = ANY($1)
		  AND (status = 'queued' OR (status = 'pending' AND next_attempt_at <= now()))
		RETURNING `+messageColumns, ids)
	if err != nil {
		return nil, err
	}
	msgs, err := pgx.CollectRows(rows, scanMessage)
	sortByID(msgs)
	return msgs, err
}

// FinishSending writes the outcome of a batch of hand-over attempts and
// refunds permanently failed messages, all in one transaction.
//
// Every update is guarded by "status = 'sending' AND attempts = <attempt>":
// if the reconciler already gave the message to another worker (because
// this one looked dead), this late result is ignored instead of
// overwriting the newer attempt or refunding twice.
func (s *Store) FinishSending(ctx context.Context, outcomes []sms.SendOutcome) error {
	var sent, retry, failed []sms.SendOutcome
	for _, o := range outcomes {
		switch o.Kind {
		case sms.OutcomeSent:
			sent = append(sent, o)
		case sms.OutcomeRetry:
			retry = append(retry, o)
		case sms.OutcomeFailed:
			failed = append(failed, o)
		}
	}

	return pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		if len(sent) > 0 {
			ids, attempts := idsAndAttempts(sent)
			refs := collect(sent, func(o sms.SendOutcome) string { return o.OperatorRef })
			if _, err := tx.Exec(ctx, `
				UPDATE messages m
				SET status = 'sent', operator_ref = u.ref, last_error = NULL,
				    sent_at = now(), updated_at = now()
				FROM unnest($1::bigint[], $2::int[], $3::text[]) AS u(id, attempt, ref)
				WHERE m.id = u.id AND m.status = 'sending' AND m.attempts = u.attempt`,
				ids, attempts, refs); err != nil {
				return err
			}
		}

		if len(retry) > 0 {
			ids, attempts := idsAndAttempts(retry)
			at := collect(retry, func(o sms.SendOutcome) time.Time { return o.RetryAt })
			errs := collect(retry, func(o sms.SendOutcome) string { return o.Error })
			if _, err := tx.Exec(ctx, `
				UPDATE messages m
				SET status = 'pending', next_attempt_at = u.retry_at, last_error = u.err, updated_at = now()
				FROM unnest($1::bigint[], $2::int[], $3::timestamptz[], $4::text[]) AS u(id, attempt, retry_at, err)
				WHERE m.id = u.id AND m.status = 'sending' AND m.attempts = u.attempt`,
				ids, attempts, at, errs); err != nil {
				return err
			}
		}

		if len(failed) > 0 {
			return failAndRefund(ctx, tx, failed)
		}
		return nil
	})
}

// failAndRefund marks messages failed and gives their cost back. Only rows
// that were really updated are refunded (RETURNING), so a refund can never
// happen twice for the same message.
func failAndRefund(ctx context.Context, tx pgx.Tx, failed []sms.SendOutcome) error {
	ids, attempts := idsAndAttempts(failed)
	errs := collect(failed, func(o sms.SendOutcome) string { return o.Error })
	rows, err := tx.Query(ctx, `
		UPDATE messages m
		SET status = 'failed', last_error = u.err, updated_at = now()
		FROM unnest($1::bigint[], $2::int[], $3::text[]) AS u(id, attempt, err)
		WHERE m.id = u.id AND m.status = 'sending' AND m.attempts = u.attempt
		RETURNING m.customer_id, m.cost`, ids, attempts, errs)
	if err != nil {
		return err
	}

	type refund struct {
		amount int64
		count  int
	}
	refunds := make(map[int64]*refund)
	var customerID, cost int64
	_, err = pgx.ForEachRow(rows, []any{&customerID, &cost}, func() error {
		r := refunds[customerID]
		if r == nil {
			r = &refund{}
			refunds[customerID] = r
		}
		r.amount += cost
		r.count++
		return nil
	})
	if err != nil {
		return err
	}

	// Lock customer rows in a fixed order (ascending id) so two concurrent
	// refund transactions can never deadlock on each other.
	for _, id := range slices.Sorted(maps.Keys(refunds)) {
		r := refunds[id]
		var balance int64
		if err := tx.QueryRow(ctx, `
			UPDATE customers SET balance = balance + $2, updated_at = now()
			WHERE id = $1
			RETURNING balance`, id, r.amount).Scan(&balance); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO balance_transactions (customer_id, kind, amount, balance_after, message_count)
			VALUES ($1, 'refund', $2, $3, $4)`, id, r.amount, balance, r.count); err != nil {
			return err
		}
	}
	return nil
}

func idsAndAttempts(outcomes []sms.SendOutcome) ([]int64, []int) {
	ids := make([]int64, len(outcomes))
	attempts := make([]int, len(outcomes))
	for i, o := range outcomes {
		ids[i], attempts[i] = o.MessageID, o.Attempt
	}
	return ids, attempts
}

func collect[T any](outcomes []sms.SendOutcome, f func(sms.SendOutcome) T) []T {
	out := make([]T, len(outcomes))
	for i, o := range outcomes {
		out[i] = f(o)
	}
	return out
}

// RequeueQueued puts queued messages back to pending so the relay pushes
// them to Redis again. Used for ids that were popped from Redis by a worker
// that died before claiming them.
func (s *Store) RequeueQueued(ctx context.Context, ids []int64) (int64, error) {
	tag, err := s.pool.Exec(ctx, `
		UPDATE messages SET status = 'pending', next_attempt_at = now(), updated_at = now()
		WHERE id = ANY($1) AND status = 'queued'`, ids)
	return tag.RowsAffected(), err
}

// SyncRedisInstance records the run id of the Redis instance the workers
// use. If it differs from the stored one, Redis restarted or failed over
// and its queues can no longer be trusted, so every queued message goes
// back to pending and the relay publishes it again.
//
// Both steps run in one transaction, so a crash cannot record the new id
// without the reset. When several workers call this at the same time, the
// row lock on system_state makes exactly one of them do the reset.
func (s *Store) SyncRedisInstance(ctx context.Context, runID string) (requeued int64, changed bool, err error) {
	err = pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		tag, err := tx.Exec(ctx, `
			INSERT INTO system_state (key, value) VALUES ('redis_run_id', $1)
			ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value
			WHERE system_state.value IS DISTINCT FROM EXCLUDED.value`, runID)
		if err != nil || tag.RowsAffected() == 0 {
			return err
		}
		changed = true
		tag, err = tx.Exec(ctx, `
			UPDATE messages SET status = 'pending', next_attempt_at = now(), updated_at = now()
			WHERE status = 'queued'`)
		requeued = tag.RowsAffected()
		return err
	})
	return requeued, changed, err
}

// RequeueStuckSending retries messages whose worker claimed them but never
// reported a result (it crashed or lost the database). The operator may or
// may not have received them, so this is at-least-once delivery.
func (s *Store) RequeueStuckSending(ctx context.Context, olderThan time.Duration) (int64, error) {
	tag, err := s.pool.Exec(ctx, `
		UPDATE messages
		SET status = 'pending', next_attempt_at = now(), updated_at = now(),
		    last_error = 'worker did not report a result in time; retrying'
		WHERE status = 'sending' AND updated_at < now() - make_interval(secs => $1)`,
		olderThan.Seconds())
	return tag.RowsAffected(), err
}

// PendingCounts returns how many messages wait in the outbox, per lane.
func (s *Store) PendingCounts(ctx context.Context) (express, normal int64, err error) {
	err = s.pool.QueryRow(ctx, `
		SELECT count(*) FILTER (WHERE express), count(*) FILTER (WHERE NOT express)
		FROM messages WHERE status = 'pending'`).Scan(&express, &normal)
	return express, normal, err
}
