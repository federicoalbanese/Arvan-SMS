package store

import (
	"cmp"
	"context"
	"errors"
	"slices"
	"time"

	"github.com/jackc/pgx/v5"

	"sms-gateway/internal/sms"
)

const messageColumns = `id, customer_id, phone, body, express, status, cost, attempts,
	COALESCE(operator_ref, ''), COALESCE(last_error, ''), deadline_at, created_at, sent_at`

func scanMessage(row pgx.CollectableRow) (sms.Message, error) {
	var m sms.Message
	err := row.Scan(&m.ID, &m.CustomerID, &m.To, &m.Text, &m.Express, &m.Status, &m.Cost,
		&m.Attempts, &m.OperatorRef, &m.LastError, &m.DeadlineAt, &m.CreatedAt, &m.SentAt)
	return m, err
}

func sortByID(msgs []sms.Message) {
	slices.SortFunc(msgs, func(a, b sms.Message) int { return cmp.Compare(a.ID, b.ID) })
}

// AcceptMessages stores the messages and charges the customer for them in a
// single transaction: either every message is stored and paid for, or
// nothing changes at all.
//
// It returns replayed=true when every message already existed under the same
// idempotency keys (a client retry); nothing is charged a second time.
func (s *Store) AcceptMessages(ctx context.Context, customerID int64, msgs []sms.NewMessage) ([]sms.Message, bool, error) {
	n := len(msgs)
	keys := make([]*string, n)
	phones := make([]string, n)
	bodies := make([]string, n)
	express := make([]bool, n)
	costs := make([]int64, n)
	deadlines := make([]*time.Time, n)
	var total int64
	for i, m := range msgs {
		if m.IdempotencyKey != "" {
			keys[i] = &m.IdempotencyKey
		}
		phones[i], bodies[i], express[i], costs[i], deadlines[i] = m.To, m.Text, m.Express, m.Cost, m.DeadlineAt
		total += m.Cost
	}

	var out []sms.Message
	var replayed bool
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		// 1. Insert all messages in one statement. A key that already exists
		//    is skipped (ON CONFLICT DO NOTHING), which is how retries are detected.
		rows, err := tx.Query(ctx, `
			INSERT INTO messages (customer_id, idempotency_key, phone, body, express, cost, deadline_at)
			SELECT $1, u.key, u.phone, u.body, u.express, u.cost, u.deadline
			FROM unnest($2::text[], $3::text[], $4::text[], $5::bool[], $6::bigint[], $7::timestamptz[])
			     AS u(key, phone, body, express, cost, deadline)
			ON CONFLICT (customer_id, idempotency_key) WHERE idempotency_key IS NOT NULL DO NOTHING
			RETURNING `+messageColumns,
			customerID, keys, phones, bodies, express, costs, deadlines)
		if err != nil {
			return err
		}
		inserted, err := pgx.CollectRows(rows, scanMessage)
		if isForeignKeyViolation(err) {
			return sms.ErrCustomerNotFound
		}
		if err != nil {
			return err
		}

		if len(inserted) < n {
			if len(inserted) > 0 {
				return sms.ErrIdempotencyConflict // some keys are new, some are not
			}
			out, err = findReplay(ctx, tx, customerID, msgs)
			replayed = err == nil
			return err
		}

		// 2. Charge. Check and debit are ONE atomic statement: the row lock
		//    serializes concurrent requests of the same customer, so the
		//    balance can reach exactly zero but never go below it.
		var balance int64
		err = tx.QueryRow(ctx, `
			UPDATE customers SET balance = balance - $2, updated_at = now()
			WHERE id = $1 AND balance >= $2
			RETURNING balance`, customerID, total).Scan(&balance)
		if errors.Is(err, pgx.ErrNoRows) {
			return sms.ErrInsufficientBalance // rolls back the inserts too
		}
		if err != nil {
			return err
		}

		// 3. Record the charge in the ledger.
		_, err = tx.Exec(ctx, `
			INSERT INTO balance_transactions (customer_id, kind, amount, balance_after, message_count)
			VALUES ($1, 'charge', $2, $3, $4)`, customerID, -total, balance, n)
		sortByID(inserted)
		out = inserted
		return err
	})
	if err != nil {
		return nil, false, err
	}
	return out, replayed, nil
}

// findReplay loads the messages an earlier request created with the same
// idempotency keys and checks that the retry is really the same request.
func findReplay(ctx context.Context, tx pgx.Tx, customerID int64, msgs []sms.NewMessage) ([]sms.Message, error) {
	keys := make([]string, len(msgs))
	for i, m := range msgs {
		keys[i] = m.IdempotencyKey
	}
	rows, err := tx.Query(ctx, `
		SELECT `+messageColumns+` FROM messages
		WHERE customer_id = $1 AND idempotency_key = ANY($2)
		ORDER BY id`, customerID, keys)
	if err != nil {
		return nil, err
	}
	existing, err := pgx.CollectRows(rows, scanMessage)
	if err != nil {
		return nil, err
	}
	if len(existing) != len(msgs) {
		return nil, sms.ErrIdempotencyConflict
	}
	for i, m := range msgs {
		e := existing[i]
		if e.To != m.To || e.Text != m.Text || e.Express != m.Express {
			return nil, sms.ErrIdempotencyConflict
		}
	}
	return existing, nil
}

// GetMessage returns one message owned by the customer.
func (s *Store) GetMessage(ctx context.Context, customerID, id int64) (sms.Message, error) {
	rows, err := s.pool.Query(ctx,
		"SELECT "+messageColumns+" FROM messages WHERE customer_id = $1 AND id = $2", customerID, id)
	if err != nil {
		return sms.Message{}, err
	}
	m, err := pgx.CollectExactlyOneRow(rows, scanMessage)
	if errors.Is(err, pgx.ErrNoRows) {
		return m, sms.ErrMessageNotFound
	}
	return m, err
}

// ListMessages returns a page of a customer's messages, newest first.
// Pagination is keyset based (id < cursor), so deep pages stay fast,
// unlike OFFSET which has to skip over every earlier row.
func (s *Store) ListMessages(ctx context.Context, f sms.MessageFilter) ([]sms.Message, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT `+messageColumns+` FROM messages
		WHERE customer_id = $1
		  AND ($2::text = '' OR status = $2)
		  AND ($3::timestamptz IS NULL OR created_at >= $3)
		  AND ($4::timestamptz IS NULL OR created_at < $4)
		  AND ($5::bigint = 0 OR id < $5)
		ORDER BY id DESC
		LIMIT $6`,
		f.CustomerID, string(f.Status), nullTime(f.From), nullTime(f.To), f.BeforeID, f.Limit)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, scanMessage)
}

// MessageStats counts a customer's messages per status in [from, to).
func (s *Store) MessageStats(ctx context.Context, customerID int64, from, to time.Time) (map[sms.Status]int64, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT status, count(*) FROM messages
		WHERE customer_id = $1 AND created_at >= $2 AND created_at < $3
		GROUP BY status`, customerID, from, to)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	stats := make(map[sms.Status]int64)
	for rows.Next() {
		var status sms.Status
		var count int64
		if err := rows.Scan(&status, &count); err != nil {
			return nil, err
		}
		stats[status] = count
	}
	return stats, rows.Err()
}

func nullTime(t time.Time) *time.Time {
	if t.IsZero() {
		return nil
	}
	return &t
}
