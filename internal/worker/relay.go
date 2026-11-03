// Package worker contains the background processes that move messages
// from the database to the operator:
//
//	Relay       Postgres (pending)  -> Redis queues (queued)
//	Sender      Redis queues        -> operator, result back to Postgres
//	Reconciler  finds and repairs messages stuck between those steps
//
// All of them are safe to run in several worker instances at once.
package worker

import (
	"context"
	"log/slog"
	"time"

	"sms-gateway/internal/sms"
)

// RelayStore is the outbox side of the relay.
type RelayStore interface {
	DispatchPending(ctx context.Context, express bool, limit int,
		publish func(context.Context, []sms.QueueItem) error) (int, error)
}

// Publisher puts messages on the dispatch queue.
type Publisher interface {
	Publish(ctx context.Context, express bool, items []sms.QueueItem) error
}

// Relay implements the transactional outbox: the API only writes to
// Postgres, and the relay copies new messages to Redis. That way an
// accepted message can never be lost between the two systems.
type Relay struct {
	store RelayStore
	pub   Publisher
	batch int
	log   *slog.Logger
}

// NewRelay creates a relay that moves up to batch messages per step.
func NewRelay(store RelayStore, pub Publisher, batch int, log *slog.Logger) *Relay {
	return &Relay{store: store, pub: pub, batch: batch, log: log}
}

// Run relays one lane until ctx is cancelled. After a full batch there is
// probably more waiting, so it continues immediately; otherwise it sleeps
// for interval. The express lane uses a much shorter interval.
func (r *Relay) Run(ctx context.Context, express bool, interval time.Duration) {
	lane := sms.Lane(express)
	publish := func(ctx context.Context, items []sms.QueueItem) error {
		return r.pub.Publish(ctx, express, items)
	}
	for {
		n, err := r.store.DispatchPending(ctx, express, r.batch, publish)
		if ctx.Err() != nil {
			return
		}
		if err != nil {
			r.log.Error("relay step failed", "lane", lane, "error", err)
		}
		if err == nil && n == r.batch {
			continue
		}
		if !sleep(ctx, interval) {
			return
		}
	}
}

// sleep waits for d and returns false if ctx was cancelled first.
func sleep(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}
