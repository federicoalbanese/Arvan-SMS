package worker

import (
	"context"
	"log/slog"
	"time"

	"sms-gateway/internal/metrics"
	"sms-gateway/internal/queue"
)

// ReconcilerStore is what the reconciler needs from Postgres.
type ReconcilerStore interface {
	RequeueQueued(ctx context.Context, ids []int64) (int64, error)
	RequeueStuckSending(ctx context.Context, olderThan time.Duration) (int64, error)
	SyncRedisInstance(ctx context.Context, runID string) (int64, bool, error)
	PendingCounts(ctx context.Context) (express, normal int64, err error)
}

// ReconcilerQueue is what the reconciler needs from Redis.
type ReconcilerQueue interface {
	InstanceID(ctx context.Context) (string, error)
	Expired(ctx context.Context, olderThan time.Duration, limit int) ([]int64, error)
	Ack(ctx context.Context, ids []int64) error
	Stats(ctx context.Context) (queue.Stats, error)
}

// ReconcilerConfig holds the recovery timeouts.
type ReconcilerConfig struct {
	VisibilityTimeout time.Duration
	SendingTimeout    time.Duration
}

const expiredBatch = 1000

// Reconciler repairs messages that got stuck because a process or Redis
// died at the wrong moment. Every message is always in exactly one place
// that "owns" it (Postgres pending, Redis queue, a worker), and each check
// below covers one hand-off between those owners:
//
//  1. Redis restarted         -> its queues may be gone: rebuild from Postgres.
//  2. popped but never claimed -> the worker died between Redis and Postgres.
//  3. claimed but never finished -> the worker died while sending.
//
// Every step is idempotent, so all worker instances can run it.
type Reconciler struct {
	store ReconcilerStore
	queue ReconcilerQueue
	cfg   ReconcilerConfig
	log   *slog.Logger
}

// NewReconciler creates a reconciler.
func NewReconciler(store ReconcilerStore, q ReconcilerQueue, cfg ReconcilerConfig, log *slog.Logger) *Reconciler {
	return &Reconciler{store: store, queue: q, cfg: cfg, log: log}
}

// Run runs all checks every interval until ctx is cancelled.
func (r *Reconciler) Run(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		r.RunOnce(ctx)
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

// RunOnce runs every check once.
func (r *Reconciler) RunOnce(ctx context.Context) {
	r.syncRedisInstance(ctx)
	r.requeueExpired(ctx)
	r.requeueStuckSending(ctx)
	r.updateGauges(ctx)
}

func (r *Reconciler) syncRedisInstance(ctx context.Context) {
	runID, err := r.queue.InstanceID(ctx)
	if err != nil {
		r.log.Error("read redis instance id", "error", err)
		return
	}
	requeued, changed, err := r.store.SyncRedisInstance(ctx, runID)
	if err != nil {
		r.log.Error("sync redis instance", "error", err)
		return
	}
	if changed {
		r.log.Warn("new redis instance detected; queued messages sent back to the relay",
			"run_id", runID, "requeued", requeued)
	}
}

// requeueExpired handles ids that a worker popped from Redis but never
// claimed in Postgres. Messages that were claimed in the meantime are not
// touched (RequeueQueued only changes status 'queued').
func (r *Reconciler) requeueExpired(ctx context.Context) {
	for {
		ids, err := r.queue.Expired(ctx, r.cfg.VisibilityTimeout, expiredBatch)
		if err != nil {
			r.log.Error("read expired in-flight ids", "error", err)
			return
		}
		if len(ids) == 0 {
			return
		}
		n, err := r.store.RequeueQueued(ctx, ids)
		if err != nil {
			r.log.Error("requeue expired ids", "error", err)
			return
		}
		if err := r.queue.Ack(ctx, ids); err != nil {
			r.log.Error("drop expired in-flight ids", "error", err)
			return
		}
		if n > 0 {
			r.log.Warn("requeued messages abandoned between redis and postgres", "count", n)
		}
		if len(ids) < expiredBatch {
			return
		}
	}
}

func (r *Reconciler) requeueStuckSending(ctx context.Context) {
	n, err := r.store.RequeueStuckSending(ctx, r.cfg.SendingTimeout)
	if err != nil {
		r.log.Error("requeue stuck sending messages", "error", err)
		return
	}
	if n > 0 {
		r.log.Warn("requeued messages whose worker did not report back", "count", n)
	}
}

func (r *Reconciler) updateGauges(ctx context.Context) {
	if express, normal, err := r.store.PendingCounts(ctx); err == nil {
		metrics.OutboxPending.WithLabelValues("express").Set(float64(express))
		metrics.OutboxPending.WithLabelValues("normal").Set(float64(normal))
	}
	if st, err := r.queue.Stats(ctx); err == nil {
		metrics.ExpressQueued.Set(float64(st.ExpressQueued))
		metrics.ActiveCustomers.Set(float64(st.ActiveCustomers))
		metrics.InFlight.Set(float64(st.InFlight))
	}
}
