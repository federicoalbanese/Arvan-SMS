package worker

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	"sms-gateway/internal/metrics"
	"sms-gateway/internal/operator"
	"sms-gateway/internal/sms"
)

// SenderStore is what the sender needs from Postgres.
type SenderStore interface {
	ClaimForSending(ctx context.Context, ids []int64) ([]sms.Message, error)
	FinishSending(ctx context.Context, outcomes []sms.SendOutcome) error
}

// SenderQueue is what the sender needs from the dispatch queue.
type SenderQueue interface {
	PopExpress(ctx context.Context, max int) ([]int64, error)
	PopNormal(ctx context.Context) ([]int64, error)
	Ack(ctx context.Context, ids []int64) error
}

// SenderConfig tunes the sender.
type SenderConfig struct {
	MaxAttempts     int
	OperatorTimeout time.Duration
	ExpressIdleWait time.Duration // poll interval when the express queue is empty
	NormalIdleWait  time.Duration // poll interval when the normal queue is empty
}

// Sender pops messages from Redis and hands them to the operator.
//
// Express and normal lanes run in separate goroutine pools (bulkheads):
// a flood of normal traffic or a slow operator response on normal
// messages cannot occupy the workers reserved for express messages.
type Sender struct {
	store SenderStore
	queue SenderQueue
	op    operator.Operator
	cfg   SenderConfig
	log   *slog.Logger
	now   func() time.Time
}

// NewSender creates a sender.
func NewSender(store SenderStore, queue SenderQueue, op operator.Operator, cfg SenderConfig, log *slog.Logger) *Sender {
	return &Sender{store: store, queue: queue, op: op, cfg: cfg, log: log, now: time.Now}
}

// Run starts workers goroutines for one lane and blocks until ctx is
// cancelled and every goroutine has finished its current batch.
func (s *Sender) Run(ctx context.Context, express bool, workers int) {
	var wg sync.WaitGroup
	for range workers {
		wg.Go(func() { s.loop(ctx, express) })
	}
	wg.Wait()
}

func (s *Sender) loop(ctx context.Context, express bool) {
	idle := s.cfg.NormalIdleWait
	if express {
		idle = s.cfg.ExpressIdleWait
	}
	for ctx.Err() == nil {
		ids, err := s.pop(ctx, express)
		if err != nil && ctx.Err() == nil {
			s.log.Error("pop failed", "lane", sms.Lane(express), "error", err)
		}
		if len(ids) == 0 {
			sleep(ctx, idle)
			continue
		}
		// The ids are off the queue now. Finish the batch even if shutdown
		// starts meanwhile, rather than leaving it to the reconciler.
		s.Process(context.WithoutCancel(ctx), ids)
	}
}

func (s *Sender) pop(ctx context.Context, express bool) ([]int64, error) {
	if express {
		// One at a time: for express, latency matters more than batching,
		// and spreading messages over all workers sends them in parallel.
		return s.queue.PopExpress(ctx, 1)
	}
	return s.queue.PopNormal(ctx)
}

// Process claims, sends and records one batch of message ids.
func (s *Sender) Process(ctx context.Context, ids []int64) {
	msgs, err := s.store.ClaimForSending(ctx, ids)
	if err != nil {
		// Not acknowledged: the reconciler re-queues these ids once the
		// visibility timeout has passed.
		s.log.Error("claim failed", "count", len(ids), "error", err)
		return
	}
	// Postgres owns the claimed messages now (status sending), so Redis can
	// forget all ids. Ids that were not claimed were duplicates.
	if err := s.queue.Ack(ctx, ids); err != nil {
		s.log.Warn("ack failed; reconciler will clean up", "error", err)
	}
	if len(msgs) == 0 {
		return
	}

	outcomes := make([]sms.SendOutcome, 0, len(msgs))
	for _, m := range msgs {
		outcomes = append(outcomes, s.send(ctx, m))
	}
	if err := s.store.FinishSending(ctx, outcomes); err != nil {
		// The messages stay in "sending"; the reconciler retries them after
		// SENDING_TIMEOUT. The operator may get them twice (at-least-once).
		s.log.Error("recording results failed", "count", len(outcomes), "error", err)
	}
}

// send hands one message to the operator and decides what happens next.
func (s *Sender) send(ctx context.Context, m sms.Message) sms.SendOutcome {
	opCtx, cancel := context.WithTimeout(ctx, s.cfg.OperatorTimeout)
	ref, err := s.op.Send(opCtx, m.To, m.Text)
	cancel()

	lane := sms.Lane(m.Express)
	out := sms.SendOutcome{MessageID: m.ID, Attempt: m.Attempts}
	switch {
	case err == nil:
		out.Kind, out.OperatorRef = sms.OutcomeSent, ref
		s.recordHandover(m)
	case errors.Is(err, operator.ErrPermanent) || m.Attempts >= s.cfg.MaxAttempts:
		out.Kind, out.Error = sms.OutcomeFailed, err.Error()
		metrics.Failed.WithLabelValues(lane).Inc()
		s.log.Warn("message failed permanently", "id", m.ID, "attempts", m.Attempts, "error", err)
	default:
		out.Kind, out.Error = sms.OutcomeRetry, err.Error()
		out.RetryAt = s.now().Add(Backoff(m.Express, m.Attempts))
		metrics.Retried.WithLabelValues(lane).Inc()
	}
	return out
}

func (s *Sender) recordHandover(m sms.Message) {
	now := s.now()
	lane := sms.Lane(m.Express)
	metrics.Sent.WithLabelValues(lane).Inc()
	metrics.Handover.WithLabelValues(lane).Observe(now.Sub(m.CreatedAt).Seconds())
	if m.DeadlineAt != nil && now.After(*m.DeadlineAt) {
		metrics.ExpressSLAMissed.Inc()
		s.log.Warn("express SLA missed", "id", m.ID, "late_by", now.Sub(*m.DeadlineAt).String())
	}
}

// Backoff is how long to wait after a failed attempt: exponential and
// capped. Express retries fast because its deadline is seconds away.
//
//	normal:  2s, 4s, 8s, 16s ... up to 5m
//	express: 100ms, 200ms, 400ms ... up to 1s
func Backoff(express bool, attempt int) time.Duration {
	base, maxWait := 2*time.Second, 5*time.Minute
	if express {
		base, maxWait = 100*time.Millisecond, time.Second
	}
	d := base
	for i := 1; i < attempt && d < maxWait; i++ {
		d *= 2
	}
	return min(d, maxWait)
}
