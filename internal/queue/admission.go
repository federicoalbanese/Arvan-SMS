package queue

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"

	"sms-gateway/internal/sms"
)

// Admission decides whether a new express message may be accepted.
//
// A latency guarantee is only possible if the amount of waiting work is
// bounded. So instead of accepting everything and missing the deadline,
// the API refuses express messages up front (and charges nothing) when:
//
//   - the express queue is already longer than we can drain within the SLA
//     (maxBacklog ~= express throughput x SLA), or
//   - one customer sends more than its per-second express quota, so a
//     single customer cannot use up the capacity of everyone else.
type Admission struct {
	rdb             redis.UniversalClient
	sched           *Scheduler
	ratePerCustomer int64
	maxBacklog      int64
	now             func() time.Time
}

// NewAdmission creates the express admission controller.
func NewAdmission(rdb redis.UniversalClient, sched *Scheduler, ratePerCustomer int, maxBacklog int64) *Admission {
	return &Admission{
		rdb:             rdb,
		sched:           sched,
		ratePerCustomer: int64(ratePerCustomer),
		maxBacklog:      maxBacklog,
		now:             time.Now,
	}
}

// AdmitExpress returns nil if the customer may send one more express
// message now. If Redis is unreachable it fails closed: we cannot keep an
// express promise we cannot track. (Normal messages do not need Redis at
// accept time, so they are still accepted.)
func (a *Admission) AdmitExpress(ctx context.Context, customerID int64) error {
	backlog, err := a.sched.ExpressBacklog(ctx)
	if err != nil {
		return fmt.Errorf("%w: %v", sms.ErrExpressUnavailable, err)
	}
	if backlog >= a.maxBacklog {
		return sms.ErrExpressOverloaded
	}

	// Fixed one-second window counter: INCR a key named after the current
	// second; the key expires shortly after the second is over.
	key := fmt.Sprintf("{sms}:rl:express:%d:%d", customerID, a.now().Unix())
	pipe := a.rdb.TxPipeline()
	count := pipe.Incr(ctx, key)
	pipe.Expire(ctx, key, 2*time.Second)
	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("%w: %v", sms.ErrExpressUnavailable, err)
	}
	if count.Val() > a.ratePerCustomer {
		return sms.ErrExpressRateLimited
	}
	return nil
}
