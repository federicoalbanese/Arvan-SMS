package queue

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"sms-gateway/internal/sms"
)

func newTestAdmission(t *testing.T, rate int, maxBacklog int64) (*Admission, *Scheduler) {
	t.Helper()
	s, rdb := newTestScheduler(t, 10)
	a := NewAdmission(rdb, s, rate, maxBacklog)
	clock := time.Unix(1_700_000_000, 0)
	a.now = func() time.Time { return clock }
	return a, s
}

func TestAdmitExpressRateLimitPerCustomer(t *testing.T) {
	a, _ := newTestAdmission(t, 3, 100)
	ctx := context.Background()

	for i := range 3 {
		if err := a.AdmitExpress(ctx, 1); err != nil {
			t.Fatalf("message %d: %v", i, err)
		}
	}
	if err := a.AdmitExpress(ctx, 1); !errors.Is(err, sms.ErrExpressRateLimited) {
		t.Fatalf("4th message: err = %v, want ErrExpressRateLimited", err)
	}
	// Another customer has its own quota.
	if err := a.AdmitExpress(ctx, 2); err != nil {
		t.Fatalf("other customer: %v", err)
	}
	// The next second starts a new window.
	a.now = func() time.Time { return time.Unix(1_700_000_001, 0) }
	if err := a.AdmitExpress(ctx, 1); err != nil {
		t.Fatalf("next second: %v", err)
	}
}

func TestAdmitExpressRejectsWhenBacklogIsFull(t *testing.T) {
	a, s := newTestAdmission(t, 100, 3)
	ctx := context.Background()

	_ = s.Publish(ctx, true, items(9, 1, 3))
	if err := a.AdmitExpress(ctx, 1); !errors.Is(err, sms.ErrExpressOverloaded) {
		t.Fatalf("err = %v, want ErrExpressOverloaded", err)
	}
	_, _ = s.PopExpress(ctx, 1)
	if err := a.AdmitExpress(ctx, 1); err != nil {
		t.Fatalf("after draining one: %v", err)
	}
}

func TestAdmitExpressFailsClosedWithoutRedis(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr(), MaxRetries: -1})
	defer rdb.Close()
	a := NewAdmission(rdb, NewScheduler(rdb, 10), 10, 10)
	mr.Close()

	if err := a.AdmitExpress(context.Background(), 1); !errors.Is(err, sms.ErrExpressUnavailable) {
		t.Fatalf("err = %v, want ErrExpressUnavailable", err)
	}
}
