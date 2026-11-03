package queue

import (
	"context"
	"os"
	"slices"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"sms-gateway/internal/sms"
)

// newTestRedis uses a real Redis when TEST_REDIS_ADDR is set (integration
// run) and an in-memory miniredis otherwise, so these tests always run.
func newTestRedis(t *testing.T) *redis.Client {
	t.Helper()
	addr := os.Getenv("TEST_REDIS_ADDR")
	if addr == "" {
		addr = miniredis.RunT(t).Addr()
	}
	rdb := redis.NewClient(&redis.Options{Addr: addr, DB: 15})
	if err := rdb.FlushDB(context.Background()).Err(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { rdb.Close() })
	return rdb
}

func newTestScheduler(t *testing.T, quantum int) (*Scheduler, *redis.Client) {
	t.Helper()
	rdb := newTestRedis(t)
	return NewScheduler(rdb, quantum), rdb
}

func items(customerID int64, from, to int64) []sms.QueueItem {
	var out []sms.QueueItem
	for id := from; id <= to; id++ {
		out = append(out, sms.QueueItem{ID: id, CustomerID: customerID})
	}
	return out
}

// The key fairness property: a light customer that arrives after a heavy
// customer's huge backlog still gets served within one round.
func TestPopNormalIsFairAcrossCustomers(t *testing.T) {
	s, _ := newTestScheduler(t, 10)
	ctx := context.Background()

	heavy := items(1, 1, 1000)    // customer 1: 1000 messages
	light := items(2, 5001, 5003) // customer 2: 3 messages, published after
	if err := s.Publish(ctx, false, heavy); err != nil {
		t.Fatal(err)
	}
	if err := s.Publish(ctx, false, light); err != nil {
		t.Fatal(err)
	}

	first, _ := s.PopNormal(ctx)
	second, _ := s.PopNormal(ctx)
	third, _ := s.PopNormal(ctx)

	if !slices.Equal(first, []int64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}) {
		t.Errorf("first turn = %v, want heavy customer's first 10", first)
	}
	if !slices.Equal(second, []int64{5001, 5002, 5003}) {
		t.Errorf("second turn = %v, want all of the light customer", second)
	}
	if third[0] != 11 {
		t.Errorf("third turn = %v, want heavy customer again from 11", third)
	}
}

func TestPopNormalRoundRobinKeepsOrderPerCustomer(t *testing.T) {
	s, _ := newTestScheduler(t, 2)
	ctx := context.Background()

	// Interleaved publishes from three customers (10, 20 and 30).
	q := func(id, customerID int64) sms.QueueItem { return sms.QueueItem{ID: id, CustomerID: customerID} }
	_ = s.Publish(ctx, false, []sms.QueueItem{q(1, 10), q(2, 20), q(3, 10), q(4, 30), q(5, 10), q(6, 20)})

	var got [][]int64
	for {
		ids, err := s.PopNormal(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if len(ids) == 0 {
			break
		}
		got = append(got, ids)
	}
	want := [][]int64{{1, 3}, {2, 6}, {4}, {5}}
	if !slices.EqualFunc(got, want, slices.Equal) {
		t.Fatalf("turns = %v, want %v", got, want)
	}
}

func TestPublishAddsCustomerToRingOnce(t *testing.T) {
	s, rdb := newTestScheduler(t, 10)
	ctx := context.Background()
	_ = s.Publish(ctx, false, items(7, 1, 3))
	_ = s.Publish(ctx, false, items(7, 4, 6))

	ring, _ := rdb.LRange(ctx, keyRing, 0, -1).Result()
	if !slices.Equal(ring, []string{"7"}) {
		t.Fatalf("ring = %v, want [7]", ring)
	}
	// After draining, the customer leaves the ring and can rejoin later.
	ids, _ := s.PopNormal(ctx)
	if len(ids) != 6 {
		t.Fatalf("popped %v", ids)
	}
	if n, _ := rdb.Exists(ctx, keyRing).Result(); n != 0 {
		t.Fatal("ring should be empty after the customer's queue drained")
	}
	_ = s.Publish(ctx, false, items(7, 7, 7))
	if ids, _ := s.PopNormal(ctx); !slices.Equal(ids, []int64{7}) {
		t.Fatalf("after rejoin popped %v", ids)
	}
}

func TestExpressIsFIFO(t *testing.T) {
	s, _ := newTestScheduler(t, 10)
	ctx := context.Background()
	_ = s.Publish(ctx, true, items(1, 1, 3))
	_ = s.Publish(ctx, true, items(2, 4, 4))

	got, _ := s.PopExpress(ctx, 10)
	if !slices.Equal(got, []int64{1, 2, 3, 4}) {
		t.Fatalf("PopExpress = %v", got)
	}
	if backlog, _ := s.ExpressBacklog(ctx); backlog != 0 {
		t.Fatalf("backlog = %d, want 0", backlog)
	}
	if empty, err := s.PopExpress(ctx, 10); err != nil || len(empty) != 0 {
		t.Fatalf("pop on empty queue = %v, %v", empty, err)
	}
}

func TestInflightAckAndExpiry(t *testing.T) {
	s, _ := newTestScheduler(t, 10)
	ctx := context.Background()
	clock := time.Unix(1_700_000_000, 0)
	s.now = func() time.Time { return clock }

	_ = s.Publish(ctx, false, items(1, 1, 3))
	popped, _ := s.PopNormal(ctx)
	if err := s.Ack(ctx, popped[:1]); err != nil { // worker claimed only id 1
		t.Fatal(err)
	}

	if expired, _ := s.Expired(ctx, 30*time.Second, 100); len(expired) != 0 {
		t.Fatalf("expired too early: %v", expired)
	}
	clock = clock.Add(31 * time.Second)
	expired, _ := s.Expired(ctx, 30*time.Second, 100)
	if !slices.Equal(expired, []int64{2, 3}) {
		t.Fatalf("expired = %v, want [2 3]", expired)
	}

	stats, err := s.Stats(ctx)
	if err != nil || stats.InFlight != 2 || stats.ActiveCustomers != 0 {
		t.Fatalf("stats = %+v, err = %v", stats, err)
	}
}
