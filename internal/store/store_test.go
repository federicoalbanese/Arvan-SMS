package store

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"sms-gateway/internal/sms"
)

// These tests need a real PostgreSQL. They are skipped unless
// TEST_DATABASE_URL is set (see `make test-integration`).
func newTestStore(t *testing.T) *Store {
	t.Helper()
	url := os.Getenv("TEST_DATABASE_URL")
	if url == "" {
		t.Skip("TEST_DATABASE_URL not set")
	}
	ctx := context.Background()
	s, err := New(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(s.Close)
	if err := s.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	return s
}

func newCustomer(t *testing.T, s *Store, balance int64) sms.Customer {
	t.Helper()
	ctx := context.Background()
	c, err := s.CreateCustomer(ctx, t.Name())
	if err != nil {
		t.Fatal(err)
	}
	if balance > 0 {
		if c, err = s.TopUp(ctx, c.ID, balance); err != nil {
			t.Fatal(err)
		}
	}
	return c
}

func msg(i int) sms.NewMessage {
	return sms.NewMessage{To: fmt.Sprintf("+98912000%04d", i), Text: "hello", Cost: 100}
}

// assertLedger checks the core accounting invariant: balance == SUM(ledger).
func assertLedger(t *testing.T, s *Store, customerID, wantBalance int64) {
	t.Helper()
	ctx := context.Background()
	c, err := s.GetCustomer(ctx, customerID)
	if err != nil {
		t.Fatal(err)
	}
	txs, err := s.ListTransactions(ctx, customerID, 10_000)
	if err != nil {
		t.Fatal(err)
	}
	var sum int64
	for _, tx := range txs {
		sum += tx.Amount
	}
	if c.Balance != wantBalance || sum != wantBalance {
		t.Fatalf("balance = %d, ledger sum = %d, want %d", c.Balance, sum, wantBalance)
	}
}

func TestMigrateIsIdempotent(t *testing.T) {
	s := newTestStore(t)
	if err := s.Migrate(context.Background()); err != nil {
		t.Fatalf("second Migrate: %v", err)
	}
}

func TestAcceptNeverOverspendsUnderConcurrency(t *testing.T) {
	s := newTestStore(t)
	c := newCustomer(t, s, 50*100) // enough for exactly 50 messages

	var wg sync.WaitGroup
	var mu sync.Mutex
	accepted, rejected := 0, 0
	for i := range 200 {
		wg.Go(func() {
			_, _, err := s.AcceptMessages(context.Background(), c.ID, []sms.NewMessage{msg(i)})
			mu.Lock()
			defer mu.Unlock()
			switch {
			case err == nil:
				accepted++
			case errors.Is(err, sms.ErrInsufficientBalance):
				rejected++
			default:
				t.Errorf("unexpected error: %v", err)
			}
		})
	}
	wg.Wait()

	if accepted != 50 || rejected != 150 {
		t.Fatalf("accepted=%d rejected=%d, want 50/150", accepted, rejected)
	}
	assertLedger(t, s, c.ID, 0)
}

func TestAcceptIsAllOrNothing(t *testing.T) {
	s := newTestStore(t)
	c := newCustomer(t, s, 250) // enough for 2 messages, not 3

	_, _, err := s.AcceptMessages(context.Background(), c.ID, []sms.NewMessage{msg(1), msg(2), msg(3)})
	if !errors.Is(err, sms.ErrInsufficientBalance) {
		t.Fatalf("err = %v, want ErrInsufficientBalance", err)
	}
	page, _ := s.ListMessages(context.Background(), sms.MessageFilter{CustomerID: c.ID, Limit: 10})
	if len(page) != 0 {
		t.Fatalf("%d messages stored after a rejected batch", len(page))
	}
	assertLedger(t, s, c.ID, 250)
}

func TestAcceptUnknownCustomer(t *testing.T) {
	s := newTestStore(t)
	_, _, err := s.AcceptMessages(context.Background(), -1, []sms.NewMessage{msg(1)})
	if !errors.Is(err, sms.ErrCustomerNotFound) {
		t.Fatalf("err = %v, want ErrCustomerNotFound", err)
	}
}

func TestAcceptIdempotency(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	c := newCustomer(t, s, 1000)

	m := msg(1)
	m.IdempotencyKey = "order-42"
	first, replayed, err := s.AcceptMessages(ctx, c.ID, []sms.NewMessage{m})
	if err != nil || replayed {
		t.Fatalf("first call: replayed=%v err=%v", replayed, err)
	}
	again, replayed, err := s.AcceptMessages(ctx, c.ID, []sms.NewMessage{m})
	if err != nil || !replayed || again[0].ID != first[0].ID {
		t.Fatalf("retry: replayed=%v err=%v id=%v want %v", replayed, err, again, first[0].ID)
	}

	other := msg(2)
	other.IdempotencyKey = "order-42"
	if _, _, err := s.AcceptMessages(ctx, c.ID, []sms.NewMessage{other}); !errors.Is(err, sms.ErrIdempotencyConflict) {
		t.Fatalf("different body, same key: err = %v, want ErrIdempotencyConflict", err)
	}
	assertLedger(t, s, c.ID, 900) // charged once
}

func TestDispatchFlow(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	c := newCustomer(t, s, 1000)

	accepted, _, err := s.AcceptMessages(ctx, c.ID, []sms.NewMessage{msg(1), msg(2), msg(3)})
	if err != nil {
		t.Fatal(err)
	}
	ids := []int64{accepted[0].ID, accepted[1].ID, accepted[2].ID}

	// Relay: a failing publish leaves the rows pending.
	_, err = s.DispatchPending(ctx, false, 1000, func(context.Context, []sms.QueueItem) error {
		return errors.New("redis down")
	})
	if err == nil {
		t.Fatal("expected publish error to be returned")
	}
	var published []sms.QueueItem
	for {
		n, err := s.DispatchPending(ctx, false, 1000, func(_ context.Context, items []sms.QueueItem) error {
			for _, it := range items {
				if it.CustomerID == c.ID {
					published = append(published, it)
				}
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
		if n == 0 {
			break
		}
	}
	if len(published) != 3 {
		t.Fatalf("published %d of our messages, want 3", len(published))
	}

	// Claim: a second claim of the same ids gets nothing (duplicate delivery).
	claimed, err := s.ClaimForSending(ctx, ids)
	if err != nil || len(claimed) != 3 || claimed[0].Attempts != 1 {
		t.Fatalf("claim: %v %+v", err, claimed)
	}
	if again, _ := s.ClaimForSending(ctx, ids); len(again) != 0 {
		t.Fatalf("second claim returned %d messages", len(again))
	}

	err = s.FinishSending(ctx, []sms.SendOutcome{
		{MessageID: ids[0], Attempt: 1, Kind: sms.OutcomeSent, OperatorRef: "op-1"},
		{MessageID: ids[1], Attempt: 1, Kind: sms.OutcomeRetry, RetryAt: time.Now().Add(time.Hour), Error: "busy"},
		{MessageID: ids[2], Attempt: 1, Kind: sms.OutcomeFailed, Error: "unreachable"},
	})
	if err != nil {
		t.Fatal(err)
	}

	want := map[int64]sms.Status{ids[0]: sms.StatusSent, ids[1]: sms.StatusPending, ids[2]: sms.StatusFailed}
	for id, status := range want {
		m, err := s.GetMessage(ctx, c.ID, id)
		if err != nil || m.Status != status {
			t.Errorf("message %d: status=%s err=%v, want %s", id, m.Status, err, status)
		}
	}
	// 3 charged (700 left), 1 refunded.
	assertLedger(t, s, c.ID, 800)
}

func TestFinishIgnoresStaleAttempt(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	c := newCustomer(t, s, 1000)
	accepted, _, _ := s.AcceptMessages(ctx, c.ID, []sms.NewMessage{msg(1)})
	id := accepted[0].ID
	queueAll(t, s)
	if _, err := s.ClaimForSending(ctx, []int64{id}); err != nil {
		t.Fatal(err)
	}

	// A result for an older attempt must not change anything (and must not refund).
	err := s.FinishSending(ctx, []sms.SendOutcome{{MessageID: id, Attempt: 0, Kind: sms.OutcomeFailed}})
	if err != nil {
		t.Fatal(err)
	}
	m, _ := s.GetMessage(ctx, c.ID, id)
	if m.Status != sms.StatusSending {
		t.Fatalf("status = %s, want sending", m.Status)
	}
	assertLedger(t, s, c.ID, 900)
}

func TestRequeueStuckSending(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	c := newCustomer(t, s, 1000)
	accepted, _, _ := s.AcceptMessages(ctx, c.ID, []sms.NewMessage{msg(1)})
	id := accepted[0].ID
	queueAll(t, s)
	if _, err := s.ClaimForSending(ctx, []int64{id}); err != nil {
		t.Fatal(err)
	}

	time.Sleep(50 * time.Millisecond)
	if _, err := s.RequeueStuckSending(ctx, 10*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	m, _ := s.GetMessage(ctx, c.ID, id)
	if m.Status != sms.StatusPending {
		t.Fatalf("status = %s, want pending", m.Status)
	}
}

func TestListMessagesPagination(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	c := newCustomer(t, s, 10_000)
	batch := make([]sms.NewMessage, 25)
	for i := range batch {
		batch[i] = msg(i)
	}
	if _, _, err := s.AcceptMessages(ctx, c.ID, batch); err != nil {
		t.Fatal(err)
	}

	var seen int
	var cursor int64
	for {
		page, err := s.ListMessages(ctx, sms.MessageFilter{CustomerID: c.ID, BeforeID: cursor, Limit: 10})
		if err != nil {
			t.Fatal(err)
		}
		if len(page) == 0 {
			break
		}
		for _, m := range page {
			if cursor != 0 && m.ID >= cursor {
				t.Fatalf("page not ordered newest first: %d after cursor %d", m.ID, cursor)
			}
		}
		seen += len(page)
		cursor = page[len(page)-1].ID
	}
	if seen != 25 {
		t.Fatalf("paged through %d messages, want 25", seen)
	}

	stats, err := s.MessageStats(ctx, c.ID, time.Now().Add(-time.Hour), time.Now().Add(time.Hour))
	if err != nil || stats[sms.StatusPending] != 25 {
		t.Fatalf("stats = %v, err = %v", stats, err)
	}
}

func TestSyncRedisInstance(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	c := newCustomer(t, s, 1000)
	accepted, _, _ := s.AcceptMessages(ctx, c.ID, []sms.NewMessage{msg(1)})
	queueAll(t, s)

	first := "run-" + time.Now().String()
	if _, changed, err := s.SyncRedisInstance(ctx, first); err != nil || !changed {
		t.Fatalf("new run id: changed=%v err=%v", changed, err)
	}
	queueAll(t, s)

	// Same Redis instance: nothing to do.
	if _, changed, _ := s.SyncRedisInstance(ctx, first); changed {
		t.Fatal("same run id reported as changed")
	}
	m, _ := s.GetMessage(ctx, c.ID, accepted[0].ID)
	if m.Status != sms.StatusQueued {
		t.Fatalf("status = %s, want queued", m.Status)
	}

	// Redis restarted: queued messages go back to pending.
	requeued, changed, err := s.SyncRedisInstance(ctx, first+"-restarted")
	if err != nil || !changed || requeued < 1 {
		t.Fatalf("restart: requeued=%d changed=%v err=%v", requeued, changed, err)
	}
	m, _ = s.GetMessage(ctx, c.ID, accepted[0].ID)
	if m.Status != sms.StatusPending {
		t.Fatalf("status = %s, want pending", m.Status)
	}
}

// queueAll relays every pending message (of any customer) to a no-op publisher.
func queueAll(t *testing.T, s *Store) {
	t.Helper()
	for _, express := range []bool{false, true} {
		for {
			n, err := s.DispatchPending(context.Background(), express, 1000,
				func(context.Context, []sms.QueueItem) error { return nil })
			if err != nil {
				t.Fatal(err)
			}
			if n == 0 {
				break
			}
		}
	}
}
