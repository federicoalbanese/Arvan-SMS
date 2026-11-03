package worker

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"slices"
	"testing"
	"time"

	"sms-gateway/internal/operator"
	"sms-gateway/internal/queue"
	"sms-gateway/internal/sms"
)

var discard = slog.New(slog.NewTextHandler(io.Discard, nil))

type fakeSenderStore struct {
	messages map[int64]sms.Message
	claimErr error
	outcomes []sms.SendOutcome
}

func (f *fakeSenderStore) ClaimForSending(_ context.Context, ids []int64) ([]sms.Message, error) {
	if f.claimErr != nil {
		return nil, f.claimErr
	}
	var out []sms.Message
	for _, id := range ids {
		if m, ok := f.messages[id]; ok {
			m.Attempts++
			out = append(out, m)
			delete(f.messages, id) // a second claim gets nothing
		}
	}
	return out, nil
}

func (f *fakeSenderStore) FinishSending(_ context.Context, outcomes []sms.SendOutcome) error {
	f.outcomes = append(f.outcomes, outcomes...)
	return nil
}

type fakeQueue struct{ acked []int64 }

func (f *fakeQueue) PopExpress(context.Context, int) ([]int64, error) { return nil, nil }
func (f *fakeQueue) PopNormal(context.Context) ([]int64, error)       { return nil, nil }
func (f *fakeQueue) Ack(_ context.Context, ids []int64) error {
	f.acked = append(f.acked, ids...)
	return nil
}

// fakeOperator fails for the numbers listed in errs.
type fakeOperator struct{ errs map[string]error }

func (f fakeOperator) Send(_ context.Context, to, _ string) (string, error) {
	if err := f.errs[to]; err != nil {
		return "", err
	}
	return "ref-" + to, nil
}

func TestSenderOutcomes(t *testing.T) {
	now := time.Now()
	store := &fakeSenderStore{messages: map[int64]sms.Message{
		1: {ID: 1, To: "ok", CreatedAt: now},
		2: {ID: 2, To: "busy", CreatedAt: now},
		3: {ID: 3, To: "blocked", CreatedAt: now},
		4: {ID: 4, To: "busy", Attempts: 4, CreatedAt: now}, // this is its 5th and last attempt
	}}
	q := &fakeQueue{}
	op := fakeOperator{errs: map[string]error{
		"busy":    errors.New("try later"),
		"blocked": fmt.Errorf("%w: unknown number", operator.ErrPermanent),
	}}
	s := NewSender(store, q, op, SenderConfig{MaxAttempts: 5, OperatorTimeout: time.Second}, discard)
	s.now = func() time.Time { return now }

	s.Process(context.Background(), []int64{1, 2, 3, 4, 99})

	want := map[int64]sms.OutcomeKind{1: sms.OutcomeSent, 2: sms.OutcomeRetry, 3: sms.OutcomeFailed, 4: sms.OutcomeFailed}
	if len(store.outcomes) != len(want) {
		t.Fatalf("got %d outcomes, want %d", len(store.outcomes), len(want))
	}
	for _, o := range store.outcomes {
		if o.Kind != want[o.MessageID] {
			t.Errorf("message %d: kind = %v, want %v", o.MessageID, o.Kind, want[o.MessageID])
		}
		if o.MessageID == 2 && !o.RetryAt.Equal(now.Add(2*time.Second)) {
			t.Errorf("retry at %v, want now+2s", o.RetryAt)
		}
	}
	// Every popped id is acknowledged, including the unknown duplicate 99.
	if !slices.Equal(q.acked, []int64{1, 2, 3, 4, 99}) {
		t.Errorf("acked = %v", q.acked)
	}
}

func TestSenderDoesNotAckWhenClaimFails(t *testing.T) {
	store := &fakeSenderStore{claimErr: errors.New("db down")}
	q := &fakeQueue{}
	s := NewSender(store, q, fakeOperator{}, SenderConfig{MaxAttempts: 5, OperatorTimeout: time.Second}, discard)

	s.Process(context.Background(), []int64{1, 2})

	if len(q.acked) != 0 {
		t.Fatalf("acked %v although the claim failed", q.acked)
	}
}

func TestBackoff(t *testing.T) {
	tests := []struct {
		express bool
		attempt int
		want    time.Duration
	}{
		{false, 1, 2 * time.Second},
		{false, 2, 4 * time.Second},
		{false, 4, 16 * time.Second},
		{false, 20, 5 * time.Minute},
		{true, 1, 100 * time.Millisecond},
		{true, 3, 400 * time.Millisecond},
		{true, 10, time.Second},
	}
	for _, tt := range tests {
		if got := Backoff(tt.express, tt.attempt); got != tt.want {
			t.Errorf("Backoff(%v, %d) = %v, want %v", tt.express, tt.attempt, got, tt.want)
		}
	}
}

type fakeReconcilerStore struct {
	runID          string
	requeuedQueued []int64
	stuckCalls     int
}

func (f *fakeReconcilerStore) RequeueQueued(_ context.Context, ids []int64) (int64, error) {
	f.requeuedQueued = append(f.requeuedQueued, ids...)
	return int64(len(ids)), nil
}
func (f *fakeReconcilerStore) RequeueStuckSending(context.Context, time.Duration) (int64, error) {
	f.stuckCalls++
	return 0, nil
}
func (f *fakeReconcilerStore) SyncRedisInstance(_ context.Context, runID string) (int64, bool, error) {
	changed := runID != f.runID
	f.runID = runID
	return 0, changed, nil
}
func (f *fakeReconcilerStore) PendingCounts(context.Context) (int64, int64, error) { return 0, 0, nil }

type fakeReconcilerQueue struct {
	runID   string
	expired []int64
	acked   []int64
}

func (f *fakeReconcilerQueue) InstanceID(context.Context) (string, error) { return f.runID, nil }
func (f *fakeReconcilerQueue) Expired(context.Context, time.Duration, int) ([]int64, error) {
	out := f.expired
	f.expired = nil
	return out, nil
}
func (f *fakeReconcilerQueue) Ack(_ context.Context, ids []int64) error {
	f.acked = append(f.acked, ids...)
	return nil
}
func (f *fakeReconcilerQueue) Stats(context.Context) (queue.Stats, error) { return queue.Stats{}, nil }

func TestReconcilerRequeuesExpiredInflight(t *testing.T) {
	st := &fakeReconcilerStore{}
	q := &fakeReconcilerQueue{runID: "a", expired: []int64{7, 8}}
	r := NewReconciler(st, q, ReconcilerConfig{VisibilityTimeout: time.Second, SendingTimeout: time.Minute}, discard)

	r.RunOnce(context.Background())

	if !slices.Equal(st.requeuedQueued, []int64{7, 8}) || !slices.Equal(q.acked, []int64{7, 8}) {
		t.Fatalf("requeued=%v acked=%v", st.requeuedQueued, q.acked)
	}
	if st.stuckCalls != 1 {
		t.Fatalf("stuck sending check ran %d times", st.stuckCalls)
	}
	if st.runID != "a" {
		t.Fatalf("redis run id not synced: %q", st.runID)
	}
}
