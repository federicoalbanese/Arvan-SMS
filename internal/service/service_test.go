package service

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"sms-gateway/internal/sms"
)

// fakeStore records what the service asked for. Balance rules themselves
// are tested against real Postgres in the store package.
type fakeStore struct {
	accepted [][]sms.NewMessage
	listed   []sms.Message
	err      error
}

func (f *fakeStore) CreateCustomer(_ context.Context, name string) (sms.Customer, error) {
	return sms.Customer{ID: 1, Name: name}, f.err
}
func (f *fakeStore) GetCustomer(_ context.Context, id int64) (sms.Customer, error) {
	return sms.Customer{ID: id}, f.err
}
func (f *fakeStore) TopUp(_ context.Context, id, amount int64) (sms.Customer, error) {
	return sms.Customer{ID: id, Balance: amount}, f.err
}
func (f *fakeStore) ListTransactions(context.Context, int64, int) ([]sms.Transaction, error) {
	return nil, f.err
}
func (f *fakeStore) AcceptMessages(_ context.Context, customerID int64, msgs []sms.NewMessage) ([]sms.Message, bool, error) {
	if f.err != nil {
		return nil, false, f.err
	}
	f.accepted = append(f.accepted, msgs)
	out := make([]sms.Message, len(msgs))
	for i, m := range msgs {
		out[i] = sms.Message{ID: int64(i + 1), CustomerID: customerID, To: m.To, Text: m.Text,
			Express: m.Express, Cost: m.Cost, DeadlineAt: m.DeadlineAt, Status: sms.StatusPending}
	}
	return out, false, nil
}
func (f *fakeStore) GetMessage(context.Context, int64, int64) (sms.Message, error) {
	return sms.Message{}, f.err
}
func (f *fakeStore) ListMessages(_ context.Context, filter sms.MessageFilter) ([]sms.Message, error) {
	return f.listed[:min(filter.Limit, len(f.listed))], f.err
}
func (f *fakeStore) MessageStats(context.Context, int64, time.Time, time.Time) (map[sms.Status]int64, error) {
	return map[sms.Status]int64{sms.StatusSent: 3, sms.StatusFailed: 1}, f.err
}

type fakeAdmission struct {
	calls int
	err   error
}

func (f *fakeAdmission) AdmitExpress(context.Context, int64) error {
	f.calls++
	return f.err
}

var testConfig = Config{PriceNormal: 100, PriceExpress: 150, ExpressSLA: 5 * time.Second, MaxBulkSize: 3}

func newTestService() (*Service, *fakeStore, *fakeAdmission) {
	st, adm := &fakeStore{}, &fakeAdmission{}
	svc := New(st, adm, testConfig)
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	svc.now = func() time.Time { return now }
	return svc, st, adm
}

func TestSendNormal(t *testing.T) {
	svc, st, adm := newTestService()
	m, _, err := svc.Send(context.Background(), SendRequest{CustomerID: 1, To: "09121234567", Text: "hi"})
	if err != nil {
		t.Fatal(err)
	}
	if m.To != "+989121234567" || m.Cost != 100 || m.DeadlineAt != nil {
		t.Errorf("stored %+v", m)
	}
	if adm.calls != 0 {
		t.Error("normal messages must not go through express admission")
	}
	if len(st.accepted) != 1 {
		t.Errorf("store called %d times", len(st.accepted))
	}
}

func TestSendExpressSetsDeadlineAndPrice(t *testing.T) {
	svc, _, adm := newTestService()
	m, _, err := svc.Send(context.Background(), SendRequest{CustomerID: 1, To: "09121234567", Text: "code 1234", Express: true})
	if err != nil {
		t.Fatal(err)
	}
	want := svc.now().Add(5 * time.Second)
	if m.Cost != 150 || m.DeadlineAt == nil || !m.DeadlineAt.Equal(want) {
		t.Errorf("cost=%d deadline=%v, want 150 and %v", m.Cost, m.DeadlineAt, want)
	}
	if adm.calls != 1 {
		t.Errorf("admission called %d times, want 1", adm.calls)
	}
}

func TestSendExpressRejectedByAdmissionIsNotCharged(t *testing.T) {
	svc, st, adm := newTestService()
	adm.err = sms.ErrExpressOverloaded
	_, _, err := svc.Send(context.Background(), SendRequest{CustomerID: 1, To: "09121234567", Text: "x", Express: true})
	if !errors.Is(err, sms.ErrExpressOverloaded) {
		t.Fatalf("err = %v", err)
	}
	if len(st.accepted) != 0 {
		t.Error("rejected express message reached the store")
	}
}

func TestSendValidation(t *testing.T) {
	svc, st, _ := newTestService()
	tests := []SendRequest{
		{CustomerID: 1, To: "123", Text: "hi"},
		{CustomerID: 1, To: "09121234567", Text: ""},
		{CustomerID: 1, To: "09121234567", Text: "hi", IdempotencyKey: strings.Repeat("k", 101)},
	}
	for _, req := range tests {
		_, _, err := svc.Send(context.Background(), req)
		var verr *sms.ValidationError
		if !errors.As(err, &verr) {
			t.Errorf("Send(%+v) err = %v, want ValidationError", req, err)
		}
	}
	if len(st.accepted) != 0 {
		t.Error("invalid message reached the store")
	}
}

func TestSendBulk(t *testing.T) {
	svc, st, _ := newTestService()
	msgs, _, err := svc.SendBulk(context.Background(), BulkRequest{
		CustomerID:     1,
		IdempotencyKey: "campaign-7",
		Messages:       []BulkItem{{To: "09121111111", Text: "a"}, {To: "09122222222", Text: "b"}},
	})
	if err != nil || len(msgs) != 2 {
		t.Fatalf("SendBulk = %d messages, %v", len(msgs), err)
	}
	got := st.accepted[0]
	if got[0].IdempotencyKey != "campaign-7#0" || got[1].IdempotencyKey != "campaign-7#1" {
		t.Errorf("keys = %q, %q", got[0].IdempotencyKey, got[1].IdempotencyKey)
	}
	if got[0].Express || got[0].Cost != 100 {
		t.Errorf("bulk messages must be normal lane at normal price: %+v", got[0])
	}
}

func TestSendBulkValidation(t *testing.T) {
	svc, _, _ := newTestService()
	ctx := context.Background()

	_, _, err := svc.SendBulk(ctx, BulkRequest{CustomerID: 1, Messages: make([]BulkItem, 4)})
	var verr *sms.ValidationError
	if !errors.As(err, &verr) || verr.Field != "messages" {
		t.Errorf("too many messages: err = %v", err)
	}

	_, _, err = svc.SendBulk(ctx, BulkRequest{CustomerID: 1, Messages: []BulkItem{
		{To: "09121111111", Text: "ok"}, {To: "bad", Text: "ok"},
	}})
	if !errors.As(err, &verr) || verr.Field != "messages[1].to" {
		t.Errorf("bad phone: err = %v, want field messages[1].to", err)
	}
}

func TestListMessagesCursor(t *testing.T) {
	svc, st, _ := newTestService()
	for id := int64(10); id >= 1; id-- {
		st.listed = append(st.listed, sms.Message{ID: id})
	}

	page, next, err := svc.ListMessages(context.Background(), sms.MessageFilter{CustomerID: 1, Limit: 4})
	if err != nil || len(page) != 4 || next != 7 {
		t.Fatalf("page=%d next=%d err=%v, want 4 messages and cursor 7", len(page), next, err)
	}

	st.listed = st.listed[:3]
	_, next, _ = svc.ListMessages(context.Background(), sms.MessageFilter{CustomerID: 1, Limit: 4})
	if next != 0 {
		t.Fatalf("last page cursor = %d, want 0", next)
	}
}

func TestListMessagesRejectsUnknownStatus(t *testing.T) {
	svc, _, _ := newTestService()
	_, _, err := svc.ListMessages(context.Background(), sms.MessageFilter{CustomerID: 1, Status: "lost"})
	var verr *sms.ValidationError
	if !errors.As(err, &verr) {
		t.Fatalf("err = %v, want ValidationError", err)
	}
}

func TestSummarize(t *testing.T) {
	svc, _, _ := newTestService()
	sum, err := svc.Summarize(context.Background(), 1, time.Time{}, time.Time{})
	if err != nil || sum.Total != 4 || sum.To.Sub(sum.From) != 24*time.Hour {
		t.Fatalf("summary = %+v, err = %v", sum, err)
	}
}

func TestTopUpRejectsNonPositive(t *testing.T) {
	svc, _, _ := newTestService()
	for _, amount := range []int64{0, -5} {
		if _, err := svc.TopUp(context.Background(), 1, amount); err == nil {
			t.Errorf("TopUp(%d) accepted", amount)
		}
	}
}
