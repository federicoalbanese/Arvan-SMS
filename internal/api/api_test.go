package api

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"sms-gateway/internal/service"
	"sms-gateway/internal/sms"
)

// stubService returns canned answers; the handlers are only responsible for
// HTTP concerns (parsing, status codes, JSON shape).
type stubService struct {
	sendErr   error
	replayed  bool
	lastSend  service.SendRequest
	listNext  int64
	lastQuery sms.MessageFilter
}

func (s *stubService) CreateCustomer(_ context.Context, name string) (sms.Customer, error) {
	return sms.Customer{ID: 1, Name: name}, nil
}
func (s *stubService) GetCustomer(_ context.Context, id int64) (sms.Customer, error) {
	if id == 404 {
		return sms.Customer{}, sms.ErrCustomerNotFound
	}
	return sms.Customer{ID: id, Balance: 500}, nil
}
func (s *stubService) TopUp(_ context.Context, id, amount int64) (sms.Customer, error) {
	return sms.Customer{ID: id, Balance: amount}, nil
}
func (s *stubService) ListTransactions(context.Context, int64, int) ([]sms.Transaction, error) {
	return []sms.Transaction{{ID: 1, Kind: "topup", Amount: 500}}, nil
}
func (s *stubService) Send(_ context.Context, req service.SendRequest) (sms.Message, bool, error) {
	s.lastSend = req
	return sms.Message{ID: 9, To: req.To, Status: sms.StatusPending}, s.replayed, s.sendErr
}
func (s *stubService) SendBulk(_ context.Context, req service.BulkRequest) ([]sms.Message, bool, error) {
	out := make([]sms.Message, len(req.Messages))
	for i := range out {
		out[i] = sms.Message{ID: int64(i + 1), Cost: 100}
	}
	return out, false, nil
}
func (s *stubService) GetMessage(_ context.Context, _, id int64) (sms.Message, error) {
	return sms.Message{ID: id}, nil
}
func (s *stubService) ListMessages(_ context.Context, f sms.MessageFilter) ([]sms.Message, int64, error) {
	s.lastQuery = f
	return []sms.Message{{ID: 5}}, s.listNext, nil
}
func (s *stubService) Summarize(context.Context, int64, time.Time, time.Time) (service.Summary, error) {
	return service.Summary{Total: 2, ByStatus: map[sms.Status]int64{sms.StatusSent: 2}}, nil
}

func do(t *testing.T, svc Service, method, path, body string, headers ...string) *httptest.ResponseRecorder {
	t.Helper()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	router := NewRouter(svc, log, func(context.Context) error { return nil })
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	for i := 0; i+1 < len(headers); i += 2 {
		req.Header.Set(headers[i], headers[i+1])
	}
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	return rec
}

func errorCode(t *testing.T, rec *httptest.ResponseRecorder) string {
	t.Helper()
	var body errorResponse
	if err := json.NewDecoder(rec.Body).Decode(&body); err != nil {
		t.Fatalf("decode error body: %v", err)
	}
	return body.Error.Code
}

func TestCreateCustomer(t *testing.T) {
	rec := do(t, &stubService{}, "POST", "/v1/customers", `{"name":"Acme"}`)
	if rec.Code != http.StatusCreated {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body)
	}
}

func TestSendMessageAccepted(t *testing.T) {
	svc := &stubService{}
	rec := do(t, svc, "POST", "/v1/customers/7/messages",
		`{"to":"09121234567","text":"hi","express":true}`, "Idempotency-Key", "k1")
	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body)
	}
	if svc.lastSend.CustomerID != 7 || !svc.lastSend.Express || svc.lastSend.IdempotencyKey != "k1" {
		t.Errorf("service got %+v", svc.lastSend)
	}
	if rec.Header().Get("Idempotent-Replayed") != "" {
		t.Error("fresh request marked as replayed")
	}
}

func TestSendMessageReplay(t *testing.T) {
	rec := do(t, &stubService{replayed: true}, "POST", "/v1/customers/7/messages", `{"to":"x","text":"hi"}`)
	if rec.Header().Get("Idempotent-Replayed") != "true" {
		t.Error("replayed request is missing the Idempotent-Replayed header")
	}
}

func TestErrorMapping(t *testing.T) {
	tests := []struct {
		err        error
		status     int
		code       string
		retryAfter string
	}{
		{sms.ErrInsufficientBalance, http.StatusPaymentRequired, "insufficient_balance", ""},
		{sms.ErrCustomerNotFound, http.StatusNotFound, "customer_not_found", ""},
		{sms.ErrIdempotencyConflict, http.StatusConflict, "idempotency_conflict", ""},
		{sms.ErrExpressRateLimited, http.StatusTooManyRequests, "express_rate_limited", "1"},
		{sms.ErrExpressOverloaded, http.StatusServiceUnavailable, "express_overloaded", "1"},
		{&sms.ValidationError{Field: "to", Reason: "bad"}, http.StatusBadRequest, "validation_error", ""},
		{errors.New("db exploded"), http.StatusInternalServerError, "internal_error", ""},
	}
	for _, tt := range tests {
		rec := do(t, &stubService{sendErr: tt.err}, "POST", "/v1/customers/1/messages", `{"to":"x","text":"y"}`)
		if rec.Code != tt.status || rec.Header().Get("Retry-After") != tt.retryAfter {
			t.Errorf("%v: status=%d retry-after=%q, want %d %q",
				tt.err, rec.Code, rec.Header().Get("Retry-After"), tt.status, tt.retryAfter)
		}
		if code := errorCode(t, rec); code != tt.code {
			t.Errorf("%v: code = %q, want %q", tt.err, code, tt.code)
		}
	}
}

func TestInternalErrorsAreNotLeaked(t *testing.T) {
	rec := do(t, &stubService{sendErr: errors.New("password=secret")}, "POST", "/v1/customers/1/messages", `{"to":"x","text":"y"}`)
	if strings.Contains(rec.Body.String(), "secret") {
		t.Fatalf("internal error leaked: %s", rec.Body)
	}
}

func TestBadRequests(t *testing.T) {
	tests := []struct{ method, path, body string }{
		{"POST", "/v1/customers/abc/messages", `{"to":"x","text":"y"}`},
		{"POST", "/v1/customers/1/messages", `{"to":"x","text":"y","expres":true}`}, // typo
		{"POST", "/v1/customers/1/messages", `not json`},
		{"GET", "/v1/customers/1/messages?from=yesterday", ``},
		{"GET", "/v1/customers/1/messages?limit=-1", ``},
	}
	for _, tt := range tests {
		rec := do(t, &stubService{}, tt.method, tt.path, tt.body)
		if rec.Code != http.StatusBadRequest {
			t.Errorf("%s %s %s: status = %d, want 400", tt.method, tt.path, tt.body, rec.Code)
		}
	}
}

func TestListMessages(t *testing.T) {
	svc := &stubService{listNext: 5}
	rec := do(t, svc, "GET", "/v1/customers/3/messages?status=sent&limit=10&cursor=99&from=2026-01-01T00:00:00Z", "")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body)
	}
	q := svc.lastQuery
	if q.CustomerID != 3 || q.Status != sms.StatusSent || q.Limit != 10 || q.BeforeID != 99 || q.From.IsZero() {
		t.Errorf("filter = %+v", q)
	}
	var body struct {
		Data       []map[string]any `json:"data"`
		NextCursor *string          `json:"next_cursor"`
	}
	_ = json.NewDecoder(rec.Body).Decode(&body)
	if len(body.Data) != 1 || body.NextCursor == nil || *body.NextCursor != "5" {
		t.Errorf("body = %+v", body)
	}
}

func TestSummaryRouteIsNotAMessageID(t *testing.T) {
	rec := do(t, &stubService{}, "GET", "/v1/customers/3/messages/summary", "")
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), `"total":2`) {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body)
	}
}

func TestSendBulk(t *testing.T) {
	rec := do(t, &stubService{}, "POST", "/v1/customers/3/messages/bulk",
		`{"messages":[{"to":"09121111111","text":"a"},{"to":"09122222222","text":"b"}]}`)
	if rec.Code != http.StatusAccepted || !strings.Contains(rec.Body.String(), `"total_cost":200`) {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body)
	}
}

func TestHealth(t *testing.T) {
	if rec := do(t, &stubService{}, "GET", "/healthz", ""); rec.Code != http.StatusOK {
		t.Fatalf("healthz status = %d", rec.Code)
	}
}
