// Package service holds the use cases of the gateway: everything the REST
// API can do. It validates input, applies pricing and express admission,
// and delegates storage to the store. It knows nothing about HTTP, so the
// same rules apply no matter how a request arrives.
package service

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"
	"unicode/utf8"

	"sms-gateway/internal/metrics"
	"sms-gateway/internal/sms"
)

// Store is the persistence the service needs. It is defined here, where it
// is used, so tests can pass an in-memory fake.
type Store interface {
	CreateCustomer(ctx context.Context, name string) (sms.Customer, error)
	GetCustomer(ctx context.Context, id int64) (sms.Customer, error)
	TopUp(ctx context.Context, id, amount int64) (sms.Customer, error)
	ListTransactions(ctx context.Context, customerID int64, limit int) ([]sms.Transaction, error)
	AcceptMessages(ctx context.Context, customerID int64, msgs []sms.NewMessage) ([]sms.Message, bool, error)
	GetMessage(ctx context.Context, customerID, id int64) (sms.Message, error)
	ListMessages(ctx context.Context, f sms.MessageFilter) ([]sms.Message, error)
	MessageStats(ctx context.Context, customerID int64, from, to time.Time) (map[sms.Status]int64, error)
}

// Admission guards the express lane (see queue.Admission).
type Admission interface {
	AdmitExpress(ctx context.Context, customerID int64) error
}

// Config holds the business settings.
type Config struct {
	PriceNormal  int64
	PriceExpress int64
	ExpressSLA   time.Duration
	MaxBulkSize  int
}

const (
	maxNameLength   = 200
	maxTopUp        = 1_000_000_000_000 // guards against typos and int64 overflow
	maxKeyLength    = 100
	defaultPageSize = 50
	maxPageSize     = 500
	maxReportRange  = 31 * 24 * time.Hour
)

// Service implements the gateway's use cases.
type Service struct {
	store     Store
	admission Admission
	cfg       Config
	now       func() time.Time
}

// New creates the service.
func New(store Store, admission Admission, cfg Config) *Service {
	return &Service{store: store, admission: admission, cfg: cfg, now: time.Now}
}

// CreateCustomer registers a customer with a zero balance.
func (s *Service) CreateCustomer(ctx context.Context, name string) (sms.Customer, error) {
	name = strings.TrimSpace(name)
	if name == "" || utf8.RuneCountInString(name) > maxNameLength {
		return sms.Customer{}, &sms.ValidationError{Field: "name", Reason: "must be 1 to 200 characters"}
	}
	return s.store.CreateCustomer(ctx, name)
}

// GetCustomer returns a customer and its balance.
func (s *Service) GetCustomer(ctx context.Context, id int64) (sms.Customer, error) {
	return s.store.GetCustomer(ctx, id)
}

// TopUp adds credit to a customer's balance.
func (s *Service) TopUp(ctx context.Context, id, amount int64) (sms.Customer, error) {
	if amount <= 0 || amount > maxTopUp {
		return sms.Customer{}, &sms.ValidationError{Field: "amount", Reason: "must be a positive integer"}
	}
	return s.store.TopUp(ctx, id, amount)
}

// ListTransactions returns the newest balance ledger entries.
func (s *Service) ListTransactions(ctx context.Context, customerID int64, limit int) ([]sms.Transaction, error) {
	if _, err := s.store.GetCustomer(ctx, customerID); err != nil {
		return nil, err
	}
	return s.store.ListTransactions(ctx, customerID, pageSize(limit))
}

// SendRequest is a single SMS to send.
type SendRequest struct {
	CustomerID     int64
	To             string
	Text           string
	Express        bool
	IdempotencyKey string
}

// Send validates, charges and stores one message. It returns replayed=true
// when the idempotency key matched an earlier identical request.
func (s *Service) Send(ctx context.Context, req SendRequest) (sms.Message, bool, error) {
	lane := sms.Lane(req.Express)
	to, err := sms.NormalizePhone(req.To)
	if err == nil {
		err = sms.ValidateText(req.Text)
	}
	if err == nil {
		err = validateKey(req.IdempotencyKey)
	}
	if err == nil && req.Express {
		// Admission happens before anything is stored or charged.
		err = s.admission.AdmitExpress(ctx, req.CustomerID)
	}
	if err != nil {
		countRejected(lane, 1, err)
		return sms.Message{}, false, err
	}

	msg := s.newMessage(to, req.Text, req.Express, req.IdempotencyKey)
	stored, replayed, err := s.store.AcceptMessages(ctx, req.CustomerID, []sms.NewMessage{msg})
	if err != nil {
		countRejected(lane, 1, err)
		return sms.Message{}, false, err
	}
	if !replayed {
		metrics.Accepted.WithLabelValues(lane).Inc()
	}
	return stored[0], replayed, nil
}

// BulkItem is one message of a bulk request.
type BulkItem struct {
	To   string
	Text string
}

// BulkRequest sends many normal messages in one all-or-nothing request.
// Bulk is for campaigns, so it always uses the normal lane.
type BulkRequest struct {
	CustomerID     int64
	Messages       []BulkItem
	IdempotencyKey string
}

// SendBulk validates every message, then charges and stores them in one
// transaction. One balance update for N messages is also what keeps
// high-volume customers from hammering their balance row.
func (s *Service) SendBulk(ctx context.Context, req BulkRequest) ([]sms.Message, bool, error) {
	lane := sms.Lane(false)
	if err := s.validateBulk(req); err != nil {
		countRejected(lane, len(req.Messages), err)
		return nil, false, err
	}

	msgs := make([]sms.NewMessage, len(req.Messages))
	for i, it := range req.Messages {
		to, _ := sms.NormalizePhone(it.To) // already validated
		var key string
		if req.IdempotencyKey != "" {
			key = fmt.Sprintf("%s#%d", req.IdempotencyKey, i)
		}
		msgs[i] = s.newMessage(to, it.Text, false, key)
	}

	stored, replayed, err := s.store.AcceptMessages(ctx, req.CustomerID, msgs)
	if err != nil {
		countRejected(lane, len(msgs), err)
		return nil, false, err
	}
	if !replayed {
		metrics.Accepted.WithLabelValues(lane).Add(float64(len(stored)))
	}
	return stored, replayed, nil
}

func (s *Service) validateBulk(req BulkRequest) error {
	if len(req.Messages) == 0 || len(req.Messages) > s.cfg.MaxBulkSize {
		return &sms.ValidationError{
			Field:  "messages",
			Reason: fmt.Sprintf("must contain 1 to %d messages", s.cfg.MaxBulkSize),
		}
	}
	if err := validateKey(req.IdempotencyKey); err != nil {
		return err
	}
	for i, it := range req.Messages {
		_, err := sms.NormalizePhone(it.To)
		if err == nil {
			err = sms.ValidateText(it.Text)
		}
		var verr *sms.ValidationError
		if errors.As(err, &verr) {
			return &sms.ValidationError{Field: fmt.Sprintf("messages[%d].%s", i, verr.Field), Reason: verr.Reason}
		}
	}
	return nil
}

func (s *Service) newMessage(to, text string, express bool, key string) sms.NewMessage {
	m := sms.NewMessage{To: to, Text: text, Express: express, Cost: s.cfg.PriceNormal, IdempotencyKey: key}
	if express {
		m.Cost = s.cfg.PriceExpress
		deadline := s.now().Add(s.cfg.ExpressSLA)
		m.DeadlineAt = &deadline
	}
	return m
}

// GetMessage returns one of the customer's messages.
func (s *Service) GetMessage(ctx context.Context, customerID, id int64) (sms.Message, error) {
	return s.store.GetMessage(ctx, customerID, id)
}

// ListMessages returns one page of the customer's messages, newest first,
// and the cursor for the next page (0 when there are no more).
func (s *Service) ListMessages(ctx context.Context, f sms.MessageFilter) ([]sms.Message, int64, error) {
	if f.Status != "" && !f.Status.Valid() {
		return nil, 0, &sms.ValidationError{Field: "status", Reason: "unknown status"}
	}
	if !f.From.IsZero() && !f.To.IsZero() && !f.From.Before(f.To) {
		return nil, 0, &sms.ValidationError{Field: "from", Reason: "must be before to"}
	}
	if _, err := s.store.GetCustomer(ctx, f.CustomerID); err != nil {
		return nil, 0, err
	}

	// Ask for one extra row: if it exists there is a next page.
	limit := pageSize(f.Limit)
	f.Limit = limit + 1
	msgs, err := s.store.ListMessages(ctx, f)
	if err != nil {
		return nil, 0, err
	}
	var next int64
	if len(msgs) > limit {
		msgs = msgs[:limit]
		next = msgs[limit-1].ID
	}
	return msgs, next, nil
}

// Summary counts a customer's messages per status in a time range.
type Summary struct {
	From     time.Time
	To       time.Time
	Total    int64
	ByStatus map[sms.Status]int64
}

// Summarize builds the summary report. The default range is the last 24h.
func (s *Service) Summarize(ctx context.Context, customerID int64, from, to time.Time) (Summary, error) {
	if to.IsZero() {
		to = s.now()
	}
	if from.IsZero() {
		from = to.Add(-24 * time.Hour)
	}
	if !from.Before(to) || to.Sub(from) > maxReportRange {
		return Summary{}, &sms.ValidationError{Field: "from", Reason: "range must be positive and at most 31 days"}
	}
	if _, err := s.store.GetCustomer(ctx, customerID); err != nil {
		return Summary{}, err
	}
	stats, err := s.store.MessageStats(ctx, customerID, from, to)
	if err != nil {
		return Summary{}, err
	}
	sum := Summary{From: from, To: to, ByStatus: stats}
	for _, n := range stats {
		sum.Total += n
	}
	return sum, nil
}

func validateKey(key string) error {
	if len(key) > maxKeyLength {
		return &sms.ValidationError{Field: "Idempotency-Key", Reason: "must be at most 100 characters"}
	}
	return nil
}

func pageSize(limit int) int {
	if limit <= 0 {
		return defaultPageSize
	}
	return min(limit, maxPageSize)
}

func countRejected(lane string, n int, err error) {
	metrics.Rejected.WithLabelValues(lane, reason(err)).Add(float64(n))
}

// reason turns an error into a short metrics label.
func reason(err error) string {
	var verr *sms.ValidationError
	switch {
	case errors.As(err, &verr):
		return "invalid"
	case errors.Is(err, sms.ErrInsufficientBalance):
		return "insufficient_balance"
	case errors.Is(err, sms.ErrCustomerNotFound):
		return "unknown_customer"
	case errors.Is(err, sms.ErrIdempotencyConflict):
		return "idempotency_conflict"
	case errors.Is(err, sms.ErrExpressRateLimited):
		return "rate_limited"
	case errors.Is(err, sms.ErrExpressOverloaded):
		return "overloaded"
	case errors.Is(err, sms.ErrExpressUnavailable):
		return "unavailable"
	default:
		return "error"
	}
}
