// Package api is the REST layer. It parses HTTP requests, calls the
// service, and turns results and errors into JSON responses. Business
// rules live in the service package, not here.
package api

import (
	"context"
	"log/slog"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"

	"sms-gateway/internal/service"
	"sms-gateway/internal/sms"
)

// Service is what the handlers need from the service layer.
type Service interface {
	CreateCustomer(ctx context.Context, name string) (sms.Customer, error)
	GetCustomer(ctx context.Context, id int64) (sms.Customer, error)
	TopUp(ctx context.Context, id, amount int64) (sms.Customer, error)
	ListTransactions(ctx context.Context, customerID int64, limit int) ([]sms.Transaction, error)
	Send(ctx context.Context, req service.SendRequest) (sms.Message, bool, error)
	SendBulk(ctx context.Context, req service.BulkRequest) ([]sms.Message, bool, error)
	GetMessage(ctx context.Context, customerID, id int64) (sms.Message, error)
	ListMessages(ctx context.Context, f sms.MessageFilter) ([]sms.Message, int64, error)
	Summarize(ctx context.Context, customerID int64, from, to time.Time) (service.Summary, error)
}

// Handler holds the dependencies of the HTTP handlers.
type Handler struct {
	svc Service
	log *slog.Logger
}

// NewRouter wires every route. ready is called by /readyz to check that
// the dependencies needed to accept traffic are reachable.
func NewRouter(svc Service, log *slog.Logger, ready func(context.Context) error) http.Handler {
	h := &Handler{svc: svc, log: log}
	mux := http.NewServeMux()

	mux.HandleFunc("POST /v1/customers", h.createCustomer)
	mux.HandleFunc("GET /v1/customers/{id}", h.getCustomer)
	mux.HandleFunc("POST /v1/customers/{id}/topup", h.topUp)
	mux.HandleFunc("GET /v1/customers/{id}/transactions", h.listTransactions)

	mux.HandleFunc("POST /v1/customers/{id}/messages", h.sendMessage)
	mux.HandleFunc("POST /v1/customers/{id}/messages/bulk", h.sendBulk)
	mux.HandleFunc("GET /v1/customers/{id}/messages", h.listMessages)
	mux.HandleFunc("GET /v1/customers/{id}/messages/summary", h.summary)
	mux.HandleFunc("GET /v1/customers/{id}/messages/{messageID}", h.getMessage)

	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, r *http.Request) {
		if err := ready(r.Context()); err != nil {
			writeJSON(w, http.StatusServiceUnavailable, map[string]string{"status": "unavailable", "error": err.Error()})
			return
		}
		writeJSON(w, http.StatusOK, map[string]string{"status": "ready"})
	})
	mux.Handle("GET /metrics", promhttp.Handler())

	return h.recoverPanics(h.logRequests(mux))
}

type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (r *statusRecorder) WriteHeader(code int) {
	r.status = code
	r.ResponseWriter.WriteHeader(code)
}

// logRequests logs every request; server errors at error level, the rest at
// debug level to keep logs quiet under high traffic.
func (h *Handler) logRequests(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(rec, r)

		level := slog.LevelDebug
		if rec.status >= 500 {
			level = slog.LevelError
		}
		h.log.Log(r.Context(), level, "http request",
			"method", r.Method, "path", r.URL.Path, "status", rec.status,
			"duration_ms", time.Since(start).Milliseconds())
	})
}

// recoverPanics turns a panic in a handler into a 500 instead of a crash.
func (h *Handler) recoverPanics(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if v := recover(); v != nil {
				h.log.Error("panic in handler", "panic", v, "path", r.URL.Path)
				writeJSON(w, http.StatusInternalServerError, errorBody("internal_error", "internal server error"))
			}
		}()
		next.ServeHTTP(w, r)
	})
}
