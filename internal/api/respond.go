package api

import (
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"time"

	"sms-gateway/internal/sms"
)

const maxBodyBytes = 1 << 20 // 1 MiB is plenty for a 1000-message bulk request

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

type errorResponse struct {
	Error errorDetail `json:"error"`
}

type errorDetail struct {
	Code    string `json:"code"`
	Message string `json:"message"`
	Field   string `json:"field,omitempty"`
}

func errorBody(code, message string) errorResponse {
	return errorResponse{Error: errorDetail{Code: code, Message: message}}
}

// writeError maps domain errors to HTTP status codes in one place.
func (h *Handler) writeError(w http.ResponseWriter, r *http.Request, err error) {
	var verr *sms.ValidationError
	if errors.As(err, &verr) {
		writeJSON(w, http.StatusBadRequest, errorResponse{Error: errorDetail{
			Code: "validation_error", Message: verr.Reason, Field: verr.Field,
		}})
		return
	}

	status, code, retryAfter := http.StatusInternalServerError, "internal_error", ""
	switch {
	case errors.Is(err, sms.ErrCustomerNotFound):
		status, code = http.StatusNotFound, "customer_not_found"
	case errors.Is(err, sms.ErrMessageNotFound):
		status, code = http.StatusNotFound, "message_not_found"
	case errors.Is(err, sms.ErrInsufficientBalance):
		status, code = http.StatusPaymentRequired, "insufficient_balance"
	case errors.Is(err, sms.ErrIdempotencyConflict):
		status, code = http.StatusConflict, "idempotency_conflict"
	case errors.Is(err, sms.ErrExpressRateLimited):
		status, code, retryAfter = http.StatusTooManyRequests, "express_rate_limited", "1"
	case errors.Is(err, sms.ErrExpressOverloaded):
		status, code, retryAfter = http.StatusServiceUnavailable, "express_overloaded", "1"
	case errors.Is(err, sms.ErrExpressUnavailable):
		status, code, retryAfter = http.StatusServiceUnavailable, "express_unavailable", "5"
	}

	message := err.Error()
	if status == http.StatusInternalServerError {
		h.log.Error("request failed", "path", r.URL.Path, "error", err)
		message = "internal server error" // do not leak internals
	}
	if retryAfter != "" {
		w.Header().Set("Retry-After", retryAfter)
	}
	writeJSON(w, status, errorBody(code, message))
}

// decodeJSON reads exactly one JSON object and rejects unknown fields, so
// typos like "expres": true are reported instead of silently ignored.
func decodeJSON(w http.ResponseWriter, r *http.Request, dst any) error {
	r.Body = http.MaxBytesReader(w, r.Body, maxBodyBytes)
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		return &sms.ValidationError{Field: "body", Reason: "invalid JSON: " + err.Error()}
	}
	if dec.More() {
		return &sms.ValidationError{Field: "body", Reason: "must contain a single JSON object"}
	}
	return nil
}

func pathID(r *http.Request, name string) (int64, error) {
	id, err := strconv.ParseInt(r.PathValue(name), 10, 64)
	if err != nil || id <= 0 {
		return 0, &sms.ValidationError{Field: name, Reason: "must be a positive integer"}
	}
	return id, nil
}

func queryInt(r *http.Request, name string) (int64, error) {
	raw := r.URL.Query().Get(name)
	if raw == "" {
		return 0, nil
	}
	v, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || v < 0 {
		return 0, &sms.ValidationError{Field: name, Reason: "must be a non-negative integer"}
	}
	return v, nil
}

func queryTime(r *http.Request, name string) (time.Time, error) {
	raw := r.URL.Query().Get(name)
	if raw == "" {
		return time.Time{}, nil
	}
	t, err := time.Parse(time.RFC3339, raw)
	if err != nil {
		return time.Time{}, &sms.ValidationError{Field: name, Reason: "must be an RFC 3339 time, e.g. 2026-01-02T15:04:05Z"}
	}
	return t, nil
}
