package sms

import (
	"errors"
	"fmt"
)

// Domain errors. The HTTP layer maps each of them to a status code.
var (
	ErrCustomerNotFound    = errors.New("customer not found")
	ErrMessageNotFound     = errors.New("message not found")
	ErrInsufficientBalance = errors.New("insufficient balance")
	ErrIdempotencyConflict = errors.New("idempotency key was already used for a different request")

	// Express admission control: we refuse express messages we cannot hand
	// over within the SLA instead of accepting them and breaking the promise.
	ErrExpressRateLimited = errors.New("express rate limit exceeded for this customer")
	ErrExpressOverloaded  = errors.New("express lane is at capacity")
	ErrExpressUnavailable = errors.New("express lane is temporarily unavailable")
)

// ValidationError reports a bad input field.
type ValidationError struct {
	Field  string
	Reason string
}

func (e *ValidationError) Error() string {
	return fmt.Sprintf("%s: %s", e.Field, e.Reason)
}

func invalid(field, reason string) error {
	return &ValidationError{Field: field, Reason: reason}
}
