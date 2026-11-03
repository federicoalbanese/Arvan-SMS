// Package sms holds the core domain types of the gateway: customers,
// messages, their lifecycle and validation rules. It does not depend on
// databases, Redis or HTTP, so it is easy to reason about and test.
package sms

import "time"

// Status is a step in a message's lifecycle:
//
//	pending -> queued -> sending -> sent
//	                        |
//	                        +----> pending (retry later)
//	                        +----> failed  (refunded)
type Status string

const (
	// StatusPending: stored in Postgres, waiting for the relay to put it on the dispatch queue.
	StatusPending Status = "pending"
	// StatusQueued: sitting in the Redis dispatch queue.
	StatusQueued Status = "queued"
	// StatusSending: claimed by a worker, being handed to the operator.
	StatusSending Status = "sending"
	// StatusSent: accepted by the operator. Final.
	StatusSent Status = "sent"
	// StatusFailed: permanently failed; the cost was refunded. Final.
	StatusFailed Status = "failed"
)

// Valid reports whether s is a known status.
func (s Status) Valid() bool {
	switch s {
	case StatusPending, StatusQueued, StatusSending, StatusSent, StatusFailed:
		return true
	}
	return false
}

// Lane returns the name of the dispatch lane, used in logs and metrics.
func Lane(express bool) string {
	if express {
		return "express"
	}
	return "normal"
}

// Customer is a business that sends SMS through the gateway.
type Customer struct {
	ID        int64
	Name      string
	Balance   int64 // smallest currency unit
	CreatedAt time.Time
}

// Message is a stored SMS together with its delivery state.
type Message struct {
	ID          int64
	CustomerID  int64
	To          string
	Text        string
	Express     bool
	Status      Status
	Cost        int64
	Attempts    int
	OperatorRef string
	LastError   string
	DeadlineAt  *time.Time // express only: promised hand-over time
	CreatedAt   time.Time
	SentAt      *time.Time
}

// NewMessage is a validated message that is about to be stored and charged.
type NewMessage struct {
	To             string
	Text           string
	Express        bool
	Cost           int64
	DeadlineAt     *time.Time
	IdempotencyKey string
}

// MessageFilter selects messages for the report endpoints.
type MessageFilter struct {
	CustomerID int64
	Status     Status    // empty: any status
	From       time.Time // zero: no lower bound (inclusive)
	To         time.Time // zero: no upper bound (exclusive)
	BeforeID   int64     // keyset pagination cursor; 0: start from newest
	Limit      int
}

// QueueItem is the minimum the dispatcher needs to schedule a message.
type QueueItem struct {
	ID         int64
	CustomerID int64
}

// OutcomeKind is what happened when a worker tried to hand a message over.
type OutcomeKind int

const (
	OutcomeSent OutcomeKind = iota
	OutcomeRetry
	OutcomeFailed
)

// SendOutcome is the result of one hand-over attempt, written back to Postgres.
type SendOutcome struct {
	MessageID   int64
	Attempt     int // must match the stored attempt counter, see store.FinishSending
	Kind        OutcomeKind
	OperatorRef string
	Error       string
	RetryAt     time.Time
}
