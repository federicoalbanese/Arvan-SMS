// Package operator abstracts the mobile operator (SMSC) that the gateway
// hands messages to. A real implementation would speak SMPP or the
// operator's HTTP API; this project ships a mock with configurable latency
// and failure rate.
package operator

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"strings"
	"sync/atomic"
	"time"
)

// Operator hands one message to a mobile operator and returns the
// operator's reference for it.
type Operator interface {
	Send(ctx context.Context, to, text string) (ref string, err error)
}

// ErrPermanent marks errors that will never succeed on retry (unknown or
// blocked number, rejected content). Any other error is treated as
// temporary and retried with backoff.
var ErrPermanent = errors.New("permanent operator error")

// UnreachablePrefix: the mock rejects numbers with this prefix permanently.
// Handy to see failures and refunds in action.
const UnreachablePrefix = "+98999"

// Mock simulates an operator.
type Mock struct {
	latency     time.Duration
	failureRate float64
	seq         atomic.Int64
}

// NewMock returns a mock operator that takes about latency per message and
// fails temporarily with probability failureRate.
func NewMock(latency time.Duration, failureRate float64) *Mock {
	return &Mock{latency: latency, failureRate: failureRate}
}

// Send implements Operator.
func (m *Mock) Send(ctx context.Context, to, _ string) (string, error) {
	if m.latency > 0 {
		// +/- 50% jitter, like a real network.
		d := m.latency/2 + rand.N(m.latency)
		select {
		case <-time.After(d):
		case <-ctx.Done():
			return "", ctx.Err()
		}
	}
	if strings.HasPrefix(to, UnreachablePrefix) {
		return "", fmt.Errorf("%w: number %s is not reachable", ErrPermanent, to)
	}
	if rand.Float64() < m.failureRate {
		return "", errors.New("operator temporarily unavailable")
	}
	return fmt.Sprintf("op-%d", m.seq.Add(1)), nil
}
