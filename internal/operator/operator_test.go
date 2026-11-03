package operator

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestMockSend(t *testing.T) {
	m := NewMock(0, 0)
	ref, err := m.Send(context.Background(), "+989121234567", "hi")
	if err != nil || ref == "" {
		t.Fatalf("Send = %q, %v", ref, err)
	}
}

func TestMockUnreachableIsPermanent(t *testing.T) {
	m := NewMock(0, 0)
	_, err := m.Send(context.Background(), UnreachablePrefix+"1234567", "hi")
	if !errors.Is(err, ErrPermanent) {
		t.Fatalf("err = %v, want ErrPermanent", err)
	}
}

func TestMockFailureIsTemporary(t *testing.T) {
	m := NewMock(0, 1)
	_, err := m.Send(context.Background(), "+989121234567", "hi")
	if err == nil || errors.Is(err, ErrPermanent) {
		t.Fatalf("err = %v, want a temporary error", err)
	}
}

func TestMockRespectsContext(t *testing.T) {
	m := NewMock(time.Second, 0)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if _, err := m.Send(ctx, "+989121234567", "hi"); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v, want DeadlineExceeded", err)
	}
}
