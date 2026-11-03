// Package e2e runs the whole pipeline in one process against real
// PostgreSQL and Redis: service -> outbox -> relay -> Redis -> sender ->
// operator -> results and refunds back in Postgres.
package e2e

import (
	"context"
	"io"
	"log/slog"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"sms-gateway/internal/operator"
	"sms-gateway/internal/queue"
	"sms-gateway/internal/service"
	"sms-gateway/internal/sms"
	"sms-gateway/internal/store"
	"sms-gateway/internal/worker"
)

func TestEndToEnd(t *testing.T) {
	dbURL, redisAddr := os.Getenv("TEST_DATABASE_URL"), os.Getenv("TEST_REDIS_ADDR")
	if dbURL == "" || redisAddr == "" {
		t.Skip("TEST_DATABASE_URL and TEST_REDIS_ADDR not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))

	db, err := store.New(ctx, dbURL)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := db.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	rdb := redis.NewClient(&redis.Options{Addr: redisAddr, DB: 14})
	defer rdb.Close()
	if err := rdb.FlushDB(ctx).Err(); err != nil {
		t.Fatal(err)
	}

	sched := queue.NewScheduler(rdb, 10)
	svc := service.New(db, queue.NewAdmission(rdb, sched, 100, 1000), service.Config{
		PriceNormal: 100, PriceExpress: 150, ExpressSLA: 5 * time.Second, MaxBulkSize: 100,
	})

	// Start the worker side.
	relay := worker.NewRelay(db, sched, 100, log)
	sender := worker.NewSender(db, sched, operator.NewMock(time.Millisecond, 0), worker.SenderConfig{
		MaxAttempts: 3, OperatorTimeout: time.Second,
		ExpressIdleWait: 5 * time.Millisecond, NormalIdleWait: 5 * time.Millisecond,
	}, log)
	workCtx, stopWorkers := context.WithCancel(ctx)
	var wg sync.WaitGroup
	wg.Go(func() { relay.Run(workCtx, true, 5*time.Millisecond) })
	wg.Go(func() { relay.Run(workCtx, false, 5*time.Millisecond) })
	wg.Go(func() { sender.Run(workCtx, true, 2) })
	wg.Go(func() { sender.Run(workCtx, false, 2) })
	defer func() {
		stopWorkers()
		wg.Wait()
	}()

	// Customer sends an OTP, a small campaign and one message to a dead number.
	c, err := svc.CreateCustomer(ctx, "e2e")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := svc.TopUp(ctx, c.ID, 1000); err != nil {
		t.Fatal(err)
	}
	otp, _, err := svc.Send(ctx, service.SendRequest{CustomerID: c.ID, To: "09120000001", Text: "code 1234", Express: true})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := svc.SendBulk(ctx, service.BulkRequest{CustomerID: c.ID, Messages: []service.BulkItem{
		{To: "09120000002", Text: "sale"}, {To: "09120000003", Text: "sale"}, {To: "09120000004", Text: "sale"},
	}}); err != nil {
		t.Fatal(err)
	}
	if _, _, err := svc.Send(ctx, service.SendRequest{CustomerID: c.ID, To: operator.UnreachablePrefix + "1234567", Text: "hi"}); err != nil {
		t.Fatal(err)
	}

	// Wait until every message reached a final state.
	var msgs []sms.Message
	for {
		msgs, _, err = svc.ListMessages(ctx, sms.MessageFilter{CustomerID: c.ID, Limit: 100})
		if err != nil {
			t.Fatal(err)
		}
		if allFinal(msgs) {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatalf("messages did not finish in time: %+v", msgs)
		case <-time.After(20 * time.Millisecond):
		}
	}

	counts := map[sms.Status]int{}
	for _, m := range msgs {
		counts[m.Status]++
	}
	if counts[sms.StatusSent] != 4 || counts[sms.StatusFailed] != 1 {
		t.Fatalf("statuses = %v, want 4 sent and 1 failed", counts)
	}

	m, _ := svc.GetMessage(ctx, c.ID, otp.ID)
	if m.SentAt == nil || m.SentAt.After(*m.DeadlineAt) {
		t.Fatalf("express message sent at %v, deadline %v", m.SentAt, m.DeadlineAt)
	}

	// 1000 - 150 (otp) - 300 (bulk) - 100 (dead number) + 100 (refund).
	got, _ := svc.GetCustomer(ctx, c.ID)
	if got.Balance != 550 {
		t.Fatalf("balance = %d, want 550", got.Balance)
	}
}

func allFinal(msgs []sms.Message) bool {
	if len(msgs) != 5 {
		return false
	}
	for _, m := range msgs {
		if m.Status != sms.StatusSent && m.Status != sms.StatusFailed {
			return false
		}
	}
	return true
}
