// Command worker delivers messages: it relays new messages from Postgres to
// the Redis queues, sends them to the operator through separate express and
// normal worker pools, and repairs messages stuck after a crash.
//
// Run as many instances as needed; they coordinate through Postgres row
// locks and atomic Redis operations, not through each other.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/redis/go-redis/v9"

	"sms-gateway/internal/config"
	"sms-gateway/internal/logging"
	"sms-gateway/internal/operator"
	"sms-gateway/internal/queue"
	"sms-gateway/internal/store"
	"sms-gateway/internal/worker"
)

func main() {
	if err := run(); err != nil {
		slog.Error("worker stopped", "error", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := config.Load()
	if err != nil {
		return err
	}
	log := logging.New(cfg.LogLevel)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	db, err := store.New(ctx, cfg.DatabaseURL)
	if err != nil {
		return err
	}
	defer db.Close()
	if err := db.Migrate(ctx); err != nil {
		return fmt.Errorf("migrate: %w", err)
	}

	rdb := redis.NewClient(&redis.Options{Addr: cfg.RedisAddr, DB: cfg.RedisDB})
	defer rdb.Close()
	if err := rdb.Ping(ctx).Err(); err != nil {
		return fmt.Errorf("connect redis: %w", err)
	}

	sched := queue.NewScheduler(rdb, cfg.FairQuantum)
	op := operator.NewMock(cfg.OperatorLatency, cfg.OperatorFailureRate)

	relay := worker.NewRelay(db, sched, cfg.RelayBatchSize, log)
	sender := worker.NewSender(db, sched, op, worker.SenderConfig{
		MaxAttempts:     cfg.MaxAttempts,
		OperatorTimeout: cfg.OperatorTimeout,
		ExpressIdleWait: cfg.ExpressPollInterval,
		NormalIdleWait:  cfg.NormalPollInterval,
	}, log)
	reconciler := worker.NewReconciler(db, sched, worker.ReconcilerConfig{
		VisibilityTimeout: cfg.VisibilityTimeout,
		SendingTimeout:    cfg.SendingTimeout,
	}, log)

	metricsSrv := startMetricsServer(cfg.MetricsAddr, log)

	var wg sync.WaitGroup
	wg.Go(func() { relay.Run(ctx, true, cfg.RelayExpressInterval) })
	wg.Go(func() { relay.Run(ctx, false, cfg.RelayNormalInterval) })
	wg.Go(func() { sender.Run(ctx, true, cfg.ExpressWorkers) })
	wg.Go(func() { sender.Run(ctx, false, cfg.NormalWorkers) })
	wg.Go(func() { reconciler.Run(ctx, cfg.ReconcileInterval) })
	log.Info("worker started",
		"express_workers", cfg.ExpressWorkers, "normal_workers", cfg.NormalWorkers,
		"express_sla", cfg.ExpressSLA.String())

	<-ctx.Done()
	log.Info("shutting down; finishing in-flight batches")
	wg.Wait()

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return metricsSrv.Shutdown(shutdownCtx)
}

// startMetricsServer exposes /metrics for Prometheus and /healthz for the
// container orchestrator.
func startMetricsServer(addr string, log *slog.Logger) *http.Server {
	mux := http.NewServeMux()
	mux.Handle("GET /metrics", promhttp.Handler())
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	srv := &http.Server{Addr: addr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() {
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Error("metrics server stopped", "error", err)
		}
	}()
	return srv
}
