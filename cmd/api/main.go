// Command api serves the public REST API: customers, top-ups, sending SMS
// and reports. It only writes to PostgreSQL; delivery is done by the
// worker process.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"

	"sms-gateway/internal/api"
	"sms-gateway/internal/config"
	"sms-gateway/internal/logging"
	"sms-gateway/internal/queue"
	"sms-gateway/internal/service"
	"sms-gateway/internal/store"
)

func main() {
	if err := run(); err != nil {
		slog.Error("api stopped", "error", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := config.Load()
	if err != nil {
		return err
	}
	log := logging.New(cfg.LogLevel)

	// ctx is cancelled on Ctrl+C or SIGTERM (docker stop, Kubernetes).
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

	// Redis is only used here for express admission. If it is down, normal
	// messages are still accepted (they wait safely in Postgres) and express
	// messages are refused with 503.
	rdb := redis.NewClient(&redis.Options{Addr: cfg.RedisAddr, DB: cfg.RedisDB})
	defer rdb.Close()
	if err := rdb.Ping(ctx).Err(); err != nil {
		log.Warn("redis not reachable; express messages are refused until it is", "error", err)
	}
	sched := queue.NewScheduler(rdb, cfg.FairQuantum)
	admission := queue.NewAdmission(rdb, sched, cfg.ExpressRatePerCustomer, cfg.ExpressMaxBacklog)

	svc := service.New(db, admission, service.Config{
		PriceNormal:  cfg.PriceNormal,
		PriceExpress: cfg.PriceExpress,
		ExpressSLA:   cfg.ExpressSLA,
		MaxBulkSize:  cfg.MaxBulkSize,
	})

	srv := &http.Server{
		Addr:              cfg.HTTPAddr,
		Handler:           api.NewRouter(svc, log, db.Ping),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	errCh := make(chan error, 1)
	go func() {
		log.Info("api listening", "addr", cfg.HTTPAddr)
		errCh <- srv.ListenAndServe()
	}()

	select {
	case err := <-errCh:
		return err
	case <-ctx.Done():
	}

	// Stop accepting new connections and let in-flight requests finish.
	log.Info("shutting down")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}
