// Package config loads runtime settings from environment variables.
//
// Every value has a sensible default for local development, so the
// services start with zero configuration. In production the same image is
// configured per environment through env vars (12-factor style).
package config

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"time"
)

// Config holds every setting used by the api and worker processes.
type Config struct {
	HTTPAddr    string // api: public REST API
	MetricsAddr string // worker: /metrics and /healthz
	DatabaseURL string
	RedisAddr   string
	RedisDB     int
	LogLevel    string

	// Prices are in the smallest currency unit (e.g. Rial). Money is always
	// an integer; floats cannot represent money exactly.
	PriceNormal  int64
	PriceExpress int64

	MaxBulkSize int

	// Express lane. ExpressSLA is the time limit we promise for handing an
	// express message to the operator. The two limits below are the
	// admission control that makes the promise keepable.
	ExpressSLA             time.Duration
	ExpressRatePerCustomer int   // max express messages per customer per second
	ExpressMaxBacklog      int64 // reject new express messages above this queue depth

	// Worker pools and dispatching.
	ExpressWorkers       int
	NormalWorkers        int
	ExpressPollInterval  time.Duration // sender wait when the express queue is empty
	NormalPollInterval   time.Duration // sender wait when the normal queue is empty
	FairQuantum          int           // messages taken from one customer per round-robin turn
	RelayBatchSize       int
	RelayExpressInterval time.Duration
	RelayNormalInterval  time.Duration
	MaxAttempts          int
	VisibilityTimeout    time.Duration // popped from Redis but never claimed in Postgres
	SendingTimeout       time.Duration // claimed but never finished (worker crashed)
	ReconcileInterval    time.Duration

	// Mock operator (stands in for a real SMSC connection).
	OperatorLatency     time.Duration
	OperatorFailureRate float64
	OperatorTimeout     time.Duration
}

// Load reads the configuration from the environment and validates it.
func Load() (Config, error) {
	var l loader
	cfg := Config{
		HTTPAddr:    l.str("HTTP_ADDR", ":8080"),
		MetricsAddr: l.str("METRICS_ADDR", ":9090"),
		DatabaseURL: l.str("DATABASE_URL", "postgres://sms:sms@localhost:5432/sms?sslmode=disable"),
		RedisAddr:   l.str("REDIS_ADDR", "localhost:6379"),
		RedisDB:     l.int("REDIS_DB", 0),
		LogLevel:    l.str("LOG_LEVEL", "info"),

		PriceNormal:  l.int64("PRICE_NORMAL", 100),
		PriceExpress: l.int64("PRICE_EXPRESS", 150),
		MaxBulkSize:  l.int("MAX_BULK_SIZE", 1000),

		ExpressSLA:             l.duration("EXPRESS_SLA", 5*time.Second),
		ExpressRatePerCustomer: l.int("EXPRESS_RATE_PER_CUSTOMER", 50),
		ExpressMaxBacklog:      l.int64("EXPRESS_MAX_BACKLOG", 1000),

		ExpressWorkers:       l.int("EXPRESS_WORKERS", 20),
		NormalWorkers:        l.int("NORMAL_WORKERS", 50),
		ExpressPollInterval:  l.duration("EXPRESS_POLL_INTERVAL", 10*time.Millisecond),
		NormalPollInterval:   l.duration("NORMAL_POLL_INTERVAL", 100*time.Millisecond),
		FairQuantum:          l.int("FAIR_QUANTUM", 10),
		RelayBatchSize:       l.int("RELAY_BATCH_SIZE", 500),
		RelayExpressInterval: l.duration("RELAY_EXPRESS_INTERVAL", 20*time.Millisecond),
		RelayNormalInterval:  l.duration("RELAY_NORMAL_INTERVAL", 200*time.Millisecond),
		MaxAttempts:          l.int("MAX_ATTEMPTS", 5),
		VisibilityTimeout:    l.duration("VISIBILITY_TIMEOUT", 30*time.Second),
		SendingTimeout:       l.duration("SENDING_TIMEOUT", 2*time.Minute),
		ReconcileInterval:    l.duration("RECONCILE_INTERVAL", 10*time.Second),

		OperatorLatency:     l.duration("OPERATOR_LATENCY", 50*time.Millisecond),
		OperatorFailureRate: l.float("OPERATOR_FAILURE_RATE", 0.02),
		OperatorTimeout:     l.duration("OPERATOR_TIMEOUT", 5*time.Second),
	}
	if l.err != nil {
		return Config{}, l.err
	}
	return cfg, cfg.validate()
}

func (c Config) validate() error {
	var errs []error
	check := func(ok bool, msg string) {
		if !ok {
			errs = append(errs, errors.New(msg))
		}
	}
	check(c.PriceNormal > 0 && c.PriceExpress > 0, "prices must be positive")
	check(c.MaxBulkSize > 0, "MAX_BULK_SIZE must be positive")
	check(c.ExpressSLA > 0, "EXPRESS_SLA must be positive")
	check(c.ExpressRatePerCustomer > 0, "EXPRESS_RATE_PER_CUSTOMER must be positive")
	check(c.ExpressMaxBacklog > 0, "EXPRESS_MAX_BACKLOG must be positive")
	check(c.ExpressWorkers > 0 && c.NormalWorkers > 0, "worker counts must be positive")
	check(c.FairQuantum > 0, "FAIR_QUANTUM must be positive")
	check(c.RelayBatchSize > 0, "RELAY_BATCH_SIZE must be positive")
	check(c.MaxAttempts > 0, "MAX_ATTEMPTS must be positive")
	check(c.OperatorFailureRate >= 0 && c.OperatorFailureRate <= 1, "OPERATOR_FAILURE_RATE must be between 0 and 1")
	check(c.SendingTimeout > c.OperatorTimeout, "SENDING_TIMEOUT must be longer than OPERATOR_TIMEOUT")
	return errors.Join(errs...)
}

// loader collects parse errors so all bad variables are reported at once.
type loader struct{ err error }

func (l *loader) fail(key, raw string, err error) {
	l.err = errors.Join(l.err, fmt.Errorf("invalid %s=%q: %w", key, raw, err))
}

func (l *loader) str(key, def string) string {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		return v
	}
	return def
}

func (l *loader) int(key string, def int) int {
	raw := l.str(key, "")
	if raw == "" {
		return def
	}
	v, err := strconv.Atoi(raw)
	if err != nil {
		l.fail(key, raw, err)
	}
	return v
}

func (l *loader) int64(key string, def int64) int64 {
	raw := l.str(key, "")
	if raw == "" {
		return def
	}
	v, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		l.fail(key, raw, err)
	}
	return v
}

func (l *loader) float(key string, def float64) float64 {
	raw := l.str(key, "")
	if raw == "" {
		return def
	}
	v, err := strconv.ParseFloat(raw, 64)
	if err != nil {
		l.fail(key, raw, err)
	}
	return v
}

func (l *loader) duration(key string, def time.Duration) time.Duration {
	raw := l.str(key, "")
	if raw == "" {
		return def
	}
	v, err := time.ParseDuration(raw)
	if err != nil {
		l.fail(key, raw, err)
	}
	return v
}
