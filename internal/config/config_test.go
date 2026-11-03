package config

import (
	"strings"
	"testing"
	"time"
)

func TestLoadDefaults(t *testing.T) {
	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() with no env vars: %v", err)
	}
	if cfg.ExpressSLA != 5*time.Second {
		t.Errorf("ExpressSLA = %v, want 5s", cfg.ExpressSLA)
	}
	if cfg.PriceExpress <= cfg.PriceNormal {
		t.Errorf("express price %d should be higher than normal price %d", cfg.PriceExpress, cfg.PriceNormal)
	}
}

func TestLoadFromEnv(t *testing.T) {
	t.Setenv("EXPRESS_SLA", "3s")
	t.Setenv("NORMAL_WORKERS", "7")

	cfg, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.ExpressSLA != 3*time.Second || cfg.NormalWorkers != 7 {
		t.Errorf("got ExpressSLA=%v NormalWorkers=%d", cfg.ExpressSLA, cfg.NormalWorkers)
	}
}

func TestLoadReportsAllInvalidValues(t *testing.T) {
	t.Setenv("EXPRESS_SLA", "soon")
	t.Setenv("NORMAL_WORKERS", "many")

	_, err := Load()
	if err == nil {
		t.Fatal("expected an error")
	}
	for _, key := range []string{"EXPRESS_SLA", "NORMAL_WORKERS"} {
		if !strings.Contains(err.Error(), key) {
			t.Errorf("error %q does not mention %s", err, key)
		}
	}
}

func TestValidateRejectsBadValues(t *testing.T) {
	t.Setenv("OPERATOR_FAILURE_RATE", "1.5")

	if _, err := Load(); err == nil {
		t.Fatal("expected validation error for failure rate above 1")
	}
}
