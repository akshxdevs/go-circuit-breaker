package main

import (
	"testing"
	"time"
)

func TestEnvDurationSeconds(t *testing.T) {
	t.Setenv("SHUTDOWN_TIMEOUT_SEC", "")
	if got := envDurationSeconds("SHUTDOWN_TIMEOUT_SEC", 10); got != 10*time.Second {
		t.Fatalf("expected fallback duration, got %s", got)
	}

	t.Setenv("SHUTDOWN_TIMEOUT_SEC", "25")
	if got := envDurationSeconds("SHUTDOWN_TIMEOUT_SEC", 10); got != 25*time.Second {
		t.Fatalf("expected parsed duration, got %s", got)
	}

	t.Setenv("SHUTDOWN_TIMEOUT_SEC", "abc")
	if got := envDurationSeconds("SHUTDOWN_TIMEOUT_SEC", 10); got != 10*time.Second {
		t.Fatalf("expected fallback on parse error, got %s", got)
	}

	t.Setenv("SHUTDOWN_TIMEOUT_SEC", "0")
	if got := envDurationSeconds("SHUTDOWN_TIMEOUT_SEC", 10); got != 10*time.Second {
		t.Fatalf("expected fallback for non-positive value, got %s", got)
	}
}
