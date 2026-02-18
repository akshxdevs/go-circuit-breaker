package httpclient

import (
	"testing"
	"time"
)

func TestCircuitBreaker_OpensAndHalfOpenRecovery(t *testing.T) {
	cb := NewCircuitBreaker(BreakerConfig{
		FailureThreshold:  2,
		OpenTimeout:       50 * time.Millisecond,
		HalfOpenMaxProbes: 2,
	})
	now := time.Now()

	if err := cb.Allow(now); err != nil {
		t.Fatalf("expected closed breaker to allow request: %v", err)
	}
	cb.OnResult(false, now)

	if err := cb.Allow(now); err != nil {
		t.Fatalf("expected closed breaker to allow second request: %v", err)
	}
	cb.OnResult(false, now)

	if err := cb.Allow(now); err != ErrCircuitOpen {
		t.Fatalf("expected open breaker to reject request, got: %v", err)
	}

	next := now.Add(60 * time.Millisecond)
	if err := cb.Allow(next); err != nil {
		t.Fatalf("expected probe in half-open state, got: %v", err)
	}
	cb.OnResult(true, next)

	if err := cb.Allow(next); err != nil {
		t.Fatalf("expected second probe in half-open state, got: %v", err)
	}
	cb.OnResult(true, next)

	if err := cb.Allow(next); err != nil {
		t.Fatalf("expected breaker to close after successful probes, got: %v", err)
	}
}

func TestCircuitBreaker_HalfOpenFailureTripsBackOpen(t *testing.T) {
	cb := NewCircuitBreaker(BreakerConfig{
		FailureThreshold:  1,
		OpenTimeout:       20 * time.Millisecond,
		HalfOpenMaxProbes: 1,
	})
	now := time.Now()

	if err := cb.Allow(now); err != nil {
		t.Fatalf("expected request to pass in closed state: %v", err)
	}
	cb.OnResult(false, now)

	if err := cb.Allow(now.Add(30 * time.Millisecond)); err != nil {
		t.Fatalf("expected half-open probe to be allowed: %v", err)
	}
	cb.OnResult(false, now.Add(30*time.Millisecond))

	if err := cb.Allow(now.Add(35 * time.Millisecond)); err != ErrCircuitOpen {
		t.Fatalf("expected breaker to reopen after failed probe, got: %v", err)
	}
}
