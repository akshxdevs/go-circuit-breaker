package httpclient

import (
	"errors"
	"sync"
	"time"
)

var ErrCircuitOpen = errors.New("circuit breaker is open")

type breakerState string

const (
	stateClosed   breakerState = "closed"
	stateOpen     breakerState = "open"
	stateHalfOpen breakerState = "half_open"
)

type BreakerConfig struct {
	FailureThreshold  int
	OpenTimeout       time.Duration
	HalfOpenMaxProbes int
}

type BreakerStats struct {
	State             string `json:"state"`
	ConsecutiveFails  int    `json:"consecutive_fails"`
	Rejected          int64  `json:"rejected"`
	StateTransitions  int64  `json:"state_transitions"`
	TripCount         int64  `json:"trip_count"`
	HalfOpenInFlight  int    `json:"half_open_in_flight"`
	HalfOpenSuccesses int    `json:"half_open_successes"`
}

type CircuitBreaker struct {
	mu sync.Mutex

	cfg BreakerConfig

	state            breakerState
	openedAt         time.Time
	consecutiveFails int

	halfOpenInFlight  int
	halfOpenSuccesses int

	rejected         int64
	stateTransitions int64
	tripCount        int64
}

func NewCircuitBreaker(cfg BreakerConfig) *CircuitBreaker {
	if cfg.FailureThreshold <= 0 {
		cfg.FailureThreshold = 3
	}
	if cfg.OpenTimeout <= 0 {
		cfg.OpenTimeout = 3 * time.Second
	}
	if cfg.HalfOpenMaxProbes <= 0 {
		cfg.HalfOpenMaxProbes = 1
	}

	return &CircuitBreaker{
		cfg:   cfg,
		state: stateClosed,
	}
}

func (cb *CircuitBreaker) Allow(now time.Time) error {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	switch cb.state {
	case stateClosed:
		return nil
	case stateOpen:
		if now.Sub(cb.openedAt) >= cb.cfg.OpenTimeout {
			cb.toStateLocked(stateHalfOpen)
			cb.halfOpenInFlight = 1
			cb.halfOpenSuccesses = 0
			return nil
		}
		cb.rejected++
		return ErrCircuitOpen
	case stateHalfOpen:
		if cb.halfOpenInFlight >= cb.cfg.HalfOpenMaxProbes {
			cb.rejected++
			return ErrCircuitOpen
		}
		cb.halfOpenInFlight++
		return nil
	default:
		cb.rejected++
		return ErrCircuitOpen
	}
}

func (cb *CircuitBreaker) OnResult(success bool, now time.Time) {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	switch cb.state {
	case stateClosed:
		if success {
			cb.consecutiveFails = 0
			return
		}
		cb.consecutiveFails++
		if cb.consecutiveFails >= cb.cfg.FailureThreshold {
			cb.toOpenLocked(now)
		}
	case stateOpen:
		return
	case stateHalfOpen:
		if cb.halfOpenInFlight > 0 {
			cb.halfOpenInFlight--
		}

		if !success {
			cb.toOpenLocked(now)
			return
		}

		cb.halfOpenSuccesses++
		if cb.halfOpenSuccesses >= cb.cfg.HalfOpenMaxProbes {
			cb.toStateLocked(stateClosed)
			cb.consecutiveFails = 0
			cb.halfOpenInFlight = 0
			cb.halfOpenSuccesses = 0
		}
	}
}

func (cb *CircuitBreaker) Snapshot() BreakerStats {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	return BreakerStats{
		State:             string(cb.state),
		ConsecutiveFails:  cb.consecutiveFails,
		Rejected:          cb.rejected,
		StateTransitions:  cb.stateTransitions,
		TripCount:         cb.tripCount,
		HalfOpenInFlight:  cb.halfOpenInFlight,
		HalfOpenSuccesses: cb.halfOpenSuccesses,
	}
}

func (cb *CircuitBreaker) Reset() {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	cb.state = stateClosed
	cb.openedAt = time.Time{}
	cb.consecutiveFails = 0
	cb.halfOpenInFlight = 0
	cb.halfOpenSuccesses = 0
	cb.rejected = 0
	cb.stateTransitions = 0
	cb.tripCount = 0
}

func (cb *CircuitBreaker) toOpenLocked(now time.Time) {
	cb.toStateLocked(stateOpen)
	cb.openedAt = now
	cb.consecutiveFails = 0
	cb.halfOpenInFlight = 0
	cb.halfOpenSuccesses = 0
	cb.tripCount++
}

func (cb *CircuitBreaker) toStateLocked(newState breakerState) {
	if cb.state != newState {
		cb.stateTransitions++
	}
	cb.state = newState
}
