package httpclient

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/httptrace"
	"sync/atomic"
	"time"
)

type Config struct {
	Timeout              time.Duration
	MaxIdleConns         int
	MaxIdleConnsPerHost  int
	MaxConnsPerHost      int
	IdleConnTimeout      time.Duration
	TLSHandshakeTimeout  time.Duration
	ExpectContinueTimout time.Duration

	FailureThreshold  int
	OpenTimeout       time.Duration
	HalfOpenMaxProbes int
}

type Stats struct {
	RequestsTotal    int64   `json:"requests_total"`
	RequestsSuccess  int64   `json:"requests_success"`
	RequestsFailure  int64   `json:"requests_failure"`
	RequestsRejected int64   `json:"requests_rejected"`
	InFlight         int64   `json:"in_flight"`
	QueueSamples     int64   `json:"queue_samples"`
	QueueWaitTotalMs float64 `json:"queue_wait_total_ms"`
	QueueWaitMaxMs   float64 `json:"queue_wait_max_ms"`
	QueueWaitAvgMs   float64 `json:"queue_wait_avg_ms"`
	ConnNew          int64   `json:"conn_new"`
	ConnReused       int64   `json:"conn_reused"`
	ConnIdleReused   int64   `json:"conn_idle_reused"`
	Breaker          BreakerStats
}

type Client struct {
	client  *http.Client
	breaker *CircuitBreaker

	requestsTotal    int64
	requestsSuccess  int64
	requestsFailure  int64
	requestsRejected int64
	inFlight         int64
	queueSamples     int64
	queueWaitTotalNs int64
	queueWaitMaxNs   int64
	connNew          int64
	connReused       int64
	connIdleReused   int64
}

func NewClient(cfg Config) *Client {
	if cfg.Timeout <= 0 {
		cfg.Timeout = 5 * time.Second
	}
	if cfg.MaxIdleConns <= 0 {
		cfg.MaxIdleConns = 100
	}
	if cfg.MaxIdleConnsPerHost <= 0 {
		cfg.MaxIdleConnsPerHost = 2
	}
	if cfg.MaxConnsPerHost <= 0 {
		cfg.MaxConnsPerHost = 2
	}
	if cfg.IdleConnTimeout <= 0 {
		cfg.IdleConnTimeout = 90 * time.Second
	}
	if cfg.TLSHandshakeTimeout <= 0 {
		cfg.TLSHandshakeTimeout = 10 * time.Second
	}
	if cfg.ExpectContinueTimout <= 0 {
		cfg.ExpectContinueTimout = time.Second
	}

	transport := &http.Transport{
		Proxy:                 http.ProxyFromEnvironment,
		DialContext:           (&net.Dialer{Timeout: 30 * time.Second, KeepAlive: 30 * time.Second}).DialContext,
		ForceAttemptHTTP2:     true,
		MaxIdleConns:          cfg.MaxIdleConns,
		MaxIdleConnsPerHost:   cfg.MaxIdleConnsPerHost,
		MaxConnsPerHost:       cfg.MaxConnsPerHost,
		IdleConnTimeout:       cfg.IdleConnTimeout,
		TLSHandshakeTimeout:   cfg.TLSHandshakeTimeout,
		ExpectContinueTimeout: cfg.ExpectContinueTimout,
	}

	return &Client{
		client: &http.Client{
			Transport: transport,
			Timeout:   cfg.Timeout,
		},
		breaker: NewCircuitBreaker(BreakerConfig{
			FailureThreshold:  cfg.FailureThreshold,
			OpenTimeout:       cfg.OpenTimeout,
			HalfOpenMaxProbes: cfg.HalfOpenMaxProbes,
		}),
	}
}

func (c *Client) Do(req *http.Request) (*http.Response, error) {
	now := time.Now()
	if err := c.breaker.Allow(now); err != nil {
		atomic.AddInt64(&c.requestsRejected, 1)
		return nil, err
	}

	atomic.AddInt64(&c.requestsTotal, 1)
	atomic.AddInt64(&c.inFlight, 1)
	defer atomic.AddInt64(&c.inFlight, -1)

	var getConnAt time.Time
	var gotConn bool

	trace := &httptrace.ClientTrace{
		GetConn: func(string) {
			getConnAt = time.Now()
		},
		GotConn: func(info httptrace.GotConnInfo) {
			gotConn = true
			if !getConnAt.IsZero() {
				wait := time.Since(getConnAt)
				atomic.AddInt64(&c.queueSamples, 1)
				atomic.AddInt64(&c.queueWaitTotalNs, wait.Nanoseconds())
				c.updateQueueMax(wait.Nanoseconds())
			}
			if info.Reused {
				atomic.AddInt64(&c.connReused, 1)
				if info.WasIdle {
					atomic.AddInt64(&c.connIdleReused, 1)
				}
				return
			}
			atomic.AddInt64(&c.connNew, 1)
		},
	}

	ctx := httptrace.WithClientTrace(req.Context(), trace)
	callReq := req.Clone(ctx)

	resp, err := c.client.Do(callReq)
	success := isSuccessful(resp, err)
	c.breaker.OnResult(success, time.Now())
	if !gotConn && errors.Is(err, context.DeadlineExceeded) {
		// Timeout before a connection was acquired is strong signal of queue pressure.
		atomic.AddInt64(&c.queueSamples, 1)
	}

	if success {
		atomic.AddInt64(&c.requestsSuccess, 1)
		return resp, nil
	}

	atomic.AddInt64(&c.requestsFailure, 1)
	return resp, err
}

func (c *Client) Snapshot() Stats {
	totalNs := atomic.LoadInt64(&c.queueWaitTotalNs)
	samples := atomic.LoadInt64(&c.queueSamples)
	avgMs := 0.0
	if samples > 0 {
		avgMs = float64(totalNs) / float64(samples) / float64(time.Millisecond)
	}

	return Stats{
		RequestsTotal:    atomic.LoadInt64(&c.requestsTotal),
		RequestsSuccess:  atomic.LoadInt64(&c.requestsSuccess),
		RequestsFailure:  atomic.LoadInt64(&c.requestsFailure),
		RequestsRejected: atomic.LoadInt64(&c.requestsRejected),
		InFlight:         atomic.LoadInt64(&c.inFlight),
		QueueSamples:     samples,
		QueueWaitTotalMs: float64(totalNs) / float64(time.Millisecond),
		QueueWaitMaxMs:   float64(atomic.LoadInt64(&c.queueWaitMaxNs)) / float64(time.Millisecond),
		QueueWaitAvgMs:   avgMs,
		ConnNew:          atomic.LoadInt64(&c.connNew),
		ConnReused:       atomic.LoadInt64(&c.connReused),
		ConnIdleReused:   atomic.LoadInt64(&c.connIdleReused),
		Breaker:          c.breaker.Snapshot(),
	}
}

func (c *Client) Reset() {
	atomic.StoreInt64(&c.requestsTotal, 0)
	atomic.StoreInt64(&c.requestsSuccess, 0)
	atomic.StoreInt64(&c.requestsFailure, 0)
	atomic.StoreInt64(&c.requestsRejected, 0)
	atomic.StoreInt64(&c.inFlight, 0)
	atomic.StoreInt64(&c.queueSamples, 0)
	atomic.StoreInt64(&c.queueWaitTotalNs, 0)
	atomic.StoreInt64(&c.queueWaitMaxNs, 0)
	atomic.StoreInt64(&c.connNew, 0)
	atomic.StoreInt64(&c.connReused, 0)
	atomic.StoreInt64(&c.connIdleReused, 0)
	c.breaker.Reset()
}

func (c *Client) updateQueueMax(waitNs int64) {
	for {
		current := atomic.LoadInt64(&c.queueWaitMaxNs)
		if waitNs <= current {
			return
		}
		if atomic.CompareAndSwapInt64(&c.queueWaitMaxNs, current, waitNs) {
			return
		}
	}
}

func isSuccessful(resp *http.Response, err error) bool {
	if err != nil {
		return false
	}
	if resp == nil {
		return false
	}
	return resp.StatusCode < http.StatusInternalServerError
}
