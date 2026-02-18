package httpclient

import (
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

func TestClient_QueueingAndReuseWithPoolExhaustion(t *testing.T) {
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(120 * time.Millisecond)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	}))
	defer target.Close()

	client := NewClient(Config{
		Timeout:             3 * time.Second,
		MaxIdleConns:        10,
		MaxIdleConnsPerHost: 1,
		MaxConnsPerHost:     1,
		FailureThreshold:    5,
		OpenTimeout:         time.Second,
		HalfOpenMaxProbes:   1,
	})

	const total = 8
	var wg sync.WaitGroup
	for i := 0; i < total; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			req, _ := http.NewRequest(http.MethodGet, target.URL, nil)
			resp, err := client.Do(req)
			if err != nil {
				t.Errorf("unexpected request error: %v", err)
				return
			}
			_, _ = io.Copy(io.Discard, resp.Body)
			_ = resp.Body.Close()
		}()
	}
	wg.Wait()

	s := client.Snapshot()
	if s.QueueSamples == 0 {
		t.Fatalf("expected queue wait samples > 0")
	}
	if s.QueueWaitMaxMs <= 0 {
		t.Fatalf("expected queue wait max > 0, got %f", s.QueueWaitMaxMs)
	}
	if s.ConnReused == 0 {
		t.Fatalf("expected at least one reused connection")
	}
}

func TestClient_CircuitFastFail(t *testing.T) {
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte("fail"))
	}))
	defer target.Close()

	client := NewClient(Config{
		Timeout:             2 * time.Second,
		MaxIdleConns:        2,
		MaxIdleConnsPerHost: 1,
		MaxConnsPerHost:     1,
		FailureThreshold:    1,
		OpenTimeout:         10 * time.Second,
		HalfOpenMaxProbes:   1,
	})

	req1, _ := http.NewRequest(http.MethodGet, target.URL, nil)
	resp, err := client.Do(req1)
	if err != nil {
		t.Fatalf("expected first request to complete with HTTP response, got err: %v", err)
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	_ = resp.Body.Close()

	req2, _ := http.NewRequest(http.MethodGet, target.URL, nil)
	start := time.Now()
	_, err = client.Do(req2)
	elapsed := time.Since(start)
	if err != ErrCircuitOpen {
		t.Fatalf("expected second request to fast-fail with open breaker, got: %v", err)
	}
	if elapsed > 50*time.Millisecond {
		t.Fatalf("expected fast fail quickly, took %s", elapsed)
	}

	s := client.Snapshot()
	if s.RequestsRejected == 0 {
		t.Fatalf("expected rejected requests metric to increase")
	}
}
