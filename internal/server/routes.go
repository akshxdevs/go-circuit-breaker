package server

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"strconv"
	"sync"
	"time"

	"go-circuit-breaker/internal/httpclient"
)

func (s *Server) RegisterRoutes() http.Handler {
	mux := http.NewServeMux()

	// Register routes
	mux.HandleFunc("/", s.HelloWorldHandler)
	mux.HandleFunc("/demo/upstream", s.UpstreamSimulationHandler)
	mux.HandleFunc("/demo/load", s.LoadClientHandler)
	mux.HandleFunc("/demo/stats", s.ClientStatsHandler)
	mux.HandleFunc("/demo/reset", s.ResetClientHandler)

	// Wrap the mux with CORS middleware
	return s.corsMiddleware(mux)
}

func (s *Server) corsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Set CORS headers
		w.Header().Set("Access-Control-Allow-Origin", "*") // Replace "*" with specific origins if needed
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS, PATCH")
		w.Header().Set("Access-Control-Allow-Headers", "Accept, Authorization, Content-Type, X-CSRF-Token")
		w.Header().Set("Access-Control-Allow-Credentials", "false") // Set to "true" if credentials are required

		// Handle preflight OPTIONS requests
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}

		// Proceed with the next handler
		next.ServeHTTP(w, r)
	})
}

func (s *Server) HelloWorldHandler(w http.ResponseWriter, r *http.Request) {
	resp := map[string]string{"message": "Hello World"}
	writeJSON(w, http.StatusOK, resp)
}

func (s *Server) UpstreamSimulationHandler(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	delayMS := queryInt(q, "delay_ms", 100)
	failMode := q.Get("fail")
	if failMode == "" {
		failMode = "never"
	}
	failRate := queryFloat(q, "fail_rate", 0.0)
	failureStatus := queryInt(q, "failure_status", http.StatusServiceUnavailable)

	time.Sleep(time.Duration(delayMS) * time.Millisecond)

	fail := false
	switch failMode {
	case "always":
		fail = true
	case "flaky":
		s.randomMu.Lock()
		fail = s.random.Float64() < failRate
		s.randomMu.Unlock()
	case "never":
		fail = false
	default:
		writeJSON(w, http.StatusBadRequest, map[string]string{
			"error": "invalid fail mode; expected never|always|flaky",
		})
		return
	}

	if fail {
		writeJSON(w, failureStatus, map[string]any{
			"ok":             false,
			"delay_ms":       delayMS,
			"fail_mode":      failMode,
			"failure_status": failureStatus,
		})
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"ok":        true,
		"delay_ms":  delayMS,
		"fail_mode": failMode,
	})
}

func (s *Server) LoadClientHandler(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	total := queryInt(q, "total", 20)
	if total <= 0 {
		total = 1
	}
	concurrency := queryInt(q, "concurrency", 10)
	if concurrency <= 0 {
		concurrency = 1
	}
	delayMS := queryInt(q, "delay_ms", 250)
	failMode := q.Get("fail")
	if failMode == "" {
		failMode = "never"
	}
	failRate := queryFloat(q, "fail_rate", 0.3)
	failureStatus := queryInt(q, "failure_status", http.StatusServiceUnavailable)

	target := fmt.Sprintf(
		"http://%s/demo/upstream?delay_ms=%d&fail=%s&fail_rate=%g&failure_status=%d",
		r.Host,
		delayMS,
		url.QueryEscape(failMode),
		failRate,
		failureStatus,
	)

	start := time.Now()
	type runStats struct {
		success  int
		failure  int
		fastFail int
	}

	var (
		wg sync.WaitGroup
		mu sync.Mutex
		rs runStats
	)

	jobs := make(chan struct{}, total)
	for i := 0; i < total; i++ {
		jobs <- struct{}{}
	}
	close(jobs)

	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range jobs {
				req, err := http.NewRequestWithContext(r.Context(), http.MethodGet, target, nil)
				if err != nil {
					mu.Lock()
					rs.failure++
					mu.Unlock()
					continue
				}

				resp, err := s.httpClient.Do(req)
				if errors.Is(err, httpclient.ErrCircuitOpen) {
					mu.Lock()
					rs.fastFail++
					mu.Unlock()
					continue
				}

				if err != nil {
					mu.Lock()
					rs.failure++
					mu.Unlock()
					continue
				}

				_, _ = io.Copy(io.Discard, resp.Body)
				_ = resp.Body.Close()
				if resp.StatusCode >= http.StatusInternalServerError {
					mu.Lock()
					rs.failure++
					mu.Unlock()
					continue
				}

				mu.Lock()
				rs.success++
				mu.Unlock()
			}
		}()
	}

	wg.Wait()
	elapsed := time.Since(start)

	writeJSON(w, http.StatusOK, map[string]any{
		"run": map[string]any{
			"total":            total,
			"concurrency":      concurrency,
			"delay_ms":         delayMS,
			"fail_mode":        failMode,
			"fail_rate":        failRate,
			"failure_status":   failureStatus,
			"duration_ms":      elapsed.Milliseconds(),
			"successful_calls": rs.success,
			"failed_calls":     rs.failure,
			"fast_fail_calls":  rs.fastFail,
			"target":           target,
		},
		"client": s.httpClient.Snapshot(),
	})
}

func (s *Server) ClientStatsHandler(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{
		"client":      s.httpClient.Snapshot(),
		"random_seed": s.randomSeed,
	})
}

func (s *Server) ResetClientHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]string{
			"error": "use POST",
		})
		return
	}

	s.httpClient.Reset()
	writeJSON(w, http.StatusOK, map[string]string{"status": "reset"})
}

func writeJSON(w http.ResponseWriter, status int, payload any) {
	jsonResp, err := json.Marshal(payload)
	if err != nil {
		http.Error(w, "Failed to marshal response", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if _, err := w.Write(jsonResp); err != nil {
		log.Printf("Failed to write response: %v", err)
	}
}

func queryInt(q url.Values, key string, fallback int) int {
	raw := q.Get(key)
	if raw == "" {
		return fallback
	}
	v, err := strconv.Atoi(raw)
	if err != nil {
		return fallback
	}
	return v
}

func queryFloat(q url.Values, key string, fallback float64) float64 {
	raw := q.Get(key)
	if raw == "" {
		return fallback
	}
	v, err := strconv.ParseFloat(raw, 64)
	if err != nil {
		return fallback
	}
	return v
}
