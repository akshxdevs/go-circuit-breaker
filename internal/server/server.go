package server

import (
	"fmt"
	"math/rand"
	"net/http"
	"os"
	"strconv"
	"sync"
	"time"

	"go-circuit-breaker/internal/httpclient"

	_ "github.com/joho/godotenv/autoload"
)

type Server struct {
	port       int
	httpClient *httpclient.Client
	random     *rand.Rand
	randomMu   sync.Mutex
	randomSeed int64
}

func NewServer() *http.Server {
	port, _ := strconv.Atoi(os.Getenv("PORT"))
	if port == 0 {
		port = 8080
	}

	clientCfg := httpclient.Config{
		Timeout:              envDurationMS("CLIENT_TIMEOUT_MS", 4000),
		MaxIdleConns:         envInt("CLIENT_MAX_IDLE_CONNS", 10),
		MaxIdleConnsPerHost:  envInt("CLIENT_MAX_IDLE_CONNS_PER_HOST", 2),
		MaxConnsPerHost:      envInt("CLIENT_MAX_CONNS_PER_HOST", 2),
		IdleConnTimeout:      envDurationMS("CLIENT_IDLE_TIMEOUT_MS", 60000),
		TLSHandshakeTimeout:  envDurationMS("CLIENT_TLS_HANDSHAKE_TIMEOUT_MS", 10000),
		ExpectContinueTimout: envDurationMS("CLIENT_EXPECT_CONTINUE_TIMEOUT_MS", 1000),
		FailureThreshold:     envInt("CB_FAILURE_THRESHOLD", 3),
		OpenTimeout:          envDurationMS("CB_OPEN_TIMEOUT_MS", 3000),
		HalfOpenMaxProbes:    envInt("CB_HALF_OPEN_MAX_PROBES", 2),
	}

	seed := time.Now().UnixNano()
	NewServer := &Server{
		port:       port,
		httpClient: httpclient.NewClient(clientCfg),
		random:     rand.New(rand.NewSource(seed)),
		randomSeed: seed,
	}

	// Declare Server config
	server := &http.Server{
		Addr:         fmt.Sprintf(":%d", NewServer.port),
		Handler:      NewServer.RegisterRoutes(),
		IdleTimeout:  time.Minute,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 30 * time.Second,
	}

	return server
}

func envInt(name string, fallback int) int {
	raw := os.Getenv(name)
	if raw == "" {
		return fallback
	}
	v, err := strconv.Atoi(raw)
	if err != nil {
		return fallback
	}
	return v
}

func envDurationMS(name string, fallbackMs int) time.Duration {
	return time.Duration(envInt(name, fallbackMs)) * time.Millisecond
}
