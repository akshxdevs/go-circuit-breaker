package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"go-circuit-breaker/internal/server"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	apiServer := server.NewServer()
	shutdownTimeout := envDurationSeconds("SHUTDOWN_TIMEOUT_SEC", 10)

	serverErrCh := make(chan error, 1)
	go func() {
		log.Printf("http server starting on %s", apiServer.Addr)
		err := apiServer.ListenAndServe()
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			serverErrCh <- fmt.Errorf("http server failed: %w", err)
			return
		}
		serverErrCh <- nil
	}()

	signalCh := make(chan os.Signal, 2)
	signal.Notify(signalCh, syscall.SIGINT, syscall.SIGTERM)
	defer signal.Stop(signalCh)

	select {
	case err := <-serverErrCh:
		if err != nil {
			return err
		}
		log.Println("http server stopped")
		return nil
	case sig := <-signalCh:
		log.Printf("received %s: starting graceful shutdown (timeout=%s)", sig.String(), shutdownTimeout)
	}

	apiServer.SetKeepAlivesEnabled(false)

	shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()
	shutdownErrCh := make(chan error, 1)
	go func() {
		shutdownErrCh <- apiServer.Shutdown(shutdownCtx)
	}()

	select {
	case sig := <-signalCh:
		log.Printf("received %s during shutdown: forcing immediate close", sig.String())
		if err := apiServer.Close(); err != nil {
			return fmt.Errorf("force close failed: %w", err)
		}
	case err := <-shutdownErrCh:
		if err != nil {
			return fmt.Errorf("graceful shutdown failed: %w", err)
		}
		log.Println("graceful shutdown complete")
	}

	// Ensure server goroutine has exited before returning.
	if err := <-serverErrCh; err != nil {
		return err
	}
	return nil
}

func envDurationSeconds(name string, fallbackSeconds int) time.Duration {
	raw := os.Getenv(name)
	if raw == "" {
		return time.Duration(fallbackSeconds) * time.Second
	}

	seconds, err := strconv.Atoi(raw)
	if err != nil || seconds <= 0 {
		return time.Duration(fallbackSeconds) * time.Second
	}
	return time.Duration(seconds) * time.Second
}
