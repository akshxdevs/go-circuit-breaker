# go-circuit-breaker

Small Go HTTP API that demonstrates connection-pool pressure, connection reuse, and circuit-breaker fast-fail behavior with an outbound `http.Client`.

## Overview

The service exposes demo endpoints that call back into a simulated upstream handler using a pooled HTTP client wrapped in a circuit breaker. It is designed to make these behaviors easy to observe locally:

- Queueing when outbound concurrency exceeds `MaxConnsPerHost`
- Reuse of existing connections versus creation of new ones
- Circuit-breaker transitions from `closed` to `open` to `half_open`
- Fast rejection of calls while the breaker is open

## Tech Stack

- Go `1.25.6`
- Standard library `net/http`
- `github.com/joho/godotenv/autoload` for optional `.env` loading

## Project Structure

```text
.
├── cmd/api/main.go             # API entrypoint and graceful shutdown
├── internal/server/            # HTTP server setup and demo routes
└── internal/httpclient/        # Instrumented pooled client and circuit breaker
```

## Quick Start

Run the API locally:

```bash
make run
```

The server listens on `http://localhost:8080` by default.

Basic health check:

```bash
curl http://localhost:8080/
```

Run the full test suite:

```bash
make test
```

Build the binary:

```bash
make build
```

## Configuration

The server auto-loads a local `.env` file if present. All settings are optional.

| Variable | Default | Purpose |
| --- | --- | --- |
| `PORT` | `8080` | HTTP server port |
| `SHUTDOWN_TIMEOUT_SEC` | `10` | Graceful shutdown timeout |
| `CLIENT_TIMEOUT_MS` | `4000` | Total timeout for outbound requests |
| `CLIENT_MAX_IDLE_CONNS` | `10` | Total idle connections in the transport |
| `CLIENT_MAX_IDLE_CONNS_PER_HOST` | `2` | Idle connections kept per host |
| `CLIENT_MAX_CONNS_PER_HOST` | `2` | Hard cap on concurrent connections per host |
| `CLIENT_IDLE_TIMEOUT_MS` | `60000` | Idle connection timeout |
| `CLIENT_TLS_HANDSHAKE_TIMEOUT_MS` | `10000` | TLS handshake timeout |
| `CLIENT_EXPECT_CONTINUE_TIMEOUT_MS` | `1000` | `Expect: 100-continue` timeout |
| `CB_FAILURE_THRESHOLD` | `3` | Consecutive failures required to open the breaker |
| `CB_OPEN_TIMEOUT_MS` | `3000` | Time spent open before allowing half-open probes |
| `CB_HALF_OPEN_MAX_PROBES` | `2` | Successful probes required to close the breaker |

## Demo Routes

| Method | Path | Purpose |
| --- | --- | --- |
| `GET` | `/` | Returns `{"message":"Hello World"}` |
| `GET` | `/demo/upstream` | Simulated upstream with configurable delay and failure mode |
| `GET` | `/demo/load` | Fires concurrent outbound calls through the pooled client |
| `GET` | `/demo/stats` | Returns current client and breaker metrics |
| `POST` | `/demo/reset` | Resets client and breaker metrics |

`/demo/upstream` and `/demo/load` support these query parameters:

| Parameter | Default | Notes |
| --- | --- | --- |
| `delay_ms` | `100` on `/demo/upstream`, `250` on `/demo/load` | Simulated upstream latency |
| `fail` | `never` | One of `never`, `always`, `flaky` |
| `fail_rate` | `0.0` on `/demo/upstream`, `0.3` on `/demo/load` | Used only when `fail=flaky` |
| `failure_status` | `503` | HTTP status returned on failure |
| `total` | `20` on `/demo/load` | Number of outbound requests to issue |
| `concurrency` | `10` on `/demo/load` | Number of workers issuing requests |

## Example Workflow

Reset metrics:

```bash
curl -X POST http://localhost:8080/demo/reset
```

Generate contention in the outbound connection pool:

```bash
curl "http://localhost:8080/demo/load?total=40&concurrency=20&delay_ms=250&fail=never"
```

Inspect queue wait and connection reuse metrics:

```bash
curl http://localhost:8080/demo/stats
```

Trip the circuit breaker and observe fast-fail behavior:

```bash
curl "http://localhost:8080/demo/load?total=30&concurrency=10&delay_ms=120&fail=always&failure_status=503"
curl http://localhost:8080/demo/stats
```

## Architecture

`cmd/api/main.go` starts the HTTP server, handles `SIGINT` and `SIGTERM`, and performs graceful shutdown using `SHUTDOWN_TIMEOUT_SEC`.

`internal/server` registers the demo routes. `/demo/load` issues concurrent requests against `/demo/upstream` on the same server so the connection-pool and breaker behavior can be exercised without external dependencies.

`internal/httpclient` provides:

- An `http.Transport` with explicit pool limits such as `MaxConnsPerHost`
- Request instrumentation for queue wait time and connection reuse via `httptrace`
- A circuit breaker with `closed`, `open`, and `half_open` states

## Development Commands

```bash
make all    # build and test
make build  # compile cmd/api/main.go to ./main
make run    # start the API
make test   # run go test ./... -v
make clean  # remove ./main
make watch  # run with air if installed, or prompt to install it
```
