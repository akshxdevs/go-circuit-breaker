# Project go-circuit-breaker

HTTP API demo that includes a connection-pooled outbound client protected by a circuit breaker.
It is built to make three behaviors observable:

- Queueing when the connection pool is exhausted
- Connection reuse vs connection churn
- Failure detection and circuit breaker fast-fail

## Getting Started

These instructions will get you a copy of the project up and running on your local machine for development and testing purposes. See deployment for notes on how to deploy the project on a live system.

## MakeFile

Run build make command with tests
```bash
make all
```

Build the application
```bash
make build
```

Run the application
```bash
make run
```

Live reload the application:
```bash
make watch
```

Run the test suite:
```bash
make test
```

Clean up binary from the last build:
```bash
make clean
```

## Circuit-breaker demo routes

Start the API:

```bash
make run
```

Reset client/breaker metrics:

```bash
curl -X POST http://localhost:8080/demo/reset
```

Run concurrent outbound calls through the pooled client:

```bash
curl \"http://localhost:8080/demo/load?total=40&concurrency=20&delay_ms=250&fail=never\"
```

Observe queueing and connection reuse metrics:

```bash
curl http://localhost:8080/demo/stats
```

Trigger failures and observe fast-fail:

```bash
curl \"http://localhost:8080/demo/load?total=30&concurrency=10&delay_ms=120&fail=always&failure_status=503\"
curl http://localhost:8080/demo/stats
```

Useful query parameters for `/demo/load`:

- `total` total outbound requests (default `20`)
- `concurrency` worker count (default `10`)
- `delay_ms` upstream delay per request (default `250`)
- `fail` one of `never|always|flaky` (default `never`)
- `fail_rate` used when `fail=flaky` (default `0.3`)
- `failure_status` HTTP status when failing (default `503`)
