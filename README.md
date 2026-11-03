# SMS Gateway

A simple SMS gateway written in Go. Businesses send SMS through a REST API,
pay from a prepaid balance, and read reports of what they sent. Messages are
handed over to a mobile operator (mocked here) by background workers.

It is built around four requirements:

| Requirement | How it is met |
|---|---|
| ~100M SMS/day, very uneven per customer | Per-customer queues served **round-robin**, so a customer with a huge backlog cannot delay others |
| **Express SMS** with a guaranteed hand-over time | Separate express lane with its own workers, plus **admission control**: we refuse what we cannot deliver in time instead of breaking the promise |
| Use the whole balance, never more | Check-and-debit is **one atomic SQL statement**; balance can reach exactly 0, never below. Every change is in a ledger |
| Reports | Paginated message list with filters, single message status, summary per status, balance ledger |

Read the design: **[docs/architecture.md](docs/architecture.md)**.
Why each choice was made: **[docs/decisions.md](docs/decisions.md)**.
Endpoints: **[docs/api.md](docs/api.md)**.

## Quick start

You need Docker (with Compose) and, for the tests and demo, Go 1.26+.

```bash
make up          # postgres, redis, api (port 8080) and 2 workers
```

Create a customer, add credit and send messages:

```bash
curl -s -X POST localhost:8080/v1/customers -d '{"name":"Acme"}'
# {"id":1,"name":"Acme","balance":0,...}

curl -s -X POST localhost:8080/v1/customers/1/topup -d '{"amount":10000}'

# normal SMS (price 100)
curl -s -X POST localhost:8080/v1/customers/1/messages \
  -d '{"to":"09121234567","text":"Your order has shipped"}'

# express SMS (price 150), e.g. an OTP; safe to retry thanks to Idempotency-Key
curl -s -X POST localhost:8080/v1/customers/1/messages \
  -H 'Idempotency-Key: login-otp-8812' \
  -d '{"to":"09121234567","text":"کد ورود شما: 4821","express":true}'

# up to 1000 messages in one all-or-nothing request
curl -s -X POST localhost:8080/v1/customers/1/messages/bulk \
  -d '{"messages":[{"to":"09121111111","text":"Sale!"},{"to":"09122222222","text":"Sale!"}]}'
```

Reports:

```bash
curl -s 'localhost:8080/v1/customers/1/messages?status=sent&limit=50'   # newest first, paginated
curl -s  localhost:8080/v1/customers/1/messages/1                       # one message
curl -s  localhost:8080/v1/customers/1/messages/summary                 # counts per status, last 24h
curl -s  localhost:8080/v1/customers/1/transactions                     # balance ledger
```

Numbers starting with `+98999` are rejected by the mock operator, so you can
see a failed message and its refund.

Stop and delete everything with `make down`.

## Demo: fairness and express SLA

With the stack running:

```bash
make loadgen
```

It floods the normal lane with 20,000 messages from one customer, then sends
50 messages from a small customer and 100 OTPs on the express lane at the
same time. A run on a laptop (2 workers, operator latency 50ms):

```
      customer     lane  messages   sent  failed     p50      p95      max  SLA missed
  heavy-sender   normal     20000  20000       0  5.577s  10.014s  15.552s           -
  light-sender   normal        50     50       0   793ms    859ms    864ms           -
    otp-sender  express       100    100       0    66ms     90ms    210ms           0
```

The small customer is served in under a second although 20,000 messages were
queued before it, and every OTP is handed over far inside the 5s SLA.

## Tests

```bash
make test               # unit tests, no services needed
make test-integration   # all tests against real Postgres and Redis (docker compose)
```

The integration tests include a concurrency test (200 parallel sends with
balance for exactly 50: exactly 50 succeed) and an end-to-end test of the
whole pipeline.

## Project layout

```
cmd/
  api/          REST API process
  worker/       relay + sender pools + reconciler process
  loadgen/      fairness / SLA demo
internal/
  sms/          domain types, message lifecycle, validation (no dependencies)
  service/      use cases: send, bulk, top-up, reports
  api/          HTTP handlers, routing, error -> status code mapping
  store/        PostgreSQL: balance, ledger, messages, outbox queries, migrations
  queue/        Redis: fair scheduler, express lane, admission control
  worker/       relay, senders, reconciler
  operator/     operator interface + mock
  config/ logging/ metrics/
docs/           architecture, decisions, API
```

## Configuration

All settings are environment variables with sensible defaults
(see [internal/config/config.go](internal/config/config.go)). The important ones:

| Variable | Default | Meaning |
|---|---|---|
| `DATABASE_URL` | `postgres://sms:sms@localhost:5432/sms` | PostgreSQL |
| `REDIS_ADDR` | `localhost:6379` | Redis |
| `PRICE_NORMAL` / `PRICE_EXPRESS` | `100` / `150` | Price per SMS, smallest currency unit |
| `EXPRESS_SLA` | `5s` | Promised max time from acceptance to hand-over |
| `EXPRESS_MAX_BACKLOG` | `1000` | Refuse express when this many are already waiting |
| `EXPRESS_RATE_PER_CUSTOMER` | `50` | Express messages per customer per second |
| `EXPRESS_WORKERS` / `NORMAL_WORKERS` | `20` / `50` | Sender goroutines per worker process |
| `FAIR_QUANTUM` | `10` | Messages taken from one customer per round-robin turn |
| `MAX_ATTEMPTS` | `5` | Hand-over attempts before a message fails (and is refunded) |
| `OPERATOR_LATENCY` / `OPERATOR_FAILURE_RATE` | `50ms` / `0.02` | Mock operator behaviour |

Metrics for Prometheus are at `:8080/metrics` (api) and `:9090/metrics` (worker).
