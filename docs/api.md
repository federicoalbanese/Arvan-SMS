# REST API

Base URL: `http://localhost:8080`. Requests and responses are JSON.
There is no authentication (out of scope); the customer id is part of the path.

Amounts and prices are integers in the smallest currency unit.

## Customers and balance

### Create a customer

`POST /v1/customers`

```json
{ "name": "Acme" }
```

`201 Created`

```json
{ "id": 1, "name": "Acme", "balance": 0, "created_at": "2026-11-03T10:00:00Z" }
```

### Get a customer (with balance)

`GET /v1/customers/{id}` → `200` with the same shape.

### Top up

`POST /v1/customers/{id}/topup`

```json
{ "amount": 10000 }
```

`200` with the updated customer.

### Balance ledger

`GET /v1/customers/{id}/transactions?limit=50` → `200`

```json
{
  "data": [
    { "id": 3, "kind": "refund", "amount": 100,  "balance_after": 9750, "message_count": 1, "created_at": "..." },
    { "id": 2, "kind": "charge", "amount": -350, "balance_after": 9650, "message_count": 3, "created_at": "..." },
    { "id": 1, "kind": "topup",  "amount": 10000,"balance_after": 10000,"message_count": 0, "created_at": "..." }
  ],
  "next_cursor": null
}
```

## Sending

### Send one SMS

`POST /v1/customers/{id}/messages`

Optional header: `Idempotency-Key: <any unique string, max 100 chars>`.

```json
{ "to": "09121234567", "text": "Your code is 4821", "express": true }
```

- `to`: Iranian (`0912…`, `+98912…`, `0098912…`, Persian digits allowed) or any E.164 number.
- `text`: must fit in one segment: 160 latin characters, or 70 characters if it contains Persian/other unicode.
- `express`: optional, default `false`.

`202 Accepted`: stored and charged; it will be handed to the operator shortly.

```json
{
  "id": 42,
  "to": "+989121234567",
  "text": "Your code is 4821",
  "express": true,
  "status": "pending",
  "cost": 150,
  "attempts": 0,
  "deadline_at": "2026-11-03T10:00:05Z",
  "created_at": "2026-11-03T10:00:00Z"
}
```

`deadline_at` (express only) is the promised latest hand-over time.

If the same `Idempotency-Key` is sent again with the same body, the original
message is returned with header `Idempotent-Replayed: true` and nothing is charged.

### Send in bulk

`POST /v1/customers/{id}/messages/bulk` (normal lane only, 1 to 1000 messages, all or nothing)

```json
{ "messages": [ { "to": "09121111111", "text": "Sale!" }, { "to": "09122222222", "text": "Sale!" } ] }
```

`202 Accepted`

```json
{ "count": 2, "total_cost": 200, "data": [ { "id": 43, "status": "pending", "...": "..." }, { "id": 44, "...": "..." } ] }
```

## Reports

### List messages

`GET /v1/customers/{id}/messages`

| Query | Meaning |
|---|---|
| `status` | `pending`, `queued`, `sending`, `sent` or `failed` |
| `from`, `to` | RFC 3339 time range on `created_at` (`from` inclusive, `to` exclusive) |
| `limit` | page size, default 50, max 500 |
| `cursor` | value of `next_cursor` from the previous page |

`200`, newest first:

```json
{ "data": [ { "id": 44, "status": "sent", "sent_at": "...", "...": "..." } ], "next_cursor": "43" }
```

`next_cursor` is `null` on the last page.

### One message

`GET /v1/customers/{id}/messages/{messageID}` → `200` with one message.
`last_error` shows why the latest attempt failed; `operator_ref` is the
operator's id for a sent message.

### Summary

`GET /v1/customers/{id}/messages/summary?from=...&to=...` (default: last 24 hours, max 31 days)

```json
{ "from": "...", "to": "...", "total": 1200, "by_status": { "sent": 1180, "failed": 5, "queued": 15 } }
```

## Message status

| Status | Meaning |
|---|---|
| `pending` | stored and paid, waiting to be queued (or waiting for a retry) |
| `queued` | in the dispatch queue |
| `sending` | a worker is handing it to the operator |
| `sent` | the operator accepted it (final) |
| `failed` | could not be handed over; the cost was refunded (final) |

## Errors

```json
{ "error": { "code": "insufficient_balance", "message": "insufficient balance" } }
```

| Status | Code | When |
|---|---|---|
| 400 | `validation_error` | bad input; `field` names the problem (e.g. `messages[3].to`) |
| 402 | `insufficient_balance` | balance is lower than the price; nothing was charged |
| 404 | `customer_not_found`, `message_not_found` | |
| 409 | `idempotency_conflict` | the key was already used for a different message |
| 429 | `express_rate_limited` | customer exceeded its express messages per second (`Retry-After`) |
| 503 | `express_overloaded` | express lane is full; we cannot keep the time limit (`Retry-After`) |
| 503 | `express_unavailable` | express lane is temporarily unavailable (`Retry-After`) |
| 500 | `internal_error` | unexpected error (details only in server logs) |

## Operations

| Endpoint | |
|---|---|
| `GET /healthz` | process is alive |
| `GET /readyz` | database reachable (ready for traffic) |
| `GET /metrics` | Prometheus metrics (api `:8080`, worker `:9090`) |
