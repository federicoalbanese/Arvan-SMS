# Design decisions

Each decision: what was chosen, why, what else was considered, and what it
costs. The full picture is in [architecture.md](architecture.md).

## 1. Go with the standard library HTTP router

**Chosen:** `net/http` with Go 1.22+ route patterns (`"POST /v1/customers/{id}/messages"`).
**Why:** method and path matching is built in now; no framework is needed
for a dozen JSON endpoints. Fewer dependencies, nothing magic to explain.
**Alternatives:** Gin, Echo, chi. Useful for large APIs with many middlewares.

## 2. PostgreSQL as the single source of truth

**Chosen:** balances, ledger and all messages in PostgreSQL.
**Why:** the balance rule needs real transactions and row locks. A message
and its charge must be stored together or not at all.
**Cost:** it is the hardest part to scale for writes; see architecture §8.

## 3. Money as integers

**Chosen:** `BIGINT` in the smallest currency unit, prices from config.
**Why:** floats cannot represent most decimal values exactly; `0.1 + 0.2 != 0.3`.

## 4. Check-and-debit in one conditional UPDATE

**Chosen:** `UPDATE customers SET balance = balance - $1 WHERE id = $2 AND balance >= $1`.
**Why:** the check and the change are one atomic step. No race, no
overspending, and the balance can reach exactly zero.
**Alternatives:**
- `SELECT` then `UPDATE`: two requests can both pass the check → overspending.
- `SELECT ... FOR UPDATE` then `UPDATE`: correct, but two round trips while holding the lock.
- Balance counter in Redis: fast, but money would live in a store that can lose recent writes.
**Cost:** requests of one customer serialize on its row. The bulk endpoint
(1000 messages, one update) is the answer for high-volume senders.

## 5. Charge at acceptance, refund on final failure

**Chosen:** debit when the API accepts the message; refund if it finally fails.
**Why:** "no messages once the balance is exhausted" must be decided at
acceptance time, so the money has to be reserved then. Refunding failures
means the customer only pays for messages the operator received, and can
still use the whole balance.
**Alternative:** charge after hand-over. Then the API cannot know whether
the balance is enough, and the customer could queue more than they can pay for.

## 6. Ledger table

**Chosen:** every top-up, charge and refund appends a row with `balance_after`.
**Why:** support and accounting can explain any balance; tests check
`balance = SUM(ledger)`.
**Cost:** one extra insert per request (per bulk request, not per message).

## 7. Idempotency-Key header

**Chosen:** optional header; unique index on `(customer_id, idempotency_key)`.
**Why:** networks fail. A client that times out does not know if the SMS was
accepted. With a key it can retry safely: same message back, no second charge.
Same idea as the Stripe API.

## 8. Transactional outbox + relay (no dual writes)

**Chosen:** the API writes only to Postgres. The `messages` table is the
outbox; a relay moves `pending` rows to Redis.
**Why:** writing to Postgres and Redis in the same request can fail half way
(stored but never queued, or queued but never stored). With the outbox, a
message is safe the moment the API returns `202`; the relay retries until
it is published.
**Cost:** a little latency (relay polling: 20ms express, 200ms normal) and
one extra status update per message.

## 9. Redis for scheduling, not Kafka or RabbitMQ

**Chosen:** Redis lists, a set, a sorted set, and Lua scripts.
**Why:** the scheduling problem here is *fairness across tens of thousands
of customers* plus a *priority lane*. Redis gives per-customer lists and an
atomic round-robin in ~20 lines of Lua. It holds only ids and can always be
rebuilt from Postgres, so its weaker durability is acceptable.
**Alternatives:**
- Kafka: excellent throughput and durability, but each partition is FIFO;
  a big customer blocks everyone sharing its partition.
- RabbitMQ: one queue per customer does not scale to tens of thousands of queues comfortably.
- Postgres as the queue (`SKIP LOCKED`): fine for FIFO; round-robin by customer in SQL is complex and costly.

## 10. Round-robin fair queuing per customer

**Chosen:** one Redis list per customer and a ring of active customers;
each turn takes at most `FAIR_QUANTUM` messages from one customer.
**Why:** a customer's wait depends on how many customers are active, not on
how big someone else's backlog is. Heavy customers still get all free capacity.
**Alternative:** per-customer rate limits only. Simpler, but they waste
capacity when the system is idle and still let many heavy customers fill the queue.

## 11. Express: isolation plus admission control

**Chosen:** separate queue and sender pool (bulkhead), fast polling, plus
refusing express messages when the backlog or the customer's rate is too high.
**Why:** isolation keeps normal traffic from slowing express down. But
isolation alone cannot guarantee a time limit: if more express messages
arrive than we can send, the queue grows forever. A guarantee needs a bound
on the work we accept. Refusing up front (with `Retry-After`, nothing
charged) is honest; accepting and then missing the deadline is not.
**Cost:** customers may see `429`/`503` on express under extreme load and
must retry or fall back to normal.

## 12. Claim before sending, with a fencing token

**Chosen:** a worker moves a message to `sending` and increments `attempts`
before calling the operator. The result is only saved if `attempts` still matches.
**Why:** Redis may deliver the same id twice (after recovery). The claim
makes sure only one worker sends it. The attempt number stops a slow, old
worker from overwriting the result of a newer attempt (or refunding twice).

## 13. Visibility timeout for popped ids

**Chosen:** popping an id also records it in an in-flight sorted set with a
timestamp; the worker acks after claiming. The reconciler requeues ids that
were never acked within 30s.
**Why:** without it, a worker that dies right after popping would lose the
id from Redis while Postgres still says `queued`. Same idea as SQS.

## 14. At-least-once delivery to the operator

**Chosen:** if a worker dies while sending, the message is sent again after a timeout.
**Why:** losing an OTP is worse than sending it twice. Exactly-once is not
possible without the operator's help.
**Improvement:** if the operator API accepts a client message id, use our
message id so the operator drops duplicates.

## 15. Keyset pagination for reports

**Chosen:** `?cursor=<last id>` → `WHERE id < cursor ORDER BY id DESC`.
**Why:** constant cost per page. `OFFSET` gets slower with every page, and
big customers have millions of messages.

## 16. Bulk endpoint

**Chosen:** `POST /messages/bulk` with up to 1000 messages, all or nothing, normal lane only.
**Why:** high-volume customers are exactly the ones who would hammer their
balance row and the API. One request, one transaction, one balance update.
Express is for single, latency-critical messages, so bulk is normal only.

## 17. Mock operator behind an interface

**Chosen:** `operator.Operator` interface; the mock has configurable latency
and failure rate, and rejects `+98999…` numbers permanently.
**Why:** lets us test retries, refunds and throughput without a real SMSC.
A real SMPP or HTTP client implements the same one-method interface.

## 18. Consumer-defined interfaces and plain layering

**Chosen:** `api` → `service` → `store`/`queue`; each layer defines the small
interface it needs from the next.
**Why:** handlers know HTTP, the service knows business rules, the store
knows SQL. Each layer is tested with simple fakes of the layer below; the
store is tested against real Postgres.
