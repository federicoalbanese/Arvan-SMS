// Package queue is the dispatch scheduler, built on Redis.
//
// It decides which message a worker sends next:
//
//   - Express messages have their own FIFO list and their own workers, so
//     they never wait behind normal traffic.
//   - Normal messages get one list per customer, and customers are served
//     round-robin: each turn takes at most "quantum" messages from one
//     customer. A customer with a million queued messages therefore cannot
//     delay a customer that sends a single message.
//
// Redis holds only message ids and scheduling state. Everything here can be
// rebuilt from PostgreSQL, which is the source of truth.
//
// Keys (they share the {sms} hash tag so the Lua scripts stay in one slot
// on Redis Cluster):
//
//	{sms}:q:express        LIST  express message ids (FIFO)
//	{sms}:q:normal:<cid>   LIST  normal message ids of one customer (FIFO)
//	{sms}:ring             LIST  customers with waiting normal messages, in round-robin order
//	{sms}:ring:members     SET   the same customers, so one is never added twice
//	{sms}:inflight         ZSET  ids popped by a worker but not yet acknowledged; score = pop time (ms)
package queue

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"

	"sms-gateway/internal/sms"
)

const (
	keyExpress      = "{sms}:q:express"
	keyNormalPrefix = "{sms}:q:normal:"
	keyRing         = "{sms}:ring"
	keyRingMembers  = "{sms}:ring:members"
	keyInflight     = "{sms}:inflight"
)

// Lua scripts run atomically inside Redis: no other command can interleave,
// so the ring and the per-customer lists never disagree.

// enqueueNormal appends messages to their customer's list and adds the
// customer to the ring if it is not there yet.
var enqueueNormal = redis.NewScript(`
-- KEYS[1] ring, KEYS[2] ring members
-- ARGV[1] queue key prefix, then pairs of (customer id, message id)
for i = 2, #ARGV, 2 do
  local cid = ARGV[i]
  redis.call('RPUSH', ARGV[1] .. cid, ARGV[i + 1])
  if redis.call('SADD', KEYS[2], cid) == 1 then
    redis.call('RPUSH', KEYS[1], cid)
  end
end
return 1
`)

// popNormal serves the next customer in the ring: it pops up to quantum of
// that customer's messages and moves the customer to the back of the ring
// if it still has messages waiting.
var popNormal = redis.NewScript(`
-- KEYS[1] ring, KEYS[2] ring members, KEYS[3] inflight
-- ARGV[1] quantum, ARGV[2] queue key prefix, ARGV[3] now (ms)
local cid = redis.call('LPOP', KEYS[1])
if not cid then return {} end
local qkey = ARGV[2] .. cid
local ids = redis.call('LPOP', qkey, ARGV[1])
if redis.call('LLEN', qkey) > 0 then
  redis.call('RPUSH', KEYS[1], cid)
else
  redis.call('SREM', KEYS[2], cid)
end
if not ids then return {} end
for _, id in ipairs(ids) do
  redis.call('ZADD', KEYS[3], ARGV[3], id)
end
return ids
`)

// popExpress pops from the express list and records the ids as in flight.
var popExpress = redis.NewScript(`
-- KEYS[1] express queue, KEYS[2] inflight
-- ARGV[1] count, ARGV[2] now (ms)
local ids = redis.call('LPOP', KEYS[1], ARGV[1])
if not ids then return {} end
for _, id in ipairs(ids) do
  redis.call('ZADD', KEYS[2], ARGV[2], id)
end
return ids
`)

// Scheduler is the Redis-backed dispatch queue.
type Scheduler struct {
	rdb     redis.UniversalClient
	quantum int
	now     func() time.Time
}

// NewScheduler creates a scheduler that takes at most quantum normal
// messages from one customer per round-robin turn.
func NewScheduler(rdb redis.UniversalClient, quantum int) *Scheduler {
	return &Scheduler{rdb: rdb, quantum: quantum, now: time.Now}
}

// Publish adds messages to the lane's queue. Called by the relay.
func (s *Scheduler) Publish(ctx context.Context, express bool, items []sms.QueueItem) error {
	if len(items) == 0 {
		return nil
	}
	if express {
		ids := make([]any, len(items))
		for i, it := range items {
			ids[i] = it.ID
		}
		return s.rdb.RPush(ctx, keyExpress, ids...).Err()
	}

	args := make([]any, 0, 1+2*len(items))
	args = append(args, keyNormalPrefix)
	for _, it := range items {
		args = append(args, it.CustomerID, it.ID)
	}
	return enqueueNormal.Run(ctx, s.rdb, []string{keyRing, keyRingMembers}, args...).Err()
}

// PopExpress takes up to max express message ids, oldest first.
func (s *Scheduler) PopExpress(ctx context.Context, max int) ([]int64, error) {
	res, err := popExpress.Run(ctx, s.rdb, []string{keyExpress, keyInflight},
		max, s.now().UnixMilli()).StringSlice()
	return parseIDs(res, err)
}

// PopNormal takes the next customer's turn: up to quantum of its message ids.
func (s *Scheduler) PopNormal(ctx context.Context) ([]int64, error) {
	res, err := popNormal.Run(ctx, s.rdb, []string{keyRing, keyRingMembers, keyInflight},
		s.quantum, keyNormalPrefix, s.now().UnixMilli()).StringSlice()
	return parseIDs(res, err)
}

// Ack removes ids from the in-flight set once Postgres has taken over
// responsibility for them (the worker claimed them).
func (s *Scheduler) Ack(ctx context.Context, ids []int64) error {
	if len(ids) == 0 {
		return nil
	}
	members := make([]any, len(ids))
	for i, id := range ids {
		members[i] = id
	}
	return s.rdb.ZRem(ctx, keyInflight, members...).Err()
}

// Expired returns in-flight ids that were popped more than olderThan ago
// and never acknowledged: the worker that popped them probably died.
func (s *Scheduler) Expired(ctx context.Context, olderThan time.Duration, limit int) ([]int64, error) {
	cutoff := s.now().Add(-olderThan).UnixMilli()
	res, err := s.rdb.ZRangeByScore(ctx, keyInflight, &redis.ZRangeBy{
		Min:   "-inf",
		Max:   strconv.FormatInt(cutoff, 10),
		Count: int64(limit),
	}).Result()
	return parseIDs(res, err)
}

// ExpressBacklog is the number of express messages waiting in Redis.
func (s *Scheduler) ExpressBacklog(ctx context.Context) (int64, error) {
	return s.rdb.LLen(ctx, keyExpress).Result()
}

// Stats is a snapshot of the queue sizes, exported as metrics.
type Stats struct {
	ExpressQueued   int64
	ActiveCustomers int64 // customers with normal messages waiting
	InFlight        int64
}

// Stats reads the current queue sizes.
func (s *Scheduler) Stats(ctx context.Context) (Stats, error) {
	pipe := s.rdb.Pipeline()
	express := pipe.LLen(ctx, keyExpress)
	ring := pipe.LLen(ctx, keyRing)
	inflight := pipe.ZCard(ctx, keyInflight)
	if _, err := pipe.Exec(ctx); err != nil {
		return Stats{}, err
	}
	return Stats{ExpressQueued: express.Val(), ActiveCustomers: ring.Val(), InFlight: inflight.Val()}, nil
}

// InstanceID returns the Redis run_id, which changes on every restart. The
// reconciler uses it to notice that Redis may have lost its queues.
func (s *Scheduler) InstanceID(ctx context.Context) (string, error) {
	info, err := s.rdb.Info(ctx, "server").Result()
	if err != nil {
		return "", err
	}
	for line := range strings.SplitSeq(info, "\n") {
		if v, ok := strings.CutPrefix(strings.TrimSpace(line), "run_id:"); ok {
			return v, nil
		}
	}
	return "", errors.New("run_id not found in INFO server")
}

func parseIDs(raw []string, err error) ([]int64, error) {
	if errors.Is(err, redis.Nil) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	ids := make([]int64, 0, len(raw))
	for _, r := range raw {
		id, err := strconv.ParseInt(r, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("bad message id %q in queue: %w", r, err)
		}
		ids = append(ids, id)
	}
	return ids, nil
}
