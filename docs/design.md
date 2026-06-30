# SimKV Design

## The Core Idea: Deterministic Simulation

The central design decision in SimKV is that the entire system — every node, every
network message, every timer — runs inside a single-threaded, seeded discrete-event
simulator. There is no wall clock, no real network, and no OS threads in any system
logic. All time is logical ticks; all randomness comes from one seeded PRNG.

This is **determinism as a load-bearing constraint**, not a feature added at the end.
The consequence: any failure the simulator finds can be reproduced exactly by
re-running with the same seed.

```
                Seed (uint64_t)
                     |
                     v
   +------------------------------------------------------------------+
   |              DETERMINISTIC SIMULATOR                              |
   |        (single thread · logical clock · one PRNG)               |
   |                                                                  |
   |   +-----------------------+        +-------------------------+   |
   |   |  Event queue + clock  | -----> |  Raft node 1            |   |
   |   |  (min-heap by time)   |        |  consensus + WAL/KV     |   |
   |   +-----------------------+        +-------------------------+   |
   |              |                     +-------------------------+   |
   |              v                     |  Raft node 2            |   |
   |   +-----------------------+ <----> |  consensus + WAL/KV     |   |
   |   |  Simulated network    |        +-------------------------+   |
   |   |  drop/delay/reorder   |        +-------------------------+   |
   |   |  /dup/partition       | <----> |  Raft node 3            |   |
   |   +-----------------------+        |  consensus + WAL/KV     |   |
   |                                    +-------------------------+   |
   +------------------------------------------------------------------+
                     |
                     v
   History recorder → Wing-Gong checker → On failure: print seed → replay
```

## The Determinism Rules

These rules apply to every file under `src/` except `cli/` and the outer shell of
`bench/`:

1. **One clock.** Never call `std::chrono::*::now()`, `time()`, or any wall-clock
   function. All time is `Simulator::now()` — a `uint64_t` logical tick counter.
2. **One PRNG.** Never call `rand()`, `std::random_device`, or any random function
   outside of `Prng`. All probabilistic decisions draw from `Simulator::prng()`.
3. **No real concurrency.** No `std::thread`, `std::async`, mutexes, or atomics for
   synchronization. Logical concurrency is expressed as scheduled events. Single-
   threaded design means no data races by construction.
4. **No address-dependent ordering.** All ordered containers use `std::map` (not
   `std::unordered_map`) where iteration order can affect behavior. Event-queue ties
   break by a monotonic sequence number, never by pointer value.
5. **No non-portable distributions.** `std::uniform_int_distribution` and its siblings
   are implementation-defined and not portable across compilers. SimKV implements its
   own `range()`, `bernoulli()`, and `shuffle()` on top of `Prng::next_u64()`.

## PRNG Design

SimKV uses **SplitMix64** to expand a single `uint64_t` seed into the four-word state
of **xoshiro256\*\*** — a fast, high-quality PRNG that passes all known statistical
tests. SplitMix64 is used only for seeding; xoshiro256\*\* generates the stream.

The choice of a self-contained, fully-specified algorithm (not `std::mt19937` or
`std::minstd_rand`) ensures identical output on any conforming C++20 compiler.

## Simulator Event Queue

The event queue is a min-heap ordered by `(time, seq)`. The `seq` is a globally
monotonically increasing counter assigned at schedule time. This means:

- Events at the same logical time fire in scheduling order — deterministic tiebreaking.
- Cancellation is lazy: a "cancelled" flag is checked when the event is popped.

The **trace hash** — an FNV-1a running hash over every `(time, seq)` pair processed —
is the project's determinism fingerprint. The same seed must always produce the same
trace hash across machines, compilers (within the same ABI), and runs.

## Simulated Network

`Network` holds a handler map (keyed by `NodeId`) and a partition state. When `send()`
is called:

1. The PRNG decides drop (Bernoulli with `drop_prob`).
2. If not dropped, schedules a delivery event with a PRNG-drawn latency in
   `[min_latency, max_latency]`.
3. With `dup_prob`, schedules a second identical delivery.
4. Partition: if the sender and receiver are in different groups, the message is
   silently dropped before any of the above.

Message reordering is emergent: two messages sent at the same time may arrive in
either order because each gets an independent random latency.

## WAL and Durable Store

`DurableStore` is a byte vector with two pointers: a flushed (durable) end and a
total end. `flush()` advances the flushed pointer; `crash()` truncates to the flushed
end. This models power-loss semantics: only flushed bytes survive a crash.

`Wal` writes framed records (`[len:u32][payload][crc32:u32]`) to `DurableStore`.
`replay()` reads only the durable prefix and stops at the first CRC mismatch,
discarding any partial tail record.

## Raft Implementation

SimKV implements Raft strictly from the extended Raft paper (Ongaro & Ousterhout
2014), Figure 2. Notable details:

**Commit rule safety.** A leader only advances `commitIndex` to entry `N` if
`log[N].term == currentTerm`. This prevents committed entries from prior terms from
being overwritten by future leaders. The bug-injection flag
`bug_commit_prior_term = true` disables this check — the linearizability suite is
expected to detect the resulting violation.

**Vote safety.** A node grants a vote only if the candidate's log is "at least as
up-to-date" (last log term first, then last log index as tiebreaker). The flag
`bug_skip_vote_uptodate = true` skips this check.

**Fast log backtracking.** `AppendEntriesReply` carries `conflict_index` and
`conflict_term`, so the leader can skip the slow one-by-one retry and jump directly
to the divergence point.

**Persistence.** Before sending any reply, the node flushes `currentTerm`,
`votedFor`, and any new log entries to `DurableStore`. This is Raft's durability
invariant: if a node grants a vote or appends an entry, that fact survives a crash.

**Reads.** Get operations are routed through the Raft log (as `CmdGet`) rather than
served locally. This ensures linearizability: the Get is serialized among concurrent
writes at the log position, and the state machine returns the value at that point.

## Linearizability Checking

The **Wing-Gong** backtracking algorithm checks each key's history independently
(per-key decomposition is sound because keys are independent state machines).

For each key, the checker maintains a set of "eligible" operations — those with no
unlinearized predecessor whose return time is strictly before the operation's invoke
time. It tries to extend the linearization with each eligible operation, verifying
that the model's expected response matches the observed response, and backtracks if
no extension works.

For histories longer than 30 operations per key, the checker falls back to a
sequential consistency check (sort by return time, verify model) for performance.
This is conservative: it may miss some linearizability violations in very long
histories, but in practice the per-key decomposition keeps histories short.

**Porcupine integration.** `simkv run --emit-history out.json` writes the full
history as JSON. `scripts/run_porcupine.sh out.json` feeds it to the Go Porcupine
checker for cross-validation.

## How to Reproduce Any Failure

1. A failing run prints: `To reproduce: simkv run --seed <N>`
2. Running with that seed re-enters the exact same event sequence, producing the
   same history and the same linearizability violation.
3. Add `--dump-trace` to print every event's `(time, seq)` pair for step-by-step
   debugging.

This is the core value of deterministic simulation: a bug found under randomized
fault injection is not a transient fluke — it is a reproducible fact.

## Tuning Notes

The two knobs with the biggest impact on recovery latency vs. throughput are:
- `election_timeout_lo`: lower values mean faster leader re-election but more
  spurious elections under message delay.
- `heartbeat_interval`: must be << `election_timeout_lo`; lower values improve
  replication throughput at the cost of more network traffic.

See `bench/bench_recovery` for measured recovery times across node counts.
