# SimKV

A deterministic-simulation-tested distributed key-value store in Modern C++20.

![SimKV demo](demo/demo.gif)

A single seeded simulator owns time, randomness, and the network, so the entire
Raft-replicated cluster runs deterministically — any bug found by randomized fault
injection replays exactly from its seed.

## What it is

- **Raft consensus** across 3–5 nodes: leader election, log replication, snapshotting,
  membership changes, and crash recovery — the complete Raft paper, not just the happy path.
- **Real multi-process deployment**: `simkv_server` runs as actual OS processes communicating
  over TCP with `fsync`-backed disk persistence and linearizable reads via read-index.
- **Deterministic discrete-event simulator**: one thread, one logical clock, one seeded
  PRNG (xoshiro256\*\*). The simulator replaces the network and clock so the entire cluster
  runs deterministically — any bug found by randomized fault injection replays exactly.
- **Fault injection** ("nemesis"): network partitions, message drop/delay/reorder/
  duplication, and crash-restart, all driven by the PRNG.
- **Linearizability checker**: Wing-Gong backtracking per key, cross-validated against
  Porcupine (Go).
- **Seed-replayable failures**: a failing run prints its seed; re-running that seed
  reproduces the exact execution.

## Quick start

```bash
cmake -B build -GNinja -DCMAKE_BUILD_TYPE=Debug
cmake --build build --parallel
ctest --test-dir build --output-on-failure

# Run a simulation with fault injection
./build/src/cli/simkv run --seed 42 --nodes 3 --ops 200 --fault-rate 0.1

# Replay any run from its seed
./build/src/cli/simkv run --seed 42 --dump-trace

# Demonstrate that the checker catches a known Raft bug 
./build/src/cli/simkv run --seed 7 --inject-commit-bug --fault-rate 0.2
```

## Seed replay demo

```
$ ./build/src/cli/simkv run --nodes 3 --ops 300 --fault-rate 0.15
simkv: seed=42 nodes=3 ops=300 fault_rate=0.15
  trace_hash=0xe60456dfc449ba6c  ops_completed=300
  PASS: history is linearizable

# Re-run with the printed seed — trace hash is identical:
$ ./build/src/cli/simkv run --seed 42 --nodes 3 --ops 300 --fault-rate 0.15
  trace_hash=0xe60456dfc449ba6c  ops_completed=300
  PASS: history is linearizable
```

Same seed → identical trace hash → identical execution.

## Bug injection story

```
$ ./build/src/cli/simkv run --seed 13 --inject-commit-bug --fault-rate 0.2
simkv: seed=13 nodes=3 ops=200 fault_rate=0.20
  [BUG INJECTION] commit_bug=true vote_bug=false
  trace_hash=0x19d5d2a41bc16b8f  ops_completed=200

  FAIL: non-linearizable history!
  Failing key: k6
  Reason: non-linearizable history for key k6

  To reproduce: simkv run --seed 13 --inject-commit-bug
```

The injected bug: the leader commits prior-term log entries by majority count alone,
without checking that the entry belongs to the current term. Raft §5.4.2 proves this
can cause a committed entry to be overwritten after a leader change. The
linearizability checker detects the resulting consistency violation, and the seed
lets you replay the exact event sequence that triggered it.

## Repository layout

```
src/
  sim/         Simulator, PRNG (xoshiro256**), EventQueue, Network
  storage/     DurableStore (WAL substrate), WAL, KvStore
  raft/        Types, RPCs, RaftLog, RaftNode (the state machine)
  harness/     History recorder, Workload generator, FaultInjector, Runner
  checker/     Wing-Gong linearizability checker + Porcupine Go glue
  cli/         CLI entry point
tests/         Unit and property tests for each layer
bench/         Throughput / latency / recovery benchmarks
scripts/       replay.sh, run_porcupine.sh
docs/design.md Full design doc
```

## Real multi-process deployment

SimKV ships a `simkv_server` binary that runs as a real OS process with TCP sockets
and `fsync`-backed disk persistence — the same Raft logic, no simulator involved.

```bash
cmake --build build --target simkv_server

# Start a 3-node cluster (three terminals or &-background)
./build/src/server/simkv_server \
  --id 1 --raft-port 7001 --client-port 8001 \
  --peers "2:127.0.0.1:7002,3:127.0.0.1:7003" \
  --client-peers "2:127.0.0.1:8002,3:127.0.0.1:8003" \
  --data-dir /tmp/node1

./build/src/server/simkv_server \
  --id 2 --raft-port 7002 --client-port 8002 \
  --peers "1:127.0.0.1:7001,3:127.0.0.1:7003" \
  --client-peers "1:127.0.0.1:8001,3:127.0.0.1:8003" \
  --data-dir /tmp/node2

./build/src/server/simkv_server \
  --id 3 --raft-port 7003 --client-port 8003 \
  --peers "1:127.0.0.1:7001,2:127.0.0.1:7002" \
  --client-peers "1:127.0.0.1:8001,2:127.0.0.1:8002" \
  --data-dir /tmp/node3

# Talk to the leader (try each node; the leader accepts writes)
printf "PUT hello world\n" | nc -w1 127.0.0.1 8003   # → +OK
printf "GET hello\n"       | nc -w1 127.0.0.1 8003   # → +world
```

GETs use read-index (Raft §6.4): the leader confirms its majority with a heartbeat
round before serving the read, guaranteeing linearizability without going through the
log. When `--client-peers` is set, followers respond with `-REDIRECT host:port` so
clients can reconnect directly to the leader without probing all nodes.

## Tests

45 simulator tests across 10 suites plus one integration test that runs real TCP processes (`ctest --test-dir build --output-on-failure`):

| Suite | What it verifies |
|---|---|
| `sim_determinism_test` | Same seed → identical `trace_hash`; different seeds diverge |
| `sim_network_test` | Drop, partition, duplication, intra-group delivery |
| `storage_wal_test` | WAL append/flush/replay; crash loses unflushed tail; CAS round-trip |
| `raft_election_test` | Exactly one leader; new leader after crash; term monotonicity; single-node |
| `raft_replication_test` | Write commits and applies; all nodes converge; crash recovery |
| `raft_partition_test` | New leader in majority; no committed entry lost; log convergence |
| `raft_snapshot_test` | Log compaction; state correct after snapshot; lagging follower catch-up via InstallSnapshot |
| `raft_membership_test` | Add server expands cluster; remove follower shrinks majority; leader removes itself |
| `raft_read_index_test` | Leader serves linearizable read; non-leader rejects; partitioned leader cannot confirm |
| `linearizability_test` | 20+ seeds × fault injection; 5-node cluster; bug injection caught; replay determinism |
| `integration_test` | 3 real OS processes over TCP; GET/PUT correctness; leader failover; data durability |

## Protocol benchmarks (simulation)

> **These numbers are from the deterministic simulator, not a real deployment.**
> The simulator runs at full CPU speed with no real I/O or network; "ops/s" and
> "ms latency" are in *simulated* time, measuring Raft protocol efficiency
> (round-trip count × configured delays) rather than wall-clock throughput.
> Recovery times (§ below) do reflect real-world expectations because they are
> dominated by the election timeout, which maps 1-to-1 to wall-clock time in
> a real deployment with the same settings.

### Throughput — simulated time (300 ops, 4 clients, closed loop)

| Nodes | Fault rate | Protocol ops/s | p50 round-trip | p99 round-trip |
|-------|-----------|----------------|----------------|----------------|
| 3     | 0%        | 182 / s        | 23 ms          | 44 ms          |
| 3     | 5%        | 182 / s        | 24 ms          | 46 ms          |
| 3     | 20%       | 171 / s        | 24 ms          | 47 ms          |
| 5     | 0%        | 107 / s        | 25 ms          | 43 ms          |
| 5     | 5%        | 105 / s        | 25 ms          | 44 ms          |
| 5     | 20%       | 100 / s        | 25 ms          | 53 ms          |

The 5-node throughput drop vs 3-node is expected: two additional replication
round-trips must complete before commit. Fault injection at 20% adds retransmits
and occasional leader elections but the protocol stays correct throughout.

### Leader crash → first successful write — real-world comparable (20 trials per config)

| Nodes | Mean   | Min    | Max    |
|-------|--------|--------|--------|
| 3     | 722 ms | 640 ms | 920 ms |
| 5     | 693 ms | 660 ms | 760 ms |

Dominated by the randomized election timeout (150–300 ms) plus one heartbeat
round. These values translate directly to real-world recovery time with the
same timeout settings.

## Porcupine cross-validation

```bash
cd src/checker/porcupine_glue && go mod tidy && go build -o simkv_porcupine .
./build/src/cli/simkv run --seed 42 --emit-history /tmp/history.json
./scripts/run_porcupine.sh /tmp/history.json
```

See [docs/design.md](docs/design.md) for the full design writeup.