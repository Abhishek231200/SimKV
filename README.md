# SimKV

A deterministic-simulation-tested distributed key-value store in Modern C++20.

![SimKV demo](demo/demo.gif)

A single seeded simulator owns time, randomness, and the network, so the entire
Raft-replicated cluster runs deterministically — any bug found by randomized fault
injection replays exactly from its seed.

## What it is

- **Raft consensus** across 3–5 nodes: leader election, log replication, persistence,
  and crash recovery, implemented strictly from the Raft paper.
- **Deterministic discrete-event simulator**: one thread, one logical clock, one seeded
  PRNG (xoshiro256\*\*). No wall clock, no real sockets, no OS threads in any system logic.
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
simkv: seed=<N> nodes=3 ops=300 fault_rate=0.15
  trace_hash=0x3a4f2b91e0c87d56  ops_completed=297
  PASS: history is linearizable

# Re-run with the printed seed:
$ ./build/src/cli/simkv run --seed <N> --nodes 3 --ops 300 --fault-rate 0.15
  trace_hash=0x3a4f2b91e0c87d56  ops_completed=297
  PASS: history is linearizable
```

Same seed → identical trace hash → identical execution.

## Bug injection story

```
$ ./build/src/cli/simkv run --seed 17 --inject-commit-bug --fault-rate 0.2
  FAIL: non-linearizable history!
  Failing key: k3
  To reproduce: simkv run --seed 17 --inject-commit-bug
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

## Tests

| Test | What it verifies |
|---|---|
| `sim_determinism_test` | Same seed → identical `trace_hash`; 50 seeds |
| `sim_network_test` | Drop, partition, duplication, intra-group delivery |
| `storage_wal_test` | WAL append/flush/replay; crash loses unflushed tail |
| `raft_election_test` | Exactly one leader; new leader after crash; term monotonicity |
| `raft_replication_test` | Write commits and applies; all nodes converge; crash recovery |
| `raft_partition_test` | New leader in majority; no committed entry lost; log convergence |
| `linearizability_test` | 20 seeds × fault injection; bug injection caught; replay determinism |

## Benchmarks

All numbers are in simulated time — the simulator runs deterministically at full CPU speed,
so these reflect protocol efficiency rather than hardware I/O.

### Throughput (300 ops, 4 clients, closed loop)

| Nodes | Fault rate | Throughput | p50 latency | p99 latency |
|-------|-----------|------------|-------------|-------------|
| 3     | 0%        | 182 ops/s  | 23 ms       | 44 ms       |
| 3     | 5%        | 182 ops/s  | 24 ms       | 46 ms       |
| 3     | 20%       | 171 ops/s  | 24 ms       | 47 ms       |
| 5     | 0%        | 107 ops/s  | 25 ms       | 43 ms       |
| 5     | 5%        | 105 ops/s  | 25 ms       | 44 ms       |
| 5     | 20%       | 100 ops/s  | 25 ms       | 53 ms       |

### Leader crash recovery (20 trials per config)

| Nodes | Mean recovery | Min  | Max  |
|-------|--------------|------|------|
| 3     | 722 ms       | 640 ms | 920 ms |
| 5     | 693 ms       | 660 ms | 760 ms |

Recovery time = time from leader crash to first successful write on the new leader.
Dominated by the randomized election timeout (150–300 ms) plus one heartbeat round.

## Porcupine cross-validation

```bash
cd src/checker/porcupine_glue && go mod tidy && go build -o simkv_porcupine .
./build/src/cli/simkv run --seed 42 --emit-history /tmp/history.json
./scripts/run_porcupine.sh /tmp/history.json
```

See [docs/design.md](docs/design.md) for the full design writeup.