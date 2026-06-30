#pragma once
#include "harness/fault_injector.hpp"
#include "harness/history.hpp"
#include "harness/workload.hpp"
#include "raft/node.hpp"
#include "sim/network.hpp"
#include "sim/simulator.hpp"
#include <memory>
#include <vector>

struct RunConfig {
    uint64_t    seed        = 42;
    std::size_t num_nodes   = 3;
    std::size_t total_ops   = 200;
    double      fault_rate  = 0.05;
    bool        dump_trace  = false;
    bool        inject_commit_bug    = false;
    bool        inject_vote_bug      = false;
    NetworkConfig net_cfg;
    WorkloadConfig workload_cfg;
    FaultConfig   fault_cfg;
};

struct RunResult {
    uint64_t    seed;
    uint64_t    trace_hash;
    History     history;
    std::size_t ops_completed;
    bool        linearizable; // set by checker after the run
    std::string failure_seed_msg; // non-empty if not linearizable
};

// Wires together the simulator, network, Raft nodes, workload, and fault injector
// for a single deterministic run. Returns the history for external checking.
RunResult run_once(RunConfig cfg);
