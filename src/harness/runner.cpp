#include "harness/runner.hpp"
#include <format>
#include <iostream>
#include <memory>

RunResult run_once(RunConfig cfg) {
    RunResult result;
    result.seed = cfg.seed;

    Simulator sim(cfg.seed);
    NetworkConfig ncfg = cfg.net_cfg;
    Network net(sim, ncfg);
    History history;

    // Build nodes.
    std::vector<NodeId> all_ids;
    for (std::size_t i = 0; i < cfg.num_nodes; ++i) {
        all_ids.push_back(static_cast<NodeId>(i + 1));
    }

    std::vector<std::unique_ptr<RaftNode>> node_ptrs;
    for (NodeId id : all_ids) {
        std::vector<NodeId> peers;
        for (NodeId p : all_ids) {
            if (p != id) peers.push_back(p);
        }
        RaftConfig rcfg;
        rcfg.bug_commit_prior_term  = cfg.inject_commit_bug;
        rcfg.bug_skip_vote_uptodate = cfg.inject_vote_bug;
        node_ptrs.push_back(std::make_unique<RaftNode>(id, peers, sim, net, rcfg));
    }

    std::vector<RaftNode*> nodes;
    for (auto& n : node_ptrs) nodes.push_back(n.get());

    // Start all nodes.
    for (auto* n : nodes) n->start();

    // Build workload.
    WorkloadConfig wcfg  = cfg.workload_cfg;
    wcfg.total_ops       = cfg.total_ops;
    Workload workload(sim, net, history, nodes, wcfg);

    // Build fault injector.
    FaultConfig fcfg = cfg.fault_cfg;
    fcfg.fault_rate  = cfg.fault_rate;
    FaultInjector nemesis(sim, net, nodes, fcfg);

    // Let the cluster elect a leader before starting the workload (allow 1 second).
    sim.run_for(1 * kSec);

    workload.start();
    nemesis.start();

    // Run until workload completes or sim time limit reached.
    constexpr SimTime kMaxRunTime = 120 * kSec;
    SimTime deadline = sim.now() + kMaxRunTime;

    while (!workload.done() && sim.now() < deadline) {
        sim.run_for(100 * kMsec);
    }

    nemesis.stop();

    result.trace_hash    = sim.trace_hash();
    result.history       = std::move(history);
    result.ops_completed = workload.ops_completed();
    result.linearizable  = true; // set by caller after checking

    if (cfg.dump_trace) {
        std::cout << std::format("seed={} trace_hash={:#x} ops={}\n",
                                 result.seed, result.trace_hash, result.ops_completed);
    }

    return result;
}
