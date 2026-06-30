#include "raft/node.hpp"
#include "sim/network.hpp"
#include "sim/simulator.hpp"
#include <format>
#include <iostream>
#include <numeric>
#include <vector>

// Measure leader-loss recovery time: time from leader crash to first successful
// write on the new leader, in microseconds (logical ticks).

static uint64_t measure_recovery(uint64_t seed, std::size_t n_nodes) {
    Simulator sim(seed);
    Network   net(sim, NetworkConfig{});

    std::vector<NodeId> all_ids;
    for (NodeId i = 1; i <= static_cast<NodeId>(n_nodes); ++i) all_ids.push_back(i);

    std::vector<std::unique_ptr<RaftNode>> nodes;
    for (NodeId id : all_ids) {
        std::vector<NodeId> peers;
        for (NodeId p : all_ids) if (p != id) peers.push_back(p);
        nodes.push_back(std::make_unique<RaftNode>(id, peers, sim, net));
        nodes.back()->start();
    }

    // Wait for initial leader.
    sim.run_for(2 * kSec);

    RaftNode* leader = nullptr;
    for (auto& n : nodes) if (n->role() == Role::Leader) { leader = n.get(); break; }
    if (!leader) return 0;

    // Commit one entry to ensure cluster is functional.
    bool init_done = false;
    leader->submit(1, 1, CmdPut{"init", "done"}, [&](CmdResult) { init_done = true; });
    sim.run_for(2 * kSec);
    if (!init_done) return 0;

    // Crash the leader and record the time.
    SimTime crash_at = sim.now();
    leader->crash();

    // Poll every 10ms until a new leader accepts a write.
    SimTime recover_at = 0;
    for (int i = 0; i < 1000 && recover_at == 0; ++i) {
        sim.run_for(10 * kMsec);
        for (auto& n : nodes) {
            if (!n->is_running() || n->role() != Role::Leader) continue;
            bool done = false;
            n->submit(1, 2, CmdPut{"after_crash", "yes"}, [&](CmdResult r) {
                if (r.ok) done = true;
            });
            sim.run_for(500 * kMsec);
            if (done) { recover_at = sim.now(); break; }
        }
    }

    return (recover_at > crash_at) ? (recover_at - crash_at) : 0;
}

int main() {
    std::cout << "nodes,election_timeout_lo_ms,trials,mean_recovery_ms,min_ms,max_ms\n";

    for (std::size_t n : {3, 5}) {
        std::vector<uint64_t> samples;
        for (uint64_t seed = 1; seed <= 20; ++seed) {
            uint64_t recovery = measure_recovery(seed, n);
            if (recovery > 0) samples.push_back(recovery);
        }

        if (samples.empty()) {
            std::cout << std::format("{},150,0,n/a,n/a,n/a\n", n);
            continue;
        }

        double mean = static_cast<double>(
            std::accumulate(samples.begin(), samples.end(), uint64_t(0))) / samples.size();
        uint64_t mn = *std::min_element(samples.begin(), samples.end());
        uint64_t mx = *std::max_element(samples.begin(), samples.end());

        std::cout << std::format("{},150,{},{:.1f},{:.1f},{:.1f}\n",
            n, samples.size(), mean / 1000.0, mn / 1000.0, mx / 1000.0);
    }
    return 0;
}
