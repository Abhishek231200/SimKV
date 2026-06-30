#include "raft/node.hpp"
#include "sim/network.hpp"
#include "sim/simulator.hpp"
#include <gtest/gtest.h>
#include <vector>

// Helper: build a 3-node cluster and run the sim for `duration` ticks.
struct Cluster {
    Simulator sim;
    Network   net;
    std::vector<std::unique_ptr<RaftNode>> nodes;
    std::vector<RaftNode*>                 ptrs;

    explicit Cluster(uint64_t seed, std::size_t n = 3)
        : sim(seed), net(sim, NetworkConfig{}) {
        std::vector<NodeId> all;
        for (NodeId i = 1; i <= static_cast<NodeId>(n); ++i) all.push_back(i);
        for (NodeId id : all) {
            std::vector<NodeId> peers;
            for (NodeId p : all) if (p != id) peers.push_back(p);
            nodes.push_back(std::make_unique<RaftNode>(id, peers, sim, net));
            ptrs.push_back(nodes.back().get());
        }
        for (auto* n : ptrs) n->start();
    }

    RaftNode* leader_after(SimTime duration) {
        sim.run_for(duration);
        RaftNode* leader = nullptr;
        for (auto* n : ptrs) {
            if (n->role() == Role::Leader) {
                if (leader != nullptr) return nullptr; // two leaders!
                leader = n;
            }
        }
        return leader;
    }

    int leader_count() const {
        int c = 0;
        for (auto* n : ptrs) if (n->role() == Role::Leader) ++c;
        return c;
    }
};

TEST(RaftElection, ExactlyOneLeaderElected) {
    for (uint64_t seed = 1; seed <= 20; ++seed) {
        Cluster c(seed);
        RaftNode* leader = c.leader_after(2 * kSec);
        EXPECT_NE(leader, nullptr) << "no leader elected at seed=" << seed;
        EXPECT_EQ(c.leader_count(), 1) << "multiple leaders at seed=" << seed;
    }
}

TEST(RaftElection, NewLeaderAfterCrash) {
    Cluster c(42);
    RaftNode* original = c.leader_after(2 * kSec);
    ASSERT_NE(original, nullptr);

    // Crash the leader.
    original->crash();
    c.sim.run_for(3 * kSec);

    // Expect a new leader among the remaining nodes.
    int leaders = 0;
    for (auto* n : c.ptrs) {
        if (n->is_running() && n->role() == Role::Leader) ++leaders;
    }
    EXPECT_EQ(leaders, 1) << "should have exactly one leader after crash";
}

TEST(RaftElection, TermMonotonicallyIncreases) {
    Cluster c(7);
    c.sim.run_for(500 * kMsec);
    Term t0 = 0;
    for (auto* n : c.ptrs) t0 = std::max(t0, n->current_term());

    c.sim.run_for(500 * kMsec);
    for (auto* n : c.ptrs) {
        EXPECT_GE(n->current_term(), t0)
            << "term decreased for node " << n->id();
    }
}

TEST(RaftElection, SingleNodeCluster) {
    Simulator sim(1);
    Network   net(sim, NetworkConfig{});
    RaftNode  node(1, {}, sim, net);
    node.start();
    sim.run_for(1 * kSec);
    EXPECT_EQ(node.role(), Role::Leader);
}
