#include "raft/node.hpp"
#include "sim/network.hpp"
#include "sim/simulator.hpp"
#include <gtest/gtest.h>
#include <memory>
#include <vector>

struct MemberCluster {
    Simulator sim;
    Network   net;
    std::vector<std::unique_ptr<RaftNode>> nodes;

    MemberCluster() : sim(42), net(sim, NetworkConfig{}) {}

    void add_node(NodeId id, std::vector<NodeId> peers) {
        RaftConfig cfg;
        nodes.push_back(std::make_unique<RaftNode>(id, peers, sim, net, cfg));
        nodes.back()->start();
    }

    void run(SimTime t) { sim.run_for(t); }

    RaftNode* leader() {
        for (auto& n : nodes)
            if (n->is_running() && n->role() == Role::Leader) return n.get();
        return nullptr;
    }

    bool submit_config(Command cmd) {
        RaftNode* l = leader();
        if (!l) return false;
        bool done = false;
        bool ok = l->submit(0, sim.now(), std::move(cmd),
                            [&done](CmdResult) { done = true; });
        if (!ok) return false;
        sim.run_for(800 * kMsec);
        return done;
    }

    bool write(const std::string& key, const std::string& val) {
        RaftNode* l = leader();
        if (!l) return false;
        bool done = false;
        bool ok = l->submit(1, sim.now(), CmdPut{key, val},
                            [&done](CmdResult) { done = true; });
        if (!ok) return false;
        sim.run_for(500 * kMsec);
        return done;
    }
};

// ──────────────────────────────────────────────────────────────────────────────

TEST(Membership, AddServerExpandsCluster) {
    MemberCluster c;
    // Start with a 3-node cluster.
    c.add_node(1, {2, 3});
    c.add_node(2, {1, 3});
    c.add_node(3, {1, 2});
    c.run(600 * kMsec);

    RaftNode* l = c.leader();
    ASSERT_NE(l, nullptr);

    // Write a value before adding the new server.
    ASSERT_TRUE(c.write("k", "before"));

    // Add node 4 to the cluster.
    c.add_node(4, {1, 2, 3}); // node 4 starts knowing existing peers
    bool submitted = c.submit_config(CmdAddServer{4});
    EXPECT_TRUE(submitted);

    // Write another value — quorum now includes node 4.
    ASSERT_TRUE(c.write("k", "after"));

    // Node 4 should have the latest value.
    RaftNode* n4 = nullptr;
    for (auto& n : c.nodes) if (n->id() == 4) { n4 = n.get(); break; }
    ASSERT_NE(n4, nullptr);
    c.run(500 * kMsec);
    auto v = n4->local_get("k");
    ASSERT_TRUE(v.has_value());
    EXPECT_EQ(*v, "after");
}

TEST(Membership, RemoveFollowerShrinksMajority) {
    MemberCluster c;
    c.add_node(1, {2, 3});
    c.add_node(2, {1, 3});
    c.add_node(3, {1, 2});
    c.run(600 * kMsec);

    RaftNode* l = c.leader();
    ASSERT_NE(l, nullptr);

    // Remove a non-leader follower.
    NodeId remove_id = 0;
    for (auto& n : c.nodes) {
        if (n->id() != l->id()) { remove_id = n->id(); break; }
    }
    ASSERT_NE(remove_id, NodeId{0});

    bool submitted = c.submit_config(CmdRemoveServer{remove_id});
    EXPECT_TRUE(submitted);

    // Cluster should still operate with 2 remaining nodes (quorum=2 of 2 remaining).
    EXPECT_TRUE(c.write("k", "after_remove"));
}

TEST(Membership, LeaderRemovesSelfStepsDown) {
    MemberCluster c;
    c.add_node(1, {2, 3});
    c.add_node(2, {1, 3});
    c.add_node(3, {1, 2});
    c.run(600 * kMsec);

    RaftNode* l = c.leader();
    ASSERT_NE(l, nullptr);
    NodeId old_leader = l->id();

    // Leader removes itself.
    bool submitted = c.submit_config(CmdRemoveServer{old_leader});
    EXPECT_TRUE(submitted);
    c.run(800 * kMsec); // let a new leader be elected

    // The removed node should no longer be leader.
    RaftNode* removed = nullptr;
    for (auto& n : c.nodes) if (n->id() == old_leader) { removed = n.get(); break; }
    ASSERT_NE(removed, nullptr);
    EXPECT_NE(removed->role(), Role::Leader);

    // A new leader should have been elected among the remaining nodes.
    RaftNode* new_l = c.leader();
    ASSERT_NE(new_l, nullptr);
    EXPECT_NE(new_l->id(), old_leader);
}
