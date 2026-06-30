#include "raft/node.hpp"
#include "sim/network.hpp"
#include "sim/simulator.hpp"
#include <gtest/gtest.h>
#include <memory>
#include <vector>

struct ReadCluster {
    Simulator sim;
    Network   net;
    std::vector<std::unique_ptr<RaftNode>> nodes;

    explicit ReadCluster(int n) : sim(42), net(sim, NetworkConfig{}) {
        std::vector<NodeId> ids;
        for (int i = 1; i <= n; ++i) ids.push_back(static_cast<NodeId>(i));
        for (NodeId id : ids) {
            std::vector<NodeId> peers;
            for (NodeId p : ids) if (p != id) peers.push_back(p);
            nodes.push_back(std::make_unique<RaftNode>(id, peers, sim, net));
        }
        for (auto& n : nodes) n->start();
    }

    void run(SimTime t) { sim.run_for(t); }

    RaftNode* leader() {
        for (auto& n : nodes) if (n->is_running() && n->role() == Role::Leader) return n.get();
        return nullptr;
    }

    bool write(const std::string& key, const std::string& val) {
        RaftNode* l = leader();
        if (!l) return false;
        bool done = false;
        l->submit(1, sim.now(), CmdPut{key, val}, [&done](CmdResult) { done = true; });
        sim.run_for(500 * kMsec);
        return done;
    }
};

// ──────────────────────────────────────────────────────────────────────────────

TEST(ReadIndex, LeaderServesLinearizableRead) {
    ReadCluster c(3);
    c.run(600 * kMsec); // elect leader

    ASSERT_TRUE(c.write("foo", "bar"));

    RaftNode* l = c.leader();
    ASSERT_NE(l, nullptr);

    bool called = false;
    bool ok     = false;
    std::optional<std::string> got;

    bool submitted = l->read_index("foo", [&](bool o, std::optional<std::string> v) {
        called = true; ok = o; got = v;
    });
    EXPECT_TRUE(submitted);

    c.run(500 * kMsec); // allow heartbeat round to complete

    EXPECT_TRUE(called);
    EXPECT_TRUE(ok);
    ASSERT_TRUE(got.has_value());
    EXPECT_EQ(*got, "bar");
}

TEST(ReadIndex, NonLeaderRejectsRead) {
    ReadCluster c(3);
    c.run(600 * kMsec);

    // Find a follower.
    RaftNode* follower = nullptr;
    for (auto& n : c.nodes) {
        if (n->is_running() && n->role() != Role::Leader) { follower = n.get(); break; }
    }
    ASSERT_NE(follower, nullptr);

    bool called = false;
    bool submitted = follower->read_index("k", [&](bool, std::optional<std::string>) {
        called = true;
    });

    EXPECT_FALSE(submitted); // follower must reject immediately
    c.run(200 * kMsec);
    EXPECT_FALSE(called); // callback never fires
}

TEST(ReadIndex, PartitionedLeaderCannotConfirm) {
    // Isolate the leader so it can no longer reach a majority.
    // The read-index timeout (2000ms) should fire and cancel the read with ok=false.
    ReadCluster c(3);
    c.run(600 * kMsec);

    RaftNode* l = c.leader();
    ASSERT_NE(l, nullptr);

    ASSERT_TRUE(c.write("k", "v"));

    // Isolate the leader; put the other two nodes together so they can still communicate.
    std::vector<NodeId> others;
    for (auto& n : c.nodes) if (n->id() != l->id()) others.push_back(n->id());
    c.net.partition({{l->id()}, others});

    bool called = false;
    bool ok     = true; // expected to flip to false once timeout fires
    bool submitted = l->read_index("k", [&](bool o, std::optional<std::string>) {
        called = true; ok = o;
    });
    EXPECT_TRUE(submitted);

    // Run well past the read_timeout (default 2000ms) so the timeout fires.
    c.run(3000 * kMsec);

    EXPECT_TRUE(called);  // timeout callback fired
    EXPECT_FALSE(ok);     // leadership confirmation failed
}

TEST(ReadIndex, ReadMissingKeyReturnsNullopt) {
    ReadCluster c(3);
    c.run(600 * kMsec);

    RaftNode* l = c.leader();
    ASSERT_NE(l, nullptr);

    bool called = false;
    bool ok     = false;
    std::optional<std::string> got = "sentinel";

    l->read_index("no_such_key", [&](bool o, std::optional<std::string> v) {
        called = true; ok = o; got = v;
    });

    c.run(500 * kMsec);

    EXPECT_TRUE(called);
    EXPECT_TRUE(ok);              // leadership confirmed
    EXPECT_FALSE(got.has_value()); // key absent → nullopt
}
