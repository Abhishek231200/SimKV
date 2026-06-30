#include "raft/node.hpp"
#include "sim/network.hpp"
#include "sim/simulator.hpp"
#include <gtest/gtest.h>
#include <memory>
#include <vector>

// Helper: build an N-node cluster with a low snapshot threshold so tests can
// trigger compaction without running thousands of ops.
struct SnapCluster {
    Simulator            sim;
    Network              net;
    std::vector<std::unique_ptr<RaftNode>> nodes;

    explicit SnapCluster(int n, std::size_t snap_threshold = 10)
        : sim(42), net(sim, NetworkConfig{}) {
        std::vector<NodeId> ids;
        for (int i = 1; i <= n; ++i) ids.push_back(static_cast<NodeId>(i));
        for (NodeId id : ids) {
            std::vector<NodeId> peers;
            for (NodeId p : ids) if (p != id) peers.push_back(p);
            RaftConfig cfg;
            cfg.snapshot_threshold = snap_threshold;
            nodes.push_back(std::make_unique<RaftNode>(id, peers, sim, net, cfg));
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
        bool submitted = false;
        bool done = false;
        submitted = l->submit(999, sim.now(), CmdPut{key, val},
                              [&done](CmdResult) { done = true; });
        if (!submitted) return false;
        sim.run_for(500 * kMsec);
        return done;
    }

    std::optional<std::string> read(const std::string& key, NodeId id) {
        for (auto& n : nodes) if (n->id() == id) return n->local_get(key);
        return std::nullopt;
    }
};

// ──────────────────────────────────────────────────────────────────────────────

TEST(Snapshot, LogGetsCompacted) {
    SnapCluster c(3, /*threshold=*/5);
    c.run(500 * kMsec); // elect leader

    // Write 12 entries — more than the threshold of 5.
    for (int i = 0; i < 12; ++i) {
        RaftNode* l = c.leader();
        ASSERT_NE(l, nullptr);
        bool done = false;
        l->submit(1, i, CmdPut{"k", std::to_string(i)},
                  [&done](CmdResult) { done = true; });
        c.run(300 * kMsec);
    }

    // After snapshotting, snap_index should be non-zero.
    RaftNode* l = c.leader();
    ASSERT_NE(l, nullptr);
    EXPECT_GT(l->snap_index(), Index{0}); // snapshot was taken
    EXPECT_GE(l->snap_index(), l->last_applied() - 5); // close to current applied index
}

TEST(Snapshot, StateCorrectAfterSnapshot) {
    SnapCluster c(3, /*threshold=*/4);
    c.run(500 * kMsec);

    // Write several keys past the threshold.
    for (int i = 0; i < 8; ++i) {
        RaftNode* l = c.leader();
        ASSERT_NE(l, nullptr);
        bool done = false;
        l->submit(1, i, CmdPut{"key" + std::to_string(i), "val" + std::to_string(i)},
                  [&done](CmdResult) { done = true; });
        c.run(300 * kMsec);
    }

    // KV state should reflect all committed writes.
    RaftNode* l = c.leader();
    ASSERT_NE(l, nullptr);
    for (int i = 0; i < 8; ++i) {
        auto v = l->local_get("key" + std::to_string(i));
        ASSERT_TRUE(v.has_value()) << "key" << i << " missing";
        EXPECT_EQ(*v, "val" + std::to_string(i));
    }
}

TEST(Snapshot, CrashRestartRestoresState) {
    SnapCluster c(3, /*threshold=*/4);
    c.run(500 * kMsec);

    // Write enough to trigger snapshotting on the leader.
    RaftNode* l = c.leader();
    ASSERT_NE(l, nullptr);
    NodeId leader_id = l->id();
    for (int i = 0; i < 6; ++i) {
        l->submit(1, i, CmdPut{"k" + std::to_string(i), "v" + std::to_string(i)},
                  [](CmdResult) {});
        c.run(200 * kMsec);
        l = c.leader();
        if (!l) { c.run(500 * kMsec); l = c.leader(); }
    }

    // Crash and restart the original leader.
    RaftNode* crashed = nullptr;
    for (auto& n : c.nodes) if (n->id() == leader_id) { crashed = n.get(); break; }
    ASSERT_NE(crashed, nullptr);
    crashed->crash();
    c.run(800 * kMsec); // let others elect a new leader
    crashed->restart();
    c.run(800 * kMsec); // let it catch up

    // After restart, state should still be correct.
    for (int i = 0; i < 6; ++i) {
        auto v = crashed->local_get("k" + std::to_string(i));
        ASSERT_TRUE(v.has_value()) << "k" << i << " missing after restart";
        EXPECT_EQ(*v, "v" + std::to_string(i));
    }
}

TEST(Snapshot, LaggingFollowerReceivesInstallSnapshot) {
    SnapCluster c(3, /*threshold=*/4);
    c.run(500 * kMsec);

    // Isolate one follower, then write enough for the leader to snapshot.
    // Include all non-isolated nodes in the leader's partition group so the leader
    // can still reach majority and commit entries.
    RaftNode* l = c.leader();
    ASSERT_NE(l, nullptr);
    NodeId follower_id = 0;
    for (auto& n : c.nodes) {
        if (n->id() != l->id()) { follower_id = n->id(); break; }
    }
    std::vector<NodeId> leader_group = {l->id()};
    for (auto& n : c.nodes) {
        if (n->id() != l->id() && n->id() != follower_id)
            leader_group.push_back(n->id());
    }
    c.net.partition({leader_group, {follower_id}});
    for (int i = 0; i < 8; ++i) {
        l->submit(1, i, CmdPut{"k" + std::to_string(i), "v" + std::to_string(i)},
                  [](CmdResult) {});
        c.run(200 * kMsec);
    }
    EXPECT_GT(l->snap_index(), Index{0}); // leader took a snapshot

    // Heal partition — follower should receive InstallSnapshot and catch up.
    c.net.heal();
    c.run(1000 * kMsec);

    RaftNode* follower = nullptr;
    for (auto& n : c.nodes) if (n->id() == follower_id) { follower = n.get(); break; }
    ASSERT_NE(follower, nullptr);

    // Follower's last_applied should now match the leader's.
    EXPECT_EQ(follower->last_applied(), l->last_applied());
    for (int i = 0; i < 8; ++i) {
        auto v = follower->local_get("k" + std::to_string(i));
        ASSERT_TRUE(v.has_value()) << "k" << i << " missing on lagging follower";
        EXPECT_EQ(*v, "v" + std::to_string(i));
    }
}
