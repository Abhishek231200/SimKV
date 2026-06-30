#include "raft/node.hpp"
#include "sim/network.hpp"
#include "sim/simulator.hpp"
#include <gtest/gtest.h>

struct Cluster3P {
    Simulator sim;
    Network   net;
    std::unique_ptr<RaftNode> n1, n2, n3;

    explicit Cluster3P(uint64_t seed)
        : sim(seed), net(sim, NetworkConfig{}) {
        n1 = std::make_unique<RaftNode>(1, std::vector<NodeId>{2,3}, sim, net);
        n2 = std::make_unique<RaftNode>(2, std::vector<NodeId>{1,3}, sim, net);
        n3 = std::make_unique<RaftNode>(3, std::vector<NodeId>{1,2}, sim, net);
        n1->start(); n2->start(); n3->start();
    }

    RaftNode* leader(SimTime wait = 2 * kSec) {
        sim.run_for(wait);
        for (auto* n : {n1.get(), n2.get(), n3.get()}) {
            if (n->role() == Role::Leader) return n;
        }
        return nullptr;
    }

    void run(SimTime t) { sim.run_for(t); }
    std::vector<RaftNode*> all() { return {n1.get(), n2.get(), n3.get()}; }

    RaftNode* find_leader() {
        for (auto* n : all()) if (n->role() == Role::Leader) return n;
        return nullptr;
    }

    int leader_count() const {
        int c = 0;
        for (auto* n : {n1.get(), n2.get(), n3.get()})
            if (n->role() == Role::Leader) ++c;
        return c;
    }
};

TEST(RaftPartition, MajorityPartitionElectsNewLeader) {
    Cluster3P c(42);
    RaftNode* leader = c.leader();
    ASSERT_NE(leader, nullptr);

    // Partition the leader into a minority.
    NodeId lid = leader->id();
    std::vector<NodeId> majority, minority = {lid};
    for (auto* n : c.all()) {
        if (n->id() != lid) majority.push_back(n->id());
    }
    c.net.partition({majority, minority});

    // Majority should elect a new leader.
    c.run(3 * kSec);

    RaftNode* new_leader = nullptr;
    for (auto* n : c.all()) {
        if (n->id() != lid && n->role() == Role::Leader) {
            new_leader = n;
            break;
        }
    }
    EXPECT_NE(new_leader, nullptr) << "no new leader in majority partition";

    // The old (now isolated) leader should step down when we heal.
    c.net.heal();
    c.run(3 * kSec);
    EXPECT_EQ(c.leader_count(), 1) << "should have exactly one leader after healing";

    // Old leader should be a follower now.
    EXPECT_NE(leader->role(), Role::Leader)
        << "stale leader should have stepped down after heal";
}

TEST(RaftPartition, NoCommittedEntriesLostAfterHeal) {
    Cluster3P c(17);
    RaftNode* leader = c.leader();
    ASSERT_NE(leader, nullptr);

    // Commit a write before partition.
    bool committed = false;
    leader->submit(1, 1, CmdPut{"key", "before_partition"},
                   [&](CmdResult) { committed = true; });
    c.run(1 * kSec);
    ASSERT_TRUE(committed);
    Index committed_idx = leader->commit_index();

    // Partition leader away.
    NodeId lid = leader->id();
    std::vector<NodeId> majority, minority = {lid};
    for (auto* n : c.all()) if (n->id() != lid) majority.push_back(n->id());
    c.net.partition({majority, minority});
    c.run(3 * kSec);

    // Heal.
    c.net.heal();
    c.run(3 * kSec);

    // All running nodes must have at least the committed entries.
    for (auto* n : c.all()) {
        EXPECT_GE(n->commit_index(), committed_idx)
            << "node " << n->id() << " lost committed entry";
    }
}

TEST(RaftPartition, NetworkHealConvergesLogs) {
    Cluster3P c(55);
    RaftNode* leader = c.leader();
    ASSERT_NE(leader, nullptr);

    // Commit several writes.
    for (int i = 0; i < 5; ++i) {
        bool done = false;
        leader->submit(1, static_cast<uint64_t>(i),
                       CmdPut{"k" + std::to_string(i), "v"},
                       [&](CmdResult) { done = true; });
        c.run(500 * kMsec);
        (void)done;
    }

    // Crash a node, let it fall behind.
    RaftNode* straggler = (leader == c.n1.get()) ? c.n2.get() : c.n1.get();
    straggler->crash();
    c.run(1 * kSec);

    // Restart the straggler — it should catch up.
    straggler->restart();
    c.run(5 * kSec);

    EXPECT_GE(straggler->commit_index(), leader->commit_index() - 1)
        << "straggler didn't catch up";
}
