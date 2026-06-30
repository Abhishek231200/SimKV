#include "raft/node.hpp"
#include "sim/network.hpp"
#include "sim/simulator.hpp"
#include <gtest/gtest.h>
#include <optional>

struct Cluster3 {
    Simulator sim;
    Network   net;
    std::unique_ptr<RaftNode> n1, n2, n3;

    explicit Cluster3(uint64_t seed)
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
};

TEST(RaftReplication, WriteCommitsAndApplies) {
    Cluster3 c(1);
    RaftNode* leader = c.leader();
    ASSERT_NE(leader, nullptr);

    bool done = false;
    CmdResult result;
    leader->submit(99, 1, CmdPut{"hello", "world"},
                   [&](CmdResult r) { done = true; result = r; });

    c.run(2 * kSec);
    EXPECT_TRUE(done) << "command never committed";
    EXPECT_TRUE(result.ok);

    // Verify commit index advanced on all running nodes.
    EXPECT_GT(c.n1->commit_index(), 0u);
}

TEST(RaftReplication, AllNodesConverge) {
    Cluster3 c(3);
    RaftNode* leader = c.leader();
    ASSERT_NE(leader, nullptr);

    bool done = false;
    leader->submit(1, 1, CmdPut{"k", "v"}, [&](CmdResult) { done = true; });
    c.run(2 * kSec);
    ASSERT_TRUE(done);

    // After commit, all logs should have the same last applied index.
    Index applied1 = c.n1->last_applied();
    Index applied2 = c.n2->last_applied();
    Index applied3 = c.n3->last_applied();
    EXPECT_EQ(applied1, applied2);
    EXPECT_EQ(applied2, applied3);
    EXPECT_GT(applied1, 0u);
}

TEST(RaftReplication, MultipleWritesOrdered) {
    Cluster3 c(5);
    RaftNode* leader = c.leader();
    ASSERT_NE(leader, nullptr);

    int done = 0;
    leader->submit(1, 1, CmdPut{"x", "1"}, [&](CmdResult) { ++done; });
    leader->submit(1, 2, CmdPut{"x", "2"}, [&](CmdResult) { ++done; });
    leader->submit(1, 3, CmdPut{"x", "3"}, [&](CmdResult) { ++done; });
    c.run(3 * kSec);
    EXPECT_EQ(done, 3);
}

TEST(RaftReplication, CrashAndRecovery) {
    Cluster3 c(9);
    RaftNode* leader = c.leader();
    ASSERT_NE(leader, nullptr);

    bool done = false;
    leader->submit(1, 1, CmdPut{"k", "v"}, [&](CmdResult) { done = true; });
    c.run(2 * kSec);
    EXPECT_TRUE(done);

    // Crash and restart a follower.
    RaftNode* follower = (leader == c.n1.get()) ? c.n2.get() : c.n1.get();
    follower->crash();
    c.run(1 * kSec);
    follower->restart();
    c.run(3 * kSec);

    // After restart, the follower should catch up.
    EXPECT_GE(follower->last_applied(), leader->last_applied() - 1);
}
