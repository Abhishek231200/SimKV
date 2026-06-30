#include "checker/own_checker.hpp"
#include "harness/runner.hpp"
#include <gtest/gtest.h>
#include <format>

// Run a set of seeds under fault injection and verify every history is linearizable.
TEST(Linearizability, RandomSeedsNoFaults) {
    for (uint64_t seed = 1; seed <= 20; ++seed) {
        RunConfig cfg;
        cfg.seed       = seed;
        cfg.num_nodes  = 3;
        cfg.total_ops  = 60;
        cfg.fault_rate = 0.0;

        RunResult result = run_once(cfg);
        CheckResult check = check_linearizable(result.history);
        EXPECT_TRUE(check.ok)
            << std::format("seed={} not linearizable: {}", seed, check.reason);
    }
}

TEST(Linearizability, RandomSeedsWithFaults) {
    for (uint64_t seed = 1; seed <= 20; ++seed) {
        RunConfig cfg;
        cfg.seed       = seed;
        cfg.num_nodes  = 3;
        cfg.total_ops  = 60;
        cfg.fault_rate = 0.15;

        RunResult result = run_once(cfg);
        CheckResult check = check_linearizable(result.history);
        EXPECT_TRUE(check.ok)
            << std::format("seed={} not linearizable under faults: {}", seed, check.reason);
    }
}

TEST(Linearizability, FiveNodeCluster) {
    for (uint64_t seed = 1; seed <= 10; ++seed) {
        RunConfig cfg;
        cfg.seed       = seed;
        cfg.num_nodes  = 5;
        cfg.total_ops  = 60;
        cfg.fault_rate = 0.10;

        RunResult result = run_once(cfg);
        CheckResult check = check_linearizable(result.history);
        EXPECT_TRUE(check.ok)
            << std::format("5-node seed={} not linearizable: {}", seed, check.reason);
    }
}

// ──────────────────────────────────────────────────────────────────────────────
// BUG INJECTION: The checker must CATCH a known Raft bug.
//
// Bug: commit prior-term log entries by majority count alone, violating the
// safety requirement that a leader may only commit entries from its current term.
// This can cause committed entries to be overwritten after a leader change.
//
// Expected: this test PASSES when the checker detects non-linearizability,
// i.e. the buggy run produces at least one non-linearizable history.
// ──────────────────────────────────────────────────────────────────────────────
TEST(BugInjection, CommitPriorTermBugDetected) {
    bool caught = false;
    for (uint64_t seed = 1; seed <= 50 && !caught; ++seed) {
        RunConfig cfg;
        cfg.seed               = seed;
        cfg.num_nodes          = 3;
        cfg.total_ops          = 80;
        cfg.fault_rate         = 0.20; // aggressive faults to expose the bug
        cfg.inject_commit_bug  = true;

        RunResult result = run_once(cfg);
        CheckResult check = check_linearizable(result.history);
        if (!check.ok) {
            caught = true;
            // This is the "story": print the seed so the bug replays exactly.
            std::cout << std::format(
                "\n[BugInjection] Non-linearizable history caught!\n"
                "  seed={}\n"
                "  failing_key={}\n"
                "  reason={}\n"
                "  To replay: simkv run --seed {} --inject-commit-bug\n\n",
                seed, check.failing_key, check.reason, seed);
        }
    }
    EXPECT_TRUE(caught)
        << "Expected bug injection to cause non-linearizability within 50 seeds";
}

// ──────────────────────────────────────────────────────────────────────────────
// Seed replay: running the same seed twice produces the exact same trace hash.
// This is the project-level determinism guard.
// ──────────────────────────────────────────────────────────────────────────────
TEST(Determinism, ReplaySameTraceHash) {
    for (uint64_t seed = 1; seed <= 10; ++seed) {
        RunConfig cfg;
        cfg.seed       = seed;
        cfg.num_nodes  = 3;
        cfg.total_ops  = 40;
        cfg.fault_rate = 0.10;

        RunResult r1 = run_once(cfg);
        RunResult r2 = run_once(cfg);
        EXPECT_EQ(r1.trace_hash, r2.trace_hash)
            << std::format("seed={} trace_hash changed between runs!", seed);
        EXPECT_EQ(r1.ops_completed, r2.ops_completed)
            << std::format("seed={} ops_completed changed between runs!", seed);
    }
}
