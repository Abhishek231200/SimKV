#include "checker/own_checker.hpp"
#include "harness/runner.hpp"
#include <chrono>
#include <cstdlib>
#include <format>
#include <fstream>
#include <iostream>
#include <string>
#include <string_view>

static void print_usage(const char* prog) {
    std::cerr << std::format(
        "Usage: {} <command> [options]\n"
        "\n"
        "Commands:\n"
        "  run       Run a single simulation and check linearizability\n"
        "  replay    Re-run a simulation with a known seed (same as run --seed N)\n"
        "\n"
        "Options for 'run':\n"
        "  --seed N           Seed (default: random from wall clock)\n"
        "  --nodes N          Number of Raft nodes (default: 3)\n"
        "  --ops N            Total client operations (default: 200)\n"
        "  --fault-rate F     Fault injection probability per interval (default: 0.05)\n"
        "  --dump-trace       Print event trace summary on completion\n"
        "  --emit-history F   Write history JSON to file F\n"
        "  --inject-commit-bug  Enable prior-term commit bug (correctness test)\n"
        "  --inject-vote-bug    Enable skip-uptodate-check bug (correctness test)\n",
        prog);
}

static uint64_t parse_u64(const char* s) {
    return static_cast<uint64_t>(std::stoull(s));
}
static double parse_double(const char* s) {
    return std::stod(s);
}

int main(int argc, char** argv) {
    if (argc < 2) { print_usage(argv[0]); return 1; }

    std::string_view cmd = argv[1];
    if (cmd != "run" && cmd != "replay") {
        std::cerr << "Unknown command: " << cmd << "\n";
        print_usage(argv[0]);
        return 1;
    }

    RunConfig cfg;
    // Default seed from wall clock (only allowed in cli/ — system logic is always seeded).
    cfg.seed = static_cast<uint64_t>(
        std::chrono::steady_clock::now().time_since_epoch().count());

    std::string emit_history_path;

    for (int i = 2; i < argc; ++i) {
        std::string_view arg = argv[i];
        if (arg == "--seed" && i + 1 < argc) {
            cfg.seed = parse_u64(argv[++i]);
        } else if (arg == "--nodes" && i + 1 < argc) {
            cfg.num_nodes = static_cast<std::size_t>(parse_u64(argv[++i]));
        } else if (arg == "--ops" && i + 1 < argc) {
            cfg.total_ops = static_cast<std::size_t>(parse_u64(argv[++i]));
        } else if (arg == "--fault-rate" && i + 1 < argc) {
            cfg.fault_rate = parse_double(argv[++i]);
        } else if (arg == "--dump-trace") {
            cfg.dump_trace = true;
        } else if (arg == "--emit-history" && i + 1 < argc) {
            emit_history_path = argv[++i];
        } else if (arg == "--inject-commit-bug") {
            cfg.inject_commit_bug = true;
        } else if (arg == "--inject-vote-bug") {
            cfg.inject_vote_bug = true;
        } else {
            std::cerr << "Unknown option: " << arg << "\n";
            print_usage(argv[0]);
            return 1;
        }
    }

    cfg.workload_cfg.total_ops = cfg.total_ops;
    cfg.workload_cfg.num_clients = std::max(std::size_t(2), cfg.num_nodes - 1);

    std::cout << std::format("simkv: seed={} nodes={} ops={} fault_rate={:.2f}\n",
                             cfg.seed, cfg.num_nodes, cfg.total_ops, cfg.fault_rate);
    if (cfg.inject_commit_bug || cfg.inject_vote_bug) {
        std::cout << std::format("  [BUG INJECTION] commit_bug={} vote_bug={}\n",
                                 cfg.inject_commit_bug, cfg.inject_vote_bug);
    }

    RunResult result = run_once(cfg);

    // Check linearizability.
    CheckResult check = check_linearizable(result.history);
    result.linearizable = check.ok;

    if (!emit_history_path.empty()) {
        std::ofstream f(emit_history_path);
        f << result.history.to_json();
        std::cout << std::format("  history written to {}\n", emit_history_path);
    }

    std::cout << std::format(
        "  trace_hash={:#018x}  ops_completed={}\n",
        result.trace_hash, result.ops_completed);

    if (check.ok) {
        std::cout << "  PASS: history is linearizable\n";
        return 0;
    } else {
        std::string bug_flags;
        if (cfg.inject_commit_bug) bug_flags += " --inject-commit-bug";
        if (cfg.inject_vote_bug)   bug_flags += " --inject-vote-bug";
        std::cerr << std::format(
            "\n  FAIL: non-linearizable history!\n"
            "  Failing key: {}\n"
            "  Reason: {}\n"
            "\n"
            "  To reproduce: simkv run --seed {}{}\n",
            check.failing_key, check.reason, cfg.seed, bug_flags);
        return 1;
    }
}
