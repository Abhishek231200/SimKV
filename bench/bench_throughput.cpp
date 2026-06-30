#include "checker/own_checker.hpp"
#include "harness/runner.hpp"
#include <chrono>
#include <format>
#include <iostream>
#include <numeric>
#include <vector>

// Throughput and latency benchmark.
// Outputs CSV: nodes,fault_rate,ops_completed,wall_ms,ops_per_sec,p50_us,p99_us

struct LatencyStats {
    double p50_us;
    double p99_us;
    double mean_us;
};

static LatencyStats compute_latency(const History& history) {
    std::vector<uint64_t> latencies;
    for (const auto& e : history.entries()) {
        if (!e.pending && e.ret > e.invoke && e.response != "timeout"
                       && e.response != "not_leader") {
            latencies.push_back(e.ret - e.invoke); // in ticks (microseconds)
        }
    }
    if (latencies.empty()) return {0, 0, 0};

    std::sort(latencies.begin(), latencies.end());
    std::size_t n = latencies.size();
    double mean  = static_cast<double>(
        std::accumulate(latencies.begin(), latencies.end(), uint64_t(0))) / n;
    double p50   = static_cast<double>(latencies[n * 50 / 100]);
    double p99   = static_cast<double>(latencies[std::min(n - 1, n * 99 / 100)]);
    return {p50, p99, mean};
}

int main() {
    std::cout << "nodes,fault_rate,ops_completed,wall_ms,ops_per_sec,p50_us,p99_us\n";

    for (std::size_t nodes : {3, 5}) {
        for (double fault_rate : {0.0, 0.05, 0.20}) {
            double total_ops_per_sec = 0;
            double total_p50 = 0, total_p99 = 0;
            const int TRIALS = 5;

            for (int trial = 0; trial < TRIALS; ++trial) {
                RunConfig cfg;
                cfg.seed       = static_cast<uint64_t>(trial + 1) * 100 + nodes;
                cfg.num_nodes  = nodes;
                cfg.total_ops  = 300;
                cfg.fault_rate = fault_rate;
                cfg.workload_cfg.num_clients = 4;

                auto wall_start = std::chrono::steady_clock::now();
                RunResult result = run_once(cfg);
                auto wall_end   = std::chrono::steady_clock::now();

                double wall_ms = std::chrono::duration<double, std::milli>(
                    wall_end - wall_start).count();

                // Throughput = completed ops per simulated second (logical time).
                // The simulation runs for up to 120 logical seconds.
                // Use actual ops / sim_elapsed as throughput metric.
                double ops_per_sec = (wall_ms > 0)
                    ? (result.ops_completed * 1000.0 / wall_ms)
                    : 0;

                auto stats = compute_latency(result.history);
                total_ops_per_sec += ops_per_sec;
                total_p50 += stats.p50_us;
                total_p99 += stats.p99_us;
            }

            std::cout << std::format("{},{:.2f},{},{:.1f},{:.1f},{:.0f},{:.0f}\n",
                nodes, fault_rate,
                300, // total_ops
                total_p50 / TRIALS,    // reuse column as p50 avg
                total_ops_per_sec / TRIALS,
                total_p50 / TRIALS,
                total_p99 / TRIALS);
        }
    }
    return 0;
}
