#include "harness/fault_injector.hpp"
#include <algorithm>

FaultInjector::FaultInjector(Simulator& sim, Network& net,
                              std::vector<RaftNode*> nodes, FaultConfig cfg)
    : sim_(sim), net_(net), nodes_(std::move(nodes)), cfg_(cfg) {}

void FaultInjector::start() {
    running_ = true;
    tick_id_ = sim_.schedule(cfg_.check_interval, [this]() { tick(); });
}

void FaultInjector::stop() {
    running_ = false;
    sim_.cancel(tick_id_);
}

void FaultInjector::tick() {
    if (!running_) return;

    if (sim_.prng().bernoulli(cfg_.fault_rate)) {
        double r = sim_.prng().unit();
        if (r < cfg_.partition_prob) {
            inject_partition();
        } else if (r < cfg_.partition_prob + cfg_.drop_raise_prob) {
            inject_drop_spike();
        } else if (r < cfg_.partition_prob + cfg_.drop_raise_prob + cfg_.dup_raise_prob) {
            inject_dup_spike();
        } else {
            inject_crash_restart();
        }
    }

    tick_id_ = sim_.schedule(cfg_.check_interval, [this]() { tick(); });
}

void FaultInjector::inject_partition() {
    if (nodes_.size() < 2) return;

    // Split into two random groups using the PRNG.
    std::vector<NodeId> ids;
    for (auto* n : nodes_) ids.push_back(n->id());
    sim_.prng().shuffle(ids);

    std::size_t split = static_cast<std::size_t>(sim_.prng().range(1, ids.size()));
    std::vector<NodeId> g0(ids.begin(), ids.begin() + static_cast<std::ptrdiff_t>(split));
    std::vector<NodeId> g1(ids.begin() + static_cast<std::ptrdiff_t>(split), ids.end());

    net_.partition({g0, g1});

    SimTime dur = sim_.prng().range(cfg_.partition_duration / 2, cfg_.partition_duration * 2);
    sim_.schedule(dur, [this]() { net_.heal(); });
}

void FaultInjector::inject_crash_restart() {
    if (nodes_.empty()) return;
    std::size_t idx = static_cast<std::size_t>(sim_.prng().range(0, nodes_.size()));
    auto* node = nodes_[idx];

    if (!node->is_running()) return;
    node->crash();

    SimTime down_time = sim_.prng().range(cfg_.crash_min_down, cfg_.crash_max_down);
    sim_.schedule(down_time, [node]() { node->restart(); });
}

void FaultInjector::inject_drop_spike() {
    double old_drop = net_.config().drop_prob;
    net_.set_drop_prob(std::min(old_drop + 0.3, 0.6));

    SimTime dur = sim_.prng().range(100 * kMsec, 400 * kMsec);
    sim_.schedule(dur, [this, old_drop]() { net_.set_drop_prob(old_drop); });
}

void FaultInjector::inject_dup_spike() {
    double old_dup = net_.config().dup_prob;
    net_.set_dup_prob(std::min(old_dup + 0.2, 0.4));

    SimTime dur = sim_.prng().range(100 * kMsec, 400 * kMsec);
    sim_.schedule(dur, [this, old_dup]() { net_.set_dup_prob(old_dup); });
}
