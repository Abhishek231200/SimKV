#include "sim/network.hpp"

Network::Network(Simulator& sim, NetworkConfig cfg)
    : sim_(sim), cfg_(std::move(cfg)) {}

void Network::set_handler(NodeId id, std::function<void(Message)> on_deliver) {
    handlers_[id] = std::move(on_deliver);
}

void Network::remove_handler(NodeId id) {
    handlers_.erase(id);
}

void Network::deliver_once(NodeId to, std::vector<uint8_t> payload) {
    auto it = handlers_.find(to);
    if (it == handlers_.end()) return; // node not registered / crashed
    auto& fn = it->second;
    // Capture by value so the closure owns the payload.
    sim_.schedule(sim_.prng().range(cfg_.min_latency, cfg_.max_latency + 1),
                  [fn, to, p = std::move(payload)]() mutable {
                      fn(Message{NodeId(-1), to, std::move(p)});
                  });
}

void Network::send(NodeId from, NodeId to, std::vector<uint8_t> payload) {
    if (partitioned(from, to)) return;
    if (sim_.prng().bernoulli(cfg_.drop_prob)) return;

    // Stamp from into a copy before potentially duplicating.
    auto send_copy = [&](std::vector<uint8_t> p) {
        auto it = handlers_.find(to);
        if (it == handlers_.end()) return;
        auto& fn = it->second;
        SimTime lat = sim_.prng().range(cfg_.min_latency, cfg_.max_latency + 1);
        NodeId  f   = from;
        sim_.schedule(lat, [fn, f, to, p = std::move(p)]() mutable {
            fn(Message{f, to, std::move(p)});
        });
    };

    send_copy(payload); // primary delivery

    if (sim_.prng().bernoulli(cfg_.dup_prob)) {
        send_copy(payload); // duplicate
    }
}

void Network::partition(std::vector<std::vector<NodeId>> groups) {
    partition_group_.clear();
    for (int g = 0; g < static_cast<int>(groups.size()); ++g) {
        for (NodeId id : groups[g]) {
            partition_group_[id] = g;
        }
    }
    partitioned_ = true;
}

void Network::heal() {
    partition_group_.clear();
    partitioned_ = false;
}

bool Network::partitioned(NodeId from, NodeId to) const {
    if (!partitioned_) return false;
    auto it_f = partition_group_.find(from);
    auto it_t = partition_group_.find(to);
    // Nodes not in any listed group are isolated (they can't reach anyone).
    if (it_f == partition_group_.end() || it_t == partition_group_.end()) return true;
    return it_f->second != it_t->second;
}
