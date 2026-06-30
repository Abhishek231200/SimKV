#pragma once
#include "sim/simulator.hpp"
#include <cstdint>
#include <functional>
#include <map>
#include <set>
#include <vector>

using NodeId = uint32_t;

struct Message;

// Abstract transport interface — used by RaftNode so it can run against either
// the simulated Network or a real TcpTransport.
class ITransport {
public:
    virtual ~ITransport() = default;
    virtual void send(NodeId from, NodeId to, std::vector<uint8_t> payload) = 0;
    virtual void set_handler(NodeId id, std::function<void(Message)> handler) = 0;
    virtual void remove_handler(NodeId id) = 0;
};

struct NetworkConfig {
    SimTime min_latency  = 1 * kMsec;
    SimTime max_latency  = 20 * kMsec;
    double  drop_prob    = 0.0;
    double  dup_prob     = 0.0;
};

struct Message {
    NodeId from;
    NodeId to;
    std::vector<uint8_t> payload;
};

// Simulated network with configurable fault injection.
// All randomness drawn from the owning Simulator's PRNG — never independently seeded.
// Partition state is maintained as a group assignment: messages only flow within a group.
class Network : public ITransport {
public:
    Network(Simulator& sim, NetworkConfig cfg);

    // Register a delivery callback for a node.
    void set_handler(NodeId id, std::function<void(Message)> on_deliver) override;
    void remove_handler(NodeId id) override;

    void send(NodeId from, NodeId to, std::vector<uint8_t> payload) override;

    // Partition: split nodes into groups. Cross-group messages are silently dropped.
    // Node IDs not listed get their own singleton group (isolated).
    void partition(std::vector<std::vector<NodeId>> groups);
    void heal(); // Remove all partitions.

    // Temporarily raise fault levels.
    void set_drop_prob(double p)   { cfg_.drop_prob = p; }
    void set_dup_prob(double p)    { cfg_.dup_prob  = p; }
    void set_latency(SimTime mn, SimTime mx) { cfg_.min_latency = mn; cfg_.max_latency = mx; }

    NetworkConfig& config() { return cfg_; }

private:
    bool partitioned(NodeId from, NodeId to) const;
    void deliver_once(NodeId to, std::vector<uint8_t> payload);

    Simulator&    sim_;
    NetworkConfig cfg_;
    // Sorted map to avoid hash-order non-determinism.
    std::map<NodeId, std::function<void(Message)>> handlers_;
    // partition_group_[node] = group index; -1 means not partitioned.
    std::map<NodeId, int> partition_group_;
    bool partitioned_ = false;
};
