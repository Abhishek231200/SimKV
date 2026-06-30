#pragma once
#include "harness/history.hpp"
#include "raft/node.hpp"
#include "sim/network.hpp"
#include "sim/simulator.hpp"
#include <functional>
#include <map>
#include <vector>

struct WorkloadConfig {
    std::size_t num_clients   = 4;
    std::size_t num_keys      = 8;   // small key space keeps histories short for checking
    std::size_t total_ops     = 200;
    SimTime     think_time    = 5 * kMsec; // delay between ops per client
    double      put_prob      = 0.4;
    double      get_prob      = 0.4;
    // cas_prob = 1 - put_prob - get_prob
};

// Assigns synthetic NodeIds to simulated clients (separate range from Raft nodes).
inline constexpr NodeId kClientIdBase = 100;

// Drives N simulated clients issuing random operations against the cluster.
// Closed-loop: one outstanding operation per client at a time.
// Retries on redirect (not-leader reply) and on timeout.
class Workload {
public:
    Workload(Simulator& sim, Network& net, History& history,
             std::vector<RaftNode*> nodes, WorkloadConfig cfg);

    void start(); // schedules the initial client operations
    bool done() const { return ops_issued_ >= cfg_.total_ops && pending_ == 0; }
    std::size_t ops_completed() const { return ops_completed_; }

private:
    struct ClientState {
        NodeId       believed_leader; // which node to send to
        uint64_t     next_request_id = 0;
        bool         waiting         = false; // currently waiting for a reply
        std::size_t  history_handle  = 0;
        SimTime      sent_at         = 0;
        EventId      timeout_id      = 0;
    };

    void issue_op(std::size_t client_idx);
    void on_reply(std::size_t client_idx, Message msg);
    void on_timeout(std::size_t client_idx);

    std::string random_key();
    std::string random_value();
    OpKind      random_op_kind();

    Simulator&              sim_;
    Network&                net_;
    History&                history_;
    std::vector<RaftNode*>  nodes_;
    WorkloadConfig          cfg_;

    std::vector<ClientState>             clients_;
    std::size_t                          ops_issued_    = 0;
    std::size_t                          ops_completed_ = 0;
    std::size_t                          pending_       = 0;

    static constexpr SimTime kOpTimeout = 2 * kSec;
};
