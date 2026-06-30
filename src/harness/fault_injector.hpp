#pragma once
#include "raft/node.hpp"
#include "sim/network.hpp"
#include "sim/simulator.hpp"
#include <vector>

struct FaultConfig {
    double  fault_rate        = 0.05; // probability per interval of injecting a fault
    SimTime check_interval    = 200 * kMsec;
    SimTime partition_duration = 800 * kMsec;
    SimTime crash_min_down    = 300 * kMsec;
    SimTime crash_max_down    = 1 * kSec;
    double  partition_prob    = 0.4;  // of faults, partition vs crash-restart
    double  drop_raise_prob   = 0.2;  // temporary packet loss increase
    double  dup_raise_prob    = 0.1;
};

// Schedules random faults driven entirely by the simulator's PRNG.
// Every fault type, timing, and target is deterministic given the seed.
class FaultInjector {
public:
    FaultInjector(Simulator& sim, Network& net,
                  std::vector<RaftNode*> nodes, FaultConfig cfg);

    void start();
    void stop();

private:
    void tick();
    void inject_partition();
    void inject_crash_restart();
    void inject_drop_spike();
    void inject_dup_spike();

    Simulator&             sim_;
    Network&               net_;
    std::vector<RaftNode*> nodes_;
    FaultConfig            cfg_;
    bool                   running_ = false;
    EventId                tick_id_ = 0;
};
