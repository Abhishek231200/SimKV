#pragma once
#include "sim/event_queue.hpp"
#include "sim/prng.hpp"
#include "sim/time.hpp"
#include <cstdint>
#include <functional>

// The deterministic simulation engine.
// One thread, one logical clock, one seeded PRNG, one event queue.
// Every random and time decision goes through this object.
class Simulator {
public:
    explicit Simulator(uint64_t seed);

    SimTime  now() const  { return clock_; }
    Prng&    prng()       { return prng_; }

    // Schedule action to fire at now()+delay. Returns a cancellable EventId.
    EventId schedule(SimTime delay, std::function<void()> action);

    // Schedule at a fixed absolute time (used internally, careful with ordering).
    EventId schedule_at(SimTime when, std::function<void()> action);

    void cancel(EventId id);

    // Process all events until the queue is empty.
    void run_until_idle();

    // Process events whose time <= now()+duration.
    void run_for(SimTime duration);

    // FNV-1a fingerprint of every (time, seq) pair popped, in order.
    // Same seed → same hash. Use this as the determinism guard.
    uint64_t trace_hash() const { return trace_hash_; }

private:
    void process_one();

    SimTime  clock_      = 0;
    uint64_t next_seq_   = 0;
    uint64_t trace_hash_;
    Prng     prng_;
    EventQueue queue_;

    static constexpr uint64_t kFnvOffset = 14695981039346656037ULL;
    static constexpr uint64_t kFnvPrime  = 1099511628211ULL;
};
