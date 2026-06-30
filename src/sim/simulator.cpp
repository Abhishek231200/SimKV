#include "sim/simulator.hpp"

Simulator::Simulator(uint64_t seed) : trace_hash_(kFnvOffset), prng_(seed) {}

EventId Simulator::schedule(SimTime delay, std::function<void()> action) {
    return schedule_at(clock_ + delay, std::move(action));
}

EventId Simulator::schedule_at(SimTime when, std::function<void()> action) {
    uint64_t seq = next_seq_++;
    return queue_.push(when, seq, std::move(action));
}

void Simulator::cancel(EventId id) {
    queue_.cancel(id);
}

void Simulator::process_one() {
    Event e = queue_.pop_min();
    clock_ = e.time;
    if (e.cancelled) return;

    // Update trace hash with (time, seq) — this is the determinism fingerprint.
    auto fold = [&](uint64_t v) {
        for (int i = 0; i < 8; ++i) {
            trace_hash_ ^= (v & 0xFF);
            trace_hash_ *= kFnvPrime;
            v >>= 8;
        }
    };
    fold(e.time);
    fold(e.seq);

    e.action();
}

void Simulator::run_until_idle() {
    while (!queue_.empty()) {
        process_one();
    }
}

uint64_t Simulator::random_range(uint64_t lo, uint64_t hi) {
    return prng_.range(lo, hi);
}

void Simulator::run_for(SimTime duration) {
    SimTime deadline = clock_ + duration;
    while (!queue_.empty() && queue_.peek_time() <= deadline) {
        process_one();
    }
    clock_ = deadline;
}
