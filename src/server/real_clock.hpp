#pragma once
#include "server/dispatch_queue.hpp"
#include "sim/simulator.hpp"   // for IClock, EventId, SimTime
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <functional>
#include <mutex>
#include <queue>
#include <random>
#include <set>
#include <thread>

// Real-time implementation of IClock.
// Timer callbacks are delivered via DispatchQueue::post() so they land on the
// same single-threaded dispatch loop as incoming network messages.
class RealClock : public IClock {
public:
    explicit RealClock(DispatchQueue& dq, uint64_t seed = 0);
    ~RealClock();

    EventId  schedule(SimTime delay_us, std::function<void()> action) override;
    void     cancel(EventId id) override;
    uint64_t random_range(uint64_t lo, uint64_t hi) override;

private:
    void run();

    struct Entry {
        std::chrono::steady_clock::time_point deadline;
        EventId id;
        std::function<void()> action;
        bool operator>(const Entry& o) const { return deadline > o.deadline; }
    };

    DispatchQueue& dq_;
    std::mt19937_64 rng_;

    mutable std::mutex mu_;
    std::condition_variable cv_;
    std::priority_queue<Entry, std::vector<Entry>, std::greater<Entry>> heap_;
    std::set<EventId> cancelled_;
    std::atomic<EventId> next_id_{1};
    bool running_ = true;
    std::thread thread_;
};
