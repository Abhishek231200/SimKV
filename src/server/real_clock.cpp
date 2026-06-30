#include "server/real_clock.hpp"

RealClock::RealClock(DispatchQueue& dq, uint64_t seed)
    : dq_(dq), rng_(seed), thread_([this] { run(); }) {}

RealClock::~RealClock() {
    {
        std::lock_guard<std::mutex> lk(mu_);
        running_ = false;
    }
    cv_.notify_all();
    if (thread_.joinable()) thread_.join();
}

EventId RealClock::schedule(SimTime delay_us, std::function<void()> action) {
    EventId id = next_id_.fetch_add(1, std::memory_order_relaxed);
    auto deadline = std::chrono::steady_clock::now()
                  + std::chrono::microseconds(delay_us);
    {
        std::lock_guard<std::mutex> lk(mu_);
        heap_.push({deadline, id, std::move(action)});
    }
    cv_.notify_one();
    return id;
}

void RealClock::cancel(EventId id) {
    std::lock_guard<std::mutex> lk(mu_);
    cancelled_.insert(id);
}

uint64_t RealClock::random_range(uint64_t lo, uint64_t hi) {
    // hi is exclusive (matching Prng::range semantics used by RaftNode)
    if (hi <= lo) return lo;
    std::uniform_int_distribution<uint64_t> dist(lo, hi - 1);
    std::lock_guard<std::mutex> lk(mu_);
    return dist(rng_);
}

void RealClock::run() {
    std::unique_lock<std::mutex> lk(mu_);
    while (running_) {
        if (heap_.empty()) {
            cv_.wait(lk, [this] { return !heap_.empty() || !running_; });
            continue;
        }

        auto deadline = heap_.top().deadline;
        auto now = std::chrono::steady_clock::now();
        if (deadline > now) {
            // Wake early only if a STRICTLY earlier timer arrives; using <= would
            // cause an infinite spin because heap_.top() == deadline is always true.
            cv_.wait_until(lk, deadline, [this, &deadline] {
                return !running_ || (!heap_.empty() && heap_.top().deadline < deadline);
            });
            continue;
        }

        // Pop the earliest-deadline entry.
        Entry e = heap_.top();
        heap_.pop();

        if (cancelled_.count(e.id)) {
            cancelled_.erase(e.id);
            continue;
        }

        // Deliver via dispatch queue (unlocked so we don't deadlock).
        lk.unlock();
        dq_.post(std::move(e.action));
        lk.lock();
    }
}
