#include "server/dispatch_queue.hpp"
#include <chrono>

void DispatchQueue::post(std::function<void()> f) {
    {
        std::lock_guard<std::mutex> lk(mu_);
        if (stopped_) return;
        q_.push_back(std::move(f));
    }
    cv_.notify_one();
}

void DispatchQueue::run_once(uint64_t timeout_us) {
    std::unique_lock<std::mutex> lk(mu_);

    // Drain all currently pending work first.
    while (!q_.empty()) {
        auto f = std::move(q_.front());
        q_.pop_front();
        lk.unlock();
        f();
        lk.lock();
    }

    if (stopped_) return;

    // Wait up to timeout_us for new work.
    auto deadline = std::chrono::steady_clock::now()
                  + std::chrono::microseconds(timeout_us);
    cv_.wait_until(lk, deadline, [this] { return !q_.empty() || stopped_; });

    // Drain whatever arrived while we were waiting.
    while (!q_.empty()) {
        auto f = std::move(q_.front());
        q_.pop_front();
        lk.unlock();
        f();
        lk.lock();
    }
}

void DispatchQueue::stop() {
    {
        std::lock_guard<std::mutex> lk(mu_);
        stopped_ = true;
    }
    cv_.notify_all();
}

bool DispatchQueue::stopped() const {
    std::lock_guard<std::mutex> lk(mu_);
    return stopped_;
}
