#pragma once
#include <condition_variable>
#include <deque>
#include <functional>
#include <mutex>

// All RaftNode calls (timer callbacks + incoming messages + client requests) are
// posted here and processed on a single dispatch thread — preserving the same
// single-threaded invariant the simulation relies on.
class DispatchQueue {
public:
    void post(std::function<void()> f);
    // Process all pending work, then wait up to timeout_us microseconds for more.
    void run_once(uint64_t timeout_us = 10'000);
    void stop();
    bool stopped() const;

private:
    mutable std::mutex      mu_;
    std::condition_variable cv_;
    std::deque<std::function<void()>> q_;
    bool stopped_ = false;
};
