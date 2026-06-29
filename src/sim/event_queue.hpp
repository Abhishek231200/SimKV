#pragma once
#include "sim/time.hpp"
#include <cstdint>
#include <functional>
#include <queue>
#include <unordered_set>
#include <vector>

using EventId = uint64_t;

struct Event {
    SimTime  time;
    uint64_t seq;       // global monotonic counter; deterministic tie-breaker
    std::function<void()> action;
    bool     cancelled = false;
};

// Min-heap ordered by (time, seq). Lazy cancellation via the cancelled flag.
// The seq field ensures that ties in time never depend on pointer or hash order.
class EventQueue {
public:
    EventId push(SimTime time, uint64_t seq, std::function<void()> action);
    void    cancel(EventId id);
    bool    empty() const;
    Event   pop_min();   // caller must check cancelled before invoking action
    SimTime peek_time() const; // time of next event; UB if empty

private:
    struct Cmp {
        bool operator()(const Event& a, const Event& b) const {
            if (a.time != b.time) return a.time > b.time; // min-heap
            return a.seq > b.seq;
        }
    };
    std::priority_queue<Event, std::vector<Event>, Cmp> heap_;
    // EventId == seq for simplicity.
    std::unordered_set<uint64_t> cancelled_;
};
