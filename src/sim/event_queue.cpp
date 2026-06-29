#include "sim/event_queue.hpp"
#include <stdexcept>

EventId EventQueue::push(SimTime time, uint64_t seq, std::function<void()> action) {
    heap_.push(Event{time, seq, std::move(action), false});
    return seq; // EventId == seq
}

void EventQueue::cancel(EventId id) {
    // We can't remove from a heap in-place; mark cancelled lazily.
    // The pop_min caller checks the cancelled flag.
    // We rebuild a cancelled marker by pushing a dummy that replaces the live one.
    // Simpler: keep a separate cancelled-id set and check on pop.
    cancelled_.insert(id);
}

bool EventQueue::empty() const {
    return heap_.empty();
}

Event EventQueue::pop_min() {
    if (heap_.empty()) throw std::logic_error("pop_min on empty EventQueue");
    Event e = heap_.top();
    heap_.pop();
    if (cancelled_.count(e.seq)) {
        cancelled_.erase(e.seq);
        e.cancelled = true;
    }
    return e;
}

SimTime EventQueue::peek_time() const {
    return heap_.top().time;
}
