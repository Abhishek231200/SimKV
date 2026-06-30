#pragma once
#include "harness/history.hpp"
#include <optional>
#include <string>
#include <vector>

// Per-key Wing-Gong linearizability checker.
// Decomposes the history by key (ops on different keys are independent) and
// checks each key's history independently. This keeps individual sub-histories
// short, making the O(n!) backtracking tractable.
//
// KV model spec:
//   State: optional<string> value (absent = no write yet)
//   Put(v)             → sets value to v; returns "ok"
//   Get()              → returns current value (or ""); state unchanged
//   Cas(exp, new)      → if current == exp: sets new, returns "ok"
//                        else: returns "cas_mismatch"
struct CheckResult {
    bool        ok;
    std::string failing_key;  // first key that failed, if !ok
    std::string reason;
};

CheckResult check_linearizable(const History& history);

// Per-key check exposed for unit testing.
struct KeyOp {
    OpKind      kind;
    std::string value;    // Put/Cas new value
    std::string expected; // Cas expected value
    SimTime     invoke;
    SimTime     ret;
    std::string response; // observed response
    bool        ok;       // reported success flag
    bool        pending;
};

bool check_key_linearizable(const std::vector<KeyOp>& ops);
