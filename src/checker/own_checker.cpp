#include "checker/own_checker.hpp"
#include <algorithm>
#include <map>
#include <optional>
#include <vector>

// ──────────────────────────────────────────────────────────────────────────────
// KV state-machine model
// ──────────────────────────────────────────────────────────────────────────────

using KvState = std::optional<std::string>; // nullopt = absent

// Apply op to state, return (new_state, expected_response).
// If the response doesn't match observed, the linearization point is invalid.
static std::pair<KvState, std::string> apply_model(const KvState& state, const KeyOp& op) {
    switch (op.kind) {
    case OpKind::Put:
        return {op.value, "ok"};
    case OpKind::Get: {
        std::string val = state.value_or("");
        return {state, val};
    }
    case OpKind::Cas: {
        std::string current = state.value_or("");
        if (current == op.expected) {
            return {op.value, "ok"};
        }
        return {state, "cas_mismatch"};
    }
    }
    return {state, ""};
}

// Check if the observed response is consistent with the model's expected response.
static bool response_matches(const KeyOp& op, const std::string& expected_resp) {
    // For gets: the response is the value read; compare directly.
    // For puts: response should be "ok".
    // For cas: "ok" or "cas_mismatch" — the observed ok flag is authoritative.
    if (op.kind == OpKind::Get) {
        // Get returns the value. op.response holds the value string.
        return op.response == expected_resp;
    }
    if (op.kind == OpKind::Put) {
        return op.ok; // Put always succeeds
    }
    if (op.kind == OpKind::Cas) {
        bool model_ok = (expected_resp == "ok");
        return op.ok == model_ok;
    }
    return false;
}

// ──────────────────────────────────────────────────────────────────────────────
// Wing-Gong backtracking algorithm
// ──────────────────────────────────────────────────────────────────────────────
//
// We maintain a bitmask of which ops have been "linearized" so far.
// At each step, we find the set of ops that are "eligible" (no unlinearized
// predecessor whose return time is strictly before this op's invoke time) and
// try to extend the linearization with each one.

static bool wg_check(const std::vector<KeyOp>& ops,
                     std::vector<bool>& linearized,
                     std::size_t n_done,
                     KvState state) {
    if (n_done == ops.size()) return true;

    for (std::size_t i = 0; i < ops.size(); ++i) {
        if (linearized[i]) continue;
        if (ops[i].pending) continue; // skip in-flight ops

        // op i is eligible if no unlinearized op j has ret[j] < invoke[i].
        // i.e. i is not forced to come after any not-yet-placed op j.
        bool eligible = true;
        for (std::size_t j = 0; j < ops.size(); ++j) {
            if (j == i || linearized[j] || ops[j].pending) continue;
            if (ops[j].ret > 0 && ops[j].ret < ops[i].invoke) {
                eligible = false;
                break;
            }
        }
        if (!eligible) continue;

        // Try linearizing op i at this point.
        auto [new_state, expected_resp] = apply_model(state, ops[i]);
        if (!response_matches(ops[i], expected_resp)) continue;

        linearized[i] = true;
        if (wg_check(ops, linearized, n_done + 1, new_state)) return true;
        linearized[i] = false;
    }
    return false;
}

// Separate ops into "certain" (definitive response) and "uncertain" (node_crashed Put).
// For uncertain Puts we enumerate subsets — they might have been committed and applied,
// or they might not have (the node crashed between commit and reply).
// Gets and Cas with node_crashed are excluded: Get outcome can't be verified, and Cas
// with unknown outcome is handled by the definitive Gets that follow it.
bool check_key_linearizable(const std::vector<KeyOp>& ops) {
    std::vector<KeyOp> certain;
    std::vector<KeyOp> uncertain_puts; // node_crashed Puts (may or may not have committed)

    for (const auto& op : ops) {
        if (op.pending) continue;
        if (op.response == "timeout" || op.response == "not_leader") continue;
        if (op.response == "node_crashed") {
            if (op.kind == OpKind::Put) {
                // Put: if committed, state changed to op.value. Try both options.
                uncertain_puts.push_back(op);
            }
            // Uncertain Get/Cas: skip — we can't determine their committed value.
            continue;
        }
        certain.push_back(op);
    }

    if (certain.empty() && uncertain_puts.empty()) return true;

    // Enumerate all 2^N subsets of uncertain Puts to try including.
    // N is typically 0-3 under moderate fault injection; capped at 20 for safety.
    int n_unc = std::min(static_cast<int>(uncertain_puts.size()), 20);

    for (int mask = 0; mask < (1 << n_unc); ++mask) {
        // Build combined set: certain ops + included uncertain Puts (with ok=true).
        std::vector<KeyOp> combined = certain;
        for (int i = 0; i < n_unc; ++i) {
            if (mask & (1 << i)) {
                KeyOp opt = uncertain_puts[i];
                opt.ok = true; // assume committed and succeeded
                combined.push_back(opt);
            }
        }

        if (combined.empty()) { return true; }

        if (combined.size() > 30) {
            // Fallback for large histories: sequential consistency (sort by ret time).
            std::vector<KeyOp> sorted = combined;
            std::sort(sorted.begin(), sorted.end(),
                      [](const KeyOp& a, const KeyOp& b) { return a.ret < b.ret; });
            KvState state;
            bool ok = true;
            for (const auto& op : sorted) {
                auto [ns, expected] = apply_model(state, op);
                if (!response_matches(op, expected)) { ok = false; break; }
                state = ns;
            }
            if (ok) return true;
            continue;
        }

        std::vector<bool> linearized(combined.size(), false);
        if (wg_check(combined, linearized, 0, std::nullopt)) return true;
    }

    return false;
}

// ──────────────────────────────────────────────────────────────────────────────
// Full history checker
// ──────────────────────────────────────────────────────────────────────────────

CheckResult check_linearizable(const History& history) {
    // Decompose by key.
    std::map<std::string, std::vector<KeyOp>> per_key;
    for (const auto& e : history.entries()) {
        if (e.pending) continue;
        per_key[e.key].push_back(KeyOp{
            e.kind, e.value, e.expected, e.invoke, e.ret,
            e.response, e.ok, e.pending
        });
    }

    for (const auto& [key, ops] : per_key) {
        if (!check_key_linearizable(ops)) {
            return CheckResult{false, key,
                               "non-linearizable history for key " + key};
        }
    }
    return CheckResult{true, "", ""};
}
