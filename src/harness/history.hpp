#pragma once
#include "raft/types.hpp"
#include "sim/time.hpp"
#include <cstdint>
#include <optional>
#include <string>
#include <vector>

enum class OpKind { Put, Get, Cas };

struct HistoryEntry {
    uint64_t    client_id;
    uint64_t    request_id;
    OpKind      kind;
    std::string key;
    std::string value;    // Put/Cas: new value; Get: unused
    std::string expected; // Cas: expected value
    SimTime     invoke;   // logical time of request send
    SimTime     ret;      // logical time of response receipt; 0 if pending
    std::string response; // e.g. "ok", "cas_mismatch", value read, "timeout"
    bool        ok;       // whether the operation succeeded
    bool        pending;  // true if no response has been recorded yet
};

// Append-only log of all operations issued by simulated clients.
class History {
public:
    // Record the invocation of an operation. Returns a handle (index) for later completion.
    std::size_t record_invoke(uint64_t client_id, uint64_t request_id,
                               OpKind kind, std::string key, SimTime invoke_time,
                               std::string value = "", std::string expected = "");

    // Fill in the response for an invocation.
    void record_return(std::size_t handle, SimTime ret_time,
                       bool ok, std::string response);

    const std::vector<HistoryEntry>& entries() const { return entries_; }

    // Emit the history as a JSON string suitable for Porcupine.
    // Operations are decomposed per-key: each key gets an independent section.
    std::string to_json() const;

private:
    std::vector<HistoryEntry> entries_;
};
