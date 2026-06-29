#pragma once
#include "raft/types.hpp"
#include <map>
#include <optional>
#include <string>

// In-memory key-value state machine.
// Commands are applied in log order. Reads reflect the applied state.
// A Get is not a Command; it reads directly from the map (after confirming
// leadership via a heartbeat round — see node.hpp for the client path).
class KvStore {
public:
    std::optional<std::string> get(const std::string& key) const;

    // Apply returns the result of the operation (for CAS: ok/fail; for Put/Delete: always ok).
    CmdResult apply(const Command& cmd);

    // Snapshot the entire store (used for log compaction, Phase 6 stretch).
    const std::map<std::string, std::string>& data() const { return data_; }

    void load(std::map<std::string, std::string> snapshot) { data_ = std::move(snapshot); }

private:
    std::map<std::string, std::string> data_; // std::map keeps deterministic iteration order
};
