#pragma once
#include <cstdint>
#include <optional>
#include <string>
#include <variant>

using Term  = uint64_t;
using Index = uint64_t; // 1-based; 0 is the "empty" sentinel

// The state-machine command stored in each Raft log entry.
struct CmdPut    { std::string key; std::string value; };
struct CmdDelete { std::string key; };
struct CmdCas    { std::string key; std::string expected; std::string value; };
// Get is routed through Raft log for linearizability. KvStore::apply returns the value.
struct CmdGet    { std::string key; };

using Command = std::variant<CmdPut, CmdDelete, CmdCas, CmdGet>;

// Result of applying a command to the KvStore.
struct CmdResult {
    bool        ok      = true;
    std::string value;  // for Get responses (not a command, but reused here)
    std::string error;
};

enum class Role { Follower, Candidate, Leader };

struct LogEntry {
    Term    term;
    Index   index;
    Command cmd;
};
