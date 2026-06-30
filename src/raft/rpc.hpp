#pragma once
#include "raft/types.hpp"
#include "sim/network.hpp"
#include <cstdint>
#include <functional>
#include <optional>
#include <stdexcept>
#include <vector>

// RPC message type tag (first byte of every network payload).
enum class MsgType : uint8_t {
    RequestVote        = 1,
    RequestVoteReply   = 2,
    AppendEntries      = 3,
    AppendEntriesReply = 4,
    ClientRequest      = 5,
    ClientReply        = 6,
    InstallSnapshot      = 7,
    InstallSnapshotReply = 8,
    ReadIndexRequest     = 9,
    ReadIndexReply       = 10,
};

// ───────────────────────── RequestVote ──────────────────────────────────────
struct RequestVote {
    Term   term;
    NodeId candidate_id;
    Index  last_log_index;
    Term   last_log_term;

    std::vector<uint8_t> encode() const;
    static RequestVote decode(const uint8_t* data, std::size_t size);
};

struct RequestVoteReply {
    Term term;
    bool vote_granted;

    std::vector<uint8_t> encode() const;
    static RequestVoteReply decode(const uint8_t* data, std::size_t size);
};

// ───────────────────────── AppendEntries ────────────────────────────────────
struct AppendEntries {
    Term                 term;
    NodeId               leader_id;
    Index                prev_log_index;
    Term                 prev_log_term;
    std::vector<LogEntry> entries;
    Index                leader_commit;

    std::vector<uint8_t> encode() const;
    static AppendEntries decode(const uint8_t* data, std::size_t size);
};

struct AppendEntriesReply {
    Term  term;
    bool  success;
    Index conflict_index; // for fast backtracking; 0 if success
    Term  conflict_term;  // 0 if success or no conflicting term

    std::vector<uint8_t> encode() const;
    static AppendEntriesReply decode(const uint8_t* data, std::size_t size);
};

// ───────────────────────── Client ───────────────────────────────────────────
struct ClientRequest {
    uint64_t client_id;
    uint64_t request_id; // per-client monotonic counter for dedup
    Command  cmd;

    std::vector<uint8_t> encode() const;
    static ClientRequest decode(const uint8_t* data, std::size_t size);
};

struct ClientReply {
    uint64_t   client_id;
    uint64_t   request_id;
    bool       success;
    CmdResult  result;
    NodeId     leader_hint; // set to known leader if redirecting; 0 if unknown

    std::vector<uint8_t> encode() const;
    static ClientReply decode(const uint8_t* data, std::size_t size);
};

// ───────────────────────── InstallSnapshot ──────────────────────────────────
// Sent by the leader to a follower whose next_index has fallen behind the
// leader's snapshot point. The follower replaces its state machine with the
// snapshot and discards any log entries it had before last_included_index.
struct InstallSnapshot {
    Term                 term;
    NodeId               leader_id;
    Index                last_included_index;
    Term                 last_included_term;
    std::vector<uint8_t> data; // serialized KV state (Encoder-format)

    std::vector<uint8_t> encode() const;
    static InstallSnapshot decode(const uint8_t* data, std::size_t size);
};

struct InstallSnapshotReply {
    Term  term;
    Index last_included_index; // echoed back so leader can advance matchIndex

    std::vector<uint8_t> encode() const;
    static InstallSnapshotReply decode(const uint8_t* data, std::size_t size);
};

// ───────────────────────── ReadIndex ────────────────────────────────────────
// Leader broadcasts ReadIndexRequest to all peers to confirm it is still leader
// before serving a linearizable read. Peers reply with ReadIndexReply; once
// a majority (including self) have replied with matching round, the read is safe.
struct ReadIndexRequest {
    Term     term;
    NodeId   leader_id;
    uint64_t round; // per-leader monotone counter; reply echoes it back

    std::vector<uint8_t> encode() const;
    static ReadIndexRequest decode(const uint8_t* data, std::size_t size);
};

struct ReadIndexReply {
    Term     term;
    uint64_t round;

    std::vector<uint8_t> encode() const;
    static ReadIndexReply decode(const uint8_t* data, std::size_t size);
};

// Decode the MsgType tag from the first byte of a payload.
inline MsgType peek_msg_type(const std::vector<uint8_t>& payload) {
    if (payload.empty()) throw std::runtime_error("empty message");
    return static_cast<MsgType>(payload[0]);
}
