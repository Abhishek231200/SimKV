#include "raft/rpc.hpp"
#include "storage/codec.hpp"
#include "storage/wal.hpp"

// ───────────────────────── LogEntry encode/decode helpers ───────────────────
static void encode_log_entry(Encoder& enc, const LogEntry& e) {
    enc.u64(e.term);
    enc.u64(e.index);
    auto cmd_bytes = Wal::encode_command(e.cmd);
    enc.u32(static_cast<uint32_t>(cmd_bytes.size()));
    for (uint8_t b : cmd_bytes) enc.u8(b);
}

static LogEntry decode_log_entry(Decoder& dec) {
    LogEntry e;
    e.term  = dec.u64();
    e.index = dec.u64();
    uint32_t cmd_len = dec.u32();
    auto sub = dec.slice(cmd_len);
    // Reconstruct bytes to pass to decode_command.
    // We need a contiguous buffer — use the slice's remaining view.
    // Since Decoder::slice advances the outer, we need the raw pointer.
    // Workaround: use the Wal helper which accepts raw pointer + size.
    // The slice Decoder starts at the right position in the original buffer.
    // Re-encode to get bytes (wasteful but correct and simple).
    std::vector<uint8_t> cmd_buf;
    while (!sub.empty()) cmd_buf.push_back(sub.u8());
    e.cmd = Wal::decode_command(cmd_buf.data(), cmd_buf.size());
    return e;
}

// ───────────────────────── RequestVote ──────────────────────────────────────
std::vector<uint8_t> RequestVote::encode() const {
    Encoder enc;
    enc.u8(static_cast<uint8_t>(MsgType::RequestVote));
    enc.u64(term);
    enc.u32(candidate_id);
    enc.u64(last_log_index);
    enc.u64(last_log_term);
    return enc.take();
}

RequestVote RequestVote::decode(const uint8_t* data, std::size_t size) {
    Decoder dec(data, size);
    dec.u8(); // skip tag
    RequestVote rv;
    rv.term            = dec.u64();
    rv.candidate_id    = dec.u32();
    rv.last_log_index  = dec.u64();
    rv.last_log_term   = dec.u64();
    return rv;
}

// ───────────────────────── RequestVoteReply ─────────────────────────────────
std::vector<uint8_t> RequestVoteReply::encode() const {
    Encoder enc;
    enc.u8(static_cast<uint8_t>(MsgType::RequestVoteReply));
    enc.u64(term);
    enc.boolean(vote_granted);
    return enc.take();
}

RequestVoteReply RequestVoteReply::decode(const uint8_t* data, std::size_t size) {
    Decoder dec(data, size);
    dec.u8();
    RequestVoteReply r;
    r.term         = dec.u64();
    r.vote_granted = dec.boolean();
    return r;
}

// ───────────────────────── AppendEntries ────────────────────────────────────
std::vector<uint8_t> AppendEntries::encode() const {
    Encoder enc;
    enc.u8(static_cast<uint8_t>(MsgType::AppendEntries));
    enc.u64(term);
    enc.u32(leader_id);
    enc.u64(prev_log_index);
    enc.u64(prev_log_term);
    enc.u64(leader_commit);
    enc.u32(static_cast<uint32_t>(entries.size()));
    for (const auto& e : entries) encode_log_entry(enc, e);
    return enc.take();
}

AppendEntries AppendEntries::decode(const uint8_t* data, std::size_t size) {
    Decoder dec(data, size);
    dec.u8();
    AppendEntries ae;
    ae.term           = dec.u64();
    ae.leader_id      = dec.u32();
    ae.prev_log_index = dec.u64();
    ae.prev_log_term  = dec.u64();
    ae.leader_commit  = dec.u64();
    uint32_t count    = dec.u32();
    ae.entries.reserve(count);
    for (uint32_t i = 0; i < count; ++i) {
        ae.entries.push_back(decode_log_entry(dec));
    }
    return ae;
}

// ───────────────────────── AppendEntriesReply ───────────────────────────────
std::vector<uint8_t> AppendEntriesReply::encode() const {
    Encoder enc;
    enc.u8(static_cast<uint8_t>(MsgType::AppendEntriesReply));
    enc.u64(term);
    enc.boolean(success);
    enc.u64(conflict_index);
    enc.u64(conflict_term);
    return enc.take();
}

AppendEntriesReply AppendEntriesReply::decode(const uint8_t* data, std::size_t size) {
    Decoder dec(data, size);
    dec.u8();
    AppendEntriesReply r;
    r.term           = dec.u64();
    r.success        = dec.boolean();
    r.conflict_index = dec.u64();
    r.conflict_term  = dec.u64();
    return r;
}

// ───────────────────────── ClientRequest ────────────────────────────────────
std::vector<uint8_t> ClientRequest::encode() const {
    Encoder enc;
    enc.u8(static_cast<uint8_t>(MsgType::ClientRequest));
    enc.u64(client_id);
    enc.u64(request_id);
    auto cmd_bytes = Wal::encode_command(cmd);
    enc.u32(static_cast<uint32_t>(cmd_bytes.size()));
    for (uint8_t b : cmd_bytes) enc.u8(b);
    return enc.take();
}

ClientRequest ClientRequest::decode(const uint8_t* data, std::size_t size) {
    Decoder dec(data, size);
    dec.u8();
    ClientRequest cr;
    cr.client_id  = dec.u64();
    cr.request_id = dec.u64();
    uint32_t len  = dec.u32();
    auto sub      = dec.slice(len);
    std::vector<uint8_t> cmd_buf;
    while (!sub.empty()) cmd_buf.push_back(sub.u8());
    cr.cmd = Wal::decode_command(cmd_buf.data(), cmd_buf.size());
    return cr;
}

// ───────────────────────── ClientReply ──────────────────────────────────────
std::vector<uint8_t> ClientReply::encode() const {
    Encoder enc;
    enc.u8(static_cast<uint8_t>(MsgType::ClientReply));
    enc.u64(client_id);
    enc.u64(request_id);
    enc.boolean(success);
    enc.boolean(result.ok);
    enc.str(result.value);
    enc.str(result.error);
    enc.u32(leader_hint);
    return enc.take();
}

ClientReply ClientReply::decode(const uint8_t* data, std::size_t size) {
    Decoder dec(data, size);
    dec.u8();
    ClientReply r;
    r.client_id   = dec.u64();
    r.request_id  = dec.u64();
    r.success     = dec.boolean();
    r.result.ok   = dec.boolean();
    r.result.value = dec.str();
    r.result.error = dec.str();
    r.leader_hint  = dec.u32();
    return r;
}
