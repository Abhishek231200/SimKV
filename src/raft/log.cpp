#include "raft/log.hpp"
#include "storage/codec.hpp"
#include "storage/durable_store.hpp"
#include "storage/wal.hpp"
#include <cassert>
#include <stdexcept>

void RaftLog::append(LogEntry entry) {
    assert(entry.index == last_index() + 1);
    entries_.push_back(std::move(entry));
}

void RaftLog::truncate_from(Index from) {
    if (from == 0 || from > last_index()) return;
    entries_.erase(entries_.begin() + static_cast<std::ptrdiff_t>(from - 1),
                   entries_.end());
}

const LogEntry& RaftLog::at(Index i) const {
    if (i == 0 || i > last_index())
        throw std::out_of_range("RaftLog::at: index out of range");
    return entries_[i - 1];
}

bool RaftLog::contains(Index index, Term term) const {
    if (index == 0) return true; // empty sentinel always matches
    if (index > last_index()) return false;
    return entries_[index - 1].term == term;
}

std::vector<LogEntry> RaftLog::slice(Index lo, Index hi) const {
    // Returns entries in (lo, hi] — half-open from above, inclusive below.
    if (lo >= hi || hi == 0) return {};
    Index start = lo + 1;
    Index end   = std::min(hi, last_index());
    if (start > end) return {};
    return {entries_.begin() + static_cast<std::ptrdiff_t>(start - 1),
            entries_.begin() + static_cast<std::ptrdiff_t>(end)};
}

Index RaftLog::last_index_for_term(Term term) const {
    for (auto it = entries_.rbegin(); it != entries_.rend(); ++it) {
        if (it->term == term) return it->index;
    }
    return 0;
}

void RaftLog::save_to(DurableStore& store) const {
    Encoder enc;
    enc.u32(static_cast<uint32_t>(entries_.size()));
    for (const auto& e : entries_) {
        enc.u64(e.term);
        enc.u64(e.index);
        auto cmd = Wal::encode_command(e.cmd);
        enc.u32(static_cast<uint32_t>(cmd.size()));
        for (uint8_t b : cmd) enc.u8(b);
    }
    auto bytes = enc.take();
    store.append(bytes);
    store.flush();
}

void RaftLog::load_from(const DurableStore& store) {
    entries_.clear();
    auto data = store.read_durable();
    if (data.empty()) return;
    Decoder dec(data);
    uint32_t count = dec.u32();
    entries_.reserve(count);
    for (uint32_t i = 0; i < count; ++i) {
        LogEntry e;
        e.term  = dec.u64();
        e.index = dec.u64();
        uint32_t cmd_len = dec.u32();
        auto sub = dec.slice(cmd_len);
        std::vector<uint8_t> cmd_buf;
        while (!sub.empty()) cmd_buf.push_back(sub.u8());
        e.cmd = Wal::decode_command(cmd_buf.data(), cmd_buf.size());
        entries_.push_back(std::move(e));
    }
}
