#include "raft/log.hpp"
#include "storage/codec.hpp"
#include "storage/idurable_store.hpp"
#include "storage/wal.hpp"
#include <cassert>
#include <stdexcept>

void RaftLog::append(LogEntry entry) {
    assert(entry.index == last_index() + 1);
    entries_.push_back(std::move(entry));
}

void RaftLog::truncate_from(Index from) {
    if (from <= base_index_) return; // can't truncate already-compacted region
    if (from > last_index()) return;
    entries_.erase(entries_.begin() + static_cast<std::ptrdiff_t>(from - base_index_ - 1),
                   entries_.end());
}

void RaftLog::set_snapshot(Index last_included_index, Term last_included_term) {
    if (last_included_index <= base_index_) return; // already compacted past here
    if (last_included_index >= last_index()) {
        // Entire log is covered by snapshot.
        entries_.clear();
    } else {
        // Drop entries up to and including last_included_index.
        entries_.erase(entries_.begin(),
                       entries_.begin() +
                           static_cast<std::ptrdiff_t>(last_included_index - base_index_));
    }
    base_index_ = last_included_index;
    base_term_  = last_included_term;
}

const LogEntry& RaftLog::at(Index i) const {
    if (i <= base_index_ || i > last_index())
        throw std::out_of_range("RaftLog::at: index out of range or compacted");
    return entries_[i - base_index_ - 1];
}

Term RaftLog::term_at(Index i) const {
    if (i == base_index_) return base_term_;
    return at(i).term;
}

bool RaftLog::contains(Index index, Term term) const {
    if (index == 0) return true;                        // empty prevLog sentinel
    if (index == base_index_) return term == base_term_; // snapshot boundary
    if (index < base_index_) return false;              // compacted away
    if (index > last_index()) return false;
    return entries_[index - base_index_ - 1].term == term;
}

std::vector<LogEntry> RaftLog::slice(Index lo, Index hi) const {
    if (lo >= hi) return {};
    Index start = std::max(lo + 1, base_index_ + 1);
    Index end   = std::min(hi, last_index());
    if (start > end) return {};
    return {entries_.begin() + static_cast<std::ptrdiff_t>(start - base_index_ - 1),
            entries_.begin() + static_cast<std::ptrdiff_t>(end   - base_index_)};
}

Index RaftLog::last_index_for_term(Term term) const {
    for (auto it = entries_.rbegin(); it != entries_.rend(); ++it) {
        if (it->term == term) return it->index;
    }
    if (base_index_ > 0 && base_term_ == term) return base_index_;
    return 0;
}

void RaftLog::save_to(IDurableStore& store) const {
    Encoder enc;
    enc.u64(base_index_);
    enc.u64(base_term_);
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

void RaftLog::load_from(const IDurableStore& store) {
    entries_.clear();
    auto data = store.read_durable();
    if (data.empty()) return;
    Decoder dec(data);
    base_index_ = dec.u64();
    base_term_  = dec.u64();
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
