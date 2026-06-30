#pragma once
#include "raft/types.hpp"
#include <cassert>
#include <vector>

// Raft in-memory log container. Index is 1-based; index 0 is empty sentinel.
// Safety properties are enforced by assertions in debug builds.
class RaftLog {
public:
    RaftLog() = default;

    // Append a new entry. term/index must be consistent with existing log.
    void append(LogEntry entry);

    // Truncate entries from index `from` onward (inclusive). Used on conflict.
    void truncate_from(Index from);

    // Accessors.
    bool  empty()       const { return entries_.empty(); }
    Index last_index()  const { return entries_.empty() ? 0 : entries_.back().index; }
    Term  last_term()   const { return entries_.empty() ? 0 : entries_.back().term; }
    std::size_t size()  const { return entries_.size(); }

    // Return the entry at index `i` (1-based). Panics if out of range.
    const LogEntry& at(Index i) const;

    // Does the log contain an entry with (index, term)? Used for prevLog check.
    bool contains(Index index, Term term) const;

    // Return entries in (lo, hi] (exclusive lo, inclusive hi), clamped to log bounds.
    std::vector<LogEntry> slice(Index lo, Index hi) const;

    // Find the last index in the log with the given term.
    Index last_index_for_term(Term term) const;

    // Serialize/deserialize the entire log for WAL-backed persistence.
    void save_to(class DurableStore& store) const;
    void load_from(const class DurableStore& store);

    const std::vector<LogEntry>& entries() const { return entries_; }

private:
    std::vector<LogEntry> entries_; // index in [1, N], stored at entries_[i-1]
};
