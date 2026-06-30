#pragma once
#include "raft/types.hpp"
#include <cassert>
#include <vector>

// Raft in-memory log container. Index is 1-based; index 0 is the empty sentinel.
//
// After log compaction (snapshotting), entries before base_index_ are discarded.
// base_index_ is the last_included_index of the most recent snapshot; base_term_ is its term.
// All absolute indices passed to public methods are unchanged — callers always use
// the global 1-based index space; the class handles the internal offset.
class RaftLog {
public:
    RaftLog() = default;

    // Append a new entry. entry.index must equal last_index()+1.
    void append(LogEntry entry);

    // Truncate entries from index `from` onward (inclusive). Used on conflict.
    void truncate_from(Index from);

    // Compact: discard all entries with index <= last_included_index.
    // Called after a snapshot is taken or installed. Sets the new base.
    void set_snapshot(Index last_included_index, Term last_included_term);

    // Accessors.
    bool  empty()       const { return entries_.empty(); }
    Index last_index()  const { return base_index_ + static_cast<Index>(entries_.size()); }
    Term  last_term()   const { return entries_.empty() ? base_term_ : entries_.back().term; }
    Index base_index()  const { return base_index_; }
    Term  base_term()   const { return base_term_; }
    std::size_t size()  const { return entries_.size(); } // entries in memory (post-compaction)

    // Return the entry at absolute index i (1-based). Panics if compacted or out of range.
    const LogEntry& at(Index i) const;

    // Return the term at absolute index i. Handles the snapshot boundary (base_index_).
    Term term_at(Index i) const;

    // Does the log contain an entry with (index, term)? Handles snapshot boundary.
    bool contains(Index index, Term term) const;

    // Return entries in (lo, hi] clamped to available range.
    std::vector<LogEntry> slice(Index lo, Index hi) const;

    // Find the last absolute index in the log that has the given term.
    Index last_index_for_term(Term term) const;

    // Serialize/deserialize for WAL-backed persistence.
    // Stores base_index_/base_term_ so the offset survives restarts.
    void save_to(class IDurableStore& store) const;
    void load_from(const class IDurableStore& store);

    const std::vector<LogEntry>& entries() const { return entries_; }

private:
    std::vector<LogEntry> entries_;          // entries_[i] holds absolute index base_index_+i+1
    Index                 base_index_ = 0;   // last_included_index of latest snapshot (0 = none)
    Term                  base_term_  = 0;   // last_included_term  of latest snapshot
};
