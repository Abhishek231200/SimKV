#pragma once
#include <cstdint>
#include <span>
#include <vector>

// Simulated durable byte store. Models a simple append-only disk with two regions:
// a flushed (durable) prefix and an unflushed tail that is lost on crash.
// This is the persistence substrate for the WAL and Raft persistent state.
class DurableStore {
public:
    void append(std::span<const uint8_t> bytes);
    void flush();  // tail becomes durable
    void crash();  // discard unflushed tail (simulates power-loss)

    // Read only the durable prefix. Used during WAL replay after crash.
    std::span<const uint8_t> read_durable() const;

    // Read everything (durable + unflushed), for normal (non-crash) reads.
    std::span<const uint8_t> read_all() const;

    // Truncate to a specific offset. Used during log compaction.
    void truncate_to(std::size_t offset);

    std::size_t durable_size() const { return flushed_end_; }
    std::size_t total_size()   const { return data_.size(); }

private:
    std::vector<uint8_t> data_;
    std::size_t          flushed_end_ = 0;
};
