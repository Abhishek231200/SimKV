#pragma once
#include "storage/idurable_store.hpp"
#include <cstdint>
#include <span>
#include <vector>

// Simulated durable byte store. Models a simple append-only disk with two regions:
// a flushed (durable) prefix and an unflushed tail that is lost on crash.
// This is the persistence substrate for the WAL and Raft persistent state.
class DurableStore : public IDurableStore {
public:
    void append(std::span<const uint8_t> bytes) override;
    void flush()  override; // tail becomes durable
    void crash()  override; // discard unflushed tail (simulates power-loss)

    std::span<const uint8_t> read_durable() const override;
    std::span<const uint8_t> read_all()     const override;

    void truncate_to(std::size_t offset) override;

    std::size_t durable_size() const override { return flushed_end_; }
    std::size_t total_size()   const override { return data_.size(); }

private:
    std::vector<uint8_t> data_;
    std::size_t          flushed_end_ = 0;
};
