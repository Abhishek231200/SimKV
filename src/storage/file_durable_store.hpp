#pragma once
#include "storage/idurable_store.hpp"
#include <cstdint>
#include <span>
#include <string>
#include <vector>

// File-backed durable store using pwrite(2) + fdatasync(2).
// flush() writes the unflushed tail to disk and calls fdatasync.
// crash() is a no-op: real files don't support simulated crashes.
// Used by the CLI when --data-dir is given; simulation always uses DurableStore.
class FileDurableStore : public IDurableStore {
public:
    // Opens (or creates) the file at `path`. Reads any existing content into memory.
    explicit FileDurableStore(const std::string& path);
    ~FileDurableStore() override;

    void append(std::span<const uint8_t> bytes) override;
    void flush()  override; // pwrite new bytes + fdatasync
    void crash()  override; // no-op for real file

    std::span<const uint8_t> read_durable() const override;
    std::span<const uint8_t> read_all()     const override;

    void truncate_to(std::size_t offset) override; // ftruncate + update buffer

    std::size_t durable_size() const override { return flushed_end_; }
    std::size_t total_size()   const override { return buffer_.size(); }

private:
    int                  fd_         = -1;
    std::vector<uint8_t> buffer_;          // in-memory mirror of file content
    std::size_t          flushed_end_ = 0; // how many bytes have been fdatasynced
};
