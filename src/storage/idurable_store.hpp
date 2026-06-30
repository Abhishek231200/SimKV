#pragma once
#include <cstdint>
#include <span>

// Abstract persistence substrate.
// DurableStore (in-memory, sim-compatible) and FileDurableStore (real pwrite/fdatasync)
// both implement this interface.
class IDurableStore {
public:
    virtual ~IDurableStore() = default;

    virtual void append(std::span<const uint8_t> bytes) = 0;
    virtual void flush()  = 0;

    // Simulate power-loss: discard any bytes appended since the last flush().
    // No-op on FileDurableStore (real files don't support simulated crashes).
    virtual void crash() = 0;

    // View of the durable (flushed) prefix only.
    virtual std::span<const uint8_t> read_durable() const = 0;
    // View of everything including unflushed tail.
    virtual std::span<const uint8_t> read_all() const = 0;

    virtual void truncate_to(std::size_t offset) = 0;

    virtual std::size_t durable_size() const = 0;
    virtual std::size_t total_size()   const = 0;
};
