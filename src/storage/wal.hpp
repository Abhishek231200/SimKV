#pragma once
#include "raft/types.hpp"
#include "storage/durable_store.hpp"
#include <cstdint>
#include <vector>

// Write-ahead log on top of DurableStore.
// Each record is: [length:u32][payload...][crc32:u32] (framing).
// Commands are serialized deterministically (little-endian).
// On crash, only flushed records survive; partial tail records are ignored during replay.
class Wal {
public:
    explicit Wal(DurableStore& store);

    void append(const Command& cmd);
    void flush();

    // Re-read the durable prefix and decode all valid records.
    std::vector<Command> replay() const;

    // Encode a single Command to bytes (used by Raft node for persistence too).
    static std::vector<uint8_t> encode_command(const Command& cmd);
    static Command decode_command(const uint8_t* data, std::size_t size);

private:
    DurableStore& store_;
};
