#include "storage/wal.hpp"
#include "storage/codec.hpp"
#include <cstring>
#include <stdexcept>

// CRC-32c (Castagnoli) — a minimal, self-contained implementation for record integrity.
static uint32_t crc32c(const uint8_t* data, std::size_t len) {
    uint32_t crc = 0xFFFFFFFF;
    for (std::size_t i = 0; i < len; ++i) {
        crc ^= data[i];
        for (int j = 0; j < 8; ++j) {
            crc = (crc >> 1) ^ (0x82F63B78U & -(crc & 1));
        }
    }
    return crc ^ 0xFFFFFFFF;
}

enum class CmdTag : uint8_t { Put = 1, Delete = 2, Cas = 3, Get = 4 };

std::vector<uint8_t> Wal::encode_command(const Command& cmd) {
    Encoder enc;
    if (auto* p = std::get_if<CmdPut>(&cmd)) {
        enc.u8(static_cast<uint8_t>(CmdTag::Put));
        enc.str(p->key);
        enc.str(p->value);
    } else if (auto* d = std::get_if<CmdDelete>(&cmd)) {
        enc.u8(static_cast<uint8_t>(CmdTag::Delete));
        enc.str(d->key);
    } else if (auto* c = std::get_if<CmdCas>(&cmd)) {
        enc.u8(static_cast<uint8_t>(CmdTag::Cas));
        enc.str(c->key);
        enc.str(c->expected);
        enc.str(c->value);
    } else if (auto* g = std::get_if<CmdGet>(&cmd)) {
        enc.u8(static_cast<uint8_t>(CmdTag::Get));
        enc.str(g->key);
    }
    return enc.take();
}

Command Wal::decode_command(const uint8_t* data, std::size_t size) {
    Decoder dec(data, size);
    auto tag = static_cast<CmdTag>(dec.u8());
    switch (tag) {
    case CmdTag::Put: {
        auto key = dec.str();
        auto val = dec.str();
        return CmdPut{std::move(key), std::move(val)};
    }
    case CmdTag::Delete: {
        auto key = dec.str();
        return CmdDelete{std::move(key)};
    }
    case CmdTag::Cas: {
        auto key = dec.str();
        auto exp = dec.str();
        auto val = dec.str();
        return CmdCas{std::move(key), std::move(exp), std::move(val)};
    }
    case CmdTag::Get: {
        auto key = dec.str();
        return CmdGet{std::move(key)};
    }
    default:
        throw std::runtime_error("WAL: unknown command tag");
    }
}

Wal::Wal(DurableStore& store) : store_(store) {}

void Wal::append(const Command& cmd) {
    auto payload = encode_command(cmd);
    uint32_t len = static_cast<uint32_t>(payload.size());
    uint32_t crc = crc32c(payload.data(), payload.size());

    Encoder frame;
    frame.u32(len);
    auto header = frame.take();
    store_.append(header);
    store_.append(payload);

    Encoder trailer;
    trailer.u32(crc);
    auto crc_bytes = trailer.take();
    store_.append(crc_bytes);
}

void Wal::flush() {
    store_.flush();
}

std::vector<Command> Wal::replay() const {
    auto durable = store_.read_durable();
    std::vector<Command> cmds;

    const uint8_t* p   = durable.data();
    const uint8_t* end = p + durable.size();

    while (p < end) {
        if (end - p < 4) break; // truncated length field
        Decoder lhdr(p, 4);
        uint32_t len = lhdr.u32();
        p += 4;

        if (static_cast<std::size_t>(end - p) < static_cast<std::size_t>(len) + 4) break;

        const uint8_t* payload = p;
        p += len;

        Decoder chdr(p, 4);
        uint32_t stored_crc  = chdr.u32();
        p += 4;

        uint32_t computed_crc = crc32c(payload, len);
        if (computed_crc != stored_crc) break; // corrupted record — stop replay

        try {
            cmds.push_back(decode_command(payload, len));
        } catch (...) {
            break;
        }
    }
    return cmds;
}
