#pragma once
#include <cassert>
#include <cstdint>
#include <span>
#include <stdexcept>
#include <string>
#include <vector>

// Deterministic little-endian encode/decode.
// Every integer is stored in little-endian byte order.
// Strings are length-prefixed with a uint32_t.
// No external dependencies — keeps the build deterministic.

class Encoder {
public:
    void u8(uint8_t v) {
        buf_.push_back(v);
    }
    void u32(uint32_t v) {
        buf_.push_back(static_cast<uint8_t>(v));
        buf_.push_back(static_cast<uint8_t>(v >> 8));
        buf_.push_back(static_cast<uint8_t>(v >> 16));
        buf_.push_back(static_cast<uint8_t>(v >> 24));
    }
    void u64(uint64_t v) {
        for (int i = 0; i < 8; ++i) {
            buf_.push_back(static_cast<uint8_t>(v & 0xFF));
            v >>= 8;
        }
    }
    void boolean(bool v) { u8(v ? 1 : 0); }
    void str(const std::string& s) {
        u32(static_cast<uint32_t>(s.size()));
        buf_.insert(buf_.end(), s.begin(), s.end());
    }
    std::vector<uint8_t> take() { return std::move(buf_); }
    const std::vector<uint8_t>& view() const { return buf_; }

private:
    std::vector<uint8_t> buf_;
};

class Decoder {
public:
    Decoder(const uint8_t* data, std::size_t size)
        : ptr_(data), end_(data + size) {}
    explicit Decoder(std::span<const uint8_t> s)
        : ptr_(s.data()), end_(s.data() + s.size()) {}

    uint8_t u8() {
        check(1);
        return *ptr_++;
    }
    uint32_t u32() {
        check(4);
        uint32_t v = static_cast<uint32_t>(ptr_[0])
                   | static_cast<uint32_t>(ptr_[1]) << 8
                   | static_cast<uint32_t>(ptr_[2]) << 16
                   | static_cast<uint32_t>(ptr_[3]) << 24;
        ptr_ += 4;
        return v;
    }
    uint64_t u64() {
        check(8);
        uint64_t v = 0;
        for (int i = 0; i < 8; ++i) {
            v |= static_cast<uint64_t>(ptr_[i]) << (8 * i);
        }
        ptr_ += 8;
        return v;
    }
    bool boolean() { return u8() != 0; }
    std::string str() {
        uint32_t len = u32();
        check(len);
        std::string s(reinterpret_cast<const char*>(ptr_), len);
        ptr_ += len;
        return s;
    }
    bool empty() const { return ptr_ >= end_; }
    std::size_t remaining() const { return static_cast<std::size_t>(end_ - ptr_); }

    // Return a sub-decoder for the next `n` bytes.
    Decoder slice(std::size_t n) {
        check(n);
        Decoder sub(ptr_, n);
        ptr_ += n;
        return sub;
    }

private:
    void check(std::size_t n) const {
        if (static_cast<std::size_t>(end_ - ptr_) < n)
            throw std::runtime_error("Decoder: buffer underrun");
    }
    const uint8_t* ptr_;
    const uint8_t* end_;
};
