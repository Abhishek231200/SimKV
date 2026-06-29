#pragma once
#include <cstdint>
#include <vector>

// Portable deterministic PRNG.
// SplitMix64 seeds xoshiro256**, which is the raw stream.
// All helpers (range, bernoulli, unit, shuffle) are built purely on next_u64()
// so output is identical across compilers and platforms.
// NEVER use std::uniform_*_distribution or std::shuffle — they are not portable.
class Prng {
public:
    explicit Prng(uint64_t seed);

    uint64_t next_u64();

    // Uniform in [lo, hi). Requires hi > lo.
    uint64_t range(uint64_t lo, uint64_t hi);

    // True with probability p in [0.0, 1.0].
    bool bernoulli(double p);

    // Uniform in [0.0, 1.0).
    double unit();

    template <class T>
    void shuffle(std::vector<T>& v) {
        for (std::size_t i = v.size(); i > 1; --i) {
            std::size_t j = static_cast<std::size_t>(range(0, i));
            std::swap(v[j], v[i - 1]);
        }
    }

private:
    uint64_t s_[4]; // xoshiro256** state
};
