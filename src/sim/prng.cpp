#include "sim/prng.hpp"
#include <cassert>
#include <limits>

static uint64_t splitmix64(uint64_t& state) {
    state += 0x9e3779b97f4a7c15ULL;
    uint64_t z = state;
    z = (z ^ (z >> 30)) * 0xbf58476d1ce4e5b9ULL;
    z = (z ^ (z >> 27)) * 0x94d049bb133111ebULL;
    return z ^ (z >> 31);
}

static uint64_t rotl64(uint64_t x, int k) {
    return (x << k) | (x >> (64 - k));
}

Prng::Prng(uint64_t seed) {
    // Seed xoshiro256** state from a single uint64 using SplitMix64.
    for (auto& w : s_) {
        w = splitmix64(seed);
    }
}

uint64_t Prng::next_u64() {
    // xoshiro256** algorithm — portable, passes all statistical tests.
    const uint64_t result = rotl64(s_[1] * 5, 7) * 9;
    const uint64_t t = s_[1] << 17;
    s_[2] ^= s_[0];
    s_[3] ^= s_[1];
    s_[1] ^= s_[2];
    s_[0] ^= s_[3];
    s_[2] ^= t;
    s_[3] = rotl64(s_[3], 45);
    return result;
}

uint64_t Prng::range(uint64_t lo, uint64_t hi) {
    assert(hi > lo);
    uint64_t n = hi - lo;
    // Lemire's method: unbiased bounded random range via rejection.
    // Rejection is deterministic given the PRNG state, so seed-replay is safe.
    uint64_t x = next_u64();
    __uint128_t m = static_cast<__uint128_t>(x) * n;
    uint64_t l = static_cast<uint64_t>(m);
    if (l < n) {
        uint64_t t = (-n) % n; // threshold for rejection
        while (l < t) {
            x = next_u64();
            m = static_cast<__uint128_t>(x) * n;
            l = static_cast<uint64_t>(m);
        }
    }
    return static_cast<uint64_t>(m >> 64) + lo;
}

bool Prng::bernoulli(double p) {
    if (p <= 0.0) return false;
    if (p >= 1.0) return true;
    // Compare raw u64 to threshold: P(true) = p.
    // threshold = p * 2^64, clamped to avoid overflow.
    constexpr double scale = static_cast<double>(std::numeric_limits<uint64_t>::max());
    uint64_t threshold = static_cast<uint64_t>(p * scale);
    return next_u64() < threshold;
}

double Prng::unit() {
    // 53-bit precision float in [0.0, 1.0).
    return static_cast<double>(next_u64() >> 11) * (1.0 / (1ULL << 53));
}
