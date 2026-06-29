#pragma once
#include <cstdint>

// Logical simulation time. 1 tick = 1 microsecond by convention.
// Never use std::chrono or wall-clock anywhere in system logic.
using SimTime = uint64_t;

inline constexpr SimTime kUsec = 1;
inline constexpr SimTime kMsec = 1'000;
inline constexpr SimTime kSec  = 1'000'000;
