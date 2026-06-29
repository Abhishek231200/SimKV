#include "storage/durable_store.hpp"

void DurableStore::append(std::span<const uint8_t> bytes) {
    data_.insert(data_.end(), bytes.begin(), bytes.end());
}

void DurableStore::flush() {
    flushed_end_ = data_.size();
}

void DurableStore::crash() {
    data_.resize(flushed_end_); // drop unflushed tail
}

std::span<const uint8_t> DurableStore::read_durable() const {
    return {data_.data(), flushed_end_};
}

std::span<const uint8_t> DurableStore::read_all() const {
    return {data_.data(), data_.size()};
}

void DurableStore::truncate_to(std::size_t offset) {
    if (offset <= flushed_end_) flushed_end_ = offset;
    if (offset <= data_.size()) data_.resize(offset);
}
