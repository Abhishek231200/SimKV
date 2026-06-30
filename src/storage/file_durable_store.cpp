#include "storage/file_durable_store.hpp"
#include <cassert>
#include <fcntl.h>
#include <stdexcept>
#include <string>
#include <sys/stat.h>
#include <unistd.h>

// fdatasync is in unistd.h but gated behind _POSIX_C_SOURCE on some platforms.
// Use fsync for performance. F_FULLFSYNC (macOS) flushes the hardware write cache
// and takes 100ms+, which blocks the dispatch thread long enough for election timers
// to fire before vote replies are sent. fsync() is sufficient for correctness.
static int simkv_datasync(int fd) { return ::fsync(fd); }
#if defined(__APPLE__)
#include <sys/fcntl.h>
#endif

FileDurableStore::FileDurableStore(const std::string& path) {
    fd_ = ::open(path.c_str(), O_RDWR | O_CREAT, 0644);
    if (fd_ < 0) throw std::runtime_error("FileDurableStore: cannot open " + path);

    // Read existing file content into buffer_.
    struct stat st{};
    if (::fstat(fd_, &st) < 0) throw std::runtime_error("FileDurableStore: fstat failed");
    auto file_size = static_cast<std::size_t>(st.st_size);

    buffer_.resize(file_size);
    if (file_size > 0) {
        ssize_t n = ::pread(fd_, buffer_.data(), file_size, 0);
        if (n < 0 || static_cast<std::size_t>(n) != file_size)
            throw std::runtime_error("FileDurableStore: pread failed");
    }
    flushed_end_ = file_size;
}

FileDurableStore::~FileDurableStore() {
    if (fd_ >= 0) ::close(fd_);
}

void FileDurableStore::append(std::span<const uint8_t> bytes) {
    buffer_.insert(buffer_.end(), bytes.begin(), bytes.end());
}

void FileDurableStore::flush() {
    // Write any bytes past flushed_end_ to the file, then fdatasync.
    if (buffer_.size() <= flushed_end_) return;
    std::size_t to_write = buffer_.size() - flushed_end_;
    ssize_t n = ::pwrite(fd_, buffer_.data() + flushed_end_, to_write,
                         static_cast<off_t>(flushed_end_));
    if (n < 0 || static_cast<std::size_t>(n) != to_write)
        throw std::runtime_error("FileDurableStore: pwrite failed");
    if (simkv_datasync(fd_) < 0)
        throw std::runtime_error("FileDurableStore: fdatasync failed");
    flushed_end_ = buffer_.size();
}

void FileDurableStore::crash() {
    // Real files survive crashes — this is a no-op by design.
    // Calling crash() on a FileDurableStore in production is a programming error.
    assert(false && "crash() called on FileDurableStore — only valid in simulation");
}

std::span<const uint8_t> FileDurableStore::read_durable() const {
    return {buffer_.data(), flushed_end_};
}

std::span<const uint8_t> FileDurableStore::read_all() const {
    return {buffer_.data(), buffer_.size()};
}

void FileDurableStore::truncate_to(std::size_t offset) {
    if (offset > buffer_.size()) return;
    buffer_.resize(offset);
    if (offset < flushed_end_) {
        if (::ftruncate(fd_, static_cast<off_t>(offset)) < 0)
            throw std::runtime_error("FileDurableStore: ftruncate failed");
        if (simkv_datasync(fd_) < 0)
            throw std::runtime_error("FileDurableStore: fdatasync failed");
        flushed_end_ = offset;
    }
}
