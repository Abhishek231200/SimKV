#include "server/tcp_transport.hpp"
#include <arpa/inet.h>
#include <cstring>
#include <cstdio>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>
#include <netdb.h>

#ifdef __APPLE__
#  define SIMKV_NOSIGPIPE_SOCK SO_NOSIGPIPE
#endif

// ─── helpers ─────────────────────────────────────────────────────────────────

static uint32_t to_be32(uint32_t v) {
    return htonl(v);
}
static uint32_t from_be32(uint32_t v) {
    return ntohl(v);
}

// ─── constructor / destructor ─────────────────────────────────────────────────

TcpTransport::TcpTransport(NodeId my_id, uint16_t listen_port,
                           std::map<NodeId, PeerAddr> peers,
                           DispatchQueue& dq)
    : my_id_(my_id), listen_port_(listen_port),
      peers_(std::move(peers)), dq_(dq)
{
    // Create listen socket.
    server_fd_ = ::socket(AF_INET, SOCK_STREAM, 0);
    if (server_fd_ < 0) return;

    int yes = 1;
    ::setsockopt(server_fd_, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(yes));

    sockaddr_in addr{};
    addr.sin_family      = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port        = htons(listen_port_);

    if (::bind(server_fd_, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0) {
        std::fprintf(stderr, "[tcp:%u] bind(%u) FAILED: %s\n", my_id_, listen_port_, strerror(errno));
        ::close(server_fd_);
        server_fd_ = -1;
        return;
    }
    ::listen(server_fd_, 64);

    accept_thread_ = std::thread([this] { accept_loop(); });
}

TcpTransport::~TcpTransport() {
    running_ = false;

    // Close listen socket so accept() unblocks.
    if (server_fd_ >= 0) {
        ::shutdown(server_fd_, SHUT_RDWR);
        ::close(server_fd_);
        server_fd_ = -1;
    }

    if (accept_thread_.joinable()) accept_thread_.join();

    // Close outgoing fds.
    {
        std::lock_guard<std::mutex> lk(send_mu_);
        for (auto& [id, fd] : send_fds_) {
            ::shutdown(fd, SHUT_RDWR);
            ::close(fd);
        }
        send_fds_.clear();
    }

    // Interrupt any recv thread blocked in read_all() by shutting down its fd.
    // Without this, threads reading from persistent peer connections would hang
    // indefinitely waiting for the peer to close the connection.
    {
        std::lock_guard<std::mutex> lk(recv_mu_);
        for (int fd : active_recv_fds_) ::shutdown(fd, SHUT_RDWR);
    }

    // Join recv threads — they exit promptly once their fds are shut down.
    std::vector<std::unique_ptr<RecvEntry>> to_join;
    {
        std::lock_guard<std::mutex> lk(recv_mu_);
        to_join = std::move(recv_threads_);
    }
    for (auto& e : to_join) if (e->thread.joinable()) e->thread.join();
}

// ─── ITransport ──────────────────────────────────────────────────────────────

void TcpTransport::set_handler(NodeId /*id*/, std::function<void(Message)> handler) {
    std::lock_guard<std::mutex> lk(handler_mu_);
    handler_ = std::move(handler);
}

void TcpTransport::remove_handler(NodeId /*id*/) {
    std::lock_guard<std::mutex> lk(handler_mu_);
    handler_ = nullptr;
}

void TcpTransport::send(NodeId from, NodeId to, std::vector<uint8_t> payload) {
    if (!running_) return;

    int fd = -1;
    {
        std::lock_guard<std::mutex> lk(send_mu_);
        auto it = send_fds_.find(to);
        if (it != send_fds_.end()) {
            fd = it->second;
        } else {
            fd = connect_to(to);
            if (fd < 0) return; // peer unreachable — drop silently
            send_fds_[to] = fd;
        }
    }

    // Wire format: [4: sender NodeId BE] [4: payload_len BE] [payload]
    uint32_t hdr_from = to_be32(static_cast<uint32_t>(from));
    uint32_t hdr_len  = to_be32(static_cast<uint32_t>(payload.size()));

    // We hold send_mu_ to avoid concurrent writes on the same fd.
    std::lock_guard<std::mutex> lk(send_mu_);

    auto do_write = [&](int wfd) -> bool {
        return write_all(wfd, reinterpret_cast<const uint8_t*>(&hdr_from), 4) &&
               write_all(wfd, reinterpret_cast<const uint8_t*>(&hdr_len),  4) &&
               write_all(wfd, payload.data(), payload.size());
    };

    if (!do_write(fd)) {
        // Connection broke (e.g. peer restarted). Evict and reconnect once.
        ::close(fd);
        send_fds_.erase(to);
        fd = connect_to(to);
        if (fd >= 0) {
            send_fds_[to] = fd;
            do_write(fd); // best-effort; silently drop if the peer is still down
        }
    }
}

// ─── private helpers ──────────────────────────────────────────────────────────

void TcpTransport::accept_loop() {
    while (running_) {
        sockaddr_in peer_addr{};
        socklen_t   peer_len = sizeof(peer_addr);
        int cfd = ::accept(server_fd_,
                           reinterpret_cast<sockaddr*>(&peer_addr), &peer_len);
        if (cfd < 0) {
            if (!running_) break;
            continue;
        }

#ifdef SIMKV_NOSIGPIPE_SOCK
        int yes = 1;
        ::setsockopt(cfd, SOL_SOCKET, SIMKV_NOSIGPIPE_SOCK, &yes, sizeof(yes));
#endif

        std::lock_guard<std::mutex> lk(recv_mu_);

        // Prune completed entries before adding a new one.
        recv_threads_.erase(
            std::remove_if(recv_threads_.begin(), recv_threads_.end(),
                [](const std::unique_ptr<RecvEntry>& e) {
                    if (e->done.load(std::memory_order_acquire)) {
                        if (e->thread.joinable()) e->thread.join();
                        return true;
                    }
                    return false;
                }),
            recv_threads_.end());

        auto entry = std::make_unique<RecvEntry>();
        RecvEntry* ep = entry.get();
        entry->thread = std::thread([this, cfd, ep] {
            recv_loop(cfd);
            ep->done.store(true, std::memory_order_release);
        });
        active_recv_fds_.insert(cfd);
        recv_threads_.push_back(std::move(entry));
    }
}

void TcpTransport::recv_loop(int fd) {
    while (running_) {
        uint8_t hdr[8];
        if (!read_all(fd, hdr, 8)) break;

        uint32_t sender_id  = from_be32(*reinterpret_cast<uint32_t*>(hdr));
        uint32_t payload_len = from_be32(*reinterpret_cast<uint32_t*>(hdr + 4));

        if (payload_len > 64 * 1024 * 1024) break; // sanity limit

        std::vector<uint8_t> payload(payload_len);
        if (!read_all(fd, payload.data(), payload_len)) break;

        Message msg{static_cast<NodeId>(sender_id), my_id_, std::move(payload)};

        std::function<void(Message)> h;
        {
            std::lock_guard<std::mutex> lk(handler_mu_);
            h = handler_;
        }
        if (h) {
            // Capture by value so the lambda owns the message.
            dq_.post([h = std::move(h), m = std::move(msg)]() mutable {
                h(std::move(m));
            });
        }
    }
    // Deregister before closing so the destructor doesn't double-shutdown this fd.
    {
        std::lock_guard<std::mutex> lk(recv_mu_);
        active_recv_fds_.erase(fd);
    }
    ::close(fd);
}

int TcpTransport::connect_to(NodeId peer) {
    auto it = peers_.find(peer);
    if (it == peers_.end()) {
        std::fprintf(stderr, "[tcp:%u] connect_to(%u): peer not found\n", my_id_, peer);
        return -1;
    }

    const PeerAddr& pa = it->second;

    // Resolve host.
    addrinfo hints{};
    hints.ai_family   = AF_INET;
    hints.ai_socktype = SOCK_STREAM;
    addrinfo* res = nullptr;
    std::string port_str = std::to_string(pa.port);
    if (::getaddrinfo(pa.host.c_str(), port_str.c_str(), &hints, &res) != 0 || !res) {
        std::fprintf(stderr, "[tcp:%u] connect_to(%u): getaddrinfo failed\n", my_id_, peer);
        return -1;
    }

    int fd = ::socket(res->ai_family, res->ai_socktype, res->ai_protocol);
    if (fd < 0) { ::freeaddrinfo(res); return -1; }

#ifdef SIMKV_NOSIGPIPE_SOCK
    int yes = 1;
    ::setsockopt(fd, SOL_SOCKET, SIMKV_NOSIGPIPE_SOCK, &yes, sizeof(yes));
#endif

    if (::connect(fd, res->ai_addr, res->ai_addrlen) < 0) {
        std::fprintf(stderr, "[tcp:%u] connect_to(%u) port=%u FAILED: %s\n",
                     my_id_, peer, pa.port, strerror(errno));
        ::close(fd);
        ::freeaddrinfo(res);
        return -1;
    }
    ::freeaddrinfo(res);
    return fd;
}

bool TcpTransport::write_all(int fd, const uint8_t* buf, size_t len) {
    size_t written = 0;
    while (written < len) {
        ssize_t n;
#ifdef MSG_NOSIGNAL
        n = ::send(fd, buf + written, len - written, MSG_NOSIGNAL);
#else
        n = ::write(fd, buf + written, len - written);
#endif
        if (n <= 0) return false;
        written += static_cast<size_t>(n);
    }
    return true;
}

bool TcpTransport::read_all(int fd, uint8_t* buf, size_t len) {
    size_t got = 0;
    while (got < len) {
        ssize_t n = ::read(fd, buf + got, len - got);
        if (n <= 0) return false;
        got += static_cast<size_t>(n);
    }
    return true;
}
