#pragma once
#include "server/dispatch_queue.hpp"
#include "sim/network.hpp"   // for ITransport, NodeId, Message
#include <atomic>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <set>
#include <string>
#include <thread>
#include <vector>

struct PeerAddr { std::string host; uint16_t port; };

// Real TCP implementation of ITransport.
// Each node opens a listen socket; outgoing connections are established lazily.
// Wire format per message (big-endian):
//   [4 bytes: sender NodeId] [4 bytes: payload_length] [payload_length bytes]
// All received messages are delivered via DispatchQueue::post().
class TcpTransport : public ITransport {
public:
    // my_id:       this node's NodeId
    // listen_port: port to accept inbound connections on
    // peers:       map from NodeId -> PeerAddr for outgoing connections
    // dq:          dispatch queue for delivering received messages
    TcpTransport(NodeId my_id, uint16_t listen_port,
                 std::map<NodeId, PeerAddr> peers,
                 DispatchQueue& dq);
    ~TcpTransport();

    void send(NodeId from, NodeId to, std::vector<uint8_t> payload) override;
    void set_handler(NodeId id, std::function<void(Message)> handler) override;
    void remove_handler(NodeId id) override;

private:
    void accept_loop();
    void recv_loop(int fd);
    int  connect_to(NodeId peer);          // returns fd or -1 on failure
    bool write_all(int fd, const uint8_t* buf, size_t len);
    bool read_all(int fd, uint8_t* buf, size_t len);

    NodeId   my_id_;
    uint16_t listen_port_;
    std::map<NodeId, PeerAddr> peers_;
    DispatchQueue& dq_;

    int server_fd_ = -1;
    std::thread accept_thread_;

    std::mutex send_mu_;
    std::map<NodeId, int> send_fds_;       // outgoing fd per peer

    struct RecvEntry {
        std::atomic<bool> done{false};
        std::thread       thread;
    };
    std::vector<std::unique_ptr<RecvEntry>> recv_threads_;
    std::set<int>                           active_recv_fds_; // incoming fds being read by recv threads
    std::mutex                              recv_mu_;         // guards recv_threads_ and active_recv_fds_

    std::function<void(Message)> handler_;
    std::mutex handler_mu_;

    std::atomic<bool> running_{true};
};
