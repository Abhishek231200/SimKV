// simkv-server — real multi-process Raft node with TCP transport.
//
// Usage:
//   simkv-server --id 1 --raft-port 7001 --client-port 8001
//                --peers 2:127.0.0.1:7002,3:127.0.0.1:7003
//                --data-dir /tmp/node1
//
// Client protocol (line-based on --client-port):
//   PUT key value\n  →  +OK\n  or  -ERR not_leader\n
//   GET key\n        →  +value\n  or  -NOT_FOUND\n  or  -ERR not_leader\n
//   DEL key\n        →  +OK\n  or  -ERR not_leader\n

#include "raft/node.hpp"
#include "raft/types.hpp"
#include "server/dispatch_queue.hpp"
#include "server/real_clock.hpp"
#include "server/tcp_transport.hpp"
#include "storage/file_durable_store.hpp"
#include "sim/network.hpp"

#include <arpa/inet.h>
#include <csignal>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <future>
#include <iostream>
#include <map>
#include <netinet/in.h>
#include <sstream>
#include <string>
#include <sys/socket.h>
#include <thread>
#include <unistd.h>
#include <vector>
#include <atomic>

// ─── signal handling ──────────────────────────────────────────────────────────

static std::atomic<bool> g_done{false};

static void handle_signal(int) { g_done = true; }

// ─── argument parsing ─────────────────────────────────────────────────────────

struct Args {
    NodeId   id          = 0;
    uint16_t raft_port   = 0;
    uint16_t client_port = 0;
    std::map<NodeId, PeerAddr> peers;
    std::map<NodeId, PeerAddr> client_peers; // optional: node → client host:port for redirects
    std::string data_dir;
};

static void usage(const char* prog) {
    std::fprintf(stderr,
        "Usage: %s --id ID --raft-port PORT --client-port PORT\n"
        "          --peers ID:HOST:PORT[,ID:HOST:PORT,...]\n"
        "          --data-dir DIR\n", prog);
    std::exit(1);
}

static Args parse_args(int argc, char* argv[]) {
    Args a;
    for (int i = 1; i < argc; ++i) {
        std::string key = argv[i];
        if (i + 1 >= argc) usage(argv[0]);
        std::string val = argv[++i];

        if (key == "--id") {
            a.id = static_cast<NodeId>(std::stoul(val));
        } else if (key == "--raft-port") {
            a.raft_port = static_cast<uint16_t>(std::stoul(val));
        } else if (key == "--client-port") {
            a.client_port = static_cast<uint16_t>(std::stoul(val));
        } else if (key == "--peers") {
            // Format: ID:HOST:PORT[,ID:HOST:PORT,...]
            std::istringstream ss(val);
            std::string token;
            while (std::getline(ss, token, ',')) {
                auto c1 = token.find(':');
                auto c2 = token.rfind(':');
                if (c1 == std::string::npos || c2 == c1) usage(argv[0]);
                NodeId nid  = static_cast<NodeId>(std::stoul(token.substr(0, c1)));
                std::string host = token.substr(c1 + 1, c2 - c1 - 1);
                uint16_t port = static_cast<uint16_t>(std::stoul(token.substr(c2 + 1)));
                a.peers[nid] = {host, port};
            }
        } else if (key == "--client-peers") {
            // Format: ID:HOST:PORT[,ID:HOST:PORT,...] — same as --peers
            std::istringstream ss(val);
            std::string token;
            while (std::getline(ss, token, ',')) {
                auto c1 = token.find(':');
                auto c2 = token.rfind(':');
                if (c1 == std::string::npos || c2 == c1) usage(argv[0]);
                NodeId nid  = static_cast<NodeId>(std::stoul(token.substr(0, c1)));
                std::string host = token.substr(c1 + 1, c2 - c1 - 1);
                uint16_t port = static_cast<uint16_t>(std::stoul(token.substr(c2 + 1)));
                a.client_peers[nid] = {host, port};
            }
        } else if (key == "--data-dir") {
            a.data_dir = val;
        } else {
            usage(argv[0]);
        }
    }
    if (a.id == 0 || a.raft_port == 0 || a.client_port == 0) usage(argv[0]);
    return a;
}

// ─── client connection handler ────────────────────────────────────────────────

// Build a not-leader error string. If we know the leader's client address, emit
// "-REDIRECT host:port" so the client can reconnect without probing all nodes.
static std::string not_leader_response(
    const RaftNode* node,
    const std::map<NodeId, PeerAddr>* client_peers)
{
    auto lid = node->leader_id();
    if (lid.has_value()) {
        auto it = client_peers->find(*lid);
        if (it != client_peers->end())
            return "-REDIRECT " + it->second.host + ":" + std::to_string(it->second.port) + "\n";
    }
    return "-ERR not_leader\n";
}

static void handle_client(int cfd, RaftNode* node, DispatchQueue* dq,
                           const std::map<NodeId, PeerAddr>* client_peers) {
    FILE* f = ::fdopen(cfd, "r+");
    if (!f) { ::close(cfd); return; }

    char line[4096];
    while (std::fgets(line, sizeof(line), f)) {
        // Trim trailing newline/CR.
        size_t len = std::strlen(line);
        while (len > 0 && (line[len-1] == '\n' || line[len-1] == '\r'))
            line[--len] = '\0';

        std::istringstream iss(line);
        std::string cmd;
        iss >> cmd;

        std::string response;

        if (cmd == "PUT") {
            std::string key, val;
            if (!(iss >> key >> val)) { response = "-ERR bad_command\n"; }
            else {
                auto p   = std::make_shared<std::promise<std::string>>();
                auto fut = p->get_future();
                dq->post([node, key, val, p, client_peers]() {
                    bool ok = node->submit(0, 0, CmdPut{key, val},
                        [p](CmdResult r) {
                            p->set_value(r.ok ? "+OK\n" : "-ERR " + r.error + "\n");
                        });
                    if (!ok) p->set_value(not_leader_response(node, client_peers));
                });
                response = fut.get();
            }
        } else if (cmd == "DEL") {
            std::string key;
            if (!(iss >> key)) { response = "-ERR bad_command\n"; }
            else {
                auto p   = std::make_shared<std::promise<std::string>>();
                auto fut = p->get_future();
                dq->post([node, key, p, client_peers]() {
                    bool ok = node->submit(0, 0, CmdDelete{key},
                        [p](CmdResult r) {
                            p->set_value(r.ok ? "+OK\n" : "-ERR " + r.error + "\n");
                        });
                    if (!ok) p->set_value(not_leader_response(node, client_peers));
                });
                response = fut.get();
            }
        } else if (cmd == "GET") {
            std::string key;
            if (!(iss >> key)) { response = "-ERR bad_command\n"; }
            else {
                auto p   = std::make_shared<std::promise<std::string>>();
                auto fut = p->get_future();
                dq->post([node, key, p, client_peers]() {
                    bool ok = node->read_index(key,
                        [p, node, client_peers](bool success, std::optional<std::string> val) {
                            if (!success) { p->set_value(not_leader_response(node, client_peers)); return; }
                            if (val) p->set_value("+" + *val + "\n");
                            else     p->set_value("-NOT_FOUND\n");
                        });
                    if (!ok) p->set_value(not_leader_response(node, client_peers));
                });
                response = fut.get();
            }
        } else {
            response = "-ERR unknown_command\n";
        }

        std::fputs(response.c_str(), f);
        std::fflush(f);
    }
    std::fclose(f);
}

// ─── client listener ──────────────────────────────────────────────────────────

static void client_listener(uint16_t port, RaftNode* node, DispatchQueue* dq,
                            const std::map<NodeId, PeerAddr>* client_peers,
                            std::atomic<bool>* done)
{
    int srv = ::socket(AF_INET, SOCK_STREAM, 0);
    if (srv < 0) return;

    int yes = 1;
    ::setsockopt(srv, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(yes));

    sockaddr_in addr{};
    addr.sin_family      = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port        = htons(port);
    ::bind(srv, reinterpret_cast<sockaddr*>(&addr), sizeof(addr));
    ::listen(srv, 64);

    while (!done->load()) {
        sockaddr_in peer{};
        socklen_t plen = sizeof(peer);
        int cfd = ::accept(srv, reinterpret_cast<sockaddr*>(&peer), &plen);
        if (cfd < 0) continue;
        // Each connection is self-contained: detach so it cleans up independently.
        std::thread([cfd, node, dq, client_peers] {
            handle_client(cfd, node, dq, client_peers);
        }).detach();
    }

    ::close(srv);
}

// ─── main ─────────────────────────────────────────────────────────────────────

int main(int argc, char* argv[]) {
    // Ignore SIGPIPE globally — write failures are caught by return values.
    ::signal(SIGPIPE, SIG_IGN);
    ::signal(SIGINT,  handle_signal);
    ::signal(SIGTERM, handle_signal);

    Args args = parse_args(argc, argv);

    // Ensure data directory exists.
    if (!args.data_dir.empty()) {
        std::filesystem::create_directories(args.data_dir);
    }

    // Collect peer IDs for RaftNode.
    std::vector<NodeId> peer_ids;
    for (auto& [id, _] : args.peers) peer_ids.push_back(id);

    // Build dispatch queue, clock, and transport.
    DispatchQueue dispatch;
    RealClock     clock(dispatch, static_cast<uint64_t>(args.id) * 6364136223846793005ULL);
    TcpTransport  transport(args.id, args.raft_port, args.peers, dispatch);

    // Build durable stores.
    std::unique_ptr<IDurableStore> meta_store, log_store, snap_store;
    if (!args.data_dir.empty()) {
        meta_store = std::make_unique<FileDurableStore>(args.data_dir + "/meta");
        log_store  = std::make_unique<FileDurableStore>(args.data_dir + "/log");
        snap_store = std::make_unique<FileDurableStore>(args.data_dir + "/snapshot");
    }

    // Create and start the Raft node.
    RaftNode node(args.id, peer_ids, clock, transport, RaftConfig{},
                  std::move(meta_store), std::move(log_store), std::move(snap_store));

    dispatch.post([&node] { node.start(); });

    std::fprintf(stderr,
        "[node %u] started — raft-port=%u client-port=%u peers=%zu\n",
        args.id, args.raft_port, args.client_port, args.peers.size());

    // Start client listener in a background thread.
    std::thread client_thread([&] {
        client_listener(args.client_port, &node, &dispatch, &args.client_peers, &g_done);
    });

    // Main dispatch loop.
    while (!g_done.load()) {
        dispatch.run_once(10'000);
    }

    dispatch.stop();
    if (client_thread.joinable()) client_thread.join();

    std::fprintf(stderr, "[node %u] shut down\n", args.id);
    return 0;
}
