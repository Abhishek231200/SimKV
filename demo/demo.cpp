//
// SimKV interactive demo
//
// Walks through 5 phases in a single deterministic simulation run:
//   1. Leader election
//   2. Write traffic
//   3. Network partition  (minority leader can't commit)
//   4. Log compaction + InstallSnapshot to lagging follower
//   5. Linearizable read-index query
//
// Designed to be recorded with `asciinema` — each phase is separated by a
// blank line and the output is self-contained with ANSI colors.

#include "raft/node.hpp"
#include "sim/network.hpp"
#include "sim/simulator.hpp"
#include <chrono>
#include <format>
#include <functional>
#include <iostream>
#include <map>
#include <optional>
#include <string>
#include <thread>
#include <vector>

// ── ANSI helpers ─────────────────────────────────────────────────────────────

static const char* RESET  = "\033[0m";
static const char* BOLD   = "\033[1m";
static const char* DIM    = "\033[2m";
static const char* GREEN  = "\033[32m";
static const char* YELLOW = "\033[33m";
static const char* RED    = "\033[31m";
static const char* CYAN   = "\033[36m";
static const char* BLUE   = "\033[34m";
static const char* MAGENTA= "\033[35m";

static void pause(int ms = 120) {
    std::this_thread::sleep_for(std::chrono::milliseconds(ms));
}

static std::string role_str(Role r) {
    switch (r) {
    case Role::Leader:    return std::string(GREEN)  + "leader"    + RESET;
    case Role::Candidate: return std::string(YELLOW) + "candidate" + RESET;
    case Role::Follower:  return std::string(BLUE)   + "follower"  + RESET;
    }
    return "?";
}

static std::string ts(SimTime t) {
    return std::format("{}{:>5}ms{}", CYAN, t / kMsec, RESET);
}

static void header(const std::string& phase, const std::string& title) {
    std::cout << "\n" << BOLD << "▶ Phase " << phase << "  " << title << RESET << "\n";
    pause(200);
}

static void line(SimTime t, const std::string& msg) {
    std::cout << "  [" << ts(t) << "] " << msg << "\n";
    std::cout.flush();
    pause(80);
}

// ── Demo ─────────────────────────────────────────────────────────────────────

int main() {
    // ── Banner ────────────────────────────────────────────────────────────────
    std::cout << "\n";
    std::cout << BOLD << "╔══════════════════════════════════════════════════════╗\n";
    std::cout <<         "║       SimKV  ·  deterministic-simulation KV          ║\n";
    std::cout <<         "╚══════════════════════════════════════════════════════╝\n" << RESET;
    std::cout << DIM << "  seed=42  ·  3 nodes  ·  Raft consensus\n" << RESET;
    pause(400);

    // ── Setup ─────────────────────────────────────────────────────────────────
    Simulator sim(42);
    Network   net(sim, NetworkConfig{});

    // Role-change log: capture role transitions for pretty-printing.
    struct Event { SimTime t; NodeId id; Role role; Term term; };
    std::vector<Event> events;

    // Build cluster with snapshot_threshold=6.
    std::vector<NodeId> all_ids = {1, 2, 3};
    std::vector<std::unique_ptr<RaftNode>> nodes;
    for (NodeId id : all_ids) {
        std::vector<NodeId> peers;
        for (NodeId p : all_ids) if (p != id) peers.push_back(p);
        RaftConfig cfg;
        cfg.snapshot_threshold = 6;
        cfg.read_timeout       = 1000 * kMsec;
        auto node = std::make_unique<RaftNode>(id, peers, sim, net, cfg);
        node->set_state_change_cb([&events, &sim](NodeId nid, Role r, Term t) {
            events.push_back({sim.now(), nid, r, t});
        });
        nodes.push_back(std::move(node));
    }
    for (auto& n : nodes) n->start();

    auto find_leader = [&]() -> RaftNode* {
        for (auto& n : nodes)
            if (n->is_running() && n->role() == Role::Leader) return n.get();
        return nullptr;
    };

    // Helper: write a key and run until it commits.
    int op_seq = 0;
    std::map<std::string, std::string> committed;
    auto write_key = [&](const std::string& k, const std::string& v) -> bool {
        RaftNode* l = find_leader();
        if (!l) return false;
        bool done = false;
        bool ok   = false;
        int  seq  = op_seq++;
        l->submit(static_cast<uint64_t>(seq), static_cast<uint64_t>(seq),
                  CmdPut{k, v}, [&](CmdResult r) { done = true; ok = r.ok; });
        sim.run_for(400 * kMsec);
        if (done && ok) committed[k] = v;
        return done && ok;
    };

    // ── Phase 1: Leader Election ───────────────────────────────────────────────
    header("1", "Leader Election");
    std::size_t events_before = events.size();
    sim.run_for(600 * kMsec);

    for (std::size_t i = events_before; i < events.size(); ++i) {
        const auto& e = events[i];
        std::string suffix;
        if (e.role == Role::Leader)
            suffix = std::string("  ") + GREEN + BOLD + "✓ quorum reached" + RESET;
        line(e.t, std::format("node-{}  {}", e.id, role_str(e.role)) +
             std::format("  {}(term {}){}", DIM, e.term, RESET) + suffix);
    }

    RaftNode* leader = find_leader();
    if (!leader) { std::cerr << "no leader elected\n"; return 1; }

    // ── Phase 2: Write Traffic ────────────────────────────────────────────────
    header("2", "Write Traffic");
    const int N_WRITES = 8;
    for (int i = 0; i < N_WRITES; ++i) {
        std::string k = "k" + std::to_string(i);
        std::string v = "val-" + std::to_string(i);
        SimTime before = sim.now();
        bool ok = write_key(k, v);
        SimTime elapsed = sim.now() - before;
        leader = find_leader();

        std::string result = ok
            ? std::string(GREEN) + "✓" + RESET + "  committed"
            : std::string(RED)   + "✗" + RESET + "  failed";
        line(sim.now(), std::format("PUT {:>4}={:<8}  {}  {}({}ms){}", k, v, result,
                                    DIM, elapsed / kMsec, RESET));
    }

    // ── Phase 3: Network Partition ────────────────────────────────────────────
    header("3", "Network Partition");
    leader = find_leader();
    if (!leader) { std::cerr << "leader lost before partition\n"; return 1; }

    // Isolate one follower; keep leader + other node in majority.
    NodeId isolated_id = 0;
    for (auto& n : nodes) {
        if (n->id() != leader->id()) { isolated_id = n->id(); break; }
    }
    NodeId other_id = 0;
    for (NodeId id : all_ids) {
        if (id != leader->id() && id != isolated_id) { other_id = id; break; }
    }

    net.partition({{leader->id(), other_id}, {isolated_id}});
    line(sim.now(), std::format("{}partition:{} {{node-{}, node-{}}} | {{node-{}}}  "
                                "{}(node-{} isolated){}",
                                YELLOW, RESET,
                                leader->id(), other_id, isolated_id,
                                DIM, isolated_id, RESET));
    pause(150);

    // Two more writes — should commit in the majority partition.
    for (int i = N_WRITES; i < N_WRITES + 2; ++i) {
        std::string k = "k" + std::to_string(i);
        std::string v = "val-" + std::to_string(i);
        bool ok = write_key(k, v);
        std::string result = ok
            ? std::string(GREEN) + "✓" + RESET + "  committed  " +
              std::string(DIM)   + "(majority still reachable)" + RESET
            : std::string(RED)   + "✗" + RESET;
        line(sim.now(), std::format("PUT {:>4}={:<8}  {}", k, v, result));
    }

    // ── Phase 4: Log Compaction + InstallSnapshot ─────────────────────────────
    header("4", "Log Compaction  +  InstallSnapshot");
    sim.run_for(300 * kMsec);

    leader = find_leader();
    if (leader && leader->snap_index() > 0) {
        line(sim.now(), std::format("{}snapshot{} on node-{}  last_applied={}  "
                                    "{}log compacted{}",
                                    MAGENTA, RESET, leader->id(),
                                    leader->snap_index(), DIM, RESET));
    }
    pause(150);

    // Heal partition — the isolated follower will receive InstallSnapshot.
    net.heal();
    line(sim.now(), std::string(GREEN) + "heal partition" + RESET +
         "  — node-" + std::to_string(isolated_id) + " rejoins");
    pause(100);

    sim.run_for(800 * kMsec);

    // Find the isolated node and show its catch-up.
    RaftNode* rejoined = nullptr;
    for (auto& n : nodes) if (n->id() == isolated_id) { rejoined = n.get(); break; }
    if (rejoined) {
        line(sim.now(), std::format("node-{}  caught up  last_applied={}  {}snap_index={}{}",
                                    rejoined->id(), rejoined->last_applied(),
                                    DIM, rejoined->snap_index(), RESET));
    }

    // ── Phase 5: Linearizable Read (read-index) ───────────────────────────────
    header("5", "Linearizable Read  (read-index)");
    leader = find_leader();
    if (!leader) { std::cerr << "no leader for read\n"; return 1; }

    std::string read_key = "k5";
    bool read_called = false;
    bool read_ok     = false;
    std::optional<std::string> read_val;

    line(sim.now(), std::format("{}read_index(\"{}\"){} — broadcasting heartbeat to confirm leadership",
                                CYAN, read_key, RESET));
    pause(200);

    bool submitted = leader->read_index(read_key, [&](bool ok, std::optional<std::string> v) {
        read_called = true;
        read_ok     = ok;
        read_val    = v;
    });

    if (!submitted) {
        line(sim.now(), std::string(RED) + "✗  not leader" + RESET);
    } else {
        sim.run_for(400 * kMsec);
        if (read_called && read_ok) {
            std::string display = read_val.has_value() ? ("\"" + *read_val + "\"") : "(absent)";
            line(sim.now(), std::format("{}✓{}  key=\"{}\"  value={}  "
                                        "{}(quorum confirmed leadership){}",
                                        GREEN, RESET, read_key, display, DIM, RESET));
        } else {
            line(sim.now(), std::string(RED) + "✗  timed out" + RESET);
        }
    }

    // ── Summary ───────────────────────────────────────────────────────────────
    std::cout << "\n";
    std::cout << BOLD << "══════════════════════════════════════════════════════\n" << RESET;

    leader = find_leader();
    NodeId lid = leader ? leader->id() : 0;
    std::cout << std::format("  {}seed=42{}  ·  3 nodes  ·  leader=node-{}\n",
                             DIM, RESET, lid);
    std::cout << std::format("  {}sim-time={} ms{}  ·  {}ops committed: {}{}\n",
                             DIM, sim.now() / kMsec, RESET,
                             DIM, static_cast<int>(committed.size()), RESET);
    std::cout << std::format("  {}features:{} leader election  ·  replication  ·  "
                             "crash recovery\n", DIM, RESET);
    std::cout << std::format("  {}         {}"
                             " partition tolerance  ·  log compaction  ·  read-index\n",
                             DIM, RESET);
    std::cout << BOLD << "══════════════════════════════════════════════════════\n" << RESET;
    std::cout << "\n";

    return 0;
}
