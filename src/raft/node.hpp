#pragma once
#include "raft/log.hpp"
#include "raft/rpc.hpp"
#include "raft/types.hpp"
#include "sim/network.hpp"
#include "sim/simulator.hpp"
#include "storage/durable_store.hpp"
#include "storage/kv_store.hpp"
#include <functional>
#include <map>
#include <optional>
#include <set>
#include <vector>

struct RaftConfig {
    SimTime election_timeout_lo  = 150 * kMsec; // randomized in [lo, 2*lo]
    SimTime heartbeat_interval   = 50  * kMsec;
    SimTime client_timeout       = 500 * kMsec; // how long a leader holds a pending req
    std::size_t max_entries_per_ae = 64;         // batch cap for AppendEntries

    // Bug injection flags — for correctness testing only.
    bool bug_commit_prior_term   = false; // skip current-term check in commit rule
    bool bug_skip_vote_uptodate  = false; // skip log up-to-date check when granting vote
};

// Callback types exposed to the harness.
using ClientCallback = std::function<void(CmdResult)>;
using StateChangeCallback = std::function<void(NodeId, Role, Term)>;

// A Raft node: a message-driven state machine that communicates only through
// the simulated Network and schedules timers as simulator events.
// No threads, no wall-clock, no real I/O — everything is deterministic.
class RaftNode {
public:
    RaftNode(NodeId id,
             std::vector<NodeId> peers,
             Simulator& sim,
             Network&   net,
             RaftConfig cfg = {});

    // Called by the harness to start the node (registers network handler, starts timers).
    void start();

    // Simulate a crash: volatile state lost, unflushed disk tail lost, handler removed.
    // Call restart() after to bring the node back.
    void crash();
    void restart();

    bool is_running() const { return running_; }

    // Submit a client command. cb fires exactly once when committed+applied or on error.
    // Returns false if this node is not the leader; caller should redirect.
    bool submit(uint64_t client_id, uint64_t request_id,
                Command cmd, ClientCallback cb);

    // Read the applied KV state (used for Get operations).
    // For linearizable reads, this should only be called by the leader after a
    // heartbeat confirms leadership — see submit() for the full read path.
    std::optional<std::string> local_get(const std::string& key) const;

    NodeId id()           const { return id_; }
    Role   role()         const { return role_; }
    Term   current_term() const { return current_term_; }
    Index  commit_index() const { return commit_index_; }
    Index  last_applied() const { return last_applied_; }
    std::optional<NodeId> leader_id() const { return leader_id_; }

    // Observe role/term changes (used by the harness for metrics).
    void set_state_change_cb(StateChangeCallback cb) { state_change_cb_ = std::move(cb); }

private:
    // ── Message dispatch ────────────────────────────────────────────────────
    void on_message(Message msg);
    void handle_request_vote(NodeId from, const RequestVote& rv);
    void handle_request_vote_reply(NodeId from, const RequestVoteReply& rvr);
    void handle_append_entries(NodeId from, const AppendEntries& ae);
    void handle_append_entries_reply(NodeId from, const AppendEntriesReply& aer);
    void handle_client_request(NodeId from, const ClientRequest& cr);

    // ── Timer callbacks ─────────────────────────────────────────────────────
    void on_election_timeout();
    void on_heartbeat();

    // ── Role transitions ────────────────────────────────────────────────────
    void become_follower(Term new_term, std::optional<NodeId> new_leader = std::nullopt);
    void become_candidate();
    void become_leader();

    // ── Replication ─────────────────────────────────────────────────────────
    void replicate_to(NodeId peer);
    void replicate_all();
    void maybe_advance_commit_index();
    void apply_committed_entries();

    // ── Timer scheduling ────────────────────────────────────────────────────
    void schedule_election_timeout();
    void cancel_election_timeout();
    void schedule_heartbeat();
    void cancel_heartbeat();

    // ── Persistence ─────────────────────────────────────────────────────────
    // Must be called before sending any reply that depends on updated state.
    void persist(); // saves currentTerm, votedFor, log

    // ── Helpers ─────────────────────────────────────────────────────────────
    bool is_log_up_to_date(Index last_idx, Term last_term) const;
    std::size_t quorum() const { return (peers_.size() + 1) / 2 + 1; }
    void send(NodeId to, std::vector<uint8_t> msg);

    // ── Identity & config ───────────────────────────────────────────────────
    NodeId              id_;
    std::vector<NodeId> peers_; // sorted, excludes self
    Simulator&          sim_;
    Network&            net_;
    RaftConfig          cfg_;
    bool                running_ = false;

    // ── Persistent state (survives crash if flushed) ────────────────────────
    Term                     current_term_ = 0;
    std::optional<NodeId>    voted_for_;
    RaftLog                  log_;
    DurableStore             meta_store_;  // stores currentTerm + votedFor
    DurableStore             log_store_;   // stores the log entries

    // ── Volatile state (reset on crash) ─────────────────────────────────────
    Role                  role_       = Role::Follower;
    Index                 commit_index_ = 0;
    Index                 last_applied_ = 0;
    std::optional<NodeId> leader_id_;

    // Votes received this election (candidate only).
    std::set<NodeId> votes_received_;

    // Leader volatile state.
    std::map<NodeId, Index> next_index_;   // next log index to send to each peer
    std::map<NodeId, Index> match_index_;  // highest replicated index per peer

    // Pending client callbacks keyed by log index.
    std::map<Index, std::pair<ClientCallback, NodeId /*client*/>> pending_clients_;

    // Timer event IDs.
    EventId election_timeout_id_ = 0;
    bool    election_scheduled_  = false;
    EventId heartbeat_id_        = 0;
    bool    heartbeat_scheduled_ = false;

    StateChangeCallback state_change_cb_;

    // Vote counting: votes[term] = count, to ignore stale replies.
    Term    election_term_ = 0;
    uint32_t vote_count_  = 0;
};
