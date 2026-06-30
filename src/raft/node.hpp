#pragma once
#include "raft/log.hpp"
#include "raft/rpc.hpp"
#include "raft/types.hpp"
#include "sim/network.hpp"
#include "sim/simulator.hpp"
#include "storage/durable_store.hpp"
#include "storage/idurable_store.hpp"
#include "storage/kv_store.hpp"
#include <functional>
#include <map>
#include <memory>
#include <optional>
#include <set>
#include <vector>

struct RaftConfig {
    SimTime election_timeout_lo  = 150 * kMsec; // randomized in [lo, 2*lo]
    SimTime heartbeat_interval   = 50  * kMsec;
    SimTime client_timeout       = 500 * kMsec;
    std::size_t max_entries_per_ae = 64;

    // Snapshot: take a snapshot every N committed entries (0 = disabled).
    std::size_t snapshot_threshold = 0;

    // Read-index: how long to wait for majority acks before cancelling (ok=false).
    SimTime read_timeout = 2000 * kMsec;

    // Bug injection flags — for correctness testing only.
    bool bug_commit_prior_term   = false;
    bool bug_skip_vote_uptodate  = false;
};

using ClientCallback      = std::function<void(CmdResult)>;
using StateChangeCallback = std::function<void(NodeId, Role, Term)>;
// Callback for read_index: ok=true + value on success; ok=false when leadership lost.
using ReadCallback        = std::function<void(bool ok, std::optional<std::string> value)>;

class RaftNode {
public:
    // By default creates in-memory DurableStore instances (simulation-safe).
    // Pass non-null store pointers to use FileDurableStore for real persistence.
    RaftNode(NodeId id,
             std::vector<NodeId> peers,
             Simulator& sim,
             Network&   net,
             RaftConfig cfg = {},
             std::unique_ptr<IDurableStore> meta_store     = nullptr,
             std::unique_ptr<IDurableStore> log_store      = nullptr,
             std::unique_ptr<IDurableStore> snapshot_store = nullptr);

    void start();
    void crash();
    void restart();

    bool is_running() const { return running_; }

    bool submit(uint64_t client_id, uint64_t request_id,
                Command cmd, ClientCallback cb);

    // Linearizable read via read-index (Raft §6.4).
    // Returns false immediately if this node is not the leader.
    // Otherwise, confirms leadership with a majority heartbeat round before
    // serving the read — cb fires once confirmed or cancelled (ok=false).
    bool read_index(const std::string& key, ReadCallback cb);

    std::optional<std::string> local_get(const std::string& key) const;

    NodeId id()           const { return id_; }
    Role   role()         const { return role_; }
    Term   current_term() const { return current_term_; }
    Index  commit_index() const { return commit_index_; }
    Index  last_applied() const { return last_applied_; }
    Index  snap_index()   const { return snap_last_index_; }
    std::optional<NodeId> leader_id() const { return leader_id_; }

    void set_state_change_cb(StateChangeCallback cb) { state_change_cb_ = std::move(cb); }

private:
    // ── Message dispatch ────────────────────────────────────────────────────
    void on_message(Message msg);
    void handle_request_vote(NodeId from, const RequestVote& rv);
    void handle_request_vote_reply(NodeId from, const RequestVoteReply& rvr);
    void handle_append_entries(NodeId from, const AppendEntries& ae);
    void handle_append_entries_reply(NodeId from, const AppendEntriesReply& aer);
    void handle_client_request(NodeId from, const ClientRequest& cr);
    void handle_install_snapshot(NodeId from, const InstallSnapshot& is);
    void handle_install_snapshot_reply(NodeId from, const InstallSnapshotReply& isr);
    void handle_read_index_request(NodeId from, const ReadIndexRequest& req);
    void handle_read_index_reply(NodeId from, const ReadIndexReply& rep);

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

    // ── Snapshotting ────────────────────────────────────────────────────────
    void take_snapshot();
    void save_snapshot();
    void load_snapshot();
    void send_install_snapshot(NodeId peer);

    // ── Read-index ──────────────────────────────────────────────────────────
    void drain_pending_reads();
    void cancel_pending_reads(); // called on step-down / crash

    // ── Membership ──────────────────────────────────────────────────────────
    void apply_membership_change(const Command& cmd);

    // ── Timer scheduling ────────────────────────────────────────────────────
    void schedule_election_timeout();
    void cancel_election_timeout();
    void schedule_heartbeat();
    void cancel_heartbeat();

    // ── Persistence ─────────────────────────────────────────────────────────
    void persist();

    // ── Helpers ─────────────────────────────────────────────────────────────
    bool is_log_up_to_date(Index last_idx, Term last_term) const;
    std::size_t quorum() const { return (peers_.size() + 1) / 2 + 1; }
    void send(NodeId to, std::vector<uint8_t> msg);

    // ── Identity & config ───────────────────────────────────────────────────
    NodeId              id_;
    std::vector<NodeId> peers_;
    Simulator&          sim_;
    Network&            net_;
    RaftConfig          cfg_;
    bool                running_ = false;

    // ── Persistent state ─────────────────────────────────────────────────────
    Term                     current_term_ = 0;
    std::optional<NodeId>    voted_for_;
    RaftLog                  log_;
    std::unique_ptr<IDurableStore> meta_store_;
    std::unique_ptr<IDurableStore> log_store_;
    std::unique_ptr<IDurableStore> snapshot_store_;

    // Snapshot state (also durable via snapshot_store_).
    Index                snap_last_index_ = 0;
    Term                 snap_last_term_  = 0;
    std::vector<uint8_t> snap_data_;       // serialized KV state at snap_last_index_

    // ── Volatile state ───────────────────────────────────────────────────────
    Role                  role_         = Role::Follower;
    Index                 commit_index_ = 0;
    Index                 last_applied_ = 0;
    std::optional<NodeId> leader_id_;
    KvStore               kv_;            // incremental state machine (not rebuilt from scratch)

    std::set<NodeId> votes_received_;

    std::map<NodeId, Index> next_index_;
    std::map<NodeId, Index> match_index_;

    std::map<Index, std::pair<ClientCallback, NodeId>> pending_clients_;

    // Read-index state (volatile — cleared on step-down and crash).
    struct PendingRead {
        uint64_t    round;       // the ReadIndexRequest round this read is waiting for
        Index       read_index;  // commit_index_ at submission time
        std::string key;
        ReadCallback cb;
        std::size_t acks;        // starts at 1 (self)
        EventId     timeout_id;  // cancels the read if majority acks don't arrive in time
    };
    std::vector<PendingRead> pending_reads_;
    uint64_t next_read_round_ = 1;

    EventId election_timeout_id_ = 0;
    bool    election_scheduled_  = false;
    EventId heartbeat_id_        = 0;
    bool    heartbeat_scheduled_ = false;

    StateChangeCallback state_change_cb_;

    Term     election_term_ = 0;
    uint32_t vote_count_    = 0;

    // Membership: at most one config-change entry may be in-flight at a time.
    bool config_change_pending_ = false;
};
