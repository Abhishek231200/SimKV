#include "raft/node.hpp"
#include "storage/codec.hpp"
#include <algorithm>
#include <cassert>

// ──────────────────────────────────────────────────────────────────────────────
// Construction / lifecycle
// ──────────────────────────────────────────────────────────────────────────────

RaftNode::RaftNode(NodeId id, std::vector<NodeId> peers,
                   Simulator& sim, Network& net, RaftConfig cfg)
    : id_(id), peers_(std::move(peers)), sim_(sim), net_(net), cfg_(cfg) {
    std::sort(peers_.begin(), peers_.end()); // deterministic iteration order
}

void RaftNode::start() {
    running_ = true;
    net_.set_handler(id_, [this](Message m) { on_message(std::move(m)); });
    // Restore persistent state if any exists.
    if (meta_store_.durable_size() > 0) {
        auto data = meta_store_.read_durable();
        Decoder dec(data);
        current_term_ = dec.u64();
        bool has_voted = dec.boolean();
        if (has_voted) voted_for_ = dec.u32();
    }
    if (log_store_.durable_size() > 0) {
        log_.load_from(log_store_);
    }
    schedule_election_timeout();
}

void RaftNode::crash() {
    running_ = false;
    // Remove from network — messages will be dropped.
    net_.remove_handler(id_);
    // Cancel timers (lazy — they'll fire but running_ check aborts them).
    election_scheduled_  = false;
    heartbeat_scheduled_ = false;
    // Simulate disk crash: unflushed tail lost.
    meta_store_.crash();
    log_store_.crash();
    // Reset volatile state.
    role_         = Role::Follower;
    commit_index_ = 0;
    last_applied_ = 0;
    leader_id_    = std::nullopt;
    votes_received_.clear();
    next_index_.clear();
    match_index_.clear();
    vote_count_   = 0;
    election_term_ = 0;
    // Fail pending client requests.
    for (auto& [idx, cb_pair] : pending_clients_) {
        cb_pair.first(CmdResult{false, "", "node_crashed"});
    }
    pending_clients_.clear();
}

void RaftNode::restart() {
    // Reload persistent state from durable storage, then start timers.
    current_term_ = 0;
    voted_for_    = std::nullopt;
    log_          = RaftLog{};
    start();
}

// ──────────────────────────────────────────────────────────────────────────────
// Client interface
// ──────────────────────────────────────────────────────────────────────────────

bool RaftNode::submit(uint64_t client_id, uint64_t request_id,
                      Command cmd, ClientCallback cb) {
    if (!running_ || role_ != Role::Leader) return false;

    Index new_index = log_.last_index() + 1;
    LogEntry entry{current_term_, new_index, std::move(cmd)};
    log_.append(entry);

    // Persist the new entry before sending to peers.
    persist();

    // Update own matchIndex.
    match_index_[id_] = new_index;
    next_index_[id_]  = new_index + 1;

    pending_clients_[new_index] = {std::move(cb), static_cast<NodeId>(client_id)};

    // Immediately replicate to peers.
    replicate_all();
    return true;
}

std::optional<std::string> RaftNode::local_get(const std::string& key) const {
    KvStore kv;
    for (Index i = 1; i <= last_applied_; ++i) {
        kv.apply(log_.at(i).cmd);
    }
    return kv.get(key);
}

// ──────────────────────────────────────────────────────────────────────────────
// Message dispatch
// ──────────────────────────────────────────────────────────────────────────────

void RaftNode::on_message(Message msg) {
    if (!running_) return;
    if (msg.payload.empty()) return;

    auto type = static_cast<MsgType>(msg.payload[0]);
    const uint8_t* data = msg.payload.data();
    std::size_t    size = msg.payload.size();

    try {
        switch (type) {
        case MsgType::RequestVote:
            handle_request_vote(msg.from, RequestVote::decode(data, size));
            break;
        case MsgType::RequestVoteReply:
            handle_request_vote_reply(msg.from, RequestVoteReply::decode(data, size));
            break;
        case MsgType::AppendEntries:
            handle_append_entries(msg.from, AppendEntries::decode(data, size));
            break;
        case MsgType::AppendEntriesReply:
            handle_append_entries_reply(msg.from, AppendEntriesReply::decode(data, size));
            break;
        case MsgType::ClientRequest:
            handle_client_request(msg.from, ClientRequest::decode(data, size));
            break;
        default:
            break;
        }
    } catch (...) {
        // Corrupted/unknown message — ignore.
    }
}

// ──────────────────────────────────────────────────────────────────────────────
// RequestVote
// ──────────────────────────────────────────────────────────────────────────────

void RaftNode::handle_request_vote(NodeId from, const RequestVote& rv) {
    if (rv.term > current_term_) {
        become_follower(rv.term);
    }

    bool grant = false;
    if (rv.term >= current_term_) {
        bool can_vote = (!voted_for_.has_value() || voted_for_ == rv.candidate_id);
        bool up_to_date = cfg_.bug_skip_vote_uptodate
                          || is_log_up_to_date(rv.last_log_index, rv.last_log_term);
        if (can_vote && up_to_date) {
            grant = true;
            voted_for_ = rv.candidate_id;
            persist();
            // Reset election timeout on granting a vote.
            cancel_election_timeout();
            schedule_election_timeout();
        }
    }

    RequestVoteReply reply{current_term_, grant};
    send(from, reply.encode());
}

void RaftNode::handle_request_vote_reply(NodeId from, const RequestVoteReply& rvr) {
    if (role_ != Role::Candidate) return;
    if (rvr.term > current_term_) { become_follower(rvr.term); return; }
    if (rvr.term < current_term_) return; // stale reply
    if (rvr.term != election_term_) return;

    if (rvr.vote_granted) {
        votes_received_.insert(from);
        vote_count_ = static_cast<uint32_t>(votes_received_.size()) + 1; // +1 for self
        if (vote_count_ >= quorum()) {
            become_leader();
        }
    }
}

// ──────────────────────────────────────────────────────────────────────────────
// AppendEntries
// ──────────────────────────────────────────────────────────────────────────────

void RaftNode::handle_append_entries(NodeId from, const AppendEntries& ae) {
    AppendEntriesReply reply{current_term_, false, 0, 0};

    if (ae.term < current_term_) {
        send(from, reply.encode());
        return;
    }

    if (ae.term > current_term_) become_follower(ae.term, from);
    else                         become_follower(current_term_, from); // same term: reset timer

    // Confirm prevLog matches.
    if (!log_.contains(ae.prev_log_index, ae.prev_log_term)) {
        // Fast backtracking: tell leader where to retry from.
        if (ae.prev_log_index > log_.last_index()) {
            reply.conflict_index = log_.last_index() + 1;
            reply.conflict_term  = 0;
        } else {
            Term ct = log_.at(ae.prev_log_index).term;
            reply.conflict_term  = ct;
            reply.conflict_index = log_.last_index_for_term(ct);
            if (reply.conflict_index == 0) reply.conflict_index = 1;
        }
        send(from, reply.encode());
        return;
    }

    // Append new entries, deleting any conflicting suffix first.
    for (const auto& entry : ae.entries) {
        if (entry.index <= log_.last_index()) {
            if (log_.at(entry.index).term != entry.term) {
                log_.truncate_from(entry.index);
            } else {
                continue; // already have this entry
            }
        }
        log_.append(entry);
    }

    persist();

    // Advance commitIndex.
    if (ae.leader_commit > commit_index_) {
        commit_index_ = std::min(ae.leader_commit, log_.last_index());
        apply_committed_entries();
    }

    reply.term    = current_term_;
    reply.success = true;
    send(from, reply.encode());
}

void RaftNode::handle_append_entries_reply(NodeId from, const AppendEntriesReply& aer) {
    if (role_ != Role::Leader) return;
    if (aer.term > current_term_) { become_follower(aer.term); return; }
    if (aer.term < current_term_) return;

    if (aer.success) {
        // Advance matchIndex and nextIndex.
        Index new_match = next_index_[from] + /* entries sent */ 0;
        // We need to know how many entries were sent; track via nextIndex before reply.
        // The reply doesn't carry this, but next_index_[from] was set to last_index+1
        // when we sent. Since success means peer has all entries up to prev+sent,
        // we can safely advance matchIndex to the maximum confirmed by next_index_.
        // Use: match = next_index[peer] - 1 (what we expected them to have).
        new_match = next_index_[from] - 1;
        match_index_[from] = std::max(match_index_[from], new_match);
        next_index_[from]  = match_index_[from] + 1;
        maybe_advance_commit_index();
    } else {
        // Fast log backtracking.
        if (aer.conflict_term == 0) {
            next_index_[from] = aer.conflict_index;
        } else {
            Index last_in_term = log_.last_index_for_term(aer.conflict_term);
            if (last_in_term > 0) {
                next_index_[from] = last_in_term + 1;
            } else {
                next_index_[from] = aer.conflict_index;
            }
        }
        if (next_index_[from] < 1) next_index_[from] = 1;
        // Retry immediately.
        replicate_to(from);
    }
}

// ──────────────────────────────────────────────────────────────────────────────
// Client request (via network from workload clients)
// ──────────────────────────────────────────────────────────────────────────────

void RaftNode::handle_client_request(NodeId from, const ClientRequest& cr) {
    if (role_ != Role::Leader) {
        ClientReply reply;
        reply.client_id   = cr.client_id;
        reply.request_id  = cr.request_id;
        reply.success     = false;
        reply.result      = CmdResult{false, "", "not_leader"};
        reply.leader_hint = leader_id_.value_or(0);
        send(from, reply.encode());
        return;
    }
    submit(cr.client_id, cr.request_id, cr.cmd,
           [this, from, cr](CmdResult res) {
               ClientReply reply;
               reply.client_id   = cr.client_id;
               reply.request_id  = cr.request_id;
               reply.success     = true;
               reply.result      = std::move(res);
               reply.leader_hint = id_;
               send(from, reply.encode());
           });
}

// ──────────────────────────────────────────────────────────────────────────────
// Role transitions
// ──────────────────────────────────────────────────────────────────────────────

void RaftNode::become_follower(Term new_term, std::optional<NodeId> new_leader) {
    bool term_changed = new_term > current_term_;
    if (term_changed) {
        current_term_ = new_term;
        voted_for_    = std::nullopt;
        persist();
    }
    if (role_ != Role::Follower || term_changed) {
        role_ = Role::Follower;
        cancel_heartbeat();
        if (state_change_cb_) state_change_cb_(id_, role_, current_term_);
    }
    leader_id_ = new_leader;
    votes_received_.clear();
    vote_count_   = 0;

    cancel_election_timeout();
    schedule_election_timeout();
}

void RaftNode::become_candidate() {
    if (!running_) return;
    current_term_++;
    role_          = Role::Candidate;
    voted_for_     = id_;
    votes_received_.clear();
    vote_count_    = 1; // vote for self
    election_term_ = current_term_;
    leader_id_     = std::nullopt;
    persist();

    if (state_change_cb_) state_change_cb_(id_, role_, current_term_);

    // Request votes from all peers.
    RequestVote rv{current_term_, id_, log_.last_index(), log_.last_term()};
    for (NodeId peer : peers_) {
        send(peer, rv.encode());
    }

    cancel_election_timeout();
    schedule_election_timeout();

    // Check immediately if we're the only node (single-node cluster).
    if (vote_count_ >= quorum()) become_leader();
}

void RaftNode::become_leader() {
    if (role_ == Role::Leader) return;
    role_      = Role::Leader;
    leader_id_ = id_;
    votes_received_.clear();
    vote_count_ = 0;

    cancel_election_timeout();

    // Initialize nextIndex and matchIndex (Raft Figure 2).
    next_index_.clear();
    match_index_.clear();
    for (NodeId peer : peers_) {
        next_index_[peer]  = log_.last_index() + 1;
        match_index_[peer] = 0;
    }
    match_index_[id_] = log_.last_index();
    next_index_[id_]  = log_.last_index() + 1;

    if (state_change_cb_) state_change_cb_(id_, role_, current_term_);

    schedule_heartbeat();
    // Send immediate heartbeat to assert leadership.
    replicate_all();
}

// ──────────────────────────────────────────────────────────────────────────────
// Replication
// ──────────────────────────────────────────────────────────────────────────────

void RaftNode::replicate_to(NodeId peer) {
    if (role_ != Role::Leader) return;

    Index prev_index = next_index_[peer] - 1;
    Term  prev_term  = (prev_index > 0 && prev_index <= log_.last_index())
                           ? log_.at(prev_index).term
                           : 0;

    auto entries = log_.slice(prev_index, // exclusive
                              std::min(prev_index + cfg_.max_entries_per_ae,
                                       log_.last_index()));

    // Advance nextIndex optimistically so replies know the window.
    if (!entries.empty()) {
        next_index_[peer] = entries.back().index + 1;
    }

    AppendEntries ae{current_term_, id_, prev_index, prev_term,
                     std::move(entries), commit_index_};
    send(peer, ae.encode());
}

void RaftNode::replicate_all() {
    for (NodeId peer : peers_) {
        replicate_to(peer);
    }
}

void RaftNode::maybe_advance_commit_index() {
    // Advance commitIndex to the highest N such that:
    // 1. A majority of matchIndex[i] >= N
    // 2. log[N].term == currentTerm   (safety: never commit prior-term by counting)
    for (Index n = log_.last_index(); n > commit_index_; --n) {
        if (!cfg_.bug_commit_prior_term && log_.at(n).term != current_term_) continue;

        std::size_t count = 0;
        for (const auto& [peer, mi] : match_index_) {
            if (mi >= n) ++count;
        }
        if (count >= quorum()) {
            commit_index_ = n;
            apply_committed_entries();
            break;
        }
    }
}

void RaftNode::apply_committed_entries() {
    KvStore kv;
    // Rebuild state from scratch (simple; snapshot optimization is Phase 6).
    for (Index i = 1; i <= last_applied_; ++i) {
        kv.apply(log_.at(i).cmd);
    }
    while (last_applied_ < commit_index_) {
        ++last_applied_;
        CmdResult result = kv.apply(log_.at(last_applied_).cmd);

        auto it = pending_clients_.find(last_applied_);
        if (it != pending_clients_.end()) {
            it->second.first(std::move(result));
            pending_clients_.erase(it);
        }
    }
}

// ──────────────────────────────────────────────────────────────────────────────
// Timer scheduling
// ──────────────────────────────────────────────────────────────────────────────

void RaftNode::schedule_election_timeout() {
    if (election_scheduled_) return;
    SimTime delay = sim_.prng().range(cfg_.election_timeout_lo,
                                      2 * cfg_.election_timeout_lo + 1);
    election_scheduled_ = true;
    election_timeout_id_ = sim_.schedule(delay, [this]() {
        election_scheduled_ = false;
        if (!running_) return;
        on_election_timeout();
    });
}

void RaftNode::cancel_election_timeout() {
    if (election_scheduled_) {
        sim_.cancel(election_timeout_id_);
        election_scheduled_ = false;
    }
}

void RaftNode::schedule_heartbeat() {
    if (heartbeat_scheduled_) return;
    heartbeat_scheduled_ = true;
    heartbeat_id_ = sim_.schedule(cfg_.heartbeat_interval, [this]() {
        heartbeat_scheduled_ = false;
        if (!running_) return;
        on_heartbeat();
    });
}

void RaftNode::cancel_heartbeat() {
    if (heartbeat_scheduled_) {
        sim_.cancel(heartbeat_id_);
        heartbeat_scheduled_ = false;
    }
}

void RaftNode::on_election_timeout() {
    if (role_ == Role::Leader) return; // leaders don't time out
    become_candidate();
}

void RaftNode::on_heartbeat() {
    if (role_ != Role::Leader) return;
    replicate_all();
    schedule_heartbeat();
}

// ──────────────────────────────────────────────────────────────────────────────
// Persistence
// ──────────────────────────────────────────────────────────────────────────────

void RaftNode::persist() {
    // Serialize currentTerm + votedFor to meta_store_.
    meta_store_.truncate_to(0);
    Encoder enc;
    enc.u64(current_term_);
    enc.boolean(voted_for_.has_value());
    if (voted_for_.has_value()) enc.u32(*voted_for_);
    auto bytes = enc.take();
    meta_store_.append(bytes);
    meta_store_.flush();

    // Persist the log.
    log_store_.truncate_to(0);
    log_.save_to(log_store_);
}

// ──────────────────────────────────────────────────────────────────────────────
// Helpers
// ──────────────────────────────────────────────────────────────────────────────

bool RaftNode::is_log_up_to_date(Index last_idx, Term last_term) const {
    // Raft §5.4.1: candidate's log is at least as up-to-date as ours iff:
    // - candidate's lastLogTerm > our lastLogTerm, OR
    // - terms are equal AND candidate's lastLogIndex >= our lastLogIndex.
    if (last_term != log_.last_term()) return last_term > log_.last_term();
    return last_idx >= log_.last_index();
}

void RaftNode::send(NodeId to, std::vector<uint8_t> msg) {
    net_.send(id_, to, std::move(msg));
}
