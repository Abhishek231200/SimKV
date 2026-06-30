#include "raft/node.hpp"
#include "storage/codec.hpp"
#include <algorithm>
#include <cassert>
#include <memory>

// ──────────────────────────────────────────────────────────────────────────────
// Construction / lifecycle
// ──────────────────────────────────────────────────────────────────────────────

RaftNode::RaftNode(NodeId id, std::vector<NodeId> peers,
                   IClock& sim, ITransport& net, RaftConfig cfg,
                   std::unique_ptr<IDurableStore> meta_store,
                   std::unique_ptr<IDurableStore> log_store,
                   std::unique_ptr<IDurableStore> snapshot_store)
    : id_(id), peers_(std::move(peers)), sim_(sim), net_(net), cfg_(cfg) {
    std::sort(peers_.begin(), peers_.end());
    meta_store_     = meta_store     ? std::move(meta_store)     : std::make_unique<DurableStore>();
    log_store_      = log_store      ? std::move(log_store)      : std::make_unique<DurableStore>();
    snapshot_store_ = snapshot_store ? std::move(snapshot_store) : std::make_unique<DurableStore>();
}

void RaftNode::start() {
    running_ = true;
    net_.set_handler(id_, [this](Message m) { on_message(std::move(m)); });

    // 1. Load snapshot first (establishes kv_ baseline and snap indices).
    load_snapshot();

    // 2. Load meta (term + voted_for).
    if (meta_store_->durable_size() > 0) {
        auto data = meta_store_->read_durable();
        Decoder dec(data);
        current_term_ = dec.u64();
        bool has_voted = dec.boolean();
        if (has_voted) voted_for_ = dec.u32();
    }

    // 3. Load log (may contain entries after the snapshot point).
    if (log_store_->durable_size() > 0) {
        log_.load_from(*log_store_);
    }

    // Apply any log entries that are past last_applied_ up to commit_index_.
    // (commit_index_ is volatile; after restart it starts equal to snap_last_index_.
    //  The leader will advance it via heartbeats.)
    apply_committed_entries();

    schedule_election_timeout();
}

void RaftNode::crash() {
    running_ = false;
    net_.remove_handler(id_);
    election_scheduled_  = false;
    heartbeat_scheduled_ = false;

    meta_store_->crash();
    log_store_->crash();
    snapshot_store_->crash();

    role_         = Role::Follower;
    commit_index_ = snap_last_index_; // snapshot is the last known durable state
    last_applied_ = snap_last_index_;
    leader_id_    = std::nullopt;
    votes_received_.clear();
    next_index_.clear();
    match_index_.clear();
    vote_count_   = 0;
    election_term_ = 0;
    config_change_pending_ = false;

    // Reset kv_ to the snapshot baseline; will be rebuilt from log on restart.
    kv_ = KvStore{};
    if (!snap_data_.empty()) {
        Decoder dec(snap_data_.data(), snap_data_.size());
        uint32_t count = dec.u32();
        std::map<std::string, std::string> kv_map;
        for (uint32_t i = 0; i < count; ++i) {
            auto k = dec.str();
            auto v = dec.str();
            kv_map[k] = v;
        }
        kv_.load(std::move(kv_map));
    }

    for (auto& [idx, cb_pair] : pending_clients_) {
        cb_pair.first(CmdResult{false, "", "node_crashed"});
    }
    pending_clients_.clear();
    for (auto& pr : pending_reads_) sim_.cancel(pr.timeout_id);
    pending_reads_.clear(); // silently drop; callers will timeout
}

void RaftNode::restart() {
    current_term_ = 0;
    voted_for_    = std::nullopt;
    log_          = RaftLog{};
    kv_           = KvStore{};
    start();
}

// ──────────────────────────────────────────────────────────────────────────────
// Client interface
// ──────────────────────────────────────────────────────────────────────────────

bool RaftNode::submit(uint64_t client_id, uint64_t request_id,
                      Command cmd, ClientCallback cb) {
    if (!running_ || role_ != Role::Leader) return false;

    // Only one membership change may be in-flight.
    bool is_config = std::holds_alternative<CmdAddServer>(cmd)
                  || std::holds_alternative<CmdRemoveServer>(cmd);
    if (is_config && config_change_pending_) return false;
    if (is_config) config_change_pending_ = true;

    Index new_index = log_.last_index() + 1;
    LogEntry entry{current_term_, new_index, std::move(cmd)};
    log_.append(entry);
    persist();

    match_index_[id_] = new_index;
    next_index_[id_]  = new_index + 1;
    pending_clients_[new_index] = {std::move(cb), static_cast<NodeId>(client_id)};
    replicate_all();
    maybe_advance_commit_index(); // handles single-node clusters (no peer replies)
    return true;
}

std::optional<std::string> RaftNode::local_get(const std::string& key) const {
    return kv_.get(key);
}

bool RaftNode::read_index(const std::string& key, ReadCallback cb) {
    if (!running_ || role_ != Role::Leader) return false;

    // Single-node cluster: we are the only quorum member — serve immediately.
    if (peers_.empty()) {
        cb(true, kv_.get(key));
        return true;
    }

    uint64_t round = next_read_round_++;
    pending_reads_.push_back({round, commit_index_, key, std::move(cb), 1, EventId{}});
    PendingRead& pr = pending_reads_.back();

    // Schedule a timeout: if majority acks don't arrive in time, cancel the read.
    pr.timeout_id = sim_.schedule(cfg_.read_timeout, [this, round]() {
        for (auto it = pending_reads_.begin(); it != pending_reads_.end(); ++it) {
            if (it->round == round) {
                it->cb(false, std::nullopt);
                pending_reads_.erase(it);
                return;
            }
        }
    });

    ReadIndexRequest req{current_term_, id_, round};
    for (NodeId peer : peers_) send(peer, req.encode());
    return true;
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
        case MsgType::InstallSnapshot:
            handle_install_snapshot(msg.from, InstallSnapshot::decode(data, size));
            break;
        case MsgType::InstallSnapshotReply:
            handle_install_snapshot_reply(msg.from, InstallSnapshotReply::decode(data, size));
            break;
        case MsgType::ReadIndexRequest:
            handle_read_index_request(msg.from, ReadIndexRequest::decode(data, size));
            break;
        case MsgType::ReadIndexReply:
            handle_read_index_reply(msg.from, ReadIndexReply::decode(data, size));
            break;
        default:
            break;
        }
    } catch (const std::exception& e) {
        std::fprintf(stderr, "[node %u] on_message EXCEPTION: %s\n", id_, e.what());
    } catch (...) {
        std::fprintf(stderr, "[node %u] on_message UNKNOWN EXCEPTION\n", id_);
    }
}

// ──────────────────────────────────────────────────────────────────────────────
// RequestVote
// ──────────────────────────────────────────────────────────────────────────────

void RaftNode::handle_request_vote(NodeId from, const RequestVote& rv) {
    if (rv.term > current_term_) become_follower(rv.term);

    bool grant = false;
    if (rv.term >= current_term_) {
        bool can_vote = (!voted_for_.has_value() || voted_for_ == rv.candidate_id);
        bool up_to_date = cfg_.bug_skip_vote_uptodate
                          || is_log_up_to_date(rv.last_log_index, rv.last_log_term);
        if (can_vote && up_to_date) {
            grant = true;
            voted_for_ = rv.candidate_id;
            persist();
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
    if (rvr.term < current_term_ || rvr.term != election_term_) return;

    if (rvr.vote_granted) {
        votes_received_.insert(from);
        vote_count_ = static_cast<uint32_t>(votes_received_.size()) + 1;
        if (vote_count_ >= quorum()) become_leader();
    }
}

// ──────────────────────────────────────────────────────────────────────────────
// AppendEntries
// ──────────────────────────────────────────────────────────────────────────────

void RaftNode::handle_append_entries(NodeId from, const AppendEntries& ae) {
    AppendEntriesReply reply{current_term_, false, 0, 0};

    if (ae.term < current_term_) { send(from, reply.encode()); return; }
    if (ae.term > current_term_) become_follower(ae.term, from);
    else                         become_follower(current_term_, from);

    // prevLog check — respects snapshot boundary via RaftLog::contains.
    if (!log_.contains(ae.prev_log_index, ae.prev_log_term)) {
        if (ae.prev_log_index > log_.last_index()) {
            reply.conflict_index = log_.last_index() + 1;
            reply.conflict_term  = 0;
        } else if (ae.prev_log_index > log_.base_index()) {
            Term ct = log_.at(ae.prev_log_index).term;
            reply.conflict_term  = ct;
            reply.conflict_index = log_.last_index_for_term(ct);
            if (reply.conflict_index == 0) reply.conflict_index = 1;
        } else {
            // prev_log_index is behind our snapshot — we can't accept this AE.
            // The leader should send InstallSnapshot instead.
            reply.conflict_index = log_.base_index() + 1;
            reply.conflict_term  = 0;
        }
        send(from, reply.encode()); return;
    }

    for (const auto& entry : ae.entries) {
        if (entry.index <= log_.last_index()) {
            if (entry.index <= log_.base_index()) continue; // already snapshotted
            if (log_.at(entry.index).term != entry.term) log_.truncate_from(entry.index);
            else continue;
        }
        log_.append(entry);
    }
    persist();

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
        Index new_match = next_index_[from] - 1;
        match_index_[from] = std::max(match_index_[from], new_match);
        next_index_[from]  = match_index_[from] + 1;
        maybe_advance_commit_index();
    } else {
        if (aer.conflict_term == 0) {
            next_index_[from] = aer.conflict_index;
        } else {
            Index last_in_term = log_.last_index_for_term(aer.conflict_term);
            next_index_[from]  = (last_in_term > 0) ? last_in_term + 1 : aer.conflict_index;
        }
        if (next_index_[from] < 1) next_index_[from] = 1;
        replicate_to(from);
    }
}

// ──────────────────────────────────────────────────────────────────────────────
// InstallSnapshot
// ──────────────────────────────────────────────────────────────────────────────

void RaftNode::handle_install_snapshot(NodeId from, const InstallSnapshot& is) {
    if (is.term < current_term_) return;
    if (is.term > current_term_) become_follower(is.term, from);
    else                         become_follower(current_term_, from);

    // Ignore stale snapshots.
    if (is.last_included_index <= snap_last_index_) {
        InstallSnapshotReply reply{current_term_, snap_last_index_};
        send(from, reply.encode());
        return;
    }

    // Install the snapshot: decode KV state and reset the state machine.
    snap_last_index_ = is.last_included_index;
    snap_last_term_  = is.last_included_term;
    snap_data_       = is.data;

    kv_ = KvStore{};
    Decoder dec(snap_data_.data(), snap_data_.size());
    uint32_t count = dec.u32();
    std::map<std::string, std::string> kv_map;
    for (uint32_t i = 0; i < count; ++i) {
        auto k = dec.str();
        auto v = dec.str();
        kv_map[k] = v;
    }
    kv_.load(std::move(kv_map));

    // Compact log past snapshot.
    log_.set_snapshot(snap_last_index_, snap_last_term_);

    commit_index_ = std::max(commit_index_, snap_last_index_);
    last_applied_ = std::max(last_applied_, snap_last_index_);

    save_snapshot();
    persist();

    InstallSnapshotReply reply{current_term_, snap_last_index_};
    send(from, reply.encode());
}

void RaftNode::handle_install_snapshot_reply(NodeId from, const InstallSnapshotReply& isr) {
    if (role_ != Role::Leader) return;
    if (isr.term > current_term_) { become_follower(isr.term); return; }

    match_index_[from] = std::max(match_index_[from], isr.last_included_index);
    next_index_[from]  = match_index_[from] + 1;
    maybe_advance_commit_index();
}

// ──────────────────────────────────────────────────────────────────────────────
// Read-index
// ──────────────────────────────────────────────────────────────────────────────

void RaftNode::handle_read_index_request(NodeId from, const ReadIndexRequest& req) {
    if (req.term < current_term_) return; // stale
    if (req.term > current_term_) become_follower(req.term, from);
    else                          become_follower(current_term_, from);

    ReadIndexReply rep{current_term_, req.round};
    send(from, rep.encode());
}

void RaftNode::handle_read_index_reply(NodeId from, const ReadIndexReply& rep) {
    if (rep.term > current_term_) { become_follower(rep.term); return; }
    if (role_ != Role::Leader || rep.term != current_term_) return;

    for (auto& pr : pending_reads_) {
        if (pr.round == rep.round) { ++pr.acks; break; }
    }
    drain_pending_reads();
}

void RaftNode::drain_pending_reads() {
    // Fire reads that have (a) quorum acks confirming leadership and
    // (b) all committed entries up to read_index applied to the state machine.
    auto it = pending_reads_.begin();
    while (it != pending_reads_.end()) {
        if (it->acks >= quorum() && last_applied_ >= it->read_index) {
            sim_.cancel(it->timeout_id);
            it->cb(true, kv_.get(it->key));
            it = pending_reads_.erase(it);
        } else {
            ++it;
        }
    }
}

void RaftNode::cancel_pending_reads() {
    for (auto& pr : pending_reads_) {
        sim_.cancel(pr.timeout_id);
        pr.cb(false, std::nullopt);
    }
    pending_reads_.clear();
}

// ──────────────────────────────────────────────────────────────────────────────
// Client request
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
        cancel_pending_reads(); // no longer leader — cancel any in-flight reads
        role_ = Role::Follower;
        cancel_heartbeat();
        if (state_change_cb_) state_change_cb_(id_, role_, current_term_);
    }
    leader_id_ = new_leader;
    votes_received_.clear();
    vote_count_    = 0;
    cancel_election_timeout();
    schedule_election_timeout();
}

void RaftNode::become_candidate() {
    if (!running_) return;
    current_term_++;
    role_          = Role::Candidate;
    voted_for_     = id_;
    votes_received_.clear();
    vote_count_    = 1;
    election_term_ = current_term_;
    leader_id_     = std::nullopt;
    persist();
    if (state_change_cb_) state_change_cb_(id_, role_, current_term_);

    RequestVote rv{current_term_, id_, log_.last_index(), log_.last_term()};
    for (NodeId peer : peers_) send(peer, rv.encode());

    cancel_election_timeout();
    schedule_election_timeout();
    if (vote_count_ >= quorum()) become_leader();
}

void RaftNode::become_leader() {
    if (role_ == Role::Leader) return;
    role_      = Role::Leader;
    leader_id_ = id_;
    votes_received_.clear();
    vote_count_ = 0;
    cancel_election_timeout();

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
    replicate_all();
}

// ──────────────────────────────────────────────────────────────────────────────
// Replication
// ──────────────────────────────────────────────────────────────────────────────

void RaftNode::replicate_to(NodeId peer) {
    if (role_ != Role::Leader) return;

    // If the follower needs entries that have been compacted, send InstallSnapshot.
    if (next_index_[peer] <= snap_last_index_) {
        send_install_snapshot(peer);
        return;
    }

    Index prev_index = next_index_[peer] - 1;
    Term  prev_term  = (prev_index > 0)
                           ? (log_.contains(prev_index, log_.term_at(prev_index))
                                  ? log_.term_at(prev_index)
                                  : 0)
                           : 0;

    auto entries = log_.slice(prev_index,
                              std::min(prev_index + cfg_.max_entries_per_ae,
                                       log_.last_index()));
    if (!entries.empty()) next_index_[peer] = entries.back().index + 1;

    AppendEntries ae{current_term_, id_, prev_index, prev_term,
                     std::move(entries), commit_index_};
    send(peer, ae.encode());
}

void RaftNode::replicate_all() {
    for (NodeId peer : peers_) replicate_to(peer);
}

void RaftNode::maybe_advance_commit_index() {
    for (Index n = log_.last_index(); n > commit_index_; --n) {
        if (n <= log_.base_index()) break; // compacted
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
    while (last_applied_ < commit_index_) {
        ++last_applied_;
        if (last_applied_ <= log_.base_index()) continue; // covered by snapshot

        const LogEntry& entry = log_.at(last_applied_);
        CmdResult result = kv_.apply(entry.cmd);

        // Handle membership changes.
        if (std::holds_alternative<CmdAddServer>(entry.cmd) ||
            std::holds_alternative<CmdRemoveServer>(entry.cmd)) {
            apply_membership_change(entry.cmd);
            config_change_pending_ = false;
        }

        auto it = pending_clients_.find(last_applied_);
        if (it != pending_clients_.end()) {
            it->second.first(std::move(result));
            pending_clients_.erase(it);
        }
    }

    // Drain any reads that were waiting for last_applied_ to catch up.
    drain_pending_reads();

    // Trigger snapshot if the log has grown past the threshold.
    if (cfg_.snapshot_threshold > 0 &&
        log_.size() > cfg_.snapshot_threshold) {
        take_snapshot();
    }
}

// ──────────────────────────────────────────────────────────────────────────────
// Snapshotting
// ──────────────────────────────────────────────────────────────────────────────

void RaftNode::take_snapshot() {
    if (last_applied_ <= snap_last_index_) return; // nothing new to snapshot

    // Capture term at the last applied entry before compaction.
    Term snap_term = log_.term_at(last_applied_);

    // Serialize the current KV state.
    const auto& kv_data = kv_.data();
    Encoder enc;
    enc.u32(static_cast<uint32_t>(kv_data.size()));
    for (const auto& [k, v] : kv_data) { enc.str(k); enc.str(v); }
    snap_data_       = enc.take();
    snap_last_index_ = last_applied_;
    snap_last_term_  = snap_term;

    // Compact log.
    log_.set_snapshot(snap_last_index_, snap_last_term_);

    save_snapshot();
    persist(); // log is now smaller; rewrite it
}

void RaftNode::save_snapshot() {
    Encoder enc;
    enc.u64(snap_last_index_);
    enc.u64(snap_last_term_);
    enc.u32(static_cast<uint32_t>(snap_data_.size()));
    for (uint8_t b : snap_data_) enc.u8(b);
    auto bytes = enc.take();
    snapshot_store_->truncate_to(0);
    snapshot_store_->append(bytes);
    snapshot_store_->flush();
}

void RaftNode::load_snapshot() {
    if (snapshot_store_->durable_size() == 0) return;
    auto raw = snapshot_store_->read_durable();
    Decoder dec(raw);
    snap_last_index_ = dec.u64();
    snap_last_term_  = dec.u64();
    uint32_t data_len = dec.u32();
    snap_data_.resize(data_len);
    for (uint32_t i = 0; i < data_len; ++i) snap_data_[i] = dec.u8();

    // Restore KV state from snapshot.
    kv_ = KvStore{};
    Decoder kv_dec(snap_data_.data(), snap_data_.size());
    uint32_t count = kv_dec.u32();
    std::map<std::string, std::string> kv_map;
    for (uint32_t i = 0; i < count; ++i) {
        auto k = kv_dec.str();
        auto v = kv_dec.str();
        kv_map[k] = v;
    }
    kv_.load(std::move(kv_map));

    commit_index_ = snap_last_index_;
    last_applied_ = snap_last_index_;
    log_.set_snapshot(snap_last_index_, snap_last_term_);
}

void RaftNode::send_install_snapshot(NodeId peer) {
    if (snap_last_index_ == 0) return; // no snapshot yet
    InstallSnapshot is{current_term_, id_,
                       snap_last_index_, snap_last_term_, snap_data_};
    send(peer, is.encode());
}

// ──────────────────────────────────────────────────────────────────────────────
// Membership changes
// ──────────────────────────────────────────────────────────────────────────────

void RaftNode::apply_membership_change(const Command& cmd) {
    if (auto* a = std::get_if<CmdAddServer>(&cmd)) {
        NodeId new_id = static_cast<NodeId>(a->id);
        if (std::find(peers_.begin(), peers_.end(), new_id) == peers_.end()
            && new_id != id_) {
            peers_.push_back(new_id);
            std::sort(peers_.begin(), peers_.end());
            // If we're the leader, start replicating to the new server.
            if (role_ == Role::Leader) {
                next_index_[new_id]  = log_.last_index() + 1;
                match_index_[new_id] = 0;
                replicate_to(new_id);
            }
        }
    } else if (auto* r = std::get_if<CmdRemoveServer>(&cmd)) {
        NodeId rem_id = static_cast<NodeId>(r->id);
        peers_.erase(std::remove(peers_.begin(), peers_.end(), rem_id), peers_.end());
        next_index_.erase(rem_id);
        match_index_.erase(rem_id);

        // If we (the leader) are removing ourselves, step down.
        if (rem_id == id_ && role_ == Role::Leader) {
            become_follower(current_term_);
        }
    }
}

// ──────────────────────────────────────────────────────────────────────────────
// Timer scheduling
// ──────────────────────────────────────────────────────────────────────────────

void RaftNode::schedule_election_timeout() {
    if (election_scheduled_) return;
    uint64_t gen = ++election_generation_;
    SimTime delay = sim_.random_range(cfg_.election_timeout_lo,
                                      2 * cfg_.election_timeout_lo + 1);
    election_scheduled_ = true;
    election_timeout_id_ = sim_.schedule(delay, [this, gen]() {
        // In real-clock mode the callback may arrive after cancel() was called,
        // because RealClock posts to the dispatch queue before the cancel can
        // intercept it.  The generation counter lets us discard stale firings.
        if (gen != election_generation_) return;
        election_scheduled_ = false;
        if (!running_) return;
        on_election_timeout();
    });
}

void RaftNode::cancel_election_timeout() {
    if (election_scheduled_) {
        sim_.cancel(election_timeout_id_);
        election_scheduled_ = false;
        election_generation_++;
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
    if (role_ == Role::Leader) return;
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
    meta_store_->truncate_to(0);
    Encoder enc;
    enc.u64(current_term_);
    enc.boolean(voted_for_.has_value());
    if (voted_for_.has_value()) enc.u32(*voted_for_);
    auto bytes = enc.take();
    meta_store_->append(bytes);
    meta_store_->flush();

    log_store_->truncate_to(0);
    log_.save_to(*log_store_);
}

// ──────────────────────────────────────────────────────────────────────────────
// Helpers
// ──────────────────────────────────────────────────────────────────────────────

bool RaftNode::is_log_up_to_date(Index last_idx, Term last_term) const {
    if (last_term != log_.last_term()) return last_term > log_.last_term();
    return last_idx >= log_.last_index();
}

void RaftNode::send(NodeId to, std::vector<uint8_t> msg) {
    net_.send(id_, to, std::move(msg));
}
