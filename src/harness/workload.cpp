#include "harness/workload.hpp"
#include <format>

Workload::Workload(Simulator& sim, Network& net, History& history,
                   std::vector<RaftNode*> nodes, WorkloadConfig cfg)
    : sim_(sim), net_(net), history_(history),
      nodes_(std::move(nodes)), cfg_(cfg) {
    clients_.resize(cfg_.num_clients);
    for (std::size_t i = 0; i < cfg_.num_clients; ++i) {
        clients_[i].believed_leader = nodes_[0]->id();
    }
}

void Workload::start() {
    for (std::size_t i = 0; i < cfg_.num_clients; ++i) {
        NodeId client_node = kClientIdBase + static_cast<NodeId>(i);
        net_.set_handler(client_node, [this, i](Message m) {
            on_reply(i, std::move(m));
        });
        // Stagger initial ops so they don't all fire simultaneously.
        sim_.schedule(cfg_.think_time * i, [this, i]() { issue_op(i); });
    }
}

std::string Workload::random_key() {
    std::size_t k = static_cast<std::size_t>(sim_.prng().range(0, cfg_.num_keys));
    return std::format("k{}", k);
}

std::string Workload::random_value() {
    uint64_t v = sim_.prng().range(0, 100);
    return std::format("v{}", v);
}

OpKind Workload::random_op_kind() {
    double r = sim_.prng().unit();
    if (r < cfg_.put_prob) return OpKind::Put;
    if (r < cfg_.put_prob + cfg_.get_prob) return OpKind::Get;
    return OpKind::Cas;
}

void Workload::issue_op(std::size_t ci) {
    if (ops_issued_ >= cfg_.total_ops) return;
    auto& cs = clients_[ci];
    if (cs.waiting) return;

    ++ops_issued_;
    ++pending_;
    cs.waiting = true;

    OpKind      kind = random_op_kind();
    std::string key  = random_key();
    std::string val, exp;
    Command     cmd;

    switch (kind) {
    case OpKind::Put:
        val = random_value();
        cmd = CmdPut{key, val};
        break;
    case OpKind::Get:
        cmd = CmdGet{key};
        break;
    case OpKind::Cas:
        exp = random_value(); // random expected — usually fails, that's fine
        val = random_value();
        cmd = CmdCas{key, exp, val};
        break;
    }

    uint64_t client_id = kClientIdBase + ci;
    uint64_t req_id    = cs.next_request_id++;

    cs.history_handle = history_.record_invoke(client_id, req_id, kind, key, sim_.now(), val, exp);
    cs.sent_at        = sim_.now();

    ClientRequest cr{client_id, req_id, cmd};
    net_.send(static_cast<NodeId>(kClientIdBase + ci),
              cs.believed_leader, cr.encode());

    cs.timeout_id = sim_.schedule(kOpTimeout, [this, ci]() { on_timeout(ci); });
}

void Workload::on_reply(std::size_t ci, Message msg) {
    auto& cs = clients_[ci];
    if (!cs.waiting) return;
    if (msg.payload.empty()) return;
    if (static_cast<MsgType>(msg.payload[0]) != MsgType::ClientReply) return;

    ClientReply reply = ClientReply::decode(msg.payload.data(), msg.payload.size());
    if (reply.request_id != cs.next_request_id - 1) return; // stale

    sim_.cancel(cs.timeout_id);
    cs.waiting = false;
    --pending_;

    if (!reply.success) {
        // Not-leader: redirect to hint or rotate.
        history_.record_return(cs.history_handle, sim_.now(), false, "not_leader");
        if (reply.leader_hint != 0) {
            cs.believed_leader = reply.leader_hint;
        } else {
            for (auto* n : nodes_) {
                if (n->id() != cs.believed_leader) { cs.believed_leader = n->id(); break; }
            }
        }
    } else {
        bool ok = reply.result.ok;
        // For Get: response is the value read (empty string = key absent).
        // For Put: response is "ok".
        // For Cas: response is "ok" or "cas_mismatch".
        OpKind kind = history_.entries()[cs.history_handle].kind;
        std::string resp;
        switch (kind) {
        case OpKind::Get:
            resp = reply.result.value; // may be "" for absent key
            break;
        case OpKind::Put:
            resp = ok ? "ok" : reply.result.error;
            break;
        case OpKind::Cas:
            resp = ok ? "ok" : "cas_mismatch";
            break;
        }
        history_.record_return(cs.history_handle, sim_.now(), ok, resp);
        if (reply.leader_hint != 0) cs.believed_leader = reply.leader_hint;
    }

    ++ops_completed_;
    sim_.schedule(cfg_.think_time, [this, ci]() { issue_op(ci); });
}

void Workload::on_timeout(std::size_t ci) {
    auto& cs = clients_[ci];
    if (!cs.waiting) return;
    cs.waiting = false;
    --pending_;

    history_.record_return(cs.history_handle, sim_.now(), false, "timeout");
    ++ops_completed_;

    // Rotate to next node.
    if (!nodes_.empty()) {
        for (auto* n : nodes_) {
            if (n->id() != cs.believed_leader) { cs.believed_leader = n->id(); break; }
        }
    }

    sim_.schedule(cfg_.think_time, [this, ci]() { issue_op(ci); });
}
