#include "storage/kv_store.hpp"

std::optional<std::string> KvStore::get(const std::string& key) const {
    auto it = data_.find(key);
    if (it == data_.end()) return std::nullopt;
    return it->second;
}

CmdResult KvStore::apply(const Command& cmd) {
    if (auto* p = std::get_if<CmdPut>(&cmd)) {
        data_[p->key] = p->value;
        return CmdResult{true, "", ""};
    }
    if (auto* d = std::get_if<CmdDelete>(&cmd)) {
        data_.erase(d->key);
        return CmdResult{true, "", ""};
    }
    if (auto* c = std::get_if<CmdCas>(&cmd)) {
        auto it = data_.find(c->key);
        std::string current = (it != data_.end()) ? it->second : "";
        if (current != c->expected) {
            return CmdResult{false, current, "cas_mismatch"};
        }
        data_[c->key] = c->value;
        return CmdResult{true, c->value, ""};
    }
    if (auto* g = std::get_if<CmdGet>(&cmd)) {
        auto it = data_.find(g->key);
        std::string val = (it != data_.end()) ? it->second : "";
        return CmdResult{true, val, ""}; // state unchanged
    }
    if (std::get_if<CmdAddServer>(&cmd) || std::get_if<CmdRemoveServer>(&cmd)) {
        // Membership changes have no KV-state effect; handled by RaftNode on commit.
        return CmdResult{true, "", ""};
    }
    return CmdResult{false, "", "unknown_command"};
}
