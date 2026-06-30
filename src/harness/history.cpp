#include "harness/history.hpp"
#include <format>
#include <map>
#include <sstream>

std::size_t History::record_invoke(uint64_t client_id, uint64_t request_id,
                                    OpKind kind, std::string key, SimTime invoke_time,
                                    std::string value, std::string expected) {
    std::size_t idx = entries_.size();
    entries_.push_back(HistoryEntry{
        client_id, request_id, kind,
        std::move(key), std::move(value), std::move(expected),
        invoke_time, 0, "", false, true
    });
    return idx;
}

void History::record_return(std::size_t handle, SimTime ret_time,
                             bool ok, std::string response) {
    auto& e    = entries_[handle];
    e.ret      = ret_time;
    e.ok       = ok;
    e.response = std::move(response);
    e.pending  = false;
}

static std::string json_escape(const std::string& s) {
    std::string out;
    out.reserve(s.size() + 2);
    out += '"';
    for (char c : s) {
        switch (c) {
        case '"':  out += "\\\""; break;
        case '\\': out += "\\\\"; break;
        case '\n': out += "\\n";  break;
        case '\r': out += "\\r";  break;
        case '\t': out += "\\t";  break;
        default:
            if (static_cast<unsigned char>(c) < 0x20) {
                char buf[8];
                snprintf(buf, sizeof(buf), "\\u%04x", static_cast<unsigned char>(c));
                out += buf;
            } else {
                out += c;
            }
        }
    }
    out += '"';
    return out;
}

std::string History::to_json() const {
    std::string out;
    out += "[\n";
    for (std::size_t i = 0; i < entries_.size(); ++i) {
        const auto& e = entries_[i];
        if (e.pending) continue; // skip in-flight ops

        out += "  {";
        out += std::format("\"id\":{},", i);
        out += std::format("\"client_id\":{},", e.client_id);
        out += std::format("\"request_id\":{},", e.request_id);

        std::string kind_str;
        switch (e.kind) {
        case OpKind::Put: kind_str = "put"; break;
        case OpKind::Get: kind_str = "get"; break;
        case OpKind::Cas: kind_str = "cas"; break;
        }
        out += std::format("\"op\":{},", json_escape(kind_str));
        out += std::format("\"key\":{},", json_escape(e.key));
        out += std::format("\"value\":{},", json_escape(e.value));
        out += std::format("\"expected\":{},", json_escape(e.expected));
        out += std::format("\"start\":{},", e.invoke);
        out += std::format("\"end\":{},", e.ret);
        out += std::format("\"ok\":{},", e.ok ? "true" : "false");
        out += std::format("\"response\":{}", json_escape(e.response));
        out += "}";
        if (i + 1 < entries_.size()) out += ",";
        out += "\n";
    }
    out += "]\n";
    return out;
}
