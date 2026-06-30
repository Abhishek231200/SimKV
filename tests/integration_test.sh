#!/usr/bin/env bash
# integration_test.sh — spin up 3 real simkv_server processes and verify
# basic GET/PUT correctness plus leader-failover data durability.
#
# Usage: integration_test.sh <path-to-simkv_server>
set -euo pipefail

SIMKV_SERVER="${1?Usage: $0 <path-to-simkv_server>}"
WORK=$(mktemp -d)
declare -a PIDS=()
KILLED_PID=""   # set when we intentionally kill the leader during failover test

cleanup() {
    for pid in "${PIDS[@]}"; do
        # Skip the PID we already killed, and never send kill(0) (process-group kill).
        if [[ -n "$pid" && "$pid" != "0" && "$pid" != "$KILLED_PID" ]]; then
            kill -9 "$pid" 2>/dev/null || true
        fi
    done
    sleep 0.2   # give ports time to release before a subsequent test run
    rm -rf "$WORK"
}
trap cleanup EXIT

# Use PID-derived ports to avoid conflicts when tests run concurrently or
# when a previous run was hard-killed (leaving processes behind on fixed ports).
BASE=$(( (($$ % 2000) + 1) * 10 + 20000 ))
RAFT1=$BASE;           RAFT2=$(( BASE+1 )); RAFT3=$(( BASE+2 ))
CLIENT1=$(( BASE+3 )); CLIENT2=$(( BASE+4 )); CLIENT3=$(( BASE+5 ))

# ── Start 3 nodes ───────────────────────────────────────────────────────────

"$SIMKV_SERVER" \
    --id 1 --raft-port "$RAFT1" --client-port "$CLIENT1" \
    --peers "2:127.0.0.1:$RAFT2,3:127.0.0.1:$RAFT3" \
    --client-peers "2:127.0.0.1:$CLIENT2,3:127.0.0.1:$CLIENT3" \
    --data-dir "$WORK/n1" 2>"$WORK/n1.log" &
PIDS+=($!)

"$SIMKV_SERVER" \
    --id 2 --raft-port "$RAFT2" --client-port "$CLIENT2" \
    --peers "1:127.0.0.1:$RAFT1,3:127.0.0.1:$RAFT3" \
    --client-peers "1:127.0.0.1:$CLIENT1,3:127.0.0.1:$CLIENT3" \
    --data-dir "$WORK/n2" 2>"$WORK/n2.log" &
PIDS+=($!)

"$SIMKV_SERVER" \
    --id 3 --raft-port "$RAFT3" --client-port "$CLIENT3" \
    --peers "1:127.0.0.1:$RAFT1,2:127.0.0.1:$RAFT2" \
    --client-peers "1:127.0.0.1:$CLIENT1,2:127.0.0.1:$CLIENT2" \
    --data-dir "$WORK/n3" 2>"$WORK/n3.log" &
PIDS+=($!)

# ── Helper: send one command to a client port ─────────────────────────────

kvcmd() { printf '%s\n' "$2" | nc -w 2 127.0.0.1 "$1" 2>/dev/null || true; }

# ── Wait for a leader (up to 8 s) ────────────────────────────────────────

LEADER_PORT=""
for i in $(seq 1 80); do
    for port in "$CLIENT1" "$CLIENT2" "$CLIENT3"; do
        resp=$(kvcmd "$port" "PUT __probe__ 1")
        if [[ "$resp" == "+OK"* ]]; then LEADER_PORT=$port; break 2; fi
    done
    sleep 0.1
done
[[ -n "$LEADER_PORT" ]] || { echo "FAIL: no leader elected within 8 s"; exit 1; }
echo "[integration] leader on port $LEADER_PORT"

# ── Basic GET / PUT / missing-key ─────────────────────────────────────────

kvcmd "$LEADER_PORT" "PUT k1 alpha" > /dev/null
kvcmd "$LEADER_PORT" "PUT k2 beta"  > /dev/null
[[ $(kvcmd "$LEADER_PORT" "GET k1") == "+alpha" ]] \
    || { echo "FAIL: GET k1"; exit 1; }
[[ $(kvcmd "$LEADER_PORT" "GET k2") == "+beta" ]] \
    || { echo "FAIL: GET k2"; exit 1; }
[[ $(kvcmd "$LEADER_PORT" "GET missing") == "-NOT_FOUND" ]] \
    || { echo "FAIL: GET missing did not return -NOT_FOUND"; exit 1; }
echo "[integration] basic GET/PUT passed"

# ── Follower redirect ─────────────────────────────────────────────────────
# A follower should respond with -REDIRECT (because --client-peers is set).

FOLLOWER_PORT=""
for port in "$CLIENT1" "$CLIENT2" "$CLIENT3"; do
    [[ "$port" != "$LEADER_PORT" ]] && { FOLLOWER_PORT=$port; break; }
done

resp=$(kvcmd "$FOLLOWER_PORT" "PUT x 1")
if [[ "$resp" == "-REDIRECT"* ]]; then
    echo "[integration] follower redirect: $resp"
elif [[ "$resp" == "-ERR not_leader"* ]]; then
    echo "[integration] follower correctly rejected write"
else
    echo "FAIL: unexpected follower response: '$resp'"; exit 1
fi

# ── Leader failover ───────────────────────────────────────────────────────

case "$LEADER_PORT" in
  "$CLIENT1") KILLED_PID="${PIDS[0]}"; SURVIVOR_PORTS=("$CLIENT2" "$CLIENT3") ;;
  "$CLIENT2") KILLED_PID="${PIDS[1]}"; SURVIVOR_PORTS=("$CLIENT1" "$CLIENT3") ;;
  "$CLIENT3") KILLED_PID="${PIDS[2]}"; SURVIVOR_PORTS=("$CLIENT1" "$CLIENT2") ;;
esac
kill -9 "$KILLED_PID"   # SIGKILL = simulate a hard crash, not a graceful shutdown
echo "[integration] killed leader (port $LEADER_PORT), waiting for new leader..."

NEW_LEADER_PORT=""
for i in $(seq 1 80); do
    for port in "${SURVIVOR_PORTS[@]}"; do
        resp=$(kvcmd "$port" "PUT __probe2__ 2")
        if [[ "$resp" == "+OK"* ]]; then NEW_LEADER_PORT=$port; break 2; fi
    done
    sleep 0.1
done
[[ -n "$NEW_LEADER_PORT" ]] \
    || { echo "FAIL: no new leader within 8 s after crash"; exit 1; }
echo "[integration] new leader on port $NEW_LEADER_PORT"

# Data written before the crash must survive on the new leader.
[[ $(kvcmd "$NEW_LEADER_PORT" "GET k1") == "+alpha" ]] \
    || { echo "FAIL: k1 lost after failover"; exit 1; }
[[ $(kvcmd "$NEW_LEADER_PORT" "GET k2") == "+beta" ]] \
    || { echo "FAIL: k2 lost after failover"; exit 1; }

echo "[integration] PASS: GET/PUT correctness and leader failover verified"
