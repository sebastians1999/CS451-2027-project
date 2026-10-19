#!/bin/bash
# WINDOW is 1 so a single message is in flight. A larger window makes the sender queue every message before the first ack.
# Edit the settings below. You do not pass arguments.

set -euo pipefail

cd "$(dirname "$0")"

PROCESSES=5
WINDOW=1
TIMEOUT_MS=5000
RTT_SKIP=2000
RTT_MAX=10000
MESSAGES=$((RTT_SKIP + RTT_MAX))
LOG_DIR="../example/output/rtt-sample"

rm -rf "$LOG_DIR"
mkdir -p "$LOG_DIR"

{
    for ((id = 1; id <= PROCESSES; id++)); do
        printf '%s localhost %s\n' "$id" $((11000 + id))
    done
} > "$LOG_DIR/hosts"

printf '%s 1\n' "$MESSAGES" > "$LOG_DIR/config"

../template_java/build.sh

LOG_DIR_ABS="$(cd "$LOG_DIR" && pwd)"
RTT_LOG="${LOG_DIR_ABS}/rtt"
echo "RTT sample: ${PROCESSES} processes, ${MESSAGES} messages, logs ${LOG_DIR_ABS}"
HOSTS="${LOG_DIR_ABS}/hosts"
CONFIG="${LOG_DIR_ABS}/config"

# run.sh waits on Java inside a command substitution, so SIGTERM has to go to the PID Java prints.
shell_pids=()
java_pids=()
cleanup() {
    local pid
    if [ "${#java_pids[@]}" -gt 0 ]; then
        for pid in "${java_pids[@]}"; do
            kill -TERM "$pid" 2>/dev/null || true
        done
    fi
    if [ "${abort_shells:-0}" -ne 1 ] || [ "${#shell_pids[@]}" -eq 0 ]; then
        return 0
    fi
    for pid in "${shell_pids[@]}"; do
        kill -TERM "$pid" 2>/dev/null || true
    done
}
trap 'abort_shells=1; cleanup' EXIT

java_pid_from() {
    local file=$1
    local pid=""
    local _
    for _ in 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20 \
             21 22 23 24 25 26 27 28 29 30 31 32 33 34 35 36 37 38 39 40; do
        if [ -f "$file" ]; then
            pid=$(sed -n 's/^My PID: \([0-9][0-9]*\).*/\1/p' "$file" || true)
            pid=${pid%%$'\n'*}
            if [ -n "$pid" ]; then
                echo "$pid"
                return 0
            fi
        fi
        sleep 0.05
    done
    return 1
}

for ((id = 1; id <= PROCESSES; id++)); do
    output=$(printf '%s/proc%02d.output' "$LOG_DIR_ABS" "$id")
    stdout=$(printf '%s/proc%02d.stdout' "$LOG_DIR_ABS" "$id")
    WINDOW="$WINDOW" TIMEOUT_MS="$TIMEOUT_MS" RTT_SKIP="$RTT_SKIP" RTT_MAX="$RTT_MAX" \
        RTT_SENDERS="$((PROCESSES - 1))" RTT_LOG="$RTT_LOG" \
        ../template_java/run.sh --id "$id" --hosts "$HOSTS" --output "$output" "$CONFIG" \
        > "$stdout" 2>&1 &
    shell_pids+=("$!")
    java_pid=$(java_pid_from "$stdout") || {
        echo "process ${id} did not start" >&2
        cat "$stdout" >&2 || true
        exit 1
    }
    java_pids+=("$java_pid")
done

ready=0
deadline=$((SECONDS + 60))
while [ "$SECONDS" -lt "$deadline" ]; do
    ready=1
    for ((id = 2; id <= PROCESSES; id++)); do
        if [ ! -f "$RTT_LOG/rtt-${id}.done" ]; then
            ready=0
            break
        fi
    done
    if [ "$ready" -eq 1 ]; then
        break
    fi
    sleep 0.2
done

if [ "$ready" -ne 1 ]; then
    echo "Timed out waiting for RTT done markers. Sending SIGTERM so the report is printed." >&2
fi

cleanup
for pid in "${shell_pids[@]}"; do
    wait "$pid" || true
done
shell_pids=()
java_pids=()
trap - EXIT

found=0
for ((id = 1; id <= PROCESSES; id++)); do
    stdout=$(printf '%s/proc%02d.stdout' "$LOG_DIR_ABS" "$id")
    if [ -f "$stdout" ] && grep -q -- '--- RTT sample ' "$stdout"; then
        found=1
        awk '/--- RTT sample /, /--- end RTT sample ---/' "$stdout"
        echo
    fi
done
if [ "$found" -ne 1 ]; then
    echo "No RTT report in the process output." >&2
    exit 1
fi
