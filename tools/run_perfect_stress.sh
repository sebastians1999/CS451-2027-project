#!/bin/bash
# Run a perfect-links stress test, then check the output files.
# Edit the three settings below. You do not pass arguments.

set -euo pipefail

cd "$(dirname "$0")"

PROCESSES=5
MESSAGES=10000
LOG_DIR="../example/output/stress-perfect"

rm -rf "$LOG_DIR"
mkdir -p "$LOG_DIR"

echo "Stress test: ${PROCESSES} processes, ${MESSAGES} messages"
echo "Logs: ${LOG_DIR}"
echo "When the prompt appears, wait, then press Enter to stop the processes."
echo

python3 stress.py perfect \
    -r ../template_java/run.sh \
    -l "$LOG_DIR" \
    -p "$PROCESSES" \
    -m "$MESSAGES"

echo
python3 validate_perfect.py "$LOG_DIR"
