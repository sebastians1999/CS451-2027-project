#!/usr/bin/env python3
"""Check milestone 1 output files written by tools/stress.py.

Reads LOG_DIR/config, LOG_DIR/hosts, and LOG_DIR/proc01.output, proc02.output, ...
The first config line is "m receiverId". Process receiverId only delivers. Every other
process only sends.

This is only for perfect links. FIFO broadcast logs both "b" and "d" lines in
every process, so it needs a different check.

A stress run may kill processes early. A sender that stopped at "b k" with k < m,
or a delivery that never arrived before SIGTERM, is reported and is not a failure.
Failures are bad lines, duplicates, gaps in "b" lines, and a delivery of a message
that sender never logged. Finite logs cannot establish eventual delivery (PL1).
"""

import os
import re
import sys

B_LINE = re.compile(r"b ([0-9]+)$")
D_LINE = re.compile(r"d ([0-9]+) ([0-9]+)$")


def fail(errors, message):
    errors.append(message)


def read_lines(path, errors):
    try:
        # Keep line endings intact so CRLF and bare CR cannot pass as Unix newlines.
        with open(path, "r", encoding="utf-8", newline="") as handle:
            for index, raw in enumerate(handle, start=1):
                if not raw.endswith("\n"):
                    fail(errors, "{}:{} does not end with a Unix newline".format(path, index))
                line = raw.removesuffix("\n")
                if not line:
                    fail(errors, "{}:{} empty line".format(path, index))
                yield index, line
    except (OSError, UnicodeError) as exc:
        fail(errors, "cannot read {}: {}".format(path, exc))


def load_config(log_dir, errors):
    path = os.path.join(log_dir, "config")
    lines = list(read_lines(path, errors))
    if len(lines) < 1:
        fail(errors, "config is empty")
        return None, None

    parts = lines[0][1].split(" ")
    if len(parts) != 2:
        fail(errors, "config must be 'm receiverId', got {!r}".format(lines[0][1]))
        return None, None

    try:
        messages = int(parts[0])
        receiver = int(parts[1])
    except ValueError:
        fail(errors, "config values must be integers, got {!r}".format(lines[0][1]))
        return None, None

    if messages < 1 or receiver < 1:
        fail(errors, "config values must be positive, got {!r}".format(lines[0][1]))
        return None, None

    return messages, receiver


def load_process_ids(log_dir, errors):
    path = os.path.join(log_dir, "hosts")
    ids = []
    for index, line in read_lines(path, errors):
        parts = line.split(" ")
        if len(parts) != 3:
            fail(errors, "hosts:{} expected 'id ip port', got {!r}".format(index, line))
            continue
        try:
            process_id = int(parts[0])
        except ValueError:
            fail(errors, "hosts:{} id is not an integer: {!r}".format(index, parts[0]))
            continue
        if process_id in ids:
            fail(errors, "hosts:{} duplicate process id {}".format(index, process_id))
        ids.append(process_id)
    if not ids:
        fail(errors, "hosts file has no process IDs")
    if ids and set(ids) != set(range(1, len(ids) + 1)):
        fail(errors, "hosts IDs must be 1..{} exactly once".format(len(ids)))
    return ids


def output_path(log_dir, process_id):
    return os.path.join(log_dir, "proc{:02d}.output".format(process_id))


def check_sender(path, messages, errors):
    sent = 0
    for index, line in read_lines(path, errors):
        match = B_LINE.fullmatch(line)
        if match is None:
            fail(errors, "{}:{} sender line must look like 'b 12', got {!r}".format(path, index, line))
            continue
        seq = int(match.group(1))
        expected = sent + 1
        if seq != expected:
            fail(errors, "{}:{} expected 'b {}', got 'b {}'".format(path, index, expected, seq))
        sent = seq

    if sent > messages:
        fail(errors, "{} logged {} sends, config allows {}".format(path, sent, messages))
    return sent


def check_receiver(path, messages, sent_by, errors):
    delivered = dict.fromkeys(sent_by, 0)

    seen = set()
    for index, line in read_lines(path, errors):
        match = D_LINE.fullmatch(line)
        if match is None:
            fail(errors, "{}:{} receiver line must look like 'd 2 12', got {!r}".format(path, index, line))
            continue

        sender = int(match.group(1))
        seq = int(match.group(2))
        where = "{}:{}".format(path, index)

        if sender not in sent_by:
            fail(errors, "{} delivery from process {}, which is not a sender".format(where, sender))
            continue
        if seq < 1 or seq > messages:
            fail(errors, "{} sequence {} is outside 1..{}".format(where, seq, messages))
            continue
        if (sender, seq) in seen:
            fail(errors, "{} duplicate delivery of {} {}".format(where, sender, seq))
            continue
        if seq > sent_by[sender]:
            fail(errors, "{} delivered {} {} but that sender only logged b 1..{}".format(
                where, sender, seq, sent_by[sender]))
            continue

        seen.add((sender, seq))
        delivered[sender] += 1

    return delivered


def main():
    if len(sys.argv) != 2:
        print("Usage: validate_perfect.py LOG_DIR", file=sys.stderr)
        return 2

    log_dir = sys.argv[1]
    if not os.path.isdir(log_dir):
        print("Not a directory: {}".format(log_dir), file=sys.stderr)
        return 2

    errors = []
    messages, receiver = load_config(log_dir, errors)
    process_ids = load_process_ids(log_dir, errors)

    if messages is None or not process_ids or receiver not in process_ids:
        if receiver is not None and process_ids and receiver not in process_ids:
            fail(errors, "receiver {} is not in the hosts file".format(receiver))
        for message in errors:
            print("FAIL {}".format(message))
        return 1

    sent_by = {}
    for process_id in process_ids:
        if process_id == receiver:
            continue
        sent_by[process_id] = check_sender(output_path(log_dir, process_id), messages, errors)

    delivered = check_receiver(output_path(log_dir, receiver), messages, sent_by, errors)

    print("config: m={} receiver={}".format(messages, receiver))
    for process_id, sent in sent_by.items():
        got = delivered[process_id]
        print("sender {}: logged b 1..{}, receiver delivered {}".format(process_id, sent, got))
        if got < sent:
            print("  note: {} sent messages were not delivered (possible if a process was stopped)".format(sent - got))

    if errors:
        print("")
        for message in errors:
            print("FAIL {}".format(message))
        print("{} problem(s)".format(len(errors)))
        return 1

    if any(sent_by[pid] < messages or delivered[pid] < messages for pid in sent_by):
        print("OK: safety checks passed; incomplete run, reliable delivery unverified")
    else:
        print("OK: all configured sends and deliveries observed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
