#!/usr/bin/env python3
"""Compare previous perfect-links defaults with the updated Main defaults.

Previous: WINDOW=1024, TIMEOUT_MS=100.
Updated: WINDOW=16, TIMEOUT_MS=max(4, round(1.2 * senders * 16 * 0.136)).
senders is the number of processes that send, the same value --senders means
in perf_sweep.py (process 1 is the receiver; hosts = senders + 1).

Each (config, senders) pair is one run unless --repeats is set. Delivered
counts come from perf_sweep.execute_run (120s run, 60s log wait, delivered
lines counted, no full correctness scan).

Example:
    python3 tools/perf_compare.py --messages 100000000 --senders 2,8,32
"""

import argparse
import os
import sys

import perf_sweep

PREVIOUS_WINDOW = 1024
PREVIOUS_TIMEOUT_MS = 100
UPDATED_WINDOW = 16


def updated_timeout_ms(senders: int) -> int:
    # Main floors at 4. The sweep's --senders is already the sender count,
    # so this does not subtract one again.
    return max(4, int(round(1.2 * senders * 16 * 0.136)))


def percent_increase(previous: float, updated: float):
    if previous == 0:
        return None
    return (updated - previous) / previous * 100.0


def fmt_count(value: float) -> str:
    rounded = round(value)
    if abs(value - rounded) < 1e-6:
        return str(int(rounded))
    return "{:.2f}".format(value)


def fmt_percent(value) -> str:
    if value is None:
        return "n/a"
    return "{:.2f}".format(value)


def require_delivered(row: dict, label: str) -> int:
    raw = row.get("delivered")
    error = row.get("error") or ""
    deaths = row.get("early_deaths") or ""
    if raw is None or raw == "" or error or deaths:
        raise SystemExit(
            "{} failed run_id={} delivered={} early_deaths={} error={}".format(
                label, row.get("run_id"), raw, deaths, error
            )
        )
    return int(raw)


def average_delivered(settings, label, senders, messages, window, timeout_ms, repeats) -> float:
    total = 0
    for index in range(repeats):
        plan = perf_sweep.Plan(
            senders=senders,
            messages=messages,
            window=window,
            timeout_mult=None,
            timeout_est_ms=float(timeout_ms),
            timeout_ms=timeout_ms,
        )
        row = perf_sweep.execute_run(plan, settings)
        delivered = require_delivered(row, label)
        total += delivered
        print(
            "{} senders={} {}/{} window={} timeout_ms={} delivered={}".format(
                label, senders, index + 1, repeats, window, timeout_ms, delivered
            ),
            flush=True,
        )
    return total / float(repeats)


def print_table(results) -> None:
    headers = ("senders", "previous_delivered", "updated_delivered", "percent_increase")
    body = []
    previous_total = 0.0
    updated_total = 0.0
    for senders, previous, updated in results:
        previous_total += previous
        updated_total += updated
        body.append(
            (
                str(senders),
                fmt_count(previous),
                fmt_count(updated),
                fmt_percent(percent_increase(previous, updated)),
            )
        )
    widths = [len(header) for header in headers]
    for cols in body:
        for index, col in enumerate(cols):
            widths[index] = max(widths[index], len(col))

    def emit(cols):
        print("  ".join(col.rjust(widths[index]) for index, col in enumerate(cols)), flush=True)

    print(flush=True)
    emit(headers)
    for cols in body:
        emit(cols)
    print(
        "overall percent increase: {}".format(
            fmt_percent(percent_increase(previous_total, updated_total))
        ),
        flush=True,
    )


def parse_args(argv=None):
    parser = argparse.ArgumentParser(
        description=(
            "Compare previous perfect-links defaults (WINDOW=1024, TIMEOUT_MS=100) "
            "with updated defaults (WINDOW=16, TIMEOUT_MS=max(4, round(1.2*senders*16*0.136)))."
        )
    )
    parser.add_argument(
        "--messages",
        type=perf_sweep._nonnegative_int(2147483647),
        default=100000000,
        help="Messages per sender (default: 100000000).",
    )
    parser.add_argument(
        "--senders",
        type=perf_sweep._int_list(1, 125),
        default=[2, 8, 32],
        help="Sender counts, each in 1..125 (default: 2,8,32). Process 1 is the receiver.",
    )
    parser.add_argument(
        "--repeats",
        type=perf_sweep._nonnegative_int(1000000),
        default=1,
        help="Runs per (config, senders). The table shows the mean delivered (default: 1).",
    )
    parser.add_argument(
        "--duration",
        type=perf_sweep._positive_float,
        default=120.0,
        help="Seconds to run before SIGTERM (default: 120).",
    )
    parser.add_argument(
        "--log-wait",
        type=perf_sweep._nonnegative_float,
        default=60.0,
        help="Seconds to wait for logs after SIGTERM (default: 60).",
    )
    parser.add_argument("--runscript", default=perf_sweep.DEFAULT_RUNSCRIPT, help="Path to run.sh")
    parser.add_argument(
        "--out",
        default=os.path.expanduser("~/perf-compare-out"),
        help="Run directory root (default: ~/perf-compare-out). Refused inside template_java.",
    )
    parser.add_argument(
        "--port-base",
        type=perf_sweep._nonnegative_int(65535),
        default=11000,
        help="Process i listens on base+i (default: 11000).",
    )
    parser.add_argument(
        "--port-timeout",
        type=perf_sweep._nonnegative_float,
        default=60.0,
        help="Seconds to wait for ports to go free (default: 60).",
    )
    return parser.parse_args(argv)


def main(argv=None) -> int:
    args = parse_args(argv)
    if args.messages < 1:
        raise SystemExit("messages must be in 1..2147483647")
    if args.repeats < 1:
        raise SystemExit("repeats must be at least 1")
    runscript = os.path.abspath(args.runscript)
    if not os.path.isfile(runscript):
        raise SystemExit("run script not found: {}".format(runscript))
    out_dir = perf_sweep.safe_out_dir(args.out, perf_sweep.REPO_ROOT)
    max_port = args.port_base + max(args.senders) + 1
    if max_port > 65535 or args.port_base + 1 < 1:
        raise SystemExit(
            "ports {}..{} fall outside 1..65535".format(args.port_base + 1, max_port)
        )
    jar = os.path.join(os.path.dirname(runscript), "bin", "da_proc.jar")
    if not os.path.isfile(jar):
        raise SystemExit("missing {} (build first)".format(jar))
    os.makedirs(out_dir, exist_ok=True)
    settings = perf_sweep.Settings(
        runscript=runscript,
        out_dir=out_dir,
        port_base=args.port_base,
        duration=args.duration,
        log_wait=args.log_wait,
        port_timeout=args.port_timeout,
        java_opts="",
        do_check=False,
        keep_outputs=False,
    )
    print(
        "previous WINDOW={} TIMEOUT_MS={}".format(PREVIOUS_WINDOW, PREVIOUS_TIMEOUT_MS),
        flush=True,
    )
    print(
        "updated WINDOW={} TIMEOUT_MS=max(4, round(1.2*senders*16*0.136))".format(UPDATED_WINDOW),
        flush=True,
    )
    results = []
    perf_sweep._install_signals()
    try:
        for senders in args.senders:
            previous = average_delivered(
                settings,
                "previous",
                senders,
                args.messages,
                PREVIOUS_WINDOW,
                PREVIOUS_TIMEOUT_MS,
                args.repeats,
            )
            updated = average_delivered(
                settings,
                "updated",
                senders,
                args.messages,
                UPDATED_WINDOW,
                updated_timeout_ms(senders),
                args.repeats,
            )
            results.append((senders, previous, updated))
    except KeyboardInterrupt:
        print("interrupted", file=sys.stderr)
        return 130
    finally:
        perf_sweep._kill_active()
    print_table(results)
    return 0


if __name__ == "__main__":
    sys.exit(main())
