#!/usr/bin/env python3

import argparse
import csv
import math
import os
import signal
import subprocess
import sys
import time
from typing import Dict, List, NamedTuple, Optional, Sequence, Set

# 120 s run (2 senders, w=64): check took 41.8 s; 10M-line synthetic files were too low.
DEFAULT_CHECK_S = 42.0

START_OVERHEAD_S = 3.0
READ_SIZE = 8 * 1024 * 1024
MAX_TOKEN = 10
# int() allows underscores; a length-n token is valid only inside _TOKEN_LO/_TOKEN_HI for n.
_TOKEN_LO = (0, 10, 100, 1000, 10000, 100000, 1000000, 10000000, 100000000, 1000000000)
_TOKEN_HI = (
    10,
    100,
    1000,
    10000,
    100000,
    1000000,
    10000000,
    100000000,
    1000000000,
    10000000000,
)

COLUMNS = [
    "run_id",
    "started_at",
    "senders",
    "processes",
    "m",
    "window",
    "timeout_mult",
    "timeout_est_ms",
    "timeout_ms",
    "duration_s",
    "log_wait_s",
    "java_opts",
    "delivered",
    "rate_per_s",
    "sent_total",
    "finished",
    "check",
    "check_errors",
    "check_first_error",
    "check_s",
    "early_deaths",
    "killed",
    "port_wait_s",
    "error",
    "wall_s",
    "run_dir",
]

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT = os.path.dirname(SCRIPT_DIR)
DEFAULT_RUNSCRIPT = os.path.join(REPO_ROOT, "template_java", "run.sh")

_ACTIVE = []


class Plan(NamedTuple):
    senders: int
    messages: int
    window: int
    timeout_mult: Optional[float]
    timeout_est_ms: float
    timeout_ms: int


class Score(NamedTuple):
    delivered: int
    sent_total: int
    finished: int
    check: str
    check_errors: int
    check_first_error: str
    check_s: float
    error: str


class Settings(NamedTuple):
    runscript: str
    out_dir: str
    port_base: int
    duration: float
    log_wait: float
    port_timeout: float
    java_opts: str
    do_check: bool
    keep_outputs: bool


def fmt_float(value) -> str:
    number = float(value)
    if number.is_integer() and abs(number) < 1e16:
        return str(int(number))
    return format(number, ".10f").rstrip("0").rstrip(".")


def resume_key(senders, messages, window, timeout_ms, duration, log_wait, java_opts):
    return (
        str(int(senders)),
        str(int(messages)),
        str(int(window)),
        str(int(timeout_ms)),
        fmt_float(duration),
        fmt_float(log_wait),
        "" if java_opts is None else str(java_opts),
    )


def positive_token(token: bytes) -> Optional[int]:
    size = len(token)
    if size < 1 or size > MAX_TOKEN or token[0] < 49 or token[0] > 57:
        return None
    try:
        value = int(token)
    except ValueError:
        return None
    if value < _TOKEN_LO[size - 1] or value >= _TOKEN_HI[size - 1]:
        return None
    return value


def iter_timeouts(
    senders: int,
    window: int,
    mults: Sequence[float],
    explicit: Optional[Sequence[int]],
    p99_us: float,
    floor_ms: int,
    duration_s: float,
):
    est = 1.2 * senders * window * p99_us / 1000.0
    # A timeout longer than the run cannot cause retransmits, so cap at duration.
    cap = duration_s * 1000.0
    seen = set()
    if explicit is not None:
        pairs = [(None, item) for item in explicit]
    else:
        pairs = []
        for mult in mults:
            raw = round(mult * est)
            if raw < floor_ms:
                raw = floor_ms
            if raw > cap:
                raw = cap
            pairs.append((mult, raw))
    for mult, raw in pairs:
        timeout_ms = int(round(raw))
        if timeout_ms in seen:
            continue
        seen.add(timeout_ms)
        yield mult, est, timeout_ms


def build_plans(
    messages: Sequence[int],
    senders: Sequence[int],
    windows: Sequence[int],
    mults: Sequence[float],
    explicit: Optional[Sequence[int]],
    p99_us: float,
    floor_ms: int,
    duration_s: float,
) -> List[Plan]:
    plans = []
    for messages_per_sender in messages:
        for sender_count in senders:
            for window in windows:
                for mult, est, timeout_ms in iter_timeouts(
                    sender_count, window, mults, explicit, p99_us, floor_ms, duration_s
                ):
                    plans.append(
                        Plan(
                            sender_count,
                            messages_per_sender,
                            window,
                            mult,
                            est,
                            timeout_ms,
                        )
                    )
    return plans


def _forbidden_cwd_name(path: str) -> Optional[str]:
    cwd = os.path.realpath(os.getcwd())
    real = os.path.realpath(path)
    rel = os.path.relpath(real, cwd)
    if rel == os.curdir:
        return None
    parts = rel.split(os.sep)
    if parts[0] == os.pardir:
        return None
    for part in parts:
        if part in ("deploy", "logs"):
            return part
    return None


def safe_out_dir(raw: str, repo_root: str) -> str:
    path = os.path.realpath(os.path.expanduser(raw))
    template = os.path.realpath(os.path.join(repo_root, "template_java"))
    if path == template or path.startswith(template + os.sep):
        raise SystemExit("refusing --out inside template_java: {}".format(path))
    forbidden = _forbidden_cwd_name(path)
    if forbidden:
        raise SystemExit(
            "refusing to create '{}' inside the working directory".format(forbidden)
        )
    return path


def reject_cwd_name(path: str) -> None:
    forbidden = _forbidden_cwd_name(path)
    if forbidden:
        raise SystemExit(
            "refusing to create '{}' inside the working directory".format(forbidden)
        )


def load_csv(path: str):
    rows = []
    keys = set()
    if not os.path.exists(path) or os.path.getsize(path) == 0:
        return rows, keys
    with open(path, "r", newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        if reader.fieldnames is None:
            return rows, keys
        if list(reader.fieldnames) != COLUMNS:
            raise SystemExit("CSV header in {} does not match this script".format(path))
        for row in reader:
            rows.append(row)
            try:
                keys.add(
                    resume_key(
                        row["senders"],
                        row["m"],
                        row["window"],
                        row["timeout_ms"],
                        row["duration_s"],
                        row["log_wait_s"],
                        row["java_opts"],
                    )
                )
            except (KeyError, TypeError, ValueError) as exc:
                raise SystemExit("bad CSV row in {}: {}".format(path, exc))
    return rows, keys


def open_csv_writer(path: str):
    parent = os.path.dirname(path)
    if parent:
        os.makedirs(parent, exist_ok=True)
    exists = os.path.exists(path) and os.path.getsize(path) > 0
    handle = open(path, "a", newline="", encoding="utf-8")
    writer = csv.DictWriter(handle, fieldnames=COLUMNS, lineterminator="\n")
    if not exists:
        writer.writeheader()
        handle.flush()
    return handle, writer


def mean_observed_log_wait(rows: Sequence[dict]) -> Optional[float]:
    total = 0.0
    count = 0
    for row in rows:
        values = []
        usable = True
        for name in ("wall_s", "duration_s", "check_s", "port_wait_s"):
            raw = row.get(name)
            if raw is None or raw == "":
                usable = False
                break
            try:
                values.append(float(raw))
            except ValueError:
                usable = False
                break
        if not usable:
            continue
        wall_s, duration_s, check_s, port_wait_s = values
        total += wall_s - duration_s - check_s - port_wait_s
        count += 1
    if count == 0:
        return None
    return total / float(count)


def mean_check_s(rows: Sequence[dict]) -> Optional[float]:
    total = 0.0
    count = 0
    for row in rows:
        raw = row.get("check_s")
        if raw is None or raw == "":
            continue
        try:
            total += float(raw)
        except ValueError:
            continue
        count += 1
    if count == 0:
        return None
    return total / float(count)


def parse_proc_net_udp(text: str, wanted: Set[int]) -> Set[int]:
    found = set()
    for line in text.splitlines():
        parts = line.split()
        if len(parts) < 2:
            continue
        field = parts[1]
        colon = field.rfind(":")
        if colon < 0:
            continue
        try:
            port = int(field[colon + 1 :], 16)
        except ValueError:
            continue
        if port in wanted:
            found.add(port)
    return found


def bound_ports(wanted: Set[int]) -> Set[int]:
    found = set()
    if not wanted:
        return found
    for path in ("/proc/net/udp", "/proc/net/udp6"):
        try:
            with open(path, "r", encoding="utf-8", errors="replace") as handle:
                found.update(parse_proc_net_udp(handle.read(), wanted))
        except OSError:
            continue
    return found


def port_set(port_base: int, senders: int) -> Set[int]:
    return set(range(port_base + 1, port_base + senders + 2))


def join_errors(left: str, right: str) -> str:
    if left and right:
        return left + "; " + right
    return left or right or ""


def _note(errors: List[int], first: List[str], message: str) -> None:
    errors[0] += 1
    if not first[0]:
        first[0] = message


def scan_sender(path: str, messages: int):
    expected = 1
    last = 0
    errors = [0]
    first = [""]
    partial = False
    saw = False
    reached_m = False
    lineno = 0
    base = os.path.basename(path)
    # Binary mode so a bare CR is not treated as a Unix newline.
    with open(path, "rb", buffering=READ_SIZE) as handle:
        for raw in handle:
            lineno += 1
            saw = True
            if raw.endswith(b"\n"):
                line = raw[:-1]
                partial = False
            else:
                line = raw
                partial = True
            parts = line.split(b" ")
            token = positive_token(parts[1]) if len(parts) == 2 and parts[0] == b"b" else None
            if token is None:
                shown = repr(line[:40])
                _note(errors, first, "{}:{} bad line {}".format(base, lineno, shown))
                continue
            if token >= messages:
                reached_m = True
            if token != expected:
                _note(
                    errors,
                    first,
                    "{}:{} expected b {}, got b {}".format(base, lineno, expected, token),
                )
                continue
            last = token
            expected += 1
    if saw and partial:
        _note(errors, first, "{}:{} partial last line".format(base, lineno))
    if last > messages:
        _note(
            errors,
            first,
            "{} logged {} sends, m is {}".format(base, last, messages),
        )
    # Finished if any b k with k >= m was logged, or the contiguous prefix reached m.
    finished = 1 if reached_m or last >= messages else 0
    return last, finished, errors[0], first[0]


def scan_receiver(path: str, limits: Sequence[int]):
    nproc = len(limits) - 1
    bitmaps = [b""] * (nproc + 1)
    for pid in range(2, nproc + 1):
        k = limits[pid]
        if k:
            bitmaps[pid] = bytearray((k + 7) // 8)
    delivered = 0
    errors = [0]
    first = [""]
    partial = False
    saw = False
    lineno = 0
    base = os.path.basename(path)
    with open(path, "rb", buffering=READ_SIZE) as handle:
        for raw in handle:
            lineno += 1
            saw = True
            if raw.endswith(b"\n"):
                line = raw[:-1]
                partial = False
            else:
                line = raw
                partial = True
            if len(line) >= 2 and line[0] == 100 and line[1] == 32:
                delivered += 1
            parts = line.split(b" ")
            if len(parts) == 3 and parts[0] == b"d":
                sender = positive_token(parts[1])
                seq = positive_token(parts[2])
            else:
                sender = None
                seq = None
            if sender is None or seq is None:
                _note(errors, first, "{}:{} bad line {}".format(base, lineno, repr(line[:40])))
                continue
            if sender < 2 or sender > nproc:
                _note(
                    errors,
                    first,
                    "{}:{} sender {} is not a sender".format(base, lineno, sender),
                )
                continue
            limit_k = limits[sender]
            if seq > limit_k:
                _note(
                    errors,
                    first,
                    "{}:{} seq {} above sender K {}".format(base, lineno, seq, limit_k),
                )
                continue
            bitmap = bitmaps[sender]
            index = seq - 1
            byte_index = index >> 3
            mask = 1 << (index & 7)
            current = bitmap[byte_index]
            if current & mask:
                _note(
                    errors,
                    first,
                    "{}:{} duplicate {} {}".format(base, lineno, sender, seq),
                )
                continue
            bitmap[byte_index] = current | mask
    if saw and partial:
        _note(errors, first, "{}:{} partial last line".format(base, lineno))
    return delivered, errors[0], first[0]


def count_delivered(path: str, read_size: int = READ_SIZE) -> int:
    pattern = b"\nd "
    keep = len(pattern) - 1
    total = 0
    # Leading newline so the first line matches the "\nd " search pattern too.
    prev = b"\n"
    with open(path, "rb", buffering=0) as handle:
        while True:
            chunk = handle.read(read_size)
            if not chunk:
                break
            data = prev + chunk
            total += data.count(pattern)
            prev = data[-keep:]
    return total


def score_directory(run_dir: str, senders: int, messages: int, do_check: bool) -> Score:
    started = time.perf_counter()
    nproc = senders + 1
    limits = [0] * (nproc + 1)
    errors = 0
    first = ""
    finished = 0
    sent_total = 0
    error = ""
    missing = []
    for pid in range(2, nproc + 1):
        path = os.path.join(run_dir, "proc{:02d}.output".format(pid))
        if not os.path.isfile(path):
            missing.append(pid)
            continue
        last, sender_finished, sender_errors, sender_first = scan_sender(path, messages)
        limits[pid] = last
        sent_total += last
        if sender_finished:
            finished = 1
        if do_check and sender_errors:
            errors += sender_errors
            if sender_first and not first:
                first = sender_first
    if missing:
        shown = ",".join(str(pid) for pid in missing[:8])
        if len(missing) > 8:
            shown += ",..."
        error = "missing sender output: {}".format(shown)
    receiver = os.path.join(run_dir, "proc01.output")
    if not os.path.isfile(receiver):
        delivered = 0
        if do_check:
            errors += 1
            if not first:
                first = "missing receiver output proc01.output"
        else:
            error = join_errors(error, "missing receiver output proc01.output")
    elif do_check:
        delivered, receiver_errors, receiver_first = scan_receiver(receiver, limits)
        errors += receiver_errors
        if receiver_first and not first:
            first = receiver_first
    else:
        delivered = count_delivered(receiver)
    elapsed = time.perf_counter() - started
    if do_check:
        status = "ok" if errors == 0 else "fail"
    else:
        status = "skipped"
        errors = 0
        first = ""
    return Score(delivered, sent_total, finished, status, errors, first, elapsed, error)


def group_alive(proc: subprocess.Popen) -> bool:
    # Reap bash first: a zombie group leader makes killpg look alive after java exited.
    proc.poll()
    try:
        os.killpg(proc.pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def signal_group(pid: int, sig: int) -> bool:
    try:
        os.killpg(pid, sig)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def _on_signal(signum, _frame):
    for proc in list(_ACTIVE):
        try:
            os.killpg(proc.pid, signal.SIGKILL)
        except OSError:
            pass
    if signum == signal.SIGINT:
        raise KeyboardInterrupt
    raise SystemExit(128 + signum)


def _wait(procs, seconds: float, deaths: Optional[List[str]], stop_when_empty: bool) -> None:
    deadline = time.perf_counter() + seconds
    seen = set()
    while True:
        for logical, proc in procs:
            if logical in seen:
                continue
            if not group_alive(proc):
                seen.add(logical)
                if deaths is not None:
                    code = proc.returncode
                    deaths.append("{}:{}".format(logical, "?" if code is None else code))
        now = time.perf_counter()
        if now >= deadline:
            return
        if stop_when_empty and len(seen) == len(procs):
            return
        time.sleep(min(1.0, deadline - now))


def _signal_live(procs, sig: int) -> int:
    count = 0
    for _logical, proc in procs:
        if group_alive(proc) and signal_group(proc.pid, sig):
            count += 1
    return count


def _reap(procs) -> None:
    for _logical, proc in procs:
        if group_alive(proc):
            signal_group(proc.pid, signal.SIGKILL)
        proc.poll()
        try:
            _ACTIVE.remove(proc)
        except ValueError:
            pass


def wait_until_free(ports: Set[int], timeout: float):
    started = time.perf_counter()
    deadline = started + timeout
    while True:
        busy = bound_ports(ports)
        if not busy:
            return time.perf_counter() - started, ""
        if time.perf_counter() >= deadline:
            waited = time.perf_counter() - started
            listed = ",".join(str(port) for port in sorted(busy))
            return waited, "ports still bound after {:.1f}s: {}".format(waited, listed)
        time.sleep(0.25)


def _fill_ports(row: dict, ports: Set[int], timeout: float) -> None:
    waited, err = wait_until_free(ports, timeout)
    row["port_wait_s"] = "{:.3f}".format(waited)
    if err:
        row["error"] = join_errors(row["error"], err)


def write_run_files(run_dir: str, plan: Plan, port_base: int):
    nproc = plan.senders + 1
    hosts_path = os.path.join(run_dir, "hosts")
    config_path = os.path.join(run_dir, "config")
    with open(hosts_path, "w", encoding="utf-8") as handle:
        for process_id in range(1, nproc + 1):
            handle.write("{} 127.0.0.1 {}\n".format(process_id, port_base + process_id))
    with open(config_path, "w", encoding="utf-8") as handle:
        handle.write("{} 1\n".format(plan.messages))
    return hosts_path, config_path


def allocate_run_dir(runs_root: str, plan: Plan):
    now = time.time()
    stamp = time.strftime("%m%d-%H%M%S", time.localtime(now))
    millis = int(now * 1000) % 1000
    base = "s{}-m{}-w{}-t{}-{}-{:03d}".format(
        plan.senders, plan.messages, plan.window, plan.timeout_ms, stamp, millis
    )
    for suffix in ("", "-2", "-3", "-4", "-5", "-6", "-7", "-8", "-9"):
        run_id = base + suffix
        path = os.path.join(runs_root, run_id)
        try:
            os.makedirs(path)
        except FileExistsError:
            continue
        return run_id, path
    raise RuntimeError("could not allocate a run directory under {}".format(runs_root))


def start_process(runscript, logical_id, hosts, config, run_dir, env):
    output = os.path.join(run_dir, "proc{:02d}.output".format(logical_id))
    stdout_path = os.path.join(run_dir, "proc{:02d}.stdout".format(logical_id))
    stderr_path = os.path.join(run_dir, "proc{:02d}.stderr".format(logical_id))
    stdout_f = open(stdout_path, "wb")
    try:
        stderr_f = open(stderr_path, "wb")
        try:
            # Signal the group: bash, its java subshell, and java share one session; bash alone leaves java up.
            proc = subprocess.Popen(
                [
                    "bash",
                    runscript,
                    "--id",
                    str(logical_id),
                    "--hosts",
                    hosts,
                    "--output",
                    output,
                    config,
                ],
                stdout=stdout_f,
                stderr=stderr_f,
                env=env,
                start_new_session=True,
            )
        finally:
            stderr_f.close()
    finally:
        stdout_f.close()
    return proc


def run_env(window: int, timeout_ms: int, java_opts: str):
    env = os.environ.copy()
    env["WINDOW"] = str(window)
    env["TIMEOUT_MS"] = str(timeout_ms)
    if java_opts:
        env["JAVA_TOOL_OPTIONS"] = java_opts
    else:
        # Unset inherited JAVA_TOOL_OPTIONS so an empty java_opts column in the CSV is accurate.
        env.pop("JAVA_TOOL_OPTIONS", None)
    return env


def _empty_row(plan: Plan, settings: Settings, run_id: str, run_dir: str, started_at: str) -> dict:
    return {
        "run_id": run_id,
        "started_at": started_at,
        "senders": plan.senders,
        "processes": plan.senders + 1,
        "m": plan.messages,
        "window": plan.window,
        "timeout_mult": "" if plan.timeout_mult is None else fmt_float(plan.timeout_mult),
        "timeout_est_ms": "{:.3f}".format(plan.timeout_est_ms),
        "timeout_ms": plan.timeout_ms,
        "duration_s": fmt_float(settings.duration),
        "log_wait_s": fmt_float(settings.log_wait),
        "java_opts": settings.java_opts,
        "delivered": "",
        "rate_per_s": "",
        "sent_total": "",
        "finished": "",
        "check": "",
        "check_errors": "",
        "check_first_error": "",
        "check_s": "",
        "early_deaths": "",
        "killed": "",
        "port_wait_s": "",
        "error": "",
        "wall_s": "",
        "run_dir": run_dir,
    }


def execute_run(plan: Plan, settings: Settings) -> dict:
    started = time.perf_counter()
    started_at = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    run_id, run_dir = allocate_run_dir(os.path.join(settings.out_dir, "runs"), plan)
    row = _empty_row(plan, settings, run_id, run_dir, started_at)
    procs = []
    try:
        hosts, config = write_run_files(run_dir, plan, settings.port_base)
        ports = port_set(settings.port_base, plan.senders)
        busy = bound_ports(ports)
        if busy:
            row["error"] = "ports busy: {}".format(",".join(str(port) for port in sorted(busy)))
            return row
        env = run_env(plan.window, plan.timeout_ms, settings.java_opts)
        try:
            for logical in range(1, plan.senders + 2):
                proc = start_process(settings.runscript, logical, hosts, config, run_dir, env)
                procs.append((logical, proc))
                _ACTIVE.append(proc)
        except Exception as exc:
            row["error"] = "start failed: {}".format(exc)
            _reap(procs)
            if procs:
                _fill_ports(row, ports, settings.port_timeout)
            return row
        deaths = []
        _wait(procs, settings.duration, deaths, False)
        row["early_deaths"] = ";".join(deaths)
        _signal_live(procs, signal.SIGTERM)
        _wait(procs, settings.log_wait, None, True)
        row["killed"] = _signal_live(procs, signal.SIGKILL)
        for _logical, proc in procs:
            proc.poll()
        _fill_ports(row, ports, settings.port_timeout)
        try:
            score = score_directory(
                run_dir, plan.senders, plan.messages, settings.do_check
            )
        except Exception as exc:
            row["error"] = join_errors(row["error"], "score: {}".format(exc))
        else:
            row["delivered"] = score.delivered
            row["rate_per_s"] = "{:.2f}".format(score.delivered / settings.duration)
            row["sent_total"] = score.sent_total
            row["finished"] = score.finished
            row["check"] = score.check
            row["check_errors"] = score.check_errors
            row["check_first_error"] = score.check_first_error
            row["check_s"] = "{:.4f}".format(score.check_s)
            if score.error:
                row["error"] = join_errors(row["error"], score.error)
        if not settings.keep_outputs:
            delete_outputs(run_dir)
        return row
    finally:
        _reap(procs)
        row["wall_s"] = "{:.3f}".format(time.perf_counter() - started)


def delete_outputs(run_dir: str) -> None:
    try:
        names = os.listdir(run_dir)
    except OSError:
        return
    for name in names:
        if name.endswith(".output"):
            try:
                os.remove(os.path.join(run_dir, name))
            except OSError:
                pass


def _blank(value) -> bool:
    return value is None or value == ""


def is_valid(row: dict) -> bool:
    try:
        if str(row.get("check")) not in ("ok", "skipped"):
            return False
        if int(row.get("finished")) != 0:
            return False
        if not _blank(row.get("early_deaths")):
            return False
        if not _blank(row.get("error")):
            return False
        check_errors = row.get("check_errors")
        if not _blank(check_errors) and int(check_errors) != 0:
            return False
        int(row.get("delivered"))
    except (TypeError, ValueError):
        return False
    return True


def _better(candidate: dict, current: dict) -> bool:
    delivered_c = int(candidate["delivered"])
    delivered_k = int(current["delivered"])
    if delivered_c != delivered_k:
        return delivered_c > delivered_k
    timeout_c = int(candidate["timeout_ms"])
    timeout_k = int(current["timeout_ms"])
    if timeout_c != timeout_k:
        return timeout_c < timeout_k
    return int(candidate["window"]) < int(current["window"])


def print_summary(rows: Sequence[dict]) -> None:
    failed = 0
    deaths = 0
    errors = 0
    valid = []
    for row in rows:
        if str(row.get("check")) == "fail":
            failed += 1
        if not _blank(row.get("early_deaths")):
            deaths += 1
        if not _blank(row.get("error")):
            errors += 1
        if is_valid(row):
            valid.append(row)
    print("failed checks: {}".format(failed))
    print("early-death runs: {}".format(deaths))
    print("error runs: {}".format(errors))
    print("valid runs: {}".format(len(valid)))
    valid.sort(
        key=lambda row: (
            -int(row["delivered"]),
            int(row["timeout_ms"]),
            int(row["window"]),
        )
    )
    print("top 10 by delivered:")
    top_fmt = "  {:>12}  {:>12}  {:>7}  {:>10}  {:>6}  {:>10}  {}"
    if not valid:
        print("  (none)")
    else:
        print(top_fmt.format("delivered", "rate_per_s", "senders", "m", "window", "timeout_ms", "run_id"))
        for row in valid[:10]:
            print(
                top_fmt.format(
                    row["delivered"],
                    row["rate_per_s"],
                    row["senders"],
                    row["m"],
                    row["window"],
                    row["timeout_ms"],
                    row["run_id"],
                )
            )
    print("best per (senders, m):")
    best = {}
    for row in valid:
        key = (int(row["senders"]), int(row["m"]))
        current = best.get(key)
        if current is None or _better(row, current):
            best[key] = row
    if not best:
        print("  (none)")
        return
    best_fmt = "  {:>7}  {:>10}  {:>12}  {:>6}  {:>10}"
    print(best_fmt.format("senders", "m", "delivered", "window", "timeout_ms"))
    for key in sorted(best):
        row = best[key]
        print(
            best_fmt.format(
                row["senders"], row["m"], row["delivered"], row["window"], row["timeout_ms"]
            )
        )


def print_dry_run(plans, done, duration, log_wait, java_opts, rows) -> None:
    already = 0
    for plan in plans:
        key = resume_key(
            plan.senders,
            plan.messages,
            plan.window,
            plan.timeout_ms,
            duration,
            log_wait,
            java_opts,
        )
        if key in done:
            already += 1
            mark = "done"
        else:
            mark = "todo"
        mult = "" if plan.timeout_mult is None else fmt_float(plan.timeout_mult)
        print(
            "s={} m={} w={} timeout_ms={} mult={} est_ms={:.3f} [{}]".format(
                plan.senders,
                plan.messages,
                plan.window,
                plan.timeout_ms,
                mult,
                plan.timeout_est_ms,
                mark,
            )
        )
    planned = len(plans)
    remaining = planned - already
    measured = mean_check_s(rows)
    if measured is None:
        check_est = DEFAULT_CHECK_S
        check_source = "default"
    else:
        check_est = measured
        check_source = "mean check_s from the CSV"
    # Log wait after SIGTERM was ~1 s in real runs; use 2 s when the CSV has no wall breakdown.
    observed_log = mean_observed_log_wait(rows)
    typical_log = 2.0 if observed_log is None else observed_log
    typical_per = duration + typical_log + START_OVERHEAD_S + check_est
    upper_per = duration + log_wait + START_OVERHEAD_S + check_est
    typical_hours = typical_per * remaining / 3600.0
    upper_hours = upper_per * remaining / 3600.0
    print("planned: {}".format(planned))
    print("already done: {}".format(already))
    print("remaining: {}".format(remaining))
    print(
        "Estimated wall time: {:.1f} h ({:.1f} days) for {} runs (about {:.0f} s per run)".format(
            typical_hours, typical_hours / 24.0, remaining, typical_per
        )
    )
    print(
        "Upper bound if every log wait runs the full {} s: {:.1f} h ({:.1f} days)".format(
            fmt_float(log_wait), upper_hours, upper_hours / 24.0
        )
    )
    print(
        "Check estimate: {} s per run ({}). --no-check saves it.".format(
            fmt_float(check_est), check_source
        )
    )


def _int_list(lo: int, hi: int):
    def parse(text: str):
        if text.strip() == "":
            raise argparse.ArgumentTypeError("empty list")
        values = []
        for part in text.split(","):
            part = part.strip()
            if part == "":
                raise argparse.ArgumentTypeError("empty list entry")
            try:
                value = int(part, 10)
            except ValueError:
                raise argparse.ArgumentTypeError("not an integer: {}".format(part))
            if value < lo or value > hi:
                raise argparse.ArgumentTypeError(
                    "{} is outside {}..{}".format(value, lo, hi)
                )
            values.append(value)
        return values

    return parse


def _float_list_positive(text: str):
    if text.strip() == "":
        raise argparse.ArgumentTypeError("empty list")
    values = []
    for part in text.split(","):
        part = part.strip()
        if part == "":
            raise argparse.ArgumentTypeError("empty list entry")
        try:
            value = float(part)
        except ValueError:
            raise argparse.ArgumentTypeError("not a number: {}".format(part))
        if not math.isfinite(value) or value <= 0:
            raise argparse.ArgumentTypeError("must be positive: {}".format(part))
        values.append(value)
    return values


def _positive_float(text: str) -> float:
    try:
        value = float(text)
    except ValueError:
        raise argparse.ArgumentTypeError("not a number: {}".format(text))
    if not math.isfinite(value) or value <= 0:
        raise argparse.ArgumentTypeError("must be positive")
    return value


def _nonnegative_float(text: str) -> float:
    try:
        value = float(text)
    except ValueError:
        raise argparse.ArgumentTypeError("not a number: {}".format(text))
    if not math.isfinite(value) or value < 0:
        raise argparse.ArgumentTypeError("must be zero or positive")
    return value


def _nonnegative_int(hi: int):
    def parse(text: str) -> int:
        try:
            value = int(text, 10)
        except ValueError:
            raise argparse.ArgumentTypeError("not an integer: {}".format(text))
        if value < 0 or value > hi:
            raise argparse.ArgumentTypeError("outside 0..{}".format(hi))
        return value

    return parse


EXAMPLES = """
examples:
  smoke run
    python3 tools/perf_sweep.py --windows 1,4 --timeouts 10,50 --senders 2,8 --messages 100000000 --duration 5 --log-wait 3
  full default sweep
    python3 tools/perf_sweep.py
  dry run
    python3 tools/perf_sweep.py --dry-run
  reduced first pass (one m, fewer windows)
    python3 tools/perf_sweep.py --messages 100000000 --windows 1,16,128,1024
"""


def parse_args(argv=None):
    parser = argparse.ArgumentParser(
        description="Sweep WINDOW and TIMEOUT_MS for perfect links and append one CSV row per run.",
        epilog=EXAMPLES,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--runscript", default=DEFAULT_RUNSCRIPT, help="Path to run.sh")
    parser.add_argument(
        "--out",
        default=os.path.expanduser("~/perf-sweep-out"),
        help="Output directory (default: ~/perf-sweep-out). Refused inside template_java.",
    )
    parser.add_argument(
        "--csv",
        default=None,
        help="CSV path (default: OUT/results.csv).",
    )
    # Defaults are already lists because argparse applies type= only to string defaults.
    parser.add_argument(
        "--windows",
        type=_int_list(1, 2147483647),
        default=[1 << shift for shift in range(11)],
        help="Window sizes (default: 1,2,4,...,1024).",
    )
    parser.add_argument(
        "--senders",
        type=_int_list(1, 125),
        default=[2, 4, 8, 16, 32, 64, 125],
        help="Sender counts, each in 1..125. Process 1 is the receiver.",
    )
    parser.add_argument(
        "--messages",
        type=_int_list(1, 2147483647),
        default=[100000000, 1000000000, 2147483647],
        help="m values, each in 1..2147483647.",
    )
    parser.add_argument(
        "--timeout-mults",
        type=_float_list_positive,
        default=[0.8, 0.9, 1.0, 1.1, 1.2],
        help="Multipliers of the timeout estimate (default: 0.8,0.9,1,1.1,1.2).",
    )
    parser.add_argument(
        "--timeouts",
        type=_int_list(1, 2147483647),
        default=None,
        help="Explicit TIMEOUT_MS values. Replaces --timeout-mults.",
    )
    parser.add_argument("--p99-us", type=_positive_float, default=136.0, help="p99 microseconds used only to centre the timeout range (default: 136).")
    parser.add_argument(
        "--timeout-floor-ms",
        type=_nonnegative_int(2147483647),
        default=1,
        help="Minimum TIMEOUT_MS when using multipliers (default: 1).",
    )
    parser.add_argument("--duration", type=_positive_float, default=120.0, help="Seconds to run before SIGTERM (default: 120).")
    parser.add_argument("--log-wait", type=_nonnegative_float, default=60.0, help="Seconds to wait for logs after SIGTERM (default: 60).")
    parser.add_argument(
        "--java-opts",
        default="",
        help="Value of JAVA_TOOL_OPTIONS for every process. Off when omitted. Recorded in the CSV.",
    )
    parser.add_argument("--port-base", type=_nonnegative_int(65535), default=11000, help="Process i listens on base+i (default: 11000).")
    parser.add_argument("--port-timeout", type=_nonnegative_float, default=60.0, help="Seconds to wait for ports to go free (default: 60).")
    parser.add_argument("--build", action="store_true", help="Run template_java/build.sh before the sweep.")
    parser.add_argument("--no-check", action="store_true", help="Skip receiver validation. Still count delivered lines and sender progress.")
    parser.add_argument("--keep-outputs", action="store_true", help="Keep procNN.output after scoring.")
    parser.add_argument("--dry-run", action="store_true", help="Print the plan and the estimated wall time. Start nothing.")
    return parser.parse_args(argv)


def run_build(runscript: str) -> None:
    build = os.path.join(os.path.dirname(os.path.abspath(runscript)), "build.sh")
    if not os.path.isfile(build):
        raise SystemExit("build script not found: {}".format(build))
    print("building {}".format(build), flush=True)
    result = subprocess.run(["bash", build])
    if result.returncode != 0:
        raise SystemExit(result.returncode)


def _install_signals() -> None:
    signal.signal(signal.SIGINT, _on_signal)
    signal.signal(signal.SIGTERM, _on_signal)


def _kill_active() -> None:
    for proc in list(_ACTIVE):
        try:
            os.killpg(proc.pid, signal.SIGKILL)
        except OSError:
            pass
        proc.poll()
    _ACTIVE.clear()


def main(argv=None) -> int:
    args = parse_args(argv)
    runscript = os.path.abspath(args.runscript)
    if not os.path.isfile(runscript):
        raise SystemExit("run script not found: {}".format(runscript))
    out_dir = safe_out_dir(args.out, REPO_ROOT)
    if args.csv:
        csv_path = os.path.realpath(os.path.expanduser(args.csv))
    else:
        csv_path = os.path.join(out_dir, "results.csv")
    reject_cwd_name(csv_path)
    max_port = args.port_base + max(args.senders) + 1
    if max_port > 65535 or args.port_base + 1 < 1:
        raise SystemExit(
            "ports {}..{} fall outside 1..65535".format(args.port_base + 1, max_port)
        )
    plans = build_plans(
        args.messages,
        args.senders,
        args.windows,
        args.timeout_mults,
        args.timeouts,
        args.p99_us,
        args.timeout_floor_ms,
        args.duration,
    )
    if os.path.exists(csv_path):
        rows, done = load_csv(csv_path)
    else:
        rows, done = [], set()
    if args.dry_run:
        print_dry_run(plans, done, args.duration, args.log_wait, args.java_opts, rows)
        return 0
    if args.build:
        run_build(runscript)
    jar = os.path.join(os.path.dirname(runscript), "bin", "da_proc.jar")
    if not os.path.isfile(jar):
        raise SystemExit("missing {} (build first or pass --build)".format(jar))
    os.makedirs(out_dir, exist_ok=True)
    settings = Settings(
        runscript=runscript,
        out_dir=out_dir,
        port_base=args.port_base,
        duration=args.duration,
        log_wait=args.log_wait,
        port_timeout=args.port_timeout,
        java_opts=args.java_opts,
        do_check=not args.no_check,
        keep_outputs=args.keep_outputs,
    )
    _install_signals()
    handle = None
    try:
        handle, writer = open_csv_writer(csv_path)
        for plan in plans:
            key = resume_key(
                plan.senders,
                plan.messages,
                plan.window,
                plan.timeout_ms,
                args.duration,
                args.log_wait,
                args.java_opts,
            )
            if key in done:
                continue
            row = execute_run(plan, settings)
            writer.writerow(row)
            handle.flush()
            done.add(key)
            rows.append(row)
            print(
                "{run_id} delivered={delivered} rate={rate_per_s} check={check} error={error}".format(
                    **row
                ),
                flush=True,
            )
    except KeyboardInterrupt:
        print("interrupted", file=sys.stderr)
        return 130
    finally:
        _kill_active()
        if handle is not None:
            handle.close()
    print_summary(rows)
    return 0


if __name__ == "__main__":
    sys.exit(main())
