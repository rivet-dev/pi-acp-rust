#!/usr/bin/env python3
"""Reproducible cold-start and RSS comparison for Rust and Node Pi ACP."""

import argparse
import json
import math
import os
import pathlib
import platform
import shutil
import statistics
import subprocess
import tempfile
import time


ROOT = pathlib.Path(__file__).resolve().parents[1]
FAKE_PI = ROOT / "fixtures" / "fake-pi.py"
RUST = ROOT / "target" / "release" / "pi-acp"
NODE = ROOT / "bench" / "node" / "node_modules" / ".bin" / "pi-acp"


def percentile(values, p):
    ordered = sorted(values)
    index = max(0, math.ceil((p / 100) * len(ordered)) - 1)
    return ordered[index]


def rss_kib(pid):
    try:
        for line in pathlib.Path(f"/proc/{pid}/status").read_text().splitlines():
            if line.startswith("VmRSS:"):
                return int(line.split()[1])
    except (FileNotFoundError, ProcessLookupError):
        pass
    return 0


def children(pid):
    path = pathlib.Path(f"/proc/{pid}/task/{pid}/children")
    try:
        direct = [int(value) for value in path.read_text().split()]
    except (FileNotFoundError, ProcessLookupError):
        return []
    result = []
    for child in direct:
        result.append(child)
        result.extend(children(child))
    return result


def write(proc, value):
    proc.stdin.write(json.dumps(value, separators=(",", ":")) + "\n")
    proc.stdin.flush()


def read_response(proc, request_id, deadline=10):
    end = time.monotonic() + deadline
    while time.monotonic() < end:
        line = proc.stdout.readline()
        if not line:
            stderr = proc.stderr.read()
            raise RuntimeError(f"adapter exited before response {request_id}: {stderr}")
        value = json.loads(line)
        if value.get("id") == request_id:
            if "error" in value:
                raise RuntimeError(f"ACP error: {value['error']}")
            return value["result"]
    raise TimeoutError(f"response {request_id} timed out")


def one_sample(command, name):
    with tempfile.TemporaryDirectory(prefix=f"pi-acp-bench-{name}-") as state:
        env = os.environ.copy()
        env["PI_ACP_PI_COMMAND"] = str(FAKE_PI)
        env["PI_ACP_STATE_DIR"] = state
        started = time.perf_counter_ns()
        proc = subprocess.Popen(
            command,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            bufsize=1,
            env=env,
        )
        try:
            write(proc, {"jsonrpc": "2.0", "id": 1, "method": "initialize", "params": {"protocolVersion": 1, "clientCapabilities": {}}})
            read_response(proc, 1)
            write(proc, {"jsonrpc": "2.0", "id": 2, "method": "session/new", "params": {"cwd": state, "mcpServers": []}})
            read_response(proc, 2)
            startup_ms = (time.perf_counter_ns() - started) / 1_000_000

            # Five polls after initialization; the median rejects allocator/page-fault noise.
            adapter_rss = []
            tree_rss = []
            for _ in range(5):
                time.sleep(0.02)
                descendants = children(proc.pid)
                adapter_rss.append(rss_kib(proc.pid))
                tree_rss.append(rss_kib(proc.pid) + sum(rss_kib(pid) for pid in descendants))
            return {
                "startupMs": startup_ms,
                "adapterRssKiB": statistics.median(adapter_rss),
                "processTreeRssKiB": statistics.median(tree_rss),
            }
        finally:
            proc.terminate()
            try:
                proc.wait(timeout=2)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait()


def summarize(samples):
    result = {"samples": samples}
    for field in ("startupMs", "adapterRssKiB", "processTreeRssKiB"):
        values = [sample[field] for sample in samples]
        result[field] = {
            "median": round(statistics.median(values), 2),
            "p95": round(percentile(values, 95), 2),
            "min": round(min(values), 2),
            "max": round(max(values), 2),
        }
    return result


def command_output(command):
    return subprocess.check_output(command, text=True, stderr=subprocess.STDOUT).strip()


def optional_command_output(command):
    try:
        return command_output(command)
    except subprocess.CalledProcessError:
        return None


def environment():
    cpu = "unknown"
    try:
        cpu = next(
            line.split(":", 1)[1].strip()
            for line in pathlib.Path("/proc/cpuinfo").read_text().splitlines()
            if line.startswith("model name")
        )
    except (FileNotFoundError, StopIteration):
        pass
    return {
        "timestampUtc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "os": platform.platform(),
        "kernel": platform.release(),
        "architecture": platform.machine(),
        "cpu": cpu,
        "logicalCpus": os.cpu_count(),
        "python": platform.python_version(),
        "node": command_output(["node", "--version"]),
        "npm": command_output(["npm", "--version"]),
        "rustc": command_output(["rustc", "--version"]),
        "rustAdapter": "0.1.0",
        "nodeAdapter": "pi-acp@0.0.31",
        "gitCommit": optional_command_output(["git", "rev-parse", "HEAD"]),
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--samples", type=int, default=30)
    parser.add_argument("--warmups", type=int, default=5)
    parser.add_argument("--output", type=pathlib.Path, default=ROOT / "bench" / "results-linux-x86_64.json")
    parser.add_argument("--skip-build", action="store_true")
    args = parser.parse_args()
    if platform.system() != "Linux" or not pathlib.Path("/proc/self/status").exists():
        raise SystemExit("benchmark RSS measurement requires Linux /proc")
    if not args.skip_build:
        subprocess.run(["cargo", "build", "--release", "--locked"], cwd=ROOT, check=True)
        subprocess.run(["npm", "ci", "--ignore-scripts"], cwd=ROOT / "bench" / "node", check=True)
    if not RUST.exists() or not NODE.exists():
        raise SystemExit("missing adapters; rerun without --skip-build")

    commands = {"rust": [str(RUST)], "node": [str(NODE)]}
    for _ in range(args.warmups):
        for name, command in commands.items():
            one_sample(command, name)

    raw = {name: [] for name in commands}
    # Interleave isolated samples so machine drift affects both implementations equally.
    for _ in range(args.samples):
        for name, command in commands.items():
            raw[name].append(one_sample(command, name))

    output = {
        "methodology": {
            "samples": args.samples,
            "warmups": args.warmups,
            "isolation": "fresh adapter and fresh state directory per sample",
            "startupBoundary": "process spawn through ACP session/new response",
            "steadyStateRss": "median of five /proc VmRSS polls at 20 ms intervals after session/new",
            "fixture": "fixtures/fake-pi.py; identical get_state/get_available_models input",
        },
        "environment": environment(),
        "results": {name: summarize(samples) for name, samples in raw.items()},
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(output, indent=2) + "\n")
    print(json.dumps(output, indent=2))


if __name__ == "__main__":
    main()
