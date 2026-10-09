#!/usr/bin/env python3
"""
StreamForge 2.0: Unified Release Gate & Quality Assurance Harness.

This script executes all release gating stages required for production release:
1. Python Replay Adapter & Protobuf Ingestion Tests (pytest)
2. Go Core Ingestion, Domain & RestAPI Unit/Integration Tests (go test)
3. Kubernetes Helm Manifest Template & Security Hardening Audit (validate_helm_manifests)
4. PostgreSQL Disaster Recovery Drill & 100% Data Parity Check (backup_restore_drill)
5. Performance Benchmark Invariant & PyArrow Reconciliation Audit (benchmark_results)

Exit Code: 0 on complete pass, 1 on any gate failure.
"""

from __future__ import annotations

import json
import subprocess
import sys
import time
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent

GREEN = "\033[92m"
RED = "\033[91m"
YELLOW = "\033[93m"
CYAN = "\033[96m"
BOLD = "\033[1m"
RESET = "\033[0m"


def run_stage(name: str, cmd: list[str], cwd: Path | None = None) -> bool:
    print(f"\n{BOLD}{CYAN}=== Gate Stage: {name} ==={RESET}")
    print(f"Command: {' '.join(cmd)}")
    start = time.perf_counter()
    res = subprocess.run(cmd, cwd=str(cwd or REPO_ROOT), capture_output=True, text=True)
    duration = time.perf_counter() - start

    if res.returncode == 0:
        print(f"{GREEN}[PASS]{RESET} {name} succeeded in {duration:.2f}s")
        if res.stdout.strip():
            # Print last few lines of summary
            lines = [l for l in res.stdout.strip().splitlines() if l.strip()]
            for line in lines[-3:]:
                print(f"  {line}")
        return True
    else:
        print(f"{RED}[FAIL]{RESET} {name} failed with exit code {res.returncode}")
        if res.stderr:
            print(f"{RED}Error Output:\n{res.stderr.strip()}{RESET}")
        elif res.stdout:
            print(f"{RED}Standard Output:\n{res.stdout.strip()}{RESET}")
        return False


def verify_benchmark_results() -> bool:
    print(f"\n{BOLD}{CYAN}=== Gate Stage: Performance Benchmark & Invariant Audit ==={RESET}")
    bench_file = REPO_ROOT / ".tmp" / "benchmarks" / "benchmark_results.json"
    if not bench_file.exists():
        print(f"{RED}[FAIL]{RESET} Benchmark results file not found at: {bench_file}")
        return False

    try:
        with open(bench_file, "r", encoding="utf-8") as f:
            data = json.load(f)

        status = data.get("status")
        reconciled = data.get("reconciliation_passed", False)
        ingestion = data.get("ingestion", {})
        query = data.get("query", {})
        baseline = data.get("baseline", {})

        eps = ingestion.get("sustained_events_per_sec", 0.0)
        p95_prod = ingestion.get("producer_latency", {}).get("p95_ms", 999.0)
        p95_query = query.get("latency", {}).get("p95_ms", 999.0)
        total_events = ingestion.get("total_events", 0)

        print(f"  - Dataset Events: {total_events:,}")
        print(f"  - Sustained Ingestion Throughput: {eps:,.1f} eps (Threshold: >= 1,000 eps)")
        print(f"  - Producer Ack Latency (p95): {p95_prod:.1f} ms (Threshold: < 250 ms)")
        print(f"  - REST Query Latency (p95): {p95_query:.1f} ms (Threshold: < 300 ms)")
        print(f"  - PyArrow Baseline Parity: {'100% Exact Match' if reconciled else 'Discrepancy'}")

        passed = (
            status == "PASS"
            and reconciled
            and eps >= 1000.0
            and p95_prod < 250.0
            and p95_query < 300.0
        )

        if passed:
            print(f"{GREEN}[PASS]{RESET} Performance benchmark metrics and PyArrow reconciliation verified.")
            return True
        else:
            print(f"{RED}[FAIL]{RESET} Performance benchmark metrics did not meet release thresholds.")
            return False
    except Exception as e:
        print(f"{RED}[FAIL]{RESET} Error reading benchmark report: {e}")
        return False


def main() -> None:
    print(f"{BOLD}{'=' * 70}")
    print(f" STREAMFORGE 2.0: PRODUCTION UNIFIED RELEASE GATE")
    print(f"{'=' * 70}{RESET}")
    print(f"Root: {REPO_ROOT}")
    print(f"Time: {time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())}")

    stages: list[tuple[str, list[str], Path | None]] = [
        ("Python Replay & Ingestion Tests", ["pytest", "tools/replay-python/tests"], REPO_ROOT),
        ("Go Core Services Unit & Integration Tests", ["go", "test", "./..."], REPO_ROOT / "services" / "core-go"),
        ("Kubernetes Helm Security & Manifest Validation", [sys.executable, "scripts/validate_helm_manifests.py"], REPO_ROOT),
        ("PostgreSQL Disaster Recovery & Invariant Drill", [sys.executable, "scripts/backup_restore_drill.py"], REPO_ROOT),
    ]

    failed_stages = []

    for name, cmd, cwd in stages:
        success = run_stage(name, cmd, cwd)
        if not success:
            failed_stages.append(name)

    bench_success = verify_benchmark_results()
    if not bench_success:
        failed_stages.append("Performance Benchmark & Invariant Audit")

    print(f"\n{BOLD}{'=' * 70}")
    if not failed_stages:
        print(f"{GREEN}{BOLD} >>> ALL RELEASE GATES PASSED: STREAMFORGE 2.0 READY FOR RELEASE <<< {RESET}")
        print(f"{'=' * 70}\n")
        sys.exit(0)
    else:
        print(f"{RED}{BOLD} >>> RELEASE GATE FAILED: {len(failed_stages)} STAGE(S) FAILED <<< {RESET}")
        for s in failed_stages:
            print(f"  - {RED}{s}{RESET}")
        print(f"{'=' * 70}\n")
        sys.exit(1)


if __name__ == "__main__":
    main()
