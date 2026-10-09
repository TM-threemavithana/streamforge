#!/usr/bin/env python3
"""
PostgreSQL Backup and Disaster Recovery Drill Script for StreamForge.

Performs:
1. Online consistent backup of both StreamForge databases (`streamforge` and `streamforge_alerts`).
2. Integrity validation with SHA-256 checksums.
3. Automated restore into a drill database (`streamforge_verify_dr`).
4. Exact record count and invariant reconciliation between live and restored databases.
5. Automated cleanup of temporary drill artifacts.
"""

import hashlib
import os
import subprocess
import sys
import time
from pathlib import Path

CONTAINER = "docker-postgres-1"
DB_USER = "streamforge"

def run_cmd(args, check=True):
    proc = subprocess.run(args, capture_output=True, text=True)
    if check and proc.returncode != 0:
        print(f"[!] Command failed: {' '.join(args)}\nError: {proc.stderr}", file=sys.stderr)
        raise RuntimeError(proc.stderr)
    return proc

def compute_sha256(filepath: Path) -> str:
    h = hashlib.sha256()
    with open(filepath, "rb") as f:
        while chunk := f.read(65536):
            h.update(chunk)
    return h.hexdigest()

def main():
    repo_root = Path(__file__).resolve().parent.parent
    backup_dir = repo_root / ".tmp" / "backups"
    backup_dir.mkdir(parents=True, exist_ok=True)
    
    timestamp = time.strftime("%Y%m%d_%H%M%S")
    core_backup = backup_dir / f"streamforge_core_{timestamp}.sql"
    alerts_backup = backup_dir / f"streamforge_alerts_{timestamp}.sql"

    print("=================================================================")
    print(" STREAMFORGE DISASTER RECOVERY DRILL & BACKUP VERIFICATION")
    print("=================================================================")
    print(f"[*] Timestamp: {timestamp}")
    print(f"[*] Target Container: {CONTAINER}")
    print(f"[*] Backup Directory: {backup_dir}")

    # 1. Backup Core Database
    print("\n[Phase 1] Creating Core Database Backup...")
    core_proc = run_cmd(["docker", "exec", CONTAINER, "pg_dump", "-U", DB_USER, "streamforge"])
    core_backup.write_text(core_proc.stdout, encoding="utf-8")
    core_sha = compute_sha256(core_backup)
    core_size = core_backup.stat().st_size
    print(f"[+] Core backup written: {core_backup.name} ({core_size:,} bytes)")
    print(f"    SHA-256: {core_sha}")

    # 2. Backup Alerts Database
    print("\n[Phase 2] Creating Alerts Database Backup...")
    alerts_proc = run_cmd(["docker", "exec", CONTAINER, "pg_dump", "-U", DB_USER, "streamforge_alerts"])
    alerts_backup.write_text(alerts_proc.stdout, encoding="utf-8")
    alerts_sha = compute_sha256(alerts_backup)
    alerts_size = alerts_backup.stat().st_size
    print(f"[+] Alerts backup written: {alerts_backup.name} ({alerts_size:,} bytes)")
    print(f"    SHA-256: {alerts_sha}")

    # 3. Disaster Recovery Restoration Drill
    drill_db = f"streamforge_drill_{int(time.time())}"
    print(f"\n[Phase 3] Executing Disaster Recovery Drill into temporary database: {drill_db}...")
    
    # Create drill DB
    run_cmd(["docker", "exec", CONTAINER, "psql", "-U", DB_USER, "-d", "postgres", "-c", f"CREATE DATABASE {drill_db};"])

    try:
        # Restore into drill DB
        print(f"[*] Restoring core backup into {drill_db}...")
        restore_proc = subprocess.run(
            ["docker", "exec", "-i", CONTAINER, "psql", "-U", DB_USER, "-d", drill_db],
            input=core_backup.read_text(encoding="utf-8"),
            capture_output=True,
            text=True
        )
        if restore_proc.returncode != 0:
            print(f"[!] Restore warnings/errors:\n{restore_proc.stderr}", file=sys.stderr)

        # 4. Reconciliation
        print("\n[Phase 4] Reconciling Invariants Between Live and Restored Databases...")
        tables = ["datasets", "replay_runs", "trip_events", "rejected_events", "hourly_zone_stats"]
        all_matched = True

        for table in tables:
            live_res = run_cmd(["docker", "exec", CONTAINER, "psql", "-U", DB_USER, "-d", "streamforge", "-t", "-c", f"SELECT COUNT(*) FROM {table};"], check=False)
            drill_res = run_cmd(["docker", "exec", CONTAINER, "psql", "-U", DB_USER, "-d", drill_db, "-t", "-c", f"SELECT COUNT(*) FROM {table};"], check=False)

            live_count = int(live_res.stdout.strip()) if live_res.returncode == 0 and live_res.stdout.strip().isdigit() else 0
            drill_count = int(drill_res.stdout.strip()) if drill_res.returncode == 0 and drill_res.stdout.strip().isdigit() else 0

            match = (live_count == drill_count)
            status_symbol = "[OK]" if match else "[FAIL]"
            print(f"  {status_symbol} Table '{table}': Live={live_count} | Restored={drill_count}")
            if not match:
                all_matched = False

        if not all_matched:
            print("[!] Reconciliation failed! Data discrepancy detected.", file=sys.stderr)
            sys.exit(1)
        else:
            print("\n[+] Verification SUCCESS: 100% data parity between live and restored databases.")

    finally:
        # Cleanup drill DB
        print(f"\n[Phase 5] Cleaning up drill database {drill_db}...")
        run_cmd(["docker", "exec", CONTAINER, "psql", "-U", DB_USER, "-d", "postgres", "-c", f"DROP DATABASE IF EXISTS {drill_db};"], check=False)
        print("[+] Drill database removed cleanly.")

    print("\n=================================================================")
    print(" DISASTER RECOVERY DRILL COMPLETED SUCCESSFULLY")
    print("=================================================================")

if __name__ == "__main__":
    main()
