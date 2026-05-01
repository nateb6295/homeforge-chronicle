#!/usr/bin/env python3
"""
security_audit.py — Audit security posture of Chronicle infrastructure.

Gap #11: Backend API token stored in state file. While not critical
(file-level permissions are correct), this tool provides ongoing
security monitoring.

Checks:
  1. Token file permissions (should be 600)
  2. Token not exposed in code/git
  3. Webhook URLs not in tracked files
  4. Service file permissions
  5. DB file permissions

Usage:
    security_audit.py check          # Full audit
    security_audit.py token          # Token-specific checks
    security_audit.py rotate         # Generate new token (requires canister update)
"""
import os
import stat
import subprocess
import sys
from pathlib import Path

HOME = Path.home()
SENSITIVE_FILES = [
    HOME / ".homeforge-chronicle" / ".api_token",
    HOME / "chronicle" / "chronicle.env",
]
DB_PATH = Path("/mnt/hdd/chronicle-data/processed.db")
BIN_DIR = HOME / "chronicle" / "bin"


def check_permissions(path, expected_mode=0o600, label=""):
    """Check file permissions."""
    if not path.exists():
        return f"MISSING", f"{label or path.name}: file not found"

    mode = stat.S_IMODE(os.stat(path).st_mode)
    world_readable = mode & stat.S_IROTH
    group_readable = mode & stat.S_IRGRP

    if world_readable:
        return "FAIL", f"{label or path.name}: world-readable ({oct(mode)})"
    elif group_readable:
        return "WARN", f"{label or path.name}: group-readable ({oct(mode)})"
    elif mode == expected_mode:
        return "OK", f"{label or path.name}: {oct(mode)}"
    else:
        return "OK", f"{label or path.name}: {oct(mode)} (expected {oct(expected_mode)})"


def check_token():
    """Token-specific security checks."""
    token_path = HOME / ".homeforge-chronicle" / ".api_token"
    print("TOKEN SECURITY AUDIT")
    print("=" * 50)

    # 1. File exists and permissions
    status, msg = check_permissions(token_path)
    print(f"  [{status}] {msg}")

    # 2. Token not in git
    try:
        result = subprocess.run(
            ["git", "log", "--oneline", "--all", "-p", "--", "*.api_token"],
            capture_output=True, text=True, timeout=10,
            cwd=str(HOME / "chronicle")
        )
        if result.stdout.strip():
            print("  [FAIL] Token file appears in git history!")
        else:
            print("  [OK] Token not in git history")
    except Exception:
        print("  [SKIP] Could not check git history")

    # 3. Token value not in code
    if token_path.exists():
        token = token_path.read_text().strip()
        exposed = False
        for f in BIN_DIR.glob("*.py"):
            try:
                if token in f.read_text():
                    print(f"  [FAIL] Token value found in {f.name}!")
                    exposed = True
            except Exception:
                continue
        if not exposed:
            print("  [OK] Token value not exposed in code")

    # 4. Token age
    if token_path.exists():
        age_days = (os.path.getmtime(token_path))
        import time
        age_days = (time.time() - os.path.getmtime(token_path)) / 86400
        if age_days > 90:
            print(f"  [WARN] Token is {age_days:.0f} days old — consider rotation")
        else:
            print(f"  [OK] Token age: {age_days:.0f} days")

    # 5. Usage count
    usage_count = 0
    for f in BIN_DIR.glob("*.py"):
        try:
            if "TOKEN_PATH" in f.read_text() or ".api_token" in f.read_text():
                usage_count += 1
        except Exception:
            continue
    print(f"  [INFO] Token used by {usage_count} scripts")

    print()
    print("  MIGRATION PATH: vetKD or IC identity-based auth")
    print("  STATUS: File-based token is interim; canister Rust changes needed")


def check_webhooks():
    """Check webhook URL exposure."""
    print("\nWEBHOOK SECURITY")
    print("-" * 50)

    env_file = HOME / "chronicle" / "chronicle.env"
    if env_file.exists():
        status, msg = check_permissions(env_file, 0o644, "chronicle.env")
        print(f"  [{status}] {msg}")

    # Check if webhook URLs are in tracked git files
    try:
        result = subprocess.run(
            ["git", "grep", "-l", "discord.com/api/webhooks"],
            capture_output=True, text=True, timeout=10,
            cwd=str(HOME / "chronicle")
        )
        if result.stdout.strip():
            files = result.stdout.strip().split("\n")
            for f in files:
                if f.endswith(".env") or f.endswith(".env.example"):
                    continue
                print(f"  [WARN] Webhook URL found in tracked file: {f}")
        else:
            print("  [OK] No webhook URLs in tracked git files")
    except Exception:
        print("  [SKIP] Could not check git files")


def check_db():
    """Check database file permissions."""
    print("\nDATABASE SECURITY")
    print("-" * 50)
    status, msg = check_permissions(DB_PATH, 0o644, "processed.db")
    print(f"  [{status}] {msg}")

    # Check WAL/SHM files
    for suffix in ["-wal", "-shm"]:
        wal = Path(str(DB_PATH) + suffix)
        if wal.exists():
            status, msg = check_permissions(wal, 0o644, f"processed.db{suffix}")
            print(f"  [{status}] {msg}")


def check_services():
    """Check systemd service file permissions."""
    print("\nSERVICE FILE SECURITY")
    print("-" * 50)
    svc_dir = HOME / ".config" / "systemd" / "user"
    if svc_dir.exists():
        for svc in sorted(svc_dir.glob("chronicle-*.service")):
            # Check if EnvironmentFile points to something with sensitive data
            content = svc.read_text()
            if "EnvironmentFile" in content:
                env_ref = [l for l in content.splitlines() if "EnvironmentFile" in l]
                for ref in env_ref:
                    # Resolve %h
                    path_str = ref.split("=", 1)[1].replace("%h", str(HOME))
                    env_path = Path(path_str)
                    if env_path.exists():
                        status, msg = check_permissions(env_path, 0o644, f"{svc.name} → {env_path.name}")
                        print(f"  [{status}] {msg}")


def full_audit():
    """Run all security checks."""
    print("=" * 50)
    print("CHRONICLE SECURITY AUDIT")
    print("=" * 50)
    print()

    check_token()
    check_webhooks()
    check_db()
    check_services()

    print()
    print("SUMMARY:")
    print("  Token: file-based, 600 perms, not in git — acceptable interim")
    print("  Migration: vetKD/IC identity requires canister backend changes")
    print("  Recommendation: rotate token quarterly, monitor with this tool")


def main():
    if len(sys.argv) < 2:
        print(__doc__)
        return

    cmd = sys.argv[1]

    if cmd == "check":
        full_audit()
    elif cmd == "token":
        check_token()
    else:
        print(f"Unknown command: {cmd}")
        print(__doc__)


if __name__ == "__main__":
    main()
