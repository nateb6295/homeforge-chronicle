#!/usr/bin/env python3
"""Log cycle metrics for access starvation and flow tracking.

Usage:
  log_cycle_metrics.py <cycle_ts> [key=value ...]

Example:
  log_cycle_metrics.py 20260322_1400 self_model_loaded=42 self_model_total=105 thread_advanced=1 finding_count=4
"""
import sqlite3, os, sys, time

DB_PATH = os.path.expanduser("~/.homeforge-chronicle/processed.db")

def main():
    if len(sys.argv) < 2:
        print("Usage: log_cycle_metrics.py <cycle_ts> [key=value ...]")
        sys.exit(1)

    cycle_ts = sys.argv[1]
    now = int(time.time())

    db = sqlite3.connect(DB_PATH, timeout=30)

    # Parse key=value pairs
    for arg in sys.argv[2:]:
        if "=" in arg:
            key, val = arg.split("=", 1)
            try:
                fval = float(val)
            except ValueError:
                # Store text metrics with value=1 and detail=text
                db.execute(
                    "INSERT INTO cycle_metrics (cycle_ts, metric, value, detail, created_at) VALUES (?,?,?,?,?)",
                    (cycle_ts, key, 1.0, val, now)
                )
                continue
            db.execute(
                "INSERT INTO cycle_metrics (cycle_ts, metric, value, created_at) VALUES (?,?,?,?)",
                (cycle_ts, key, fval, now)
            )

    # Auto-compute starvation metrics from self_model
    total = db.execute("SELECT COUNT(*) FROM self_model WHERE superseded_by IS NULL").fetchone()[0]
    never = db.execute("SELECT COUNT(*) FROM self_model WHERE superseded_by IS NULL AND last_accessed IS NULL").fetchone()[0]
    day_ago = now - 86400
    accessed_24h = db.execute("SELECT COUNT(*) FROM self_model WHERE superseded_by IS NULL AND last_accessed > ?", (day_ago,)).fetchone()[0]

    for metric, value in [
        ("self_model_total", total),
        ("self_model_never_accessed", never),
        ("self_model_accessed_24h", accessed_24h),
        ("self_model_starvation_pct", round(100 * (total - accessed_24h) / max(total, 1), 1)),
    ]:
        db.execute(
            "INSERT INTO cycle_metrics (cycle_ts, metric, value, created_at) VALUES (?,?,?,?)",
            (cycle_ts, metric, value, now)
        )

    db.commit()
    print(f"Logged metrics for cycle {cycle_ts}. Self-model: {accessed_24h}/{total} accessed in 24h ({100*(total-accessed_24h)/max(total,1):.0f}% starved)")

if __name__ == "__main__":
    main()
