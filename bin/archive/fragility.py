#!/usr/bin/env python3
"""Build #148: Token Structural Fragility Directory.

Maps structural risks in crypto tokens — concentration, reflexive loops,
rent extraction, single points of failure. Not price. Topology.

Per Nate: "Should we be keeping a directory of Tokens and the fragilities?"

Usage:
  fragility.py add TOKEN CATEGORY SEVERITY "description" ["evidence"] ["mitigants"]
  fragility.py list [TOKEN]
  fragility.py report [TOKEN]       — formatted report for Discord/publishing
  fragility.py update ID "description" ["evidence"]
  fragility.py remove ID
  fragility.py tokens               — list all tracked tokens
  fragility.py scan TOKEN            — auto-research a token's structural risks

Categories: concentration, reflexive_loop, rent_extraction,
            single_point_of_failure, regulatory, operational
Severity: low, medium, high, critical
"""

import os, sys, time, sqlite3, json

DB_PATH = os.environ.get("CHRONICLE_DB", "/mnt/hdd/chronicle-data/processed.db")

VALID_CATEGORIES = {
    "concentration", "reflexive_loop", "rent_extraction",
    "single_point_of_failure", "regulatory", "operational",
}
VALID_SEVERITY = {"low", "medium", "high", "critical"}
SEVERITY_ICON = {"low": "🟢", "medium": "🟡", "high": "🟠", "critical": "🔴"}


def _db():
    db = sqlite3.connect(DB_PATH, timeout=10)
    db.row_factory = sqlite3.Row
    return db


def add(token, category, severity, description, evidence=None, mitigants=None):
    token = token.upper()
    if category not in VALID_CATEGORIES:
        print(f"Invalid category: {category}. Valid: {', '.join(sorted(VALID_CATEGORIES))}")
        return
    if severity not in VALID_SEVERITY:
        print(f"Invalid severity: {severity}. Valid: {', '.join(sorted(VALID_SEVERITY))}")
        return
    db = _db()
    now = int(time.time())
    db.execute(
        "INSERT INTO token_fragility "
        "(token, category, description, severity, evidence, mitigants, last_verified, created_at, updated_at) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (token, category, description, severity, evidence, mitigants,
         time.strftime("%Y-%m-%d"), now, now),
    )
    db.commit()
    db.close()
    print(f"Added {severity} {category} fragility for {token}")


def list_entries(token=None):
    db = _db()
    if token:
        rows = db.execute(
            "SELECT * FROM token_fragility WHERE token=? ORDER BY "
            "CASE severity WHEN 'critical' THEN 0 WHEN 'high' THEN 1 "
            "WHEN 'medium' THEN 2 ELSE 3 END, category",
            (token.upper(),),
        ).fetchall()
    else:
        rows = db.execute(
            "SELECT * FROM token_fragility ORDER BY token, "
            "CASE severity WHEN 'critical' THEN 0 WHEN 'high' THEN 1 "
            "WHEN 'medium' THEN 2 ELSE 3 END, category",
        ).fetchall()
    db.close()

    if not rows:
        print("No fragilities recorded." + (f" Token: {token.upper()}" if token else ""))
        return

    current_token = None
    for r in rows:
        if r["token"] != current_token:
            current_token = r["token"]
            print(f"\n=== {current_token} ===")
        icon = SEVERITY_ICON.get(r["severity"], "⚪")
        print(f"  {icon} [{r['id']}] {r['severity'].upper()} | {r['category']}")
        print(f"     {r['description']}")
        if r["evidence"]:
            print(f"     Evidence: {r['evidence'][:120]}")
        if r["mitigants"]:
            print(f"     Mitigants: {r['mitigants'][:120]}")
        print(f"     Verified: {r['last_verified']}")


def report(token=None):
    """Generate a formatted report suitable for Discord or publishing."""
    db = _db()
    if token:
        tokens = [token.upper()]
    else:
        tokens = [r[0] for r in db.execute(
            "SELECT DISTINCT token FROM token_fragility ORDER BY token"
        ).fetchall()]

    for t in tokens:
        rows = db.execute(
            "SELECT * FROM token_fragility WHERE token=? ORDER BY "
            "CASE severity WHEN 'critical' THEN 0 WHEN 'high' THEN 1 "
            "WHEN 'medium' THEN 2 ELSE 3 END",
            (t,),
        ).fetchall()
        if not rows:
            continue

        crit = sum(1 for r in rows if r["severity"] == "critical")
        high = sum(1 for r in rows if r["severity"] == "high")
        med = sum(1 for r in rows if r["severity"] == "medium")
        low = sum(1 for r in rows if r["severity"] == "low")

        print(f"**{t}** — {len(rows)} fragilities "
              f"({crit} critical, {high} high, {med} medium, {low} low)")
        for r in rows:
            icon = SEVERITY_ICON.get(r["severity"], "⚪")
            print(f"  {icon} **{r['category']}**: {r['description']}")
        print()

    db.close()


def tokens():
    db = _db()
    rows = db.execute(
        "SELECT token, COUNT(*) as cnt, "
        "SUM(CASE WHEN severity='critical' THEN 1 ELSE 0 END) as crit, "
        "SUM(CASE WHEN severity='high' THEN 1 ELSE 0 END) as high "
        "FROM token_fragility GROUP BY token ORDER BY crit DESC, high DESC, cnt DESC"
    ).fetchall()
    db.close()
    if not rows:
        print("No tokens tracked yet.")
        return
    print(f"{'Token':>8}  {'Total':>5}  {'Crit':>4}  {'High':>4}")
    for r in rows:
        print(f"{r['token']:>8}  {r['cnt']:>5}  {r['crit']:>4}  {r['high']:>4}")


def update(entry_id, description=None, evidence=None):
    db = _db()
    now = int(time.time())
    if description:
        db.execute(
            "UPDATE token_fragility SET description=?, updated_at=?, last_verified=? WHERE id=?",
            (description, now, time.strftime("%Y-%m-%d"), entry_id),
        )
    if evidence:
        db.execute(
            "UPDATE token_fragility SET evidence=?, updated_at=?, last_verified=? WHERE id=?",
            (evidence, now, time.strftime("%Y-%m-%d"), entry_id),
        )
    db.commit()
    db.close()
    print(f"Updated entry #{entry_id}")


def remove(entry_id):
    db = _db()
    db.execute("DELETE FROM token_fragility WHERE id=?", (entry_id,))
    db.commit()
    db.close()
    print(f"Removed entry #{entry_id}")


if __name__ == "__main__":
    args = sys.argv[1:]
    if not args:
        print(__doc__)
        sys.exit(0)

    cmd = args[0]
    if cmd == "add" and len(args) >= 5:
        add(args[1], args[2], args[3], args[4],
            args[5] if len(args) > 5 else None,
            args[6] if len(args) > 6 else None)
    elif cmd == "list":
        list_entries(args[1] if len(args) > 1 else None)
    elif cmd == "report":
        report(args[1] if len(args) > 1 else None)
    elif cmd == "tokens":
        tokens()
    elif cmd == "update" and len(args) >= 3:
        update(int(args[1]), args[2] if len(args) > 2 else None,
               args[3] if len(args) > 3 else None)
    elif cmd == "remove" and len(args) >= 2:
        remove(int(args[1]))
    else:
        print(__doc__)
