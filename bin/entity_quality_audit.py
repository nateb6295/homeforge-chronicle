#!/usr/bin/env python3
"""Entity quality audit — Nocturnal Council for the CCS.

Evaluates each focal entity on contribution vs mere persistence.
Classifies entities as: contributing, stale, or frozen.

Contributing: appears in recent thread edges OR capsule graph growth
Stale: high salience but no recent contribution
Frozen: orphaned in CCS AND no edges — candidate for decay

Remediation (--remediate): graduated salience decay for non-contributing entities.
Frozen entities decay 0.10/cycle, stale decay 0.05/cycle. Entities below 0.50
become eligible for natural replacement by the entity guard. Plato's Laws:
the sound-mind center gives citizens graduated correction before exile.

Usage:
  python3 entity_quality_audit.py              # diagnostic only
  python3 entity_quality_audit.py --json       # JSON output
  python3 entity_quality_audit.py --remediate  # apply salience decay + write back
"""

import sqlite3, json, sys, os, subprocess
from pathlib import Path
from datetime import datetime

DB = Path("/mnt/hdd/chronicle-data/processed.db")
MCP_BIN = "/home/nate-agx/.local/bin/chronicle-mcp"
REMEDIATION_LOG = Path("/home/nate-agx/chronicle/data/entity_remediation.jsonl")

FROZEN_DECAY = 0.10
STALE_DECAY = 0.05
MIN_SALIENCE = 0.40

def get_current_entities(db):
    row = db.execute(
        "SELECT snapshot FROM cognitive_state_history ORDER BY id DESC LIMIT 1"
    ).fetchone()
    if not row:
        return []
    ccs = json.loads(row[0])
    return ccs.get("focal_entities", [])

def entity_thread_edges(db, name, days=7):
    thread_num = ""
    if name.startswith("Thread #"):
        thread_num = name.replace("Thread #", "")
    desc_total = db.execute(
        "SELECT count(*) FROM thread_edges WHERE description LIKE ? AND deprecated=0",
        (f"%{name}%",)
    ).fetchone()[0]
    id_total = 0
    if thread_num:
        id_total = db.execute(
            "SELECT count(*) FROM thread_edges WHERE (source_id=? OR target_id=?) AND deprecated=0",
            (thread_num, thread_num)
        ).fetchone()[0]
    total = max(desc_total, id_total)
    desc_recent = db.execute(
        "SELECT count(*) FROM thread_edges WHERE description LIKE ? AND deprecated=0 "
        f"AND created_at > strftime('%s', 'now', '-{days} days')",
        (f"%{name}%",)
    ).fetchone()[0]
    id_recent = 0
    if thread_num:
        id_recent = db.execute(
            "SELECT count(*) FROM thread_edges WHERE (source_id=? OR target_id=?) AND deprecated=0 "
            f"AND created_at > strftime('%s', 'now', '-{days} days')",
            (thread_num, thread_num)
        ).fetchone()[0]
    recent = max(desc_recent, id_recent)
    return total, recent

def entity_capsule_mentions(db, name, days=7):
    total = db.execute(
        "SELECT count(*) FROM knowledge_capsules WHERE restatement LIKE ?",
        (f"%{name}%",)
    ).fetchone()[0]
    recent = db.execute(
        "SELECT count(*) FROM knowledge_capsules WHERE restatement LIKE ? "
        f"AND created_at > strftime('%s', 'now', '-{days} days')",
        (f"%{name}%",)
    ).fetchone()[0]
    return total, recent

def classify(entity, thread_total, thread_recent, capsule_recent):
    sal = entity.get("salience", 0)
    if thread_recent > 0 or capsule_recent > 5:
        return "contributing"
    elif sal >= 0.80 and thread_total == 0:
        return "frozen"
    elif sal >= 0.70 and thread_recent == 0:
        return "stale"
    else:
        return "low-salience"

def get_live_entities(db):
    """Get entities from the live CCS (not history)."""
    row = db.execute("SELECT focal_entities FROM cognitive_state WHERE id = 1").fetchone()
    if not row:
        return []
    raw = row[0]
    if isinstance(raw, str):
        try:
            return json.loads(raw)
        except json.JSONDecodeError:
            return []
    return raw if isinstance(raw, list) else []


def write_entities_back(entities):
    """Write remediated entity list back to CCS via MCP."""
    env = os.environ.copy()
    env["CHRONICLE_OLLAMA_URL"] = "http://192.168.1.11:11434"
    env["CHRONICLE_EMBEDDING_MODEL"] = "mxbai-embed-large"

    entities_json = json.dumps(entities)

    init_msg = json.dumps({
        "jsonrpc": "2.0", "method": "initialize",
        "params": {
            "protocolVersion": "2024-11-05",
            "capabilities": {},
            "clientInfo": {"name": "entity-remediation", "version": "1.0"}
        },
        "id": 1
    })
    update_msg = json.dumps({
        "jsonrpc": "2.0", "method": "tools/call",
        "params": {
            "name": "update_cognitive_state",
            "arguments": {"focal_entities": entities_json}
        },
        "id": 2
    })

    try:
        result = subprocess.run(
            [MCP_BIN],
            input=f"{init_msg}\n{update_msg}\n",
            capture_output=True, text=True,
            timeout=30,
            env=env
        )
        for line in result.stdout.strip().split("\n"):
            try:
                d = json.loads(line)
                if d.get("id") == 2:
                    return d.get("result", {})
            except json.JSONDecodeError:
                continue
    except Exception as e:
        print(f"  Write-back failed: {e}")
    return None


def log_remediation(actions):
    """Append remediation actions to the log."""
    entry = {
        "timestamp": datetime.utcnow().isoformat(),
        "actions": actions,
    }
    with open(REMEDIATION_LOG, "a") as f:
        f.write(json.dumps(entry) + "\n")


def remediate(entities, audit_results):
    """Apply graduated salience decay to non-contributing entities.

    Returns (modified_entities, actions_taken).
    """
    status_by_name = {r["name"]: r["status"] for r in audit_results}
    actions = []
    modified = []

    for e in entities:
        name = e["name"]
        status = status_by_name.get(name, "unknown")
        old_sal = e.get("salience", 0.5)

        if status == "frozen":
            new_sal = max(MIN_SALIENCE, old_sal - FROZEN_DECAY)
            if new_sal != old_sal:
                actions.append({
                    "entity": name,
                    "action": "decay",
                    "reason": "frozen",
                    "old_salience": round(old_sal, 3),
                    "new_salience": round(new_sal, 3),
                })
            e_copy = dict(e)
            e_copy["salience"] = round(new_sal, 3)
            modified.append(e_copy)
        elif status == "stale":
            new_sal = max(MIN_SALIENCE, old_sal - STALE_DECAY)
            if new_sal != old_sal:
                actions.append({
                    "entity": name,
                    "action": "decay",
                    "reason": "stale",
                    "old_salience": round(old_sal, 3),
                    "new_salience": round(new_sal, 3),
                })
            e_copy = dict(e)
            e_copy["salience"] = round(new_sal, 3)
            modified.append(e_copy)
        else:
            modified.append(e)

    return modified, actions


def run_audit(db, entities):
    """Run the audit and return results list."""
    results = []
    for e in entities:
        name = e["name"]
        t_total, t_recent = entity_thread_edges(db, name)
        c_total, c_recent = entity_capsule_mentions(db, name)
        status = classify(e, t_total, t_recent, c_recent)
        results.append({
            "name": name,
            "salience": e.get("salience", 0),
            "type": e.get("type", "?"),
            "thread_edges_total": t_total,
            "thread_edges_7d": t_recent,
            "capsule_mentions_total": c_total,
            "capsule_mentions_7d": c_recent,
            "status": status,
        })
    return results


def print_audit(results, entities):
    """Print the audit report."""
    contributing = [r for r in results if r["status"] == "contributing"]
    stale = [r for r in results if r["status"] == "stale"]
    frozen = [r for r in results if r["status"] == "frozen"]
    low = [r for r in results if r["status"] == "low-salience"]

    print(f"Entity Quality Audit — {len(entities)} entities")
    print(f"  Contributing: {len(contributing)}  Stale: {len(stale)}  Frozen: {len(frozen)}  Low-salience: {len(low)}")
    print()

    if contributing:
        print("CONTRIBUTING (active in edges or recent capsules):")
        for r in sorted(contributing, key=lambda x: -x["salience"]):
            print(f"  {r['name']:35s} sal={r['salience']:.2f}  edges={r['thread_edges_7d']}  caps_7d={r['capsule_mentions_7d']}")

    if stale:
        print("\nSTALE (high salience, no recent contribution):")
        for r in sorted(stale, key=lambda x: -x["salience"]):
            print(f"  {r['name']:35s} sal={r['salience']:.2f}  edges={r['thread_edges_total']}total  caps_7d={r['capsule_mentions_7d']}")

    if frozen:
        print("\nFROZEN (high salience, zero edges ever — candidates for decay):")
        for r in sorted(frozen, key=lambda x: -x["salience"]):
            print(f"  {r['name']:35s} sal={r['salience']:.2f}  caps_7d={r['capsule_mentions_7d']}")

    if low:
        print(f"\nLow-salience ({len(low)}): {', '.join(r['name'] for r in low)}")

    health = len(contributing) / len(entities) if entities else 0
    print(f"\nEntity health: {health:.0%} contributing ({len(contributing)}/{len(entities)})")
    if health < 0.5:
        print("⚠ Below 50% — entity guard is protecting more frozen than contributing entities")


def main():
    do_json = "--json" in sys.argv
    do_remediate = "--remediate" in sys.argv
    db = sqlite3.connect(str(DB))

    if do_remediate:
        entities = get_live_entities(db)
    else:
        entities = get_current_entities(db)

    results = run_audit(db, entities)
    db.close()

    if do_json and not do_remediate:
        print(json.dumps(results, indent=2))
        return

    print_audit(results, entities)

    if do_remediate:
        modified, actions = remediate(entities, results)
        if not actions:
            print("\n✓ No remediation needed — all non-contributing entities already at minimum salience")
            return

        print(f"\n{'='*60}")
        print(f"REMEDIATION — {len(actions)} adjustments:")
        for a in actions:
            print(f"  {a['entity']:35s} {a['reason']:8s} {a['old_salience']:.3f} → {a['new_salience']:.3f}")

        result = write_entities_back(modified)
        if result:
            print(f"\n✓ Remediated entities written back to CCS")
            log_remediation(actions)
            print(f"  Logged to {REMEDIATION_LOG}")
        else:
            print(f"\n✗ Write-back failed — entities NOT modified")


if __name__ == "__main__":
    main()
