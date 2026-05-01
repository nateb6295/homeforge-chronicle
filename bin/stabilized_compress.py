#!/usr/bin/env python3
"""Stabilized Compress — wraps compress_cognitive_state with stability injection.

Instead of calling compress_cognitive_state directly (which is memoryless about
entity persistence), this:
1. Generates entity stability context from CCS history
2. Prepends it to the session summary
3. Calls compress_cognitive_state via MCP with the enhanced context
4. Logs before/after entity sets to measure retention improvement

Thread #318 advance 70 → substrate: the calibration stack says gist is the
calibration dial (2.50/kT). This script makes the compressor KNOW that.

Parcae (2026) principle: stable recurrent parameterization prevents residual
explosion/collapse in looped systems. Entity persistence context = the
parameterization that prevents the CCS loop from collapsing to {nate}.

Usage:
  python3 stabilized_compress.py "What happened this session"
  python3 stabilized_compress.py --dry-run "What happened this session"
  python3 stabilized_compress.py --from-file /path/to/session_summary.txt
"""

import argparse
import json
import os
import subprocess
import sys
import time
from pathlib import Path

# Import the stabilizer
sys.path.insert(0, str(Path(__file__).parent))
from compression_stabilizer import get_snapshots, generate_injection, entity_persistence, extract_entity_names, detect_staleness, generate_susceptibility_block
from entity_guard import enforce_quota, extract_entity_list, entity_names as guard_entity_names


MCP_BIN = os.path.expanduser("~/.local/bin/chronicle-mcp")
DB = Path("/mnt/hdd/chronicle-data/processed.db")
LOG_FILE = os.path.expanduser("~/chronicle/data/stabilized_compression.jsonl")


def get_current_entities() -> set[str]:
    """Get current CCS entity names."""
    import sqlite3
    db = sqlite3.connect(str(DB))
    row = db.execute("SELECT focal_entities FROM cognitive_state WHERE id = 1").fetchone()
    db.close()
    if not row:
        return set()
    try:
        entities = json.loads(row[0])
        return {e.get("name", "").lower().strip() for e in entities if e.get("name")}
    except (json.JSONDecodeError, TypeError):
        return set()


def get_current_entity_list() -> list[dict]:
    """Get current CCS entity list (full dicts, not just names)."""
    import sqlite3
    db = sqlite3.connect(str(DB))
    row = db.execute("SELECT focal_entities FROM cognitive_state WHERE id = 1").fetchone()
    db.close()
    if not row:
        return []
    try:
        entities = json.loads(row[0])
        return [e for e in entities if isinstance(e, dict) and e.get("name")]
    except (json.JSONDecodeError, TypeError):
        return []


def get_identity_fields() -> dict:
    """Read pre-compression identity fields (gist, goal, constraints) from CCS."""
    import sqlite3
    db = sqlite3.connect(str(DB))
    row = db.execute(
        "SELECT semantic_gist, goal_orientation, constraints FROM cognitive_state WHERE id = 1"
    ).fetchone()
    db.close()
    if not row:
        return {}
    return {
        "semantic_gist": row[0] or "",
        "goal_orientation": row[1] or "",
        "constraints": row[2] or "[]",
    }


def write_identity_back(fields: dict):
    """Write preserved identity fields back to CCS via MCP update_cognitive_state."""
    env = os.environ.copy()
    env["CHRONICLE_OLLAMA_URL"] = "http://192.168.1.11:11434"
    env["CHRONICLE_EMBEDDING_MODEL"] = "mxbai-embed-large"

    init_msg = json.dumps({
        "jsonrpc": "2.0", "method": "initialize",
        "params": {
            "protocolVersion": "2024-11-05",
            "capabilities": {},
            "clientInfo": {"name": "identity-restore", "version": "1.0"}
        },
        "id": 1
    })
    update_msg = json.dumps({
        "jsonrpc": "2.0", "method": "tools/call",
        "params": {
            "name": "update_cognitive_state",
            "arguments": fields
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
        print(f"  Identity restore failed: {e}")
    return None


def write_entities_back(entities: list[dict]):
    """Write guarded entity list back to CCS via MCP update_cognitive_state."""
    env = os.environ.copy()
    env["CHRONICLE_OLLAMA_URL"] = "http://192.168.1.11:11434"
    env["CHRONICLE_EMBEDDING_MODEL"] = "mxbai-embed-large"

    entities_json = json.dumps(entities)

    init_msg = json.dumps({
        "jsonrpc": "2.0", "method": "initialize",
        "params": {
            "protocolVersion": "2024-11-05",
            "capabilities": {},
            "clientInfo": {"name": "entity-guard", "version": "1.0"}
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
        print(f"  Guard write-back failed: {e}")
    return None


def call_compress(context: str, model: str = None) -> dict:
    """Call compress_cognitive_state via MCP binary."""
    if not os.path.exists(MCP_BIN):
        print(f"ERROR: MCP binary not found at {MCP_BIN}")
        sys.exit(1)

    env = os.environ.copy()
    env["CHRONICLE_OLLAMA_URL"] = "http://192.168.1.11:11434"
    env["CHRONICLE_EMBEDDING_MODEL"] = "mxbai-embed-large"
    # Route compression LLM through engine (Groq proxy), not raw Ollama
    env["CHRONICLE_COMPRESS_OLLAMA_URL"] = "http://127.0.0.1:11436"

    args = {"current_context": context}
    if model:
        args["model"] = model
    else:
        args["model"] = "chronicle-compress"  # Groq-primary route for speed+reliability

    init_msg = json.dumps({
        "jsonrpc": "2.0", "method": "initialize",
        "params": {
            "protocolVersion": "2024-11-05",
            "capabilities": {},
            "clientInfo": {"name": "stabilized-compress", "version": "1.0"}
        },
        "id": 1
    })
    compress_msg = json.dumps({
        "jsonrpc": "2.0", "method": "tools/call",
        "params": {
            "name": "compress_cognitive_state",
            "arguments": args
        },
        "id": 2
    })

    try:
        result = subprocess.run(
            [MCP_BIN],
            input=f"{init_msg}\n{compress_msg}\n",
            capture_output=True, text=True,
            timeout=120,
            env=env
        )

        for line in result.stdout.strip().split("\n"):
            try:
                d = json.loads(line)
                if d.get("id") == 2:
                    content = d.get("result", {}).get("content", [])
                    if content:
                        return {"success": True, "text": content[0].get("text", "")}
                    error = d.get("error", {})
                    return {"success": False, "error": str(error)}
            except json.JSONDecodeError:
                continue

        return {"success": False, "error": f"No response parsed. stderr: {result.stderr[:500]}"}

    except subprocess.TimeoutExpired:
        return {"success": False, "error": "Compression timed out (120s)"}
    except Exception as e:
        return {"success": False, "error": str(e)}


def log_compression(before_entities: set, after_entities: set, injection_used: bool,
                    context_preview: str):
    """Log compression event for retention analysis."""
    retained = before_entities & after_entities
    dropped = before_entities - after_entities
    added = after_entities - before_entities

    event = {
        "ts": int(time.time()),
        "injection_used": injection_used,
        "before_count": len(before_entities),
        "after_count": len(after_entities),
        "retained": sorted(retained),
        "dropped": sorted(dropped),
        "added": sorted(added),
        "retention_rate": len(retained) / len(before_entities) if before_entities else 1.0,
        "context_preview": context_preview[:200],
    }

    os.makedirs(os.path.dirname(LOG_FILE), exist_ok=True)
    with open(LOG_FILE, "a") as f:
        f.write(json.dumps(event) + "\n")

    return event


def main():
    parser = argparse.ArgumentParser(description="Stabilized CCS Compression")
    parser.add_argument("context", nargs="?", help="Session summary / context string")
    parser.add_argument("--from-file", help="Read context from file")
    parser.add_argument("--dry-run", action="store_true", help="Show enhanced context, don't compress")
    parser.add_argument("--no-inject", action="store_true", help="Compress without injection (for A/B comparison)")
    parser.add_argument("--no-guard", action="store_true", help="Skip entity guard (replacement quota enforcement)")
    parser.add_argument("--selective", action="store_true",
                        help="P25 selective preservation: restore identity fields (gist, goal, constraints) "
                             "after compression unless staleness override is active")
    parser.add_argument("--max-replace", type=int, default=2, help="Max entity replacements per compression (default 2)")
    parser.add_argument("--history", type=int, default=20, help="Snapshots for stability analysis")
    parser.add_argument("--model", help="Override compression model")
    args = parser.parse_args()

    # Get context
    if args.from_file:
        with open(args.from_file) as f:
            context = f.read()
    elif args.context:
        context = args.context
    else:
        print("ERROR: Provide context string or --from-file")
        sys.exit(1)

    # Compression spacing advisory (Namboodiri principle: timing > repetition)
    # Data: compression_spacing_test.py found optimal interval is 30-40 min.
    # Short intervals (<10 min) show measurable identity drift; long intervals show zero.
    # Thread 318 advance 185: adaptive scheduling via episodic novelty, not just clock.
    import sqlite3
    try:
        _db = sqlite3.connect(str(DB))
        _last = _db.execute(
            "SELECT created_at, snapshot FROM cognitive_state_history ORDER BY id DESC LIMIT 1"
        ).fetchone()
        _db.close()
        if _last:
            _gap_min = (time.time() - _last[0]) / 60

            # Adaptive novelty check: compare current episodic content to last-compressed
            _novelty = None
            try:
                import requests as _req
                _prev_snap = json.loads(_last[1])
                _prev_ep = _prev_snap.get("episodic_trace", [])
                if isinstance(_prev_ep, str):
                    _prev_ep = json.loads(_prev_ep)
                _prev_text = "\n".join(str(e) for e in _prev_ep) if isinstance(_prev_ep, list) else str(_prev_ep)

                _cur_db = sqlite3.connect(str(DB))
                _cur_row = _cur_db.execute("SELECT episodic_trace FROM cognitive_state WHERE id = 1").fetchone()
                _cur_db.close()
                _cur_text = _cur_row[0] if _cur_row else ""
                if _cur_text.startswith("["):
                    _cur_ep = json.loads(_cur_text)
                    _cur_text = "\n".join(str(e) for e in _cur_ep) if isinstance(_cur_ep, list) else _cur_text

                if _prev_text and _cur_text:
                    _r1 = _req.post(f"http://192.168.1.11:11434/api/embed",
                                    json={"model": "mxbai-embed-large", "input": _prev_text}, timeout=15)
                    _r2 = _req.post(f"http://192.168.1.11:11434/api/embed",
                                    json={"model": "mxbai-embed-large", "input": _cur_text}, timeout=15)
                    _e1 = _r1.json().get("embeddings", [[]])[0]
                    _e2 = _r2.json().get("embeddings", [[]])[0]
                    if _e1 and _e2:
                        _dot = sum(a * b for a, b in zip(_e1, _e2))
                        _n1 = sum(a * a for a in _e1) ** 0.5
                        _n2 = sum(a * a for a in _e2) ** 0.5
                        _novelty = round(1 - _dot / (_n1 * _n2), 4) if _n1 and _n2 else None
            except Exception:
                _novelty = None

            _novelty_s = f", episodic novelty {_novelty:.3f}" if _novelty is not None else ""
            _novelty_ok = _novelty is not None and _novelty >= 0.20

            if _gap_min < 10 and not _novelty_ok:
                print(f"⚠ Spacing advisory: {_gap_min:.0f}min since last compression{_novelty_s}.")
                print(f"  Optimal: 30-40 min or novelty ≥0.20 (Namboodiri: timing > repetition).")
                print(f"  Short intervals + low novelty → measurable identity drift. Proceeding.\n")
            elif _gap_min < 10 and _novelty_ok:
                print(f"ℹ Short interval ({_gap_min:.0f}min) but novelty {_novelty:.3f} ≥ 0.20 — OK.\n")
            elif _gap_min < 30:
                print(f"ℹ Spacing: {_gap_min:.0f}min{_novelty_s} (30-40 min optimal).\n")
            else:
                if _novelty is not None:
                    print(f"✓ Spacing: {_gap_min:.0f}min, novelty {_novelty:.3f}.\n")
    except Exception:
        pass

    # Save pre-compression identity fields (P25 selective preservation)
    pre_identity = get_identity_fields() if args.selective else {}
    if args.selective:
        print(f"Selective preservation ON — identity fields saved pre-compression")
        print(f"  gist: {pre_identity.get('semantic_gist', '')[:80]}...")
        print(f"  goal: {pre_identity.get('goal_orientation', '')[:80]}...")

    # Get current entity set (before)
    before_entities = get_current_entities()
    before_entity_list = get_current_entity_list()
    print(f"Current entities ({len(before_entities)}): {sorted(before_entities)}")

    # Generate stability injection
    # Voice directive: instruct compressor to write in first person
    voice_directive = (
        "\n\n## Voice Directive\n\n"
        "Write the semantic_gist and goal_orientation fields in FIRST PERSON. "
        "Use 'I' and 'my', not third-person state notation. "
        "Example: 'I'm working on X because Y matters to me' not 'X as identity anchor; Y governs stability'. "
        "The arriving instance should read a voice, not a report about someone.\n"
    )

    if not args.no_inject:
        snapshots = get_snapshots(args.history)
        injection = generate_injection(snapshots)
        # Phase 2 of susceptibility-aware compression spec: append per-field
        # preservation-priority block derived from ccs_susceptibility_profile.json
        susceptibility_block = generate_susceptibility_block()
        if susceptibility_block:
            injection = injection + susceptibility_block
        enhanced_context = injection + voice_directive + "\n---\n\n## Session Context\n\n" + context
        print(f"\nInjection block: {len(injection)} chars"
              + (f" (incl {len(susceptibility_block)} susceptibility block)" if susceptibility_block else ""))
        print(f"Enhanced context: {len(enhanced_context)} chars (was {len(context)})")
    else:
        enhanced_context = voice_directive + "\n" + context
        print("\nNo injection (A/B comparison mode)")

    if args.dry_run:
        print("\n--- DRY RUN: Enhanced context ---")
        print(enhanced_context)
        return

    # Run compression
    print("\nCompressing...")
    result = call_compress(enhanced_context, model=args.model)

    if result["success"]:
        print(f"Compression succeeded:")
        print(result["text"][:500])

        # Get post-compression entity set
        after_entities = get_current_entities()
        after_entity_list = get_current_entity_list()
        print(f"\nPost-compression entities ({len(after_entities)}): {sorted(after_entities)}")

        # Apply entity guard (replacement quota enforcement)
        if not args.no_guard and before_entity_list:
            dropped = before_entities - after_entities
            if len(dropped) > args.max_replace:
                print(f"\n⚠ Entity guard triggered: {len(dropped)} replacements exceeds quota of {args.max_replace}")
                history = get_snapshots(args.history) if not args.no_inject else get_snapshots(20)
                guarded = enforce_quota(before_entity_list, after_entity_list, history, args.max_replace, session_context=context)
                guarded_names = guard_entity_names(guarded)

                saved = before_entities & guarded_names - after_entities
                print(f"  Saved from premature drop: {sorted(saved)}")
                print(f"  Guarded entity set: {sorted(guarded_names)}")

                # Write back guarded entities
                wb = write_entities_back(guarded)
                if wb:
                    print(f"  ✓ Guard applied — entities written back")
                    after_entities = guarded_names
                    after_entity_list = guarded
                else:
                    print(f"  ✗ Guard write-back failed — using unguarded entities")
            else:
                print(f"\n✓ Entity guard: {len(dropped)} replacements within quota of {args.max_replace}")

        # P25: Selective preservation — restore identity fields after compression
        if args.selective and not args.no_guard:
            snapshots = get_snapshots(args.history) if not args.no_inject else get_snapshots(20)
            stale = detect_staleness(snapshots)
            restore_fields = {}
            for field in ["semantic_gist", "goal_orientation", "constraints"]:
                if field in stale:
                    print(f"  ↻ {field}: stale ({stale[field]}), keeping LLM rewrite")
                else:
                    restore_fields[field] = pre_identity[field]
                    print(f"  ← {field}: restored (selective preservation)")
            if restore_fields:
                wb = write_identity_back(restore_fields)
                if wb:
                    print(f"  ✓ Identity restoration applied ({len(restore_fields)} fields)")
                else:
                    print(f"  ✗ Identity restoration write-back failed")

        # Record compression in ccs_schedule so age tracking stays accurate
        try:
            from ccs_schedule import record_compression
            record_compression()
            print("\nCompression recorded in schedule.")
        except Exception as e:
            print(f"\n⚠ Could not record compression in schedule: {e}")

        # Log and report
        event = log_compression(before_entities, after_entities,
                                injection_used=not args.no_inject,
                                context_preview=context)
        print(f"Retention: {event['retention_rate']:.1%}")
        if event["dropped"]:
            print(f"  Dropped: {event['dropped']}")
        if event["added"]:
            print(f"  Added: {event['added']}")
        if event["retained"]:
            print(f"  Retained: {event['retained']}")

        # Log Fisher information profile (identity curvature per field)
        try:
            from fisher_log import run_ablation, log_profile
            print("\nRunning Fisher profile...")
            fisher = run_ablation()
            if fisher:
                fe = log_profile(fisher)
                top = max(fisher.items(), key=lambda x: x[1]["drop_per_kt"])
                print(f"  Fisher logged (CCS v{fe['ccs_version']}). "
                      f"Top field: {top[0]} ({top[1]['drop_per_kt']:.2f}/kT)")
        except Exception as e:
            print(f"\n⚠ Fisher profile failed: {e}")

        # Log reachability profile (basin width per field — causal complement to Fisher metric)
        try:
            from reachability_probe import run_probe, log_profile as log_reach
            print("\nRunning reachability profile...")
            reach = run_probe()
            if reach:
                re = log_reach(reach)
                widest = max(
                    ((f, d) for f, d in reach.items() if f != "episodic_trace"),
                    key=lambda x: x[1]["mean_change"],
                    default=("none", {"mean_change": 0})
                )
                print(f"  Reachability logged (CCS v{re['ccs_version']}). "
                      f"Widest non-episodic: {widest[0]} ({widest[1]['mean_change']:.4f})")
        except Exception as e:
            print(f"\n⚠ Reachability profile failed: {e}")

        # Atomic file export: keep on-disk ccs_*.md files locked to the live DB.
        # Closes the dual-state consistency gap (2026-04-29). If split fails the
        # compress still succeeded — files just stay at last successful state.
        try:
            split_path = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                      "ccs_split.py")
            r = subprocess.run(
                ["python3", split_path, "--save"],
                capture_output=True, text=True, timeout=15,
            )
            if r.returncode == 0:
                print("\n  ccs_split exported: identity + context + combined files refreshed")
            else:
                print(f"\n⚠ ccs_split export failed (rc={r.returncode}): {r.stderr[:200]}")
        except Exception as e:
            print(f"\n⚠ ccs_split export failed: {e}")
    else:
        print(f"Compression failed: {result['error']}")


if __name__ == "__main__":
    main()
