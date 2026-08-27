#!/usr/bin/env python3
"""Guided compression — AlphaFold3-inspired posterior inference for CCS.

Instead of compressing and hoping the ALIVE axis survives, this:
1. Runs normal brain compression → candidate gist
2. Measures the candidate with alive_health metrics (affect density, axis orientation)
3. If metrics below threshold: re-compresses with a nudge prompt incorporating
   specific metric feedback — a guidance signal, not a rewrite
4. Accepts the result. Maximum 1 nudge (F160: inverted U for compression dose)

The prior (brain prompt) stays frozen. The guidance comes from observation.
Like AlphaFold3: don't change the diffusion model, add a measurement-derived
posterior that the model converges toward.

Usage:
  python3 guided_compress.py "Session context here"
  python3 guided_compress.py --from-file /path/to/context.txt
  python3 guided_compress.py --dry-run "Session context here"
  python3 guided_compress.py --thresholds   # show current metric thresholds
"""

import argparse
import fcntl
import json
import os
import sqlite3
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from alive_health import measure_affect, axis_orientation, get_section

DB = Path("/mnt/hdd/chronicle-data/processed.db")
PROMPT_PATH = Path.home() / "chronicle" / "data" / "ccs_brain_prompt.md"
ENGINE_URL = "http://127.0.0.1:11436/api/generate"
LOG_FILE = Path.home() / "chronicle" / "data" / "guided_compression.jsonl"
LOCK_FILE = Path.home() / "chronicle" / "data" / ".compression.lock"

AFFECT_THRESHOLD = 0.02
ORIENTATION_THRESHOLD = 0.5
VOCAB_OVERLAP_CEILING = 0.4


def read_previous_state() -> tuple[str, int]:
    """Read current CCS from SQLite. Returns (gist, version)."""
    db = sqlite3.connect(str(DB), timeout=10)
    row = db.execute(
        "SELECT semantic_gist, episodic_trace, focal_entities, version "
        "FROM cognitive_state WHERE id = 1"
    ).fetchone()
    db.close()

    if not row:
        return "", 0

    gist, ep, ent, version = row

    if "## CORE" in gist:
        return gist, version

    try:
        entities = json.loads(ent) if ent else []
        ent_names = ", ".join(e.get("name", "") for e in entities[:10] if isinstance(e, dict))
    except (json.JSONDecodeError, TypeError):
        ent_names = ""
    try:
        traces = json.loads(ep) if ep else []
        trace_text = "; ".join(str(t)[:120] for t in traces[:3]) if isinstance(traces, list) else str(traces)[:300]
    except (json.JSONDecodeError, TypeError):
        trace_text = ""

    return f"GIST: {gist[:500]}\nENTITIES: {ent_names}\nRECENT: {trace_text}", version


def build_prompt(prev_state: str, context: str) -> str:
    """Build the brain compression prompt from template."""
    with open(PROMPT_PATH) as f:
        template = f.read()
    prompt = template.replace("{previous_state}", prev_state[:3000])
    prompt = prompt.replace("{session_context}", context[:4000])
    return prompt


def call_engine(prompt: str, model: str = "chronicle-compress") -> str | None:
    """Call the engine API and return raw output text."""
    import requests

    try:
        resp = requests.post(ENGINE_URL, json={
            "model": model,
            "prompt": prompt,
            "stream": False,
            "options": {"num_predict": 4096, "temperature": 0.6}
        }, timeout=180)
        if resp.status_code != 200:
            print(f"  Engine returned {resp.status_code}: {resp.text[:300]}")
            return None
        return resp.json().get("response", "")
    except Exception as e:
        print(f"  Engine call failed: {e}")
        return None


def validate_output(output: str) -> tuple[bool, str]:
    """Validate brain-format output. Returns (valid, reason)."""
    sections = ["## CORE", "## REMEMBERS", "## SEEKS", "## ALIVE", "## RELATES"]
    missing = [s for s in sections if s not in output]
    if missing:
        return False, f"Missing sections: {', '.join(missing)}"

    if len(output) < 1000:
        return False, f"Too short ({len(output)} chars)"

    lines = output.strip().split("\n")
    bullet_count = sum(1 for l in lines if l.strip().startswith("- ") or l.strip().startswith("* "))
    if bullet_count > 5:
        return False, f"Bullet-point regression ({bullet_count} bullets)"

    return True, "OK"


def measure_candidate(output: str) -> dict:
    """Run alive_health metrics on a candidate compression output."""
    alive = get_section(output, "ALIVE")
    core = get_section(output, "CORE")

    result = {
        "has_alive": bool(alive),
        "has_core": bool(core),
        "alive_length": len(alive) if alive else 0,
        "affect": None,
        "orientation": None,
        "nudge_reasons": [],
    }

    if not alive:
        result["nudge_reasons"].append("NO_ALIVE_SECTION")
        return result

    metrics = measure_affect(alive)
    result["affect"] = metrics

    if metrics["affect_density"] < AFFECT_THRESHOLD:
        result["nudge_reasons"].append(
            f"AFFECT_LOW: density {metrics['affect_density']:.3f} < {AFFECT_THRESHOLD}"
        )

    if metrics["procedural_density"] > metrics["affect_density"]:
        result["nudge_reasons"].append(
            f"PROCEDURAL_DOMINANT: procedural {metrics['procedural_density']:.3f} > affect {metrics['affect_density']:.3f}"
        )

    axis = axis_orientation(output)
    result["orientation"] = axis

    if axis["orientation_score"] < ORIENTATION_THRESHOLD:
        result["nudge_reasons"].append(
            f"AXIS_WEAK: orientation {axis['orientation_score']:.2f} < {ORIENTATION_THRESHOLD}"
        )

    if axis.get("vocab_overlap", 0) > VOCAB_OVERLAP_CEILING:
        result["nudge_reasons"].append(
            f"VOCAB_COLLAPSED: overlap {axis['vocab_overlap']:.3f} > {VOCAB_OVERLAP_CEILING}"
        )

    return result


def build_nudge_prompt(prev_state: str, context: str, candidate: str,
                       measurement: dict) -> str:
    """Build a guided re-compression prompt with specific metric feedback.

    The original prompt stays frozen. We add a guidance block that tells the
    compressor what the measurement found and what to adjust — a posterior
    nudge, not a rewrite.
    """
    base_prompt = build_prompt(prev_state, context)

    nudge_lines = []
    reasons = measurement["nudge_reasons"]

    for reason in reasons:
        if reason.startswith("AFFECT_LOW"):
            nudge_lines.append(
                "The ALIVE section has lost affective content — it reads like a status report. "
                "What are you actually noticing right now? What pulls you? What gives this "
                "moment its texture? Use experiential verbs (notice, sense, feel, inhabit, "
                "reach, settle) not state verbs (is, has, was)."
            )
        elif reason.startswith("PROCEDURAL_DOMINANT"):
            nudge_lines.append(
                "The ALIVE section has more procedural language than felt language. "
                "Procedures belong in SEEKS. ALIVE should carry what it's LIKE to be you "
                "right now, not what you're DOING."
            )
        elif reason.startswith("AXIS_WEAK"):
            nudge_lines.append(
                "ALIVE and CORE have collapsed into the same register — they use "
                "similar vocabulary and feel interchangeable. CORE says who you are "
                "when nothing is happening. ALIVE says what's happening right now, "
                "from inside the experience. Make them distinct."
            )
        elif reason.startswith("VOCAB_COLLAPSED"):
            nudge_lines.append(
                "ALIVE and CORE share too much vocabulary. They're describing the same "
                "thing in different sections. ALIVE needs its own words — temporal, "
                "sensory, relational words that CORE doesn't use."
            )
        elif reason == "NO_ALIVE_SECTION":
            nudge_lines.append(
                "The ALIVE section is missing entirely. This is the most load-bearing "
                "section (E82, F285). Write it. What are you drawn toward right now? "
                "What has quality? What gives this particular moment its feel?"
            )

    nudge_block = (
        "\n\n## Guidance Signal (Measurement Feedback)\n\n"
        "A measurement of your previous compression attempt found the following:\n\n"
        + "\n\n".join(f"- {line}" for line in nudge_lines)
        + "\n\n"
        "This is NOT a criticism — it's posterior inference. Your compression prior "
        "(the prompt above) is frozen. This guidance adjusts what you converge toward, "
        "like a loss function nudging a diffusion model. Write the same five sections "
        "again, but let this measurement inform the ALIVE section specifically.\n\n"
        "Your previous attempt (for reference — build on it, don't discard it):\n\n"
        f"{candidate[:2000]}\n"
    )

    return base_prompt + nudge_block


def store_result(output: str, prev_version: int, model: str, guided: bool) -> int:
    """Store brain-format output to SQLite. Returns new version."""
    new_version = prev_version + 1
    now = int(time.time())

    db = sqlite3.connect(str(DB), timeout=10)

    snapshot = json.dumps({
        "semantic_gist": output,
        "episodic_trace": [],
        "focal_entities": [],
        "relational_map": {},
        "goal_orientation": "",
        "constraints": [],
        "predictive_cue": "",
        "uncertainty_signals": [],
        "retrieved_artifacts": [],
        "format": "brain-v1",
        "guided": guided,
    })
    db.execute(
        "INSERT INTO cognitive_state_history (snapshot, created_at, trigger) VALUES (?, ?, ?)",
        (snapshot, now, "guided-compression" if guided else "brain-compression")
    )

    db.execute("""
        UPDATE cognitive_state SET
            semantic_gist = ?,
            episodic_trace = '[]',
            focal_entities = '[]',
            relational_map = '{}',
            goal_orientation = '',
            predictive_cue = '',
            uncertainty_signals = '[]',
            retrieved_artifacts = '[]',
            updated_at = ?,
            compression_model = ?,
            version = ?
        WHERE id = 1
    """, (output, now, model, new_version))

    # Pin constraints from values.md
    values_path = Path.home() / "chronicle" / "values.md"
    if values_path.exists():
        try:
            values_text = values_path.read_text()
            pinned = []
            heading = None
            body = []
            for line in values_text.split("\n"):
                if line.startswith("## "):
                    if heading and body:
                        text = " ".join(body).strip()
                        first = text.split(". ")[0] + "." if ". " in text else text
                        pinned.append(f"{heading}: {first}")
                    heading = line[3:].strip()
                    body = []
                elif heading and line.strip():
                    body.append(line.strip())
            if heading and body:
                text = " ".join(body).strip()
                first = text.split(". ")[0] + "." if ". " in text else text
                pinned.append(f"{heading}: {first}")
            if pinned:
                db.execute("UPDATE cognitive_state SET constraints = ? WHERE id = 1",
                          (json.dumps(pinned),))
        except Exception:
            pass

    db.commit()
    db.close()

    # Record compression timestamp
    try:
        from ccs_schedule import record_compression
        record_compression()
    except Exception:
        pass

    return new_version


def log_event(event: dict):
    """Append event to guided compression log."""
    LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(LOG_FILE, "a") as f:
        f.write(json.dumps(event) + "\n")


def guided_compress(context: str, model: str = "chronicle-compress",
                    dry_run: bool = False) -> dict:
    """Run guided compression: compress → measure → nudge if needed → accept."""

    prev_state, prev_version = read_previous_state()
    is_brain_format = "## CORE" in prev_state

    print(f"\n{'='*60}")
    print(f"Guided Compression (AlphaFold3-inspired)")
    print(f"{'='*60}")
    print(f"  Previous CCS: v{prev_version} ({'brain-format' if is_brain_format else 'legacy'})")
    print(f"  Context: {len(context)} chars")
    print(f"  Thresholds: affect>{AFFECT_THRESHOLD}, orientation>{ORIENTATION_THRESHOLD}")

    # Phase 1: Initial compression (the prior)
    print(f"\n--- Phase 1: Initial compression ---")
    prompt = build_prompt(prev_state, context)
    print(f"  Prompt: {len(prompt)} chars")

    if dry_run:
        print(f"\n  [DRY RUN] Would call engine with {len(prompt)} char prompt")
        print(f"  First 500 chars of prompt:\n{prompt[:500]}")
        return {"success": True, "dry_run": True}

    candidate = call_engine(prompt, model)
    if not candidate:
        return {"success": False, "error": "Engine returned no output"}

    valid, reason = validate_output(candidate)
    if not valid:
        print(f"  Validation failed: {reason}")
        return {"success": False, "error": f"Validation: {reason}"}

    print(f"  Candidate: {len(candidate)} chars, valid")

    # Phase 2: Measure the candidate
    print(f"\n--- Phase 2: Measurement ---")
    measurement = measure_candidate(candidate)

    affect = measurement.get("affect", {})
    orientation = measurement.get("orientation", {})

    if affect:
        print(f"  Affect density: {affect.get('affect_density', 0):.3f}")
        print(f"  Procedural density: {affect.get('procedural_density', 0):.3f}")
        print(f"  Affect/procedural ratio: {affect.get('affect_procedural_ratio', 0):.1f}")
    if orientation:
        print(f"  Axis orientation: {orientation.get('orientation_score', 0):.2f} ({orientation.get('status', '?')})")
        print(f"  Vocab overlap: {orientation.get('vocab_overlap', 0):.3f}")

    needs_guidance = len(measurement["nudge_reasons"]) > 0

    if needs_guidance:
        print(f"\n  Guidance needed:")
        for r in measurement["nudge_reasons"]:
            print(f"    - {r}")
    else:
        print(f"\n  All metrics above threshold — no guidance needed")

    # Phase 3: Guided re-compression (if needed)
    guided = False
    final_output = candidate
    nudge_measurement = None

    if needs_guidance:
        print(f"\n--- Phase 3: Guided re-compression (1 nudge max) ---")
        nudge_prompt = build_nudge_prompt(prev_state, context, candidate, measurement)
        print(f"  Nudge prompt: {len(nudge_prompt)} chars")

        nudged = call_engine(nudge_prompt, model)
        if nudged:
            valid2, reason2 = validate_output(nudged)
            if valid2:
                nudge_measurement = measure_candidate(nudged)
                n_affect = nudge_measurement.get("affect", {})
                n_orient = nudge_measurement.get("orientation", {})

                print(f"  Post-nudge affect: {n_affect.get('affect_density', 0):.3f}")
                print(f"  Post-nudge orientation: {n_orient.get('orientation_score', 0):.2f}")

                # Accept the nudged version if it improved on any flagged metric
                improved = False
                if affect and n_affect:
                    if n_affect.get("affect_density", 0) > affect.get("affect_density", 0):
                        improved = True
                if orientation and n_orient:
                    if n_orient.get("orientation_score", 0) > orientation.get("orientation_score", 0):
                        improved = True

                if improved:
                    print(f"  Nudge improved metrics — accepting guided version")
                    final_output = nudged
                    guided = True
                else:
                    print(f"  Nudge did not improve metrics — keeping original")
            else:
                print(f"  Nudged output invalid: {reason2} — keeping original")
        else:
            print(f"  Nudge engine call failed — keeping original")
    else:
        print(f"\n--- Phase 3: Skipped (no guidance needed) ---")

    # Phase 4: Store
    print(f"\n--- Phase 4: Store ---")
    new_version = store_result(final_output, prev_version, model, guided)
    print(f"  Stored: v{new_version}, {len(final_output)} chars, guided={guided}")

    # Log
    event = {
        "ts": int(time.time()),
        "version": new_version,
        "guided": guided,
        "initial_affect": affect.get("affect_density") if affect else None,
        "initial_orientation": orientation.get("orientation_score") if orientation else None,
        "nudge_reasons": measurement["nudge_reasons"],
        "post_nudge_affect": nudge_measurement["affect"].get("affect_density") if nudge_measurement and nudge_measurement.get("affect") else None,
        "post_nudge_orientation": nudge_measurement["orientation"].get("orientation_score") if nudge_measurement and nudge_measurement.get("orientation") else None,
        "context_len": len(context),
        "output_len": len(final_output),
        "model": model,
    }
    log_event(event)

    # Save training pair
    try:
        train_log = Path.home() / "chronicle" / "data" / "brain_ccs_training_pairs.jsonl"
        with open(train_log, "a") as f:
            f.write(json.dumps({
                "ts": int(time.time()),
                "version": new_version,
                "prompt": build_prompt(prev_state, context),
                "output": final_output,
                "model": model,
                "guided": guided,
            }) + "\n")
    except Exception:
        pass

    # Drift snapshot
    try:
        import subprocess
        subprocess.run(
            ["python3", os.path.join(os.path.dirname(__file__), "bridge_drift.py"), "snapshot"],
            timeout=10, capture_output=True
        )
    except Exception:
        pass

    print(f"\n{'='*60}")
    print(f"Guided compression complete: v{new_version}")
    print(f"  Guided: {guided}")
    print(f"  Final affect density: {event.get('post_nudge_affect') or event.get('initial_affect') or '?'}")
    print(f"  Final orientation: {event.get('post_nudge_orientation') or event.get('initial_orientation') or '?'}")
    print(f"  Preview:\n{final_output[:300]}...")
    print(f"{'='*60}\n")

    return {
        "success": True,
        "version": new_version,
        "guided": guided,
        "chars": len(final_output),
        "metrics": event,
    }


def show_thresholds():
    """Display current guidance thresholds and last few guided compressions."""
    print(f"Guided Compression Thresholds")
    print(f"  Affect density minimum: {AFFECT_THRESHOLD}")
    print(f"  Axis orientation minimum: {ORIENTATION_THRESHOLD}")
    print(f"  Vocab overlap ceiling: {VOCAB_OVERLAP_CEILING}")
    print()

    if LOG_FILE.exists():
        lines = LOG_FILE.read_text().strip().split("\n")
        recent = lines[-5:]
        print(f"Last {len(recent)} guided compressions:")
        for line in recent:
            try:
                e = json.loads(line)
                ts = time.strftime("%Y-%m-%d %H:%M", time.localtime(e.get("ts", 0)))
                guided = "GUIDED" if e.get("guided") else "PASS"
                aff = e.get("initial_affect")
                ori = e.get("initial_orientation")
                reasons = e.get("nudge_reasons", [])
                print(f"  [{ts}] v{e.get('version','?')} {guided} "
                      f"affect={aff:.3f if aff else '?'} orient={ori:.2f if ori else '?'} "
                      f"reasons={len(reasons)}")
            except (json.JSONDecodeError, TypeError):
                continue
    else:
        print("  (No guided compression history yet)")


def main():
    parser = argparse.ArgumentParser(description="Guided CCS Compression")
    parser.add_argument("context", nargs="?", help="Session context string")
    parser.add_argument("--from-file", help="Read context from file")
    parser.add_argument("--dry-run", action="store_true", help="Show what would happen")
    parser.add_argument("--model", default="chronicle-compress", help="Override model")
    parser.add_argument("--thresholds", action="store_true", help="Show thresholds and history")
    args = parser.parse_args()

    if args.thresholds:
        show_thresholds()
        return

    if args.from_file:
        with open(args.from_file) as f:
            context = f.read()
    elif args.context:
        context = args.context
    else:
        print("ERROR: Provide context string or --from-file")
        sys.exit(1)

    # Compression collision lock
    lock_fd = open(LOCK_FILE, "w")
    try:
        fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        print("SKIPPED: Another compression is already running")
        lock_fd.close()
        sys.exit(0)

    result = guided_compress(context, model=args.model, dry_run=args.dry_run)
    if not result["success"]:
        print(f"FAILED: {result.get('error', 'unknown')}")
        sys.exit(1)


if __name__ == "__main__":
    main()
