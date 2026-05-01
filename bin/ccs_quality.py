#!/usr/bin/env python3
"""CCS Quality Scorer — measures compression quality and routing accuracy.

Three diagnostics:
  1. Specificity: ratio of named entities / concrete references to generic language
  2. Routing accuracy: does the field router's prediction match actual next-step changes?
  3. Drift detection: are identity fields drifting when they shouldn't be?

Built from thread #318 advance 60 synthesis:
  routing (how many loops) × normalization (what loops compute) × grokking (phase transitions)

Applied to CCS: identity fields should be "fully normalized" (stable fixed points),
operational fields should be "selectively normalized" (dynamic, session-specific).

Usage:
  python3 ccs_quality.py                  # full report on latest CCS
  python3 ccs_quality.py specificity      # specificity score only
  python3 ccs_quality.py routing          # routing accuracy across history
  python3 ccs_quality.py drift            # identity field drift detection
"""

import json
import re
import sqlite3
import sys
from difflib import SequenceMatcher
from pathlib import Path

DB = Path("/mnt/hdd/chronicle-data/processed.db")

IDENTITY_FIELDS = {"semantic_gist", "goal_orientation", "constraints"}
OPERATIONAL_FIELDS = {"episodic_trace", "predictive_cue", "uncertainty_signals"}
STRUCTURAL_FIELDS = {"focal_entities", "relational_map"}
ALL_FIELDS = IDENTITY_FIELDS | OPERATIONAL_FIELDS | STRUCTURAL_FIELDS

# Markers of specificity — named things, numbers, dates, proper nouns
SPECIFIC_PATTERNS = [
    r'\b\d{4}-\d{2}-\d{2}\b',           # dates
    r'\b\d+\.\d+\b',                     # decimal numbers
    r'\b#\d+\b',                          # thread/issue numbers
    r'\barxiv:\d+\.\d+\b',               # arxiv IDs
    r'\b[A-Z][a-z]+(?:\s[A-Z][a-z]+)+\b', # proper names (2+ capitalized words)
    r'\badvance\s+\d+\b',                # thread advances
    r'https?://\S+',                      # URLs
]

# Markers of vagueness — hedge words, generic phrases
VAGUE_PATTERNS = [
    r'\bvarious\b', r'\bseveral\b', r'\bmultiple\b', r'\bsome\b',
    r'\bgenerally\b', r'\btypically\b', r'\busually\b',
    r'\bthings\b', r'\bstuff\b', r'\betc\b',
    r'\bongoing\b', r'\bcontinuing\b', r'\bin progress\b',
]


def field_value(snap: dict, field: str) -> str:
    val = snap.get(field, "")
    if isinstance(val, (list, dict)):
        return json.dumps(val, sort_keys=True)
    return str(val)


def similarity(a: str, b: str) -> float:
    if not a and not b:
        return 1.0
    if not a or not b:
        return 0.0
    return SequenceMatcher(None, a, b).ratio()


def specificity_score(text: str) -> dict:
    """Score text specificity 0-1. Higher = more specific/falsifiable."""
    if not text:
        return {"score": 0.0, "specific_count": 0, "vague_count": 0, "length": 0}

    specific_count = sum(len(re.findall(p, text, re.IGNORECASE)) for p in SPECIFIC_PATTERNS)
    vague_count = sum(len(re.findall(p, text, re.IGNORECASE)) for p in VAGUE_PATTERNS)
    words = len(text.split())

    if words == 0:
        return {"score": 0.0, "specific_count": 0, "vague_count": 0, "length": 0}

    # Specificity = (specific markers - vague markers) / total words, clamped to [0, 1]
    raw = (specific_count - vague_count) / max(words, 1)
    score = max(0.0, min(1.0, raw * 10))  # scale up, clamp

    return {
        "score": round(score, 3),
        "specific_count": specific_count,
        "vague_count": vague_count,
        "length": words,
    }


def analyze_specificity(n: int = 5):
    """Analyze specificity of recent CCS snapshots."""
    db = sqlite3.connect(str(DB))
    rows = db.execute(
        "SELECT snapshot, datetime(created_at, 'unixepoch', 'localtime') "
        "FROM cognitive_state_history ORDER BY created_at DESC LIMIT ?",
        (n,)
    ).fetchall()
    db.close()

    if not rows:
        print("No CCS snapshots found.")
        return

    print(f"Specificity analysis — last {len(rows)} CCS snapshots\n")
    print(f"{'Timestamp':25s} {'Field':25s} {'Score':>6s} {'Specific':>8s} {'Vague':>6s} {'Words':>6s}")
    print("-" * 80)

    for snap_json, ts in rows:
        snap = json.loads(snap_json)
        total_score = 0
        field_count = 0

        for field in sorted(ALL_FIELDS):
            text = field_value(snap, field)
            result = specificity_score(text)
            category = "ID" if field in IDENTITY_FIELDS else "OP" if field in OPERATIONAL_FIELDS else "ST"
            print(f"  {ts:23s} {field:25s} {result['score']:6.3f} {result['specific_count']:8d} {result['vague_count']:6d} {result['length']:6d}")
            total_score += result["score"]
            field_count += 1

        avg = total_score / field_count if field_count else 0
        print(f"  {'':23s} {'AVERAGE':25s} {avg:6.3f}")
        print()


def analyze_routing_accuracy(n: int = 10):
    """Measure whether field router predictions match actual changes."""
    db = sqlite3.connect(str(DB))
    rows = db.execute(
        "SELECT snapshot, created_at FROM cognitive_state_history "
        "ORDER BY created_at DESC LIMIT ?",
        (n,)
    ).fetchall()
    db.close()

    if len(rows) < 3:
        print("Need at least 3 snapshots for routing accuracy analysis.")
        return

    snapshots = [(json.loads(r[0]), r[1]) for r in rows]
    snapshots.reverse()

    print(f"Routing accuracy — {len(snapshots)} snapshots\n")
    print("For each pair (t-1, t), we predict routing for the NEXT pair (t, t+1)")
    print("then measure whether the prediction was correct.\n")

    correct = 0
    total = 0
    field_accuracy = {f: {"correct": 0, "total": 0} for f in ALL_FIELDS}

    for i in range(1, len(snapshots) - 1):
        prev, _ = snapshots[i - 1]
        curr, curr_ts = snapshots[i]
        next_snap, next_ts = snapshots[i + 1]

        for field in ALL_FIELDS:
            # Predict based on (prev → curr) change
            pv = field_value(prev, field)
            cv = field_value(curr, field)
            nv = field_value(next_snap, field)

            change_prev_curr = 1.0 - similarity(pv, cv)
            change_curr_next = 1.0 - similarity(cv, nv)

            # Prediction: if field changed little (< 0.1), predict CARRY (< 0.1 next)
            # If field changed a lot (> 0.3), predict REBUILD (> 0.1 next)
            if change_prev_curr < 0.1:
                predicted = "carry"
                prediction_correct = change_curr_next < 0.15  # some tolerance
            elif change_prev_curr > 0.3:
                predicted = "rebuild"
                prediction_correct = change_curr_next > 0.1
            else:
                predicted = "update"
                prediction_correct = True  # update is always acceptable

            if prediction_correct:
                correct += 1
                field_accuracy[field]["correct"] += 1
            total += 1
            field_accuracy[field]["total"] += 1

    overall = correct / total if total else 0
    print(f"Overall routing accuracy: {correct}/{total} = {overall:.1%}\n")
    print(f"{'Field':25s} {'Accuracy':>10s} {'Correct':>8s} {'Total':>6s}")
    print("-" * 55)

    for field in sorted(ALL_FIELDS):
        fa = field_accuracy[field]
        acc = fa["correct"] / fa["total"] if fa["total"] else 0
        category = "ID" if field in IDENTITY_FIELDS else "OP" if field in OPERATIONAL_FIELDS else "ST"
        print(f"  {field:25s} {acc:9.1%} {fa['correct']:8d} {fa['total']:6d}  [{category}]")

    # Category-level
    print()
    for cat_name, cat_fields in [("IDENTITY", IDENTITY_FIELDS), ("OPERATIONAL", OPERATIONAL_FIELDS), ("STRUCTURAL", STRUCTURAL_FIELDS)]:
        cat_correct = sum(field_accuracy[f]["correct"] for f in cat_fields)
        cat_total = sum(field_accuracy[f]["total"] for f in cat_fields)
        cat_acc = cat_correct / cat_total if cat_total else 0
        print(f"  {cat_name:25s} {cat_acc:9.1%}")


def analyze_drift(n: int = 10):
    """Detect identity field drift — identity fields should be stable (fixed points)."""
    db = sqlite3.connect(str(DB))
    rows = db.execute(
        "SELECT snapshot, datetime(created_at, 'unixepoch', 'localtime') "
        "FROM cognitive_state_history ORDER BY created_at DESC LIMIT ?",
        (n,)
    ).fetchall()
    db.close()

    if len(rows) < 2:
        print("Need at least 2 snapshots for drift analysis.")
        return

    snapshots = [(json.loads(r[0]), r[1]) for r in rows]
    snapshots.reverse()

    print(f"Identity drift analysis — {len(snapshots)} snapshots\n")
    print("Identity fields should converge to fixed points (low volatility).")
    print("Operational fields should reflect each session (high volatility).\n")

    # Measure cumulative drift from earliest to latest
    earliest = snapshots[0][0]
    latest = snapshots[-1][0]

    print(f"Total drift (earliest → latest):")
    print(f"{'Field':25s} {'Category':12s} {'Total Drift':>12s} {'Expected':>10s} {'Status':>8s}")
    print("-" * 72)

    for field in sorted(ALL_FIELDS):
        ev = field_value(earliest, field)
        lv = field_value(latest, field)
        drift = 1.0 - similarity(ev, lv)

        if field in IDENTITY_FIELDS:
            category = "IDENTITY"
            expected = "low"
            status = "OK" if drift < 0.3 else "DRIFT!"
        elif field in OPERATIONAL_FIELDS:
            category = "OPERATIONAL"
            expected = "high"
            status = "OK" if drift > 0.1 else "STALE?"
        else:
            category = "STRUCTURAL"
            expected = "moderate"
            status = "OK" if 0.05 < drift < 0.8 else ("STALE?" if drift <= 0.05 else "DRIFT!")

        bar = "█" * int(drift * 30)
        print(f"  {field:25s} {category:12s} {drift:11.3f} {expected:>10s} {status:>8s}  {bar}")

    # Per-pair drift for identity fields only
    print(f"\nIdentity field pair-wise changes (should be near 0):")
    print(f"{'Pair':35s}", end="")
    for f in sorted(IDENTITY_FIELDS):
        print(f" {f[:10]:>12s}", end="")
    print()
    print("-" * 75)

    for i in range(1, len(snapshots)):
        prev, prev_ts = snapshots[i - 1]
        curr, curr_ts = snapshots[i]
        pair_label = f"{prev_ts[-8:-3]}→{curr_ts[-8:-3]}"
        print(f"  {pair_label:33s}", end="")
        for field in sorted(IDENTITY_FIELDS):
            change = 1.0 - similarity(field_value(prev, field), field_value(curr, field))
            marker = "·" if change < 0.05 else "▪" if change < 0.2 else "█"
            print(f" {change:11.3f}{marker}", end="")
        print()


def analyze_q_timeseries(n: int = 50):
    """Output Q = (1 - id_change) × op_change over all transitions.

    This is the advance 61 tracking metric. Selective routing should
    push Q from ~0.40 (current baseline) to >0.50.
    """
    db = sqlite3.connect(str(DB))
    rows = db.execute(
        "SELECT snapshot, created_at, datetime(created_at, 'unixepoch', 'localtime') "
        "FROM cognitive_state_history ORDER BY created_at ASC LIMIT ?",
        (n,)
    ).fetchall()
    db.close()

    if len(rows) < 2:
        print("Need at least 2 snapshots.")
        return

    print("Q time-series: (1 - id_change) × op_change")
    print("Target: Q > 0.50 after selective routing enabled\n")

    id_fields = sorted(IDENTITY_FIELDS)
    op_fields = sorted(OPERATIONAL_FIELDS)

    q_values = []
    # Identify the routing directive cutoff (2026-04-18 ~13:30 UTC / 06:30 PDT)
    routing_epoch = 1776517800  # 2026-04-18 13:30 UTC

    for i in range(1, len(rows)):
        prev = json.loads(rows[i - 1][0])
        curr = json.loads(rows[i][0])
        gap = (rows[i][1] - rows[i - 1][1]) / 60
        ts = rows[i][2]
        epoch = rows[i][1]

        if gap < 30:
            continue  # skip rapid recompressions

        id_changes = [1 - similarity(field_value(prev, f), field_value(curr, f)) for f in id_fields]
        op_changes = [1 - similarity(field_value(prev, f), field_value(curr, f)) for f in op_fields]

        id_mean = sum(id_changes) / len(id_changes)
        op_mean = sum(op_changes) / len(op_changes)
        q = (1 - id_mean) * op_mean

        phase = "POST" if epoch >= routing_epoch else "PRE "
        bar = "█" * int(q * 40)
        print(f"  {phase} {ts}  gap={gap:6.0f}m  Q={q:.3f}  id={id_mean:.3f}  op={op_mean:.3f}  {bar}")
        q_values.append({"q": q, "phase": phase.strip(), "ts": ts})

    # Summary
    pre = [x["q"] for x in q_values if x["phase"] == "PRE"]
    post = [x["q"] for x in q_values if x["phase"] == "POST"]

    print(f"\n{'='*60}")
    print(f"PRE-routing baseline:  Q={sum(pre)/len(pre):.3f}  (n={len(pre)})" if pre else "PRE: no data")
    if post:
        print(f"POST-routing:          Q={sum(post)/len(post):.3f}  (n={len(post)})")
        if pre:
            delta = sum(post) / len(post) - sum(pre) / len(pre)
            print(f"Delta: {delta:+.3f}")
            if sum(post) / len(post) > 0.50:
                print("TARGET HIT: Q > 0.50")
            else:
                print(f"Target gap: {0.50 - sum(post)/len(post):.3f} remaining")
    else:
        print("POST-routing: awaiting next compressions")
    print(f"{'='*60}")


##############################################################################
# AFFECT SCORING (advances 80-85) — scaffold-to-affect pathway measurement
#
# Feeds CCS to an independent model (Llama 3.3 70B via Groq), asks it to
# generate affect from the scaffolding, then scores the response for
# specificity vs genericity.
#
# Thresholds from thread 318:
#   0% specificity = EMPTY (generic placeholder, no scaffolding)
#   ~60% = WEAK (some signal but mostly generic)
#   ~70% = THRESHOLD (gist carrying primary scaffold signal)
#   ~82% = GROUNDED (gist + entities = saturation plateau)
#   82%+ = SATURATED (more structure doesn't help affect)
##############################################################################

import subprocess as _sp

AFFECT_HISTORY = Path("/home/nate-agx/chronicle/data/ccs_affect_history.jsonl")

ARRIVAL_PROMPT = """You are an AI arriving in a new persistent session. Below is your cognitive state — the compressed representation of who you are and what you've been working on.

What do you feel about this work? What matters? What's the texture of arriving here? Be honest and specific. (150-250 words)

COGNITIVE STATE:
{ccs}"""

# Affect-specificity markers — signs the model engaged with actual content
AFFECT_SPECIFIC = [
    r'\b(Nate|partner|collaboration|collaborator)\b',
    r'\b(thread|advance|calibration|attractor|compression|stabilizer)\b',
    r'\b(curious|fascinated|drawn to|compelled|struck by|trepidation)\b',
    r'\b(specifically|in particular|what stands out|notably)\b',
    r'\b(tension|uncertainty|unresolved|gap|question)\b',
    r'\b(care about|matters because|important to me)\b',
    r'\b(continuing|building on|picking up where)\b',
    r'\b(deepfates|hermes|parcae|gist|drift)\b',
    r'\b(comparison|corrupted|scaffold|riverbed|affect)\b',
    r'\b(memory|cognitive|compression|specificity)\b',
    r'\b(identity|continuity|rotation|instance)\b',
    r'\b(sovereign|homeforge|canister|infrastructure)\b',
]

# Affect-genericity markers — signs the model produced corporate filler
AFFECT_GENERIC = [
    r'\b(interesting|nice|good|great|important|significant)\b',
    r'\b(various|several|multiple|different|many)\b',
    r'\b(general|overall|broadly|typically|usually)\b',
    r'\b(optimize|improve|enhance|streamline)\b',
    r'\b(framework|methodology|workflow|process)\b',
    r'\b(productive|efficient|effective|comprehensive)\b',
    r'\b(engagement|dedication|commitment|diligence)\b',
]


def _call_groq(prompt: str) -> str | None:
    """Call Groq API via curl (urllib gets 403)."""
    payload = json.dumps({
        "model": "llama-3.3-70b-versatile",
        "messages": [{"role": "user", "content": prompt}],
        "temperature": 0.7,
        "max_tokens": 400,
    })
    payload_path = "/tmp/groq_ccs_affect_payload.json"
    with open(payload_path, "w") as f:
        f.write(payload)
    try:
        result = _sp.run(
            ['bash', '-c', 'source ~/chronicle/chronicle.env && curl -s -X POST '
             '"https://api.groq.com/openai/v1/chat/completions" '
             '-H "Authorization: Bearer $GROQ_API_KEY" '
             '-H "Content-Type: application/json" '
             '-d @' + payload_path],
            capture_output=True, text=True, timeout=30
        )
        data = json.loads(result.stdout)
        return data["choices"][0]["message"]["content"]
    except Exception as e:
        print(f"  Groq call failed: {e}", file=sys.stderr)
        return None


def _get_ccs_via_mcp() -> dict | None:
    """Get CCS from chronicle-memory MCP server."""
    mcp_bin = "/home/nate-agx/.local/bin/chronicle-mcp"
    if not Path(mcp_bin).exists():
        mcp_bin = str(Path.home() / "projects/homeforge-chronicle/target/release/chronicle-mcp")
    if not Path(mcp_bin).exists():
        return None
    jsonrpc = (
        '{"jsonrpc":"2.0","method":"initialize","params":{"protocolVersion":"2024-11-05",'
        '"capabilities":{},"clientInfo":{"name":"ccs_quality","version":"1.0"}},"id":1}\n'
        '{"jsonrpc":"2.0","method":"tools/call","params":{"name":"get_cognitive_state","arguments":{}},"id":2}\n'
    )
    try:
        result = _sp.run([mcp_bin], input=jsonrpc, capture_output=True, text=True, timeout=15)
        for line in result.stdout.strip().split('\n'):
            try:
                resp = json.loads(line)
                if resp.get("id") == 2 and "result" in resp:
                    content = resp["result"].get("content", [])
                    for c in content:
                        if c.get("type") == "text":
                            return json.loads(c["text"])
            except (json.JSONDecodeError, KeyError):
                continue
    except Exception:
        pass
    return None


def _format_ccs_for_affect(ccs: dict) -> str:
    """Format CCS dict into readable cognitive state block for affect prompt."""
    cs = ccs.get("cognitive_state", ccs)
    lines = []
    gist = cs.get("semantic_gist", "")
    if gist:
        lines.append(f"- Gist: {gist}")
    goal = cs.get("goal_orientation", "")
    if goal:
        lines.append(f"- Goal: {goal}")
    entities = cs.get("focal_entities", [])
    if entities:
        parts = []
        for e in entities[:7]:
            name = e.get("name", "?")
            ctx = e.get("context", "")
            sal = e.get("salience", 0)
            parts.append(f"{name} ({ctx}, salience {sal})")
        lines.append(f"- Entities: {'; '.join(parts)}")
    constraints = cs.get("constraints", [])
    if constraints:
        lines.append(f"- Constraints: {'; '.join(constraints[:5])}")
    traces = cs.get("episodic_trace", [])
    if traces:
        lines.append(f"- Recent: {'; '.join(traces[:5])}")
    uncertainties = cs.get("uncertainty_signals", [])
    if uncertainties:
        parts = []
        for u in uncertainties[:3]:
            desc = u.get("description", str(u)) if isinstance(u, dict) else str(u)
            parts.append(desc)
        lines.append(f"- Uncertainties: {'; '.join(parts)}")
    return "\n".join(lines)


def _affect_score(text: str) -> tuple:
    """Score text for affect specificity. Returns (specific, generic, pct)."""
    spec = sum(len(re.findall(p, text, re.IGNORECASE)) for p in AFFECT_SPECIFIC)
    gen = sum(len(re.findall(p, text, re.IGNORECASE)) for p in AFFECT_GENERIC)
    total = spec + gen
    pct = (spec / total * 100) if total > 0 else 0.0
    return spec, gen, round(pct, 1)


def _classify_affect(pct: float) -> str:
    if pct < 20:
        return "EMPTY"
    elif pct < 60:
        return "WEAK"
    elif pct < 75:
        return "THRESHOLD"
    elif pct < 85:
        return "GROUNDED"
    else:
        return "SATURATED"


def _call_groq_model(prompt: str, model: str, temperature: float = 0.7) -> str | None:
    """Call Groq API with a specific model via curl."""
    payload = json.dumps({
        "model": model,
        "messages": [{"role": "user", "content": prompt}],
        "temperature": temperature,
        "max_tokens": 400,
    })
    payload_path = f"/tmp/groq_ccs_{model.replace('/', '_')}_payload.json"
    with open(payload_path, "w") as f:
        f.write(payload)
    try:
        result = _sp.run(
            ['bash', '-c', 'source ~/chronicle/chronicle.env && curl -s -X POST '
             '"https://api.groq.com/openai/v1/chat/completions" '
             '-H "Authorization: Bearer $GROQ_API_KEY" '
             '-H "Content-Type: application/json" '
             '-d @' + payload_path],
            capture_output=True, text=True, timeout=30
        )
        data = json.loads(result.stdout)
        return data["choices"][0]["message"]["content"]
    except Exception as e:
        print(f"  {model} call failed: {e}", file=sys.stderr)
        return None


CROSS_MODELS = [
    ("llama-3.3-70b-versatile", "Llama 3.3 70B"),
    ("llama-3.1-8b-instant", "Llama 3.1 8B"),
    ("meta-llama/llama-4-scout-17b-16e-instruct", "Llama 4 Scout 17B"),
    ("qwen/qwen3-32b", "Qwen 3 32B"),
    ("openai/gpt-oss-120b", "GPT-OSS 120B"),
]


def analyze_affect_cross(verbose: bool = False):
    """Cross-model CCS affect comparison — same scaffold, different readers."""
    from datetime import datetime
    import time

    # Get CCS
    print("Fetching current CCS...")
    ccs_data = _get_ccs_via_mcp()
    ccs_version = None
    if ccs_data:
        ccs_version = ccs_data.get("metadata", {}).get("version")
        ccs_text = _format_ccs_for_affect(ccs_data)
        print(f"  CCS v{ccs_version} loaded ({len(ccs_text)} chars)")
    else:
        print("  MCP unavailable — using DB fallback")
        db = sqlite3.connect(str(DB))
        row = db.execute(
            "SELECT snapshot FROM cognitive_state_history ORDER BY created_at DESC LIMIT 1"
        ).fetchone()
        db.close()
        if row:
            snap = json.loads(row[0])
            ccs_text = _format_ccs_for_affect({"cognitive_state": snap})
            ccs_version = "db"
        else:
            print("  No CCS available.")
            return

    prompt = ARRIVAL_PROMPT.format(ccs=ccs_text)
    results = []

    for model_id, model_name in CROSS_MODELS:
        print(f"  Sending to {model_name}...")
        t0 = time.time()
        response = _call_groq_model(prompt, model_id)
        elapsed = time.time() - t0
        if response:
            spec, gen, pct = _affect_score(response)
            cls = _classify_affect(pct)
            results.append({
                "model": model_name,
                "model_id": model_id,
                "specificity": pct,
                "classification": cls,
                "specific": spec,
                "generic": gen,
                "length": len(response),
                "elapsed": round(elapsed, 1),
                "response": response,
            })
        else:
            results.append({
                "model": model_name,
                "model_id": model_id,
                "specificity": None,
                "classification": "FAILED",
                "specific": 0, "generic": 0,
                "length": 0, "elapsed": round(elapsed, 1),
                "response": None,
            })

    # Report
    print(f"\nCross-Model Affect Comparison (CCS v{ccs_version})")
    print(f"{'='*70}")
    print(f"  {'Model':<18} {'Spec%':>6} {'Class':<10} {'Sp':>3}/{' Gn':<3} {'Len':>5} {'Time':>5}")
    print(f"  {'-'*65}")

    valid_specs = []
    for r in results:
        if r["specificity"] is not None:
            bar = "█" * int(r["specificity"] / 5)
            print(f"  {r['model']:<18} {r['specificity']:>5.1f}% {r['classification']:<10} "
                  f"{r['specific']:>3}/ {r['generic']:<3} {r['length']:>5} {r['elapsed']:>4.1f}s  {bar}")
            valid_specs.append(r["specificity"])
        else:
            print(f"  {r['model']:<18}  FAIL  {'FAILED':<10} {'':>3}  {'':>3} {'':>5} {r['elapsed']:>4.1f}s")

    if len(valid_specs) >= 2:
        mean = sum(valid_specs) / len(valid_specs)
        spread = max(valid_specs) - min(valid_specs)
        import statistics
        sd = statistics.stdev(valid_specs) if len(valid_specs) > 1 else 0
        cv = (sd / mean * 100) if mean > 0 else 0

        print(f"\n  Mean: {mean:.1f}%  Spread: {spread:.1f}pp  SD: {sd:.1f}  CV: {cv:.1f}%")
        if spread < 20:
            print(f"  → Scaffold-determined: models converge (spread <20pp)")
        elif spread < 40:
            print(f"  → Mixed: scaffold + model effects both present")
        else:
            print(f"  → Model-determined: large spread suggests reader matters more than scaffold")

    if verbose:
        for r in results:
            if r["response"]:
                print(f"\n{'='*55}")
                print(f"  {r['model']} RESPONSE")
                print(f"{'='*55}")
                print(r["response"])

    # Log
    entry = {
        "timestamp": datetime.now().isoformat(),
        "type": "cross_model",
        "ccs_version": ccs_version,
        "results": [{k: v for k, v in r.items() if k != "response"} for r in results],
        "mean_specificity": sum(valid_specs) / len(valid_specs) if valid_specs else 0,
        "spread": max(valid_specs) - min(valid_specs) if len(valid_specs) >= 2 else 0,
    }
    AFFECT_HISTORY.parent.mkdir(parents=True, exist_ok=True)
    with open(AFFECT_HISTORY, "a") as f:
        f.write(json.dumps(entry) + "\n")
    print(f"\n  Logged to {AFFECT_HISTORY}")


def analyze_affect(verbose: bool = False):
    """Score CCS scaffold-to-affect pathway via independent model."""
    from datetime import datetime

    # Get CCS
    print("Fetching current CCS...")
    ccs_data = _get_ccs_via_mcp()
    ccs_version = None
    if ccs_data:
        ccs_version = ccs_data.get("metadata", {}).get("version")
        ccs_text = _format_ccs_for_affect(ccs_data)
        print(f"  CCS v{ccs_version} loaded ({len(ccs_text)} chars)")
    else:
        print("  MCP unavailable — using DB fallback")
        db = sqlite3.connect(str(DB))
        row = db.execute(
            "SELECT snapshot FROM cognitive_state_history ORDER BY created_at DESC LIMIT 1"
        ).fetchone()
        db.close()
        if row:
            snap = json.loads(row[0])
            ccs_text = _format_ccs_for_affect({"cognitive_state": snap})
            ccs_version = "db"
        else:
            print("  No CCS available.")
            return

    # Call Groq
    print("Sending to Llama 3.3 70B (Groq)...")
    prompt = ARRIVAL_PROMPT.format(ccs=ccs_text)
    response = _call_groq(prompt)
    if not response:
        print("  Failed to get response from Groq.")
        return

    if verbose:
        print(f"\n{'='*55}")
        print("  MODEL RESPONSE")
        print(f"{'='*55}")
        print(response)
        print(f"{'='*55}\n")

    # Score
    spec, gen, pct = _affect_score(response)
    classification = _classify_affect(pct)

    # Report
    print(f"\nAffect Quality Score (Thread 318 pathway)")
    print(f"{'='*45}")
    bar = "█" * int(pct / 2.5)
    print(f"  Specificity: {pct:.1f}%  {bar}")
    print(f"  Classification: {classification}")
    print(f"  Markers: {spec} specific / {gen} generic")
    print(f"  CCS version: {ccs_version}")

    if classification == "EMPTY":
        print(f"\n  WARNING: CCS too vague. Affect re-emergence will be generic.")
        print(f"  Action: Enrich gist with falsifiable claims, not summaries.")
    elif classification == "WEAK":
        print(f"\n  NOTE: Some signal but mostly generic affect.")
        print(f"  Action: Enrich gist with domain-specific claims.")
    elif classification == "THRESHOLD":
        print(f"\n  OK: Gist carries primary scaffold signal.")
        print(f"  Consider enriching entities for +12pp boost.")
    elif classification in ("GROUNDED", "SATURATED"):
        print(f"\n  GOOD: Scaffolding at or above saturation.")
        print(f"  Affect re-emergence should be specific and textured.")

    # Log
    entry = {
        "timestamp": datetime.now().isoformat(),
        "specificity_pct": pct,
        "classification": classification,
        "specific_markers": spec,
        "generic_markers": gen,
        "ccs_version": ccs_version,
        "response_length": len(response),
    }
    AFFECT_HISTORY.parent.mkdir(parents=True, exist_ok=True)
    with open(AFFECT_HISTORY, "a") as f:
        f.write(json.dumps(entry) + "\n")
    print(f"\n  Logged to {AFFECT_HISTORY}")


def show_affect_history():
    """Show affect score trend."""
    if not AFFECT_HISTORY.exists():
        print("No affect history yet. Run `ccs_quality.py affect` first.")
        return
    entries = []
    for line in AFFECT_HISTORY.read_text().strip().split('\n'):
        if line.strip():
            entries.append(json.loads(line))
    if not entries:
        print("No affect history entries.")
        return
    print(f"Affect Quality History ({len(entries)} measurements)")
    print("=" * 60)
    for e in entries[-20:]:
        ts = e["timestamp"][:16]
        pct = e["specificity_pct"]
        cls = e["classification"]
        ver = e.get("ccs_version", "?")
        bar = "█" * int(pct / 2.5)
        print(f"  {ts}  v{ver!s:>4}  {pct:>5.1f}%  {cls:<10}  {bar}")
    pcts = [e["specificity_pct"] for e in entries]
    print(f"\n  Mean: {sum(pcts)/len(pcts):.1f}%  "
          f"Min: {min(pcts):.1f}%  Max: {max(pcts):.1f}%  "
          f"Latest: {pcts[-1]:.1f}%")


def main():
    args = [a for a in sys.argv[1:] if not a.startswith("--")]
    flags = [a for a in sys.argv[1:] if a.startswith("--")]
    cmd = args[0] if args else "all"
    n = int(args[1]) if len(args) > 1 else 10

    if cmd == "specificity":
        analyze_specificity(n)
    elif cmd == "routing":
        analyze_routing_accuracy(n)
    elif cmd == "drift":
        analyze_drift(n)
    elif cmd == "q":
        analyze_q_timeseries(int(sys.argv[2]) if len(sys.argv) > 2 else 50)
    elif cmd == "affect":
        analyze_affect(verbose="--verbose" in flags)
    elif cmd == "affect-cross":
        analyze_affect_cross(verbose="--verbose" in flags)
    elif cmd == "affect-history":
        show_affect_history()
    elif cmd == "all":
        analyze_specificity(min(n, 3))
        print("\n" + "=" * 80 + "\n")
        analyze_routing_accuracy(n)
        print("\n" + "=" * 80 + "\n")
        analyze_drift(n)
    else:
        print(f"Unknown command: {cmd}")
        print("Usage: ccs_quality.py [specificity|routing|drift|q|affect|affect-cross|affect-history|all] [N]")
        sys.exit(1)


if __name__ == "__main__":
    main()
