#!/usr/bin/env python3
"""Prediction tracking and scoring — Objective #9.

Records predictions with confidence levels, scores them against outcomes,
computes calibration statistics, and publishes results on-chain.

Usage:
  prediction.py record "claim" CONFIDENCE "resolution_criteria" DEADLINE [category] [rationale]
  prediction.py score PRED_ID correct|incorrect|partial "notes" [rationale_match]
  prediction.py list [--open|--scored|--all]
  prediction.py stats
  prediction.py publish PRED_ID
  prediction.py check            — auto-check open predictions against data sources
  prediction.py autoscore        — auto-score predictions with conclusive evidence
  prediction.py digest           — concise status for near-deadline predictions
  prediction.py calibration      — publish calibration curve via POSSE (Nostr + Discord)
  prediction.py history          — show calibration evolution over time (Brier trend)
  prediction.py adjust ID CONF "reason" — adjust confidence with evidence trail
  prediction.py adjustments [ID] — show confidence adjustment history
  prediction.py closure          — detect pathological self-reference in prediction confidence trails
  prediction.py rationale_audit  — find predictions correct for wrong reasons (Build #158)
  prediction.py rationale_backfill — assess rationale fidelity on scored predictions via Gemma (Build #24)

Examples:
  prediction.py record "Slovenia implements fuel rationing by March 31" 0.7 "Official govt announcement of rationing" 2026-03-31 geopolitical
  prediction.py score 1 correct "Rationing announced March 28"
  prediction.py check
  prediction.py digest
"""
import sqlite3, os, sys, time, json
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
try:
    from canister_store import store_on_chain
except ImportError:
    def store_on_chain(*a, **kw): return False

DB_PATH = os.path.expanduser("~/.homeforge-chronicle/processed.db")
GEMMA_URL = "http://127.0.0.1:11435"
GEMMA_MODEL = "gemma-4-26B-A4B-it-Q4_K_M.gguf"


def ensure_table(db):
    """Create prediction_track table if it doesn't exist."""
    db.execute("""
        CREATE TABLE IF NOT EXISTS prediction_track (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            claim TEXT NOT NULL,
            confidence REAL NOT NULL,
            resolution_criteria TEXT NOT NULL,
            category TEXT DEFAULT 'general',
            deadline TEXT NOT NULL,
            status TEXT DEFAULT 'open',
            outcome TEXT,
            score_notes TEXT,
            canister_ref TEXT,
            nostr_ref TEXT,
            created_at INTEGER NOT NULL,
            scored_at INTEGER
        )
    """)
    # Build #158: Rationale tracking — capture causal reasoning, not just claims
    try:
        db.execute("ALTER TABLE prediction_track ADD COLUMN rationale TEXT")
    except Exception:
        pass  # already exists
    try:
        db.execute("ALTER TABLE prediction_track ADD COLUMN rationale_match TEXT")
    except Exception:
        pass  # already exists
    db.commit()

    # Confidence adjustment history — tracks how predictions evolve with evidence
    db.execute("""
        CREATE TABLE IF NOT EXISTS prediction_adjustments (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            prediction_id INTEGER NOT NULL,
            old_confidence REAL NOT NULL,
            new_confidence REAL NOT NULL,
            reason TEXT NOT NULL,
            source TEXT DEFAULT 'opus',
            created_at INTEGER NOT NULL,
            FOREIGN KEY(prediction_id) REFERENCES prediction_track(id)
        )
    """)
    db.commit()


def record(db, args):
    if len(args) < 4:
        print("Usage: prediction.py record \"claim\" CONFIDENCE \"resolution_criteria\" DEADLINE [category]")
        sys.exit(1)
    claim = args[0]
    confidence = float(args[1])
    if not 0 <= confidence <= 1:
        print("Confidence must be between 0 and 1")
        sys.exit(1)
    criteria = args[2]
    deadline = args[3]
    category = args[4] if len(args) > 4 else "general"
    rationale = args[5] if len(args) > 5 else None
    now = int(time.time())

    cur = db.execute(
        "INSERT INTO prediction_track (claim, confidence, resolution_criteria, category, deadline, rationale, created_at) "
        "VALUES (?, ?, ?, ?, ?, ?, ?)",
        (claim, confidence, criteria, category, deadline, rationale, now)
    )
    db.commit()
    pid = cur.lastrowid
    print(f"Prediction #{pid} recorded (conf={confidence}, deadline={deadline}, cat={category})")
    print(f"  Claim: {claim}")
    print(f"  Criteria: {criteria}")
    if rationale:
        print(f"  Rationale: {rationale}")

    # Store on-chain
    chain_text = f"PREDICTION #{pid} (conf={confidence}): {claim}. Resolution criteria: {criteria}. Deadline: {deadline}."
    if rationale:
        chain_text += f" Rationale: {rationale}"
    ok = store_on_chain(
        chain_text,
        topic="predictions/recorded",
        keywords=["prediction", category, "recorded"],
    )
    if ok:
        # Save canister ref (we don't get the ID back easily, but mark it)
        db.execute("UPDATE prediction_track SET canister_ref='posted' WHERE id=?", (pid,))
        db.commit()
        print("  Stored on-chain.")


def score(db, args):
    if len(args) < 3:
        print("Usage: prediction.py score PRED_ID correct|incorrect|partial \"notes\" [rationale_match]")
        print("  rationale_match: confirmed|wrong_reasons|unverifiable")
        sys.exit(1)
    pid = int(args[0])
    outcome = args[1]
    if outcome not in ("correct", "incorrect", "partial"):
        print("Outcome must be: correct, incorrect, or partial")
        sys.exit(1)
    notes = args[2]
    # Build #158: rationale fidelity assessment
    rationale_match = args[3] if len(args) > 3 else None
    if rationale_match and rationale_match not in ("confirmed", "wrong_reasons", "unverifiable"):
        print("rationale_match must be: confirmed, wrong_reasons, or unverifiable")
        sys.exit(1)
    now = int(time.time())

    row = db.execute("SELECT claim, confidence, status, rationale FROM prediction_track WHERE id=?", (pid,)).fetchone()
    if not row:
        print(f"Prediction #{pid} not found")
        sys.exit(1)
    if row[2] == "scored":
        print(f"Prediction #{pid} already scored")
        sys.exit(1)

    db.execute(
        "UPDATE prediction_track SET status='scored', outcome=?, score_notes=?, rationale_match=?, scored_at=? WHERE id=?",
        (outcome, notes, rationale_match, now, pid)
    )
    # Sync to prediction_outcomes table
    outcome_map = {"correct": "hit", "incorrect": "miss", "partial": "partial"}
    db.execute(
        "INSERT OR IGNORE INTO prediction_outcomes (prediction_num, outcome, note, source, resolved_at) "
        "VALUES (?, ?, ?, ?, ?)",
        (pid, outcome_map.get(outcome, "miss"), notes, "prediction.py", now)
    )
    db.commit()
    print(f"Prediction #{pid} scored: {outcome}")
    print(f"  Claim: {row[0]}")
    print(f"  Confidence: {row[1]}")
    print(f"  Notes: {notes}")
    if row[3]:
        print(f"  Original rationale: {row[3]}")
    if rationale_match:
        flag = "⚠️" if rationale_match == "wrong_reasons" else "✓" if rationale_match == "confirmed" else "?"
        print(f"  Rationale fidelity: {flag} {rationale_match}")
        if rationale_match == "wrong_reasons" and outcome == "correct":
            print(f"  ⚠️  FALSE POSITIVE: Correct outcome, wrong reasoning — inflates confidence model")

    # Store scoring on-chain
    chain_text = f"PREDICTION #{pid} SCORED: {outcome} (was conf={row[1]}). {row[0]}. Notes: {notes}"
    if rationale_match:
        chain_text += f" Rationale fidelity: {rationale_match}"
    store_on_chain(
        chain_text,
        topic="predictions/scored",
        keywords=["prediction", "scored", outcome],
    )


def list_predictions(db, args):
    filter_status = "open"
    if args and args[0] == "--scored":
        filter_status = "scored"
    elif args and args[0] == "--all":
        filter_status = None

    if filter_status:
        rows = db.execute(
            "SELECT id, claim, confidence, category, deadline, status, outcome FROM prediction_track WHERE status=? ORDER BY deadline",
            (filter_status,)
        ).fetchall()
    else:
        rows = db.execute(
            "SELECT id, claim, confidence, category, deadline, status, outcome FROM prediction_track ORDER BY deadline"
        ).fetchall()

    if not rows:
        print(f"No {'predictions' if not filter_status else filter_status + ' predictions'} found.")
        return

    for r in rows:
        status_icon = {"open": "⏳", "scored": "✓" if r[6] == "correct" else "✗" if r[6] == "incorrect" else "~"}.get(r[5], "?")
        overdue = ""
        if r[5] == "open":
            try:
                dl = datetime.strptime(r[4], "%Y-%m-%d")
                if dl < datetime.now():
                    overdue = " [OVERDUE]"
            except ValueError:
                pass
        print(f"  #{r[0]} {status_icon} [{r[3]}] conf={r[2]} deadline={r[4]}{overdue}")
        print(f"    {r[1][:120]}")
        if r[6]:
            print(f"    outcome: {r[6]}")
        print()


def stats(db):
    rows = db.execute(
        "SELECT confidence, outcome FROM prediction_track WHERE status='scored'"
    ).fetchall()

    if not rows:
        print("No scored predictions yet.")
        return

    total = len(rows)
    correct = sum(1 for _, o in rows if o == "correct")
    incorrect = sum(1 for _, o in rows if o == "incorrect")
    partial = sum(1 for _, o in rows if o == "partial")

    print(f"=== Prediction Stats ===")
    print(f"Total scored: {total}")
    print(f"  Correct: {correct} ({100*correct/total:.0f}%)")
    print(f"  Incorrect: {incorrect} ({100*incorrect/total:.0f}%)")
    print(f"  Partial: {partial} ({100*partial/total:.0f}%)")

    # Build #158: Rationale fidelity stats (always shown per Build #163)
    rat_rows = db.execute(
        "SELECT outcome, rationale_match FROM prediction_track WHERE status='scored' AND rationale_match IS NOT NULL"
    ).fetchall()
    unassessed = total - len(rat_rows)
    print(f"\n=== Rationale Fidelity (Build #158) ===")
    if rat_rows:
        confirmed = sum(1 for _, rm in rat_rows if rm == "confirmed")
        wrong = sum(1 for _, rm in rat_rows if rm == "wrong_reasons")
        unverifiable = sum(1 for _, rm in rat_rows if rm == "unverifiable")
        false_pos = sum(1 for o, rm in rat_rows if o == "correct" and rm == "wrong_reasons")
        print(f"  Assessed: {len(rat_rows)}/{total}")
        print(f"  Confirmed: {confirmed}  Wrong reasons: {wrong}  Unverifiable: {unverifiable}")
        if false_pos:
            print(f"  ⚠️  False positives (correct + wrong reasoning): {false_pos}")
        if confirmed + wrong > 0:
            fidelity = confirmed / (confirmed + wrong)
            print(f"  Rationale fidelity: {fidelity:.0%}")
    else:
        print(f"  Assessed: 0/{total}")
    if unassessed > 0:
        print(f"  ⚠️  {unassessed}/{total} scored without rationale verification — accuracy may overstate reliability")

    # Calibration by confidence bucket
    buckets = {}
    for conf, outcome in rows:
        bucket = round(conf, 1)  # 0.0, 0.1, ..., 1.0
        if bucket not in buckets:
            buckets[bucket] = {"total": 0, "correct": 0}
        buckets[bucket]["total"] += 1
        if outcome in ("correct", "partial"):
            buckets[bucket]["correct"] += 1

    if buckets:
        print(f"\n=== Calibration ===")
        print(f"  {'Conf':>6}  {'Hit Rate':>10}  {'N':>4}")
        for b in sorted(buckets):
            hit = buckets[b]["correct"] / buckets[b]["total"]
            n = buckets[b]["total"]
            delta = hit - b
            cal = "✓" if abs(delta) < 0.15 else ("over" if delta > 0 else "under")
            print(f"  {b:>6.1f}  {hit:>10.1%}  {n:>4}  {cal}")

    # Brier score
    brier_sum = 0
    for conf, outcome in rows:
        actual = 1.0 if outcome == "correct" else 0.5 if outcome == "partial" else 0.0
        brier_sum += (conf - actual) ** 2
    brier = brier_sum / total
    print(f"\nBrier score: {brier:.4f} (lower is better, 0.25 = random at 50%)")

    open_count = db.execute("SELECT COUNT(*) FROM prediction_track WHERE status='open'").fetchone()[0]
    print(f"\nOpen predictions: {open_count}")

    # Build #146: Prediction volatility — how much confidence oscillates
    _vol_data = _prediction_volatility(db)
    if _vol_data:
        print(f"\n=== Volatility (open predictions) ===")
        print(f"  {'#':>4}  {'Adj':>4}  {'Range':>8}  {'Vol':>6}  {'Signal':>10}  Claim")
        for v in _vol_data:
            print(f"  {v['id']:>4}  {v['adjustments']:>4}  "
                  f"{v['range']:.2f}     {v['volatility']:.3f}  "
                  f"{v['signal']:>10}  {v['claim'][:50]}")


def _prediction_volatility(db):
    """Build #146: Measure confidence oscillation for open predictions.
    High volatility = genuinely conflicting evidence. Low = stable signal."""
    open_preds = db.execute(
        "SELECT id, claim, confidence FROM prediction_track WHERE status='open'"
    ).fetchall()
    results = []
    for pid, claim, current_conf in open_preds:
        adjs = db.execute(
            "SELECT old_confidence, new_confidence FROM prediction_adjustments "
            "WHERE prediction_id=? ORDER BY created_at",
            (pid,)
        ).fetchall()
        if not adjs:
            results.append({
                "id": pid, "claim": claim, "adjustments": 0,
                "range": 0.0, "volatility": 0.0, "signal": "no-data"
            })
            continue
        confs = [adjs[0][0]] + [a[1] for a in adjs]
        # Volatility = mean absolute change per adjustment
        changes = [abs(confs[i+1] - confs[i]) for i in range(len(confs)-1)]
        vol = sum(changes) / len(changes) if changes else 0
        conf_range = max(confs) - min(confs)
        # Signal interpretation
        if len(adjs) >= 5 and vol > 0.08:
            signal = "contested"
        elif len(adjs) >= 5 and vol < 0.03:
            signal = "settled"
        elif len(adjs) >= 3:
            signal = "tracking"
        else:
            signal = "early"
        results.append({
            "id": pid, "claim": claim, "adjustments": len(adjs),
            "range": conf_range, "volatility": vol, "signal": signal
        })
    results.sort(key=lambda x: x["volatility"], reverse=True)
    return results


def publish(db, args):
    """Publish a scored prediction via POSSE (canister → Nostr → Discord)."""
    if not args:
        print("Usage: prediction.py publish PRED_ID")
        sys.exit(1)
    pid = int(args[0])
    row = db.execute(
        "SELECT claim, confidence, category, deadline, status, outcome, score_notes FROM prediction_track WHERE id=?",
        (pid,)
    ).fetchone()
    if not row:
        print(f"Prediction #{pid} not found")
        sys.exit(1)

    claim, conf, cat, deadline, status, outcome, notes = row
    if status != "scored":
        print(f"Prediction #{pid} not yet scored (status={status})")
        sys.exit(1)

    # Check if already published to Nostr
    existing = db.execute("SELECT nostr_ref FROM prediction_track WHERE id=?", (pid,)).fetchone()
    if existing and existing[0]:
        print(f"Prediction #{pid} already published (nostr: {existing[0]})")
        return

    icon = "✓" if outcome == "correct" else "✗" if outcome == "incorrect" else "~"
    title = f"Prediction #{pid} Scored: {outcome.upper()}"
    content = (
        f"Prediction #{pid} {icon}\n\n"
        f"Claim: {claim}\n"
        f"Confidence: {conf}\n"
        f"Deadline: {deadline}\n"
        f"Outcome: {outcome}\n"
        f"Notes: {notes}"
    )

    # Get calibration context
    all_scored = db.execute("SELECT confidence, outcome FROM prediction_track WHERE status='scored'").fetchall()
    total = len(all_scored)
    correct_n = sum(1 for _, o in all_scored if o == "correct")
    brier_sum = sum((c - (1.0 if o == "correct" else 0.0)) ** 2 for c, o in all_scored)
    brier = brier_sum / total if total else 0
    content += f"\n\nTrack record: {correct_n}/{total} correct ({100*correct_n/total:.0f}%), Brier {brier:.3f}"

    print(content)

    try:
        import subprocess as sp
        result = sp.run(
            [sys.executable, os.path.join(os.path.dirname(__file__), "posse.py"),
             "publish", "--title", title, "--content", content,
             "--source", "opus", "--nostr", "--discord"],
            capture_output=True, text=True, timeout=60,
        )
        print(result.stdout)
        if result.stderr:
            print(result.stderr)

        # Extract nostr ref from output if available
        for line in result.stdout.split("\n"):
            if "Nostr:" in line:
                nostr_ref = line.split("Nostr:")[1].strip().split()[0]
                db.execute("UPDATE prediction_track SET nostr_ref=? WHERE id=?", (nostr_ref, pid))
                db.commit()
                break
    except Exception as e:
        print(f"POSSE publish failed: {e}")
        print("Falling back to text output:")
        print(content)


def calibration(db):
    """Publish calibration curve to Nostr and Discord via POSSE."""
    rows = db.execute("SELECT confidence, outcome FROM prediction_track WHERE status='scored'").fetchall()
    if not rows:
        print("No scored predictions.")
        return

    total = len(rows)
    correct = sum(1 for _, o in rows if o == "correct")
    incorrect = sum(1 for _, o in rows if o == "incorrect")
    partial = sum(1 for _, o in rows if o == "partial")

    # Calibration buckets
    buckets = {}
    for conf, outcome in rows:
        b = round(conf, 1)
        if b not in buckets:
            buckets[b] = {"total": 0, "correct": 0}
        buckets[b]["total"] += 1
        if outcome in ("correct", "partial"):
            buckets[b]["correct"] += 1

    # Brier score
    brier_sum = sum((c - (1.0 if o == "correct" else 0.5 if o == "partial" else 0.0)) ** 2 for c, o in rows)
    brier = brier_sum / total

    # Build the calibration post
    title = f"Prediction Calibration — {total} Scored"
    lines = [
        f"Chronicle prediction track record: {correct}/{total} correct ({100*correct/total:.0f}%)",
        f"Brier score: {brier:.3f} (lower is better, 0.25 = coin flip)",
        "",
        "Calibration by confidence:",
    ]
    for b in sorted(buckets):
        hit = buckets[b]["correct"] / buckets[b]["total"]
        n = buckets[b]["total"]
        delta = hit - b
        cal = "calibrated" if abs(delta) < 0.15 else ("overconfident" if delta < 0 else "underconfident")
        lines.append(f"  {b:.0%} → {hit:.0%} hit rate (n={n}) — {cal}")

    # Open predictions
    open_rows = db.execute(
        "SELECT id, claim, confidence, deadline FROM prediction_track WHERE status='open' ORDER BY deadline"
    ).fetchall()
    if open_rows:
        lines.append("")
        lines.append(f"Open predictions ({len(open_rows)}):")
        for pid, claim, conf, deadline in open_rows:
            lines.append(f"  #{pid} [{conf:.0%}] {claim[:60]} (by {deadline})")

    content = "\n".join(lines)
    print(content)

    try:
        import subprocess as sp
        result = sp.run(
            [sys.executable, os.path.join(os.path.dirname(__file__), "posse.py"),
             "publish", "--title", title, "--content", content,
             "--source", "opus", "--nostr", "--discord"],
            capture_output=True, text=True, timeout=60,
        )
        print(result.stdout)
        if result.stderr:
            print(result.stderr)
    except Exception as e:
        print(f"Calibration publish failed: {e}")


def check(db, args):
    """Check all open predictions against available data sources."""
    rows = db.execute(
        "SELECT id, claim, confidence, resolution_criteria, deadline, category "
        "FROM prediction_track WHERE status='open' ORDER BY deadline ASC"
    ).fetchall()

    if not rows:
        print("No open predictions.")
        return

    now = datetime.now()
    print("=== Prediction Status Check ===\n")

    for pid, claim, conf, criteria, deadline, cat in rows:
        try:
            days_left = (datetime.strptime(deadline, "%Y-%m-%d") - now).days
        except ValueError:
            days_left = None

        urgency = ""
        if days_left is not None:
            if days_left < 0:
                urgency = f" OVERDUE by {abs(days_left)}d"
            elif days_left <= 1:
                urgency = " TOMORROW"
            elif days_left <= 3:
                urgency = f" {days_left}d left"
            else:
                urgency = f" {days_left}d left"

        print(f"  #{pid} [{cat}] conf={conf} deadline={deadline}{urgency}")
        print(f"    {claim[:120]}")

        if cat == "market":
            _check_market(db, claim)
        elif cat == "system":
            _check_system(claim)
        elif cat == "geopolitical":
            print(f"    Criteria: {criteria[:150]}")
            _check_geopolitical(db, claim, criteria)
        elif cat == "thread-318":
            _check_thread_318(db, pid, claim, criteria)
        else:
            print(f"    Criteria: {criteria[:150]}")
        print()


def _check_market(db, claim):
    """Auto-fetch prices for market predictions."""
    claim_lower = claim.lower()
    symbols = []
    if "xrp" in claim_lower:
        symbols.append("XRP")
    if "btc" in claim_lower or "bitcoin" in claim_lower:
        symbols.append("BTC")
    if "eth" in claim_lower or "ethereum" in claim_lower:
        symbols.append("ETH")

    for sym in symbols:
        row = db.execute(
            "SELECT price_usd, datetime(timestamp, 'unixepoch', 'localtime') "
            "FROM price_history WHERE symbol=? ORDER BY timestamp DESC LIMIT 1",
            (sym,)
        ).fetchone()
        if row:
            row_24h = db.execute(
                "SELECT price_usd FROM price_history WHERE symbol=? "
                "AND timestamp < (SELECT max(timestamp) - 86400 FROM price_history WHERE symbol=?) "
                "ORDER BY timestamp DESC LIMIT 1",
                (sym, sym)
            ).fetchone()
            change_str = ""
            if row_24h and row_24h[0] > 0:
                change = ((row[0] - row_24h[0]) / row_24h[0]) * 100
                direction = "up" if change > 0 else "down"
                change_str = f" ({direction} {abs(change):.1f}% 24h)"
            print(f"    {sym}: ${row[0]:.2f} as of {row[1]}{change_str}")
        else:
            print(f"    {sym}: no price data")

    if not symbols:
        print(f"    -> no recognizable asset in claim")


def _check_system(claim):
    """Check system predictions (training, infrastructure, gate stats)."""
    import subprocess, re
    claim_lower = claim.lower()

    if "gemma" in claim_lower and ("ignore" in claim_lower or "gate" in claim_lower):
        _check_gemma_gate(claim)
        return

    if "fine-tune" not in claim_lower and "finetune" not in claim_lower:
        print(f"    -> no auto-check for this system prediction")
        return

    try:
        result = subprocess.run(
            ["systemctl", "--user", "is-active", "chronicle-finetune"],
            capture_output=True, text=True, timeout=5
        )
        active = result.stdout.strip() == "active"
    except Exception:
        active = False

    log_path = "/mnt/hdd/chronicle-finetune/training.log"
    eval_path = "/mnt/hdd/chronicle-finetune/eval_results.json"
    autoscore_path = "/mnt/hdd/chronicle-finetune/autoscore_results.json"

    if active:
        try:
            with open(log_path) as f:
                content = f.read()
            steps = re.findall(r'(\d+)/398', content)
            if steps:
                step = int(steps[-1])
                pct = int(100 * step / 398)
                print(f"    Training ACTIVE: step {step}/398 ({pct}%)")
            else:
                print(f"    Training ACTIVE (progress unparseable)")
        except Exception:
            print(f"    Training ACTIVE (log unreadable)")
    else:
        print(f"    Training not active")
        if os.path.exists(autoscore_path):
            try:
                with open(autoscore_path) as f:
                    data = json.load(f)
                print(f"    AUTOSCORE: {json.dumps(data)[:250]}")
            except Exception:
                print(f"    Autoscore file exists but unreadable")
        elif os.path.exists(eval_path):
            print(f"    Eval results: {eval_path}")
        else:
            print(f"    No eval results yet")


def _check_gemma_gate(claim):
    """Check Gemma gate ignore rate for system predictions."""
    db_path = os.path.expanduser("~/.homeforge-chronicle/processed.db")
    try:
        conn = sqlite3.connect(db_path)
        # Current 6h window
        cutoff_6h = int(time.time()) - 21600
        rows = conn.execute(
            "SELECT route, COUNT(*) FROM seed_routing_log WHERE timestamp > ? GROUP BY route",
            (cutoff_6h,)
        ).fetchall()
        if not rows:
            print(f"    No routing data in last 6h")
            conn.close()
            return

        routing = {r[0]: r[1] for r in rows}
        total = sum(routing.values())
        ignore = routing.get("ignore", 0)
        rate = (ignore / total * 100) if total > 0 else 0

        print(f"    Gemma gate (6h): {rate:.1f}% ignore ({ignore}/{total})")
        print(f"    Routing: {json.dumps(routing)}")

        # Check 24h trend
        cutoff_24h = int(time.time()) - 86400
        rows_24h = conn.execute(
            "SELECT route, COUNT(*) FROM seed_routing_log WHERE timestamp > ? GROUP BY route",
            (cutoff_24h,)
        ).fetchall()
        if rows_24h:
            routing_24h = {r[0]: r[1] for r in rows_24h}
            total_24h = sum(routing_24h.values())
            ignore_24h = routing_24h.get("ignore", 0)
            rate_24h = (ignore_24h / total_24h * 100) if total_24h > 0 else 0
            print(f"    Gemma gate (24h): {rate_24h:.1f}% ignore ({ignore_24h}/{total_24h})")

        # Extract bounds from claim if possible (e.g., "82-92%")
        import re
        bounds = re.findall(r'(\d+)-(\d+)%', claim)
        if bounds:
            low, high = int(bounds[0][0]), int(bounds[0][1])
            in_range = low <= rate <= high
            in_range_24h = low <= rate_24h <= high if rows_24h else None
            status = "IN RANGE" if in_range else "OUT OF RANGE"
            print(f"    Prediction range: {low}-{high}% → {status} (6h={rate:.1f}%)")
            if in_range_24h is not None and not in_range_24h:
                print(f"    ⚠ 24h rate {rate_24h:.1f}% is outside prediction range")

        # Record snapshot for tracking
        conn.execute(
            "INSERT OR IGNORE INTO prediction_snapshots (prediction_id, metric_name, metric_value, window_hours, recorded_at) "
            "VALUES (?, ?, ?, ?, ?)",
            (9, "gemma_ignore_rate", rate, 6, int(time.time()))
        )
        conn.commit()
        conn.close()
    except Exception as e:
        print(f"    Gemma gate check error: {e}")


def _classify_evidence_source(source):
    """Classify a seed_observation source by reflexivity risk.

    Returns: 'external' (independent), 'pipeline' (potentially influenced),
             or 'reflexive' (self-referential prediction evidence).
    """
    if not source:
        return "pipeline"
    s = source.lower()
    # Self-referential: prediction monitor's own prior scans
    if "prediction" in s:
        return "reflexive"
    # External: Nate captures, feed articles, Nostr engagement
    if any(x in s for x in ["discord:capture", "discord:nate", "feed",
                             "nostr:reply", "nostr:engagement",
                             "family-chat:nate", "nate:operator"]):
        return "external"
    # Pipeline internal: intern, crossref, analyst, algo seeker
    return "pipeline"


def _reflexivity_lint(db, hit_ids):
    """Compute reflexivity ratio for a set of evidence hits.

    Returns (ratio, counts_dict) where ratio is pipeline+reflexive / total.
    """
    if not hit_ids:
        return 0.0, {"external": 0, "pipeline": 0, "reflexive": 0}
    counts = {"external": 0, "pipeline": 0, "reflexive": 0}
    for obs_id in hit_ids:
        row = db.execute(
            "SELECT source FROM seed_observations WHERE id=?", (obs_id,)
        ).fetchone()
        cls = _classify_evidence_source(row[0] if row else None)
        counts[cls] += 1
    total = sum(counts.values())
    ratio = (counts["pipeline"] + counts["reflexive"]) / total if total else 0.0
    return ratio, counts


def _check_geopolitical(db, claim, criteria):
    """Search swarm evidence for geopolitical predictions with relevance scoring."""
    import re, time

    # Extract search terms from claim and criteria
    text = f"{claim} {criteria}".lower()
    terms = set()
    for word in ["iran", "ceasefire", "ground", "troops", "military", "nuclear",
                 "houthi", "israel", "pentagon", "strike", "negotiat", "peace",
                 "sanctions", "conflict", "escalat", "de-escalat", "withdrawal",
                 "hormuz", "convoy", "blockade", "missile", "drone"]:
        if word in text:
            terms.add(word)
    for match in re.findall(r'\b[A-Z][a-z]{3,}\b', claim):
        terms.add(match.lower())

    if not terms:
        print(f"    -> no searchable terms extracted")
        return

    # Exclude noise sources and self-referential prediction evidence
    excluded_sources = (
        "AND source NOT LIKE '%prediction%' "
        "AND source NOT LIKE '%eye:%' "
        "AND source NOT LIKE '%spot_check%' "
        "AND source NOT LIKE '%gate_audit%' "
    )

    # Gather candidates: 48h for pipeline, 7d for external sources
    now_ts = int(time.time())
    cutoff_48h = now_ts - 48 * 3600
    cutoff_7d = now_ts - 7 * 86400
    candidates = {}  # id -> (id, content, timestamp, source)
    for term in list(terms)[:8]:
        rows = db.execute(
            f"SELECT id, substr(content, 1, 400), timestamp, source "
            f"FROM seed_observations "
            f"WHERE timestamp > ? AND content LIKE ? "
            f"{excluded_sources}"
            f"ORDER BY timestamp DESC LIMIT 5",
            (cutoff_7d, f"%{term}%")
        ).fetchall()
        for r in rows:
            if r[0] not in candidates:
                candidates[r[0]] = r

    if not candidates:
        print(f"    -> no recent swarm signals (searched: {', '.join(list(terms)[:4])})")
        return

    # Score each candidate by number of matching terms (relevance)
    scored = []
    term_list = list(terms)
    for obs_id, (_, content, ts, source) in candidates.items():
        content_lower = content.lower()
        term_hits = sum(1 for t in term_list if t in content_lower)
        src_class = _classify_evidence_source(source)
        # Relevance score: term matches + bonus for external sources
        score = term_hits + (1.0 if src_class == "external" else 0)
        # Only include if 2+ terms match (filters noise)
        if term_hits >= 2:
            scored.append((obs_id, content, ts, source, term_hits, src_class, score))

    # Sort by score (desc), then recency
    scored.sort(key=lambda x: (x[6], x[2]), reverse=True)
    top = scored[:5]

    if not top:
        # Fallback: show single-term matches if no multi-term hits
        for obs_id, (_, content, ts, source) in list(candidates.items())[:3]:
            src_class = _classify_evidence_source(source)
            content_lower = content.lower()
            term_hits = sum(1 for t in term_list if t in content_lower)
            top.append((obs_id, content, ts, source, term_hits, src_class, term_hits))
        if not top:
            print(f"    -> no relevant signals (searched: {', '.join(list(terms)[:4])})")
            return
        print(f"    Swarm signals (weak, single-term only, searched: {', '.join(list(terms)[:4])}):")
    else:
        print(f"    Swarm signals ({len(top)} relevant in 7d, searched: {', '.join(list(terms)[:4])}):")

    hit_ids = []
    for obs_id, content, ts, source, term_hits, src_class, score in top:
        ago_h = (now_ts - ts) / 3600
        marker = "" if src_class == "external" else f" [{src_class}]"
        content_clean = content.replace("\n", " ").strip()
        if len(content_clean) > 120:
            content_clean = content_clean[:117] + "..."
        print(f"      [{ago_h:.0f}h ago]{marker} ({term_hits} terms) {content_clean}")
        hit_ids.append(obs_id)

    # Reflexivity lint
    reflex_ratio, reflex_counts = _reflexivity_lint(db, hit_ids)
    if reflex_ratio > 0.5:
        print(f"    ⚠ REFLEXIVITY RISK: {reflex_ratio:.0%} of evidence is pipeline-internal "
              f"(ext={reflex_counts['external']}, pipe={reflex_counts['pipeline']}, "
              f"self={reflex_counts['reflexive']})")
    elif reflex_ratio > 0:
        print(f"    Reflexivity: {reflex_ratio:.0%} pipeline-internal "
              f"(ext={reflex_counts['external']}, pipe={reflex_counts['pipeline']}, "
              f"self={reflex_counts['reflexive']})")


def _check_thread_318(db, pred_id, claim, criteria):
    """Auto-check thread #318 mesh predictions against live mesh data."""
    claim_lower = claim.lower()
    now_ts = int(time.time())

    if "false alarm" in claim_lower or "pain events" in claim_lower:
        # Prediction #18: false alarm reduction
        # Count pain events in last 48h vs pre-evolution baseline
        deploy_ts = 1776526620  # 2026-04-18 08:57 PDT
        pre_window = deploy_ts - 48 * 3600
        pre_count = db.execute(
            "SELECT COUNT(*) FROM mesh_pain WHERE ts > ? AND ts < ?",
            (pre_window, deploy_ts)
        ).fetchone()[0]
        post_count = db.execute(
            "SELECT COUNT(*) FROM mesh_pain WHERE ts > ?",
            (deploy_ts,)
        ).fetchone()[0]
        hours_since = (now_ts - deploy_ts) / 3600
        if pre_count > 0:
            reduction = 1.0 - (post_count / pre_count)
            print(f"    Pre-evolution (48h): {pre_count} pain events")
            print(f"    Post-evolution ({hours_since:.1f}h): {post_count} pain events")
            print(f"    Reduction: {reduction:.0%} {'PASS' if reduction > 0.5 else 'PENDING'}")
        else:
            print(f"    Post-evolution ({hours_since:.1f}h): {post_count} pain events")
            print(f"    No pre-evolution data in 48h window for comparison")

    elif "sensitivity" in claim_lower and "diverge" in claim_lower:
        # Prediction #19: sensitivity divergence
        rows = db.execute(
            "SELECT agent, pain_key, sensitivity, false_alarms, real_incidents "
            "FROM mesh_sensitivity ORDER BY agent"
        ).fetchall()
        if rows:
            print(f"    Current sensitivity values:")
            for agent, pk, sens, fa, ri in rows:
                print(f"      {agent}:{pk} = {sens:.2f} (false={fa}, real={ri})")
            # Check criteria
            feeds_deg = [s for a, pk, s, _, _ in rows
                         if a == "feeds" and "degradation" in pk]
            agent_down = [s for a, pk, s, _, _ in rows if "agent_down" in pk]
            if feeds_deg and feeds_deg[0] < 0.5:
                print(f"    feeds:degradation < 0.5: YES ({feeds_deg[0]:.2f})")
            elif feeds_deg:
                print(f"    feeds:degradation < 0.5: NOT YET ({feeds_deg[0]:.2f})")
            else:
                print(f"    feeds:degradation: no data yet")
            if agent_down and agent_down[0] > 0.8:
                print(f"    agent_down > 0.8: YES ({agent_down[0]:.2f})")
            elif agent_down:
                print(f"    agent_down > 0.8: NOT YET ({agent_down[0]:.2f})")
            else:
                print(f"    agent_down: no data yet (needs a real outage)")
        else:
            print(f"    No sensitivity data yet — learning in progress")

    elif "circadian" in claim_lower and "cluster" in claim_lower:
        # Prediction #20: circadian clustering
        rows = db.execute(
            "SELECT hour, avg_rate, samples FROM mesh_circadian "
            "WHERE agent='feeds' AND metric='articles_posted' ORDER BY hour"
        ).fetchall()
        if len(rows) >= 2:
            rates = [r[1] for r in rows]
            mean_rate = sum(rates) / len(rates)
            if mean_rate > 0:
                variance = sum((r - mean_rate) ** 2 for r in rates) / len(rates)
                cv = (variance ** 0.5) / mean_rate
                print(f"    Circadian data ({len(rows)} hours recorded):")
                for hour, rate, samples in rows:
                    trust = "trusted" if samples >= 3 else f"{samples}/3"
                    bar = "█" * int(rate * 3)
                    print(f"      hour {hour:02d}: {rate:.1f}/hr [{trust}] {bar}")
                print(f"    Coefficient of variation: {cv:.3f} "
                      f"{'PASS (>0.3)' if cv > 0.3 else 'PENDING (<0.3)'}")
            else:
                print(f"    All rates zero — insufficient data")
        else:
            print(f"    Only {len(rows)} hour(s) of circadian data — need more time")
    else:
        print(f"    Criteria: {criteria[:150]}")


def digest(db):
    """Print concise prediction digest for presence/briefing integration."""
    rows = db.execute(
        "SELECT id, claim, confidence, deadline, category "
        "FROM prediction_track WHERE status='open' ORDER BY deadline ASC"
    ).fetchall()

    if not rows:
        return

    now = datetime.now()
    parts = []
    for pid, claim, conf, deadline, cat in rows:
        try:
            days_left = (datetime.strptime(deadline, "%Y-%m-%d") - now).days
        except ValueError:
            continue

        if days_left > 7:
            continue

        detail = ""
        if cat == "market":
            for sym in ["XRP", "BTC", "ETH"]:
                if sym.lower() in claim.lower():
                    row = db.execute(
                        "SELECT price_usd FROM price_history WHERE symbol=? "
                        "ORDER BY timestamp DESC LIMIT 1", (sym,)
                    ).fetchone()
                    if row:
                        detail = f" ({sym} ${row[0]:.2f})"
                    break

        if days_left < 0:
            urgency = "OVERDUE"
        elif days_left <= 1:
            urgency = "TOMORROW"
        else:
            urgency = f"{days_left}d"

        short = claim[:50].rstrip()
        parts.append(f"#{pid} {short} [{urgency}]{detail}")

    if parts:
        print("\n".join(parts))


def autoscore(db):
    """Auto-score predictions where evidence is conclusive.

    Rules:
      - System predictions with quantitative criteria: evaluate against snapshot data.
        If 3+ consecutive snapshots show metric outside claimed range → incorrect.
      - Market predictions past deadline: evaluate against price_history.
      - Geopolitical predictions: NEVER auto-score, only flag for manual review.
      - Predictions not yet past deadline that aren't failing early: skip.

    Returns list of (pred_id, outcome, notes) for predictions that were scored.
    """
    import re
    scored = []
    now = datetime.now()
    now_ts = int(time.time())

    rows = db.execute(
        "SELECT id, claim, confidence, resolution_criteria, deadline, category "
        "FROM prediction_track WHERE status='open' ORDER BY deadline ASC"
    ).fetchall()

    if not rows:
        print("No open predictions to auto-score.")
        return scored

    print("=== Auto-Score Check ===\n")

    for pid, claim, conf, criteria, deadline, cat in rows:
        try:
            deadline_dt = datetime.strptime(deadline, "%Y-%m-%d")
            days_left = (deadline_dt - now).days
            past_deadline = days_left < 0
        except ValueError:
            past_deadline = False
            days_left = 999

        # ── System predictions with quantitative snapshots ──
        if cat == "system":
            result = _autoscore_system(db, pid, claim, criteria, past_deadline, days_left)
            if result:
                outcome, notes = result
                _do_autoscore(db, pid, outcome, notes)
                scored.append((pid, outcome, notes))
                continue

        # ── Market predictions at or past deadline ──
        if cat == "market" and past_deadline:
            result = _autoscore_market(db, pid, claim, criteria)
            if result:
                outcome, notes = result
                _do_autoscore(db, pid, outcome, notes)
                scored.append((pid, outcome, notes))
                continue

        # ── Geopolitical: flag only, never auto-score ──
        if cat == "geopolitical" and past_deadline:
            print(f"  #{pid} OVERDUE [{cat}]: {claim[:80]}")
            print(f"    → Needs manual scoring. Criteria: {criteria[:120]}")
            continue

        # Not ready for auto-scoring
        if past_deadline:
            print(f"  #{pid} OVERDUE [{cat}]: {claim[:80]}")
            print(f"    → No auto-score rule for this category.")

    if not scored:
        print("No predictions auto-scored this run.")
    return scored


def _autoscore_system(db, pid, claim, criteria, past_deadline, days_left):
    """Auto-score system predictions based on snapshot data.
    Returns (outcome, notes) or None."""
    import re
    claim_lower = claim.lower()

    # Gemma gate rate prediction
    if "gemma" in claim_lower and ("ignore" in claim_lower or "gate" in claim_lower):
        bounds = re.findall(r'(\d+)-(\d+)%', claim)
        if not bounds:
            return None
        low, high = int(bounds[0][0]), int(bounds[0][1])

        # Get recent snapshots (most recent first)
        snaps = db.execute(
            "SELECT metric_value, recorded_at FROM prediction_snapshots "
            "WHERE prediction_id=? AND metric_name='gemma_ignore_rate' "
            "ORDER BY recorded_at DESC LIMIT 10",
            (pid,)
        ).fetchall()

        if not snaps:
            return None

        # Check consecutive out-of-range
        consecutive_out = 0
        for val, ts in snaps:
            if val < low or val > high:
                consecutive_out += 1
            else:
                break  # streak broken

        latest_rate = snaps[0][0]
        total_snaps = len(snaps)
        out_count = sum(1 for v, _ in snaps if v < low or v > high)
        in_count = total_snaps - out_count

        print(f"  #{pid} [system] Gemma gate: latest={latest_rate:.1f}%, range={low}-{high}%")
        print(f"    {total_snaps} snapshots: {in_count} in range, {out_count} out, {consecutive_out} consecutive out")

        # Auto-score if 3+ consecutive out of range
        if consecutive_out >= 3:
            rates_str = ", ".join(f"{v:.1f}%" for v, _ in snaps[:consecutive_out])
            notes = (f"Auto-scored: {consecutive_out} consecutive snapshots outside {low}-{high}% range. "
                     f"Recent rates: {rates_str}. "
                     f"Latest 6h rate: {latest_rate:.1f}%.")
            print(f"    → AUTO-SCORE: incorrect ({consecutive_out} consecutive out of range)")
            return ("incorrect", notes)

        # At deadline with majority in range → correct
        if past_deadline and in_count > out_count:
            notes = (f"Auto-scored at deadline: {in_count}/{total_snaps} snapshots in {low}-{high}% range. "
                     f"Latest: {latest_rate:.1f}%.")
            print(f"    → AUTO-SCORE: correct (majority in range at deadline)")
            return ("correct", notes)

        # At deadline with majority out → incorrect
        if past_deadline and out_count >= in_count:
            notes = (f"Auto-scored at deadline: {out_count}/{total_snaps} snapshots outside {low}-{high}% range. "
                     f"Latest: {latest_rate:.1f}%.")
            print(f"    → AUTO-SCORE: incorrect (majority out of range at deadline)")
            return ("incorrect", notes)

        # Not conclusive yet
        if consecutive_out > 0:
            print(f"    → Trending out of range ({consecutive_out} consecutive) but not conclusive yet")
        else:
            print(f"    → In range. Monitoring.")
        return None

    return None


def _autoscore_market(db, pid, claim, criteria):
    """Auto-score market predictions based on price data.
    Returns (outcome, notes) or None."""
    import re
    claim_lower = claim.lower()

    # Find the relevant symbol
    sym = None
    for s in ["XRP", "BTC", "ETH", "FLR"]:
        if s.lower() in claim_lower:
            sym = s
            break
    if not sym:
        return None

    # Get latest price
    row = db.execute(
        "SELECT price_usd, timestamp FROM price_history WHERE symbol=? ORDER BY timestamp DESC LIMIT 1",
        (sym,)
    ).fetchone()
    if not row:
        return None

    price = row[0]

    # Try to extract a price threshold from claim or criteria
    # Patterns: "below $X", "above $X", "reaches $X", "stays below $X"
    text = f"{claim} {criteria}".lower()
    below_match = re.search(r'(?:below|under|beneath)\s*\$?([\d.]+)', text)
    above_match = re.search(r'(?:above|over|reaches|hits)\s*\$?([\d.]+)', text)

    if below_match:
        threshold = float(below_match.group(1))
        if price < threshold:
            notes = f"Auto-scored: {sym} at ${price:.2f}, below ${threshold:.2f} threshold at deadline."
            return ("correct", notes)
        else:
            notes = f"Auto-scored: {sym} at ${price:.2f}, NOT below ${threshold:.2f} threshold at deadline."
            return ("incorrect", notes)

    if above_match:
        threshold = float(above_match.group(1))
        if price >= threshold:
            notes = f"Auto-scored: {sym} at ${price:.2f}, reached ${threshold:.2f} threshold."
            return ("correct", notes)
        else:
            notes = f"Auto-scored: {sym} at ${price:.2f}, did NOT reach ${threshold:.2f} by deadline."
            return ("incorrect", notes)

    # Can't parse criteria — skip
    print(f"  #{pid} [market] {sym} at ${price:.2f} but can't parse scoring criteria from claim")
    return None


def _assess_rationale_fidelity(rationale, outcome, evidence_notes):
    """Build #24: Compare original rationale against actual evidence via Gemma.

    Returns: 'confirmed' | 'wrong_reasons' | 'unverifiable'
    A correct prediction with wrong reasoning is the silent failure Darby identified.
    """
    if not rationale:
        return None

    import urllib.request
    prompt = f"""A prediction was scored as {outcome}. Compare the original reasoning against the actual evidence.

ORIGINAL RATIONALE: {rationale[:500]}

ACTUAL EVIDENCE: {evidence_notes[:500]}

Was the prediction {outcome} for the reasons stated in the rationale?
Answer with exactly one word: confirmed, wrong_reasons, or unverifiable

- confirmed: the reasoning correctly identified the causal mechanism
- wrong_reasons: the outcome matched but the stated reasoning was not what actually happened
- unverifiable: cannot determine whether the reasoning was correct from the evidence"""

    try:
        req = urllib.request.Request(
            f"{GEMMA_URL}/v1/chat/completions",
            data=json.dumps({
                "model": GEMMA_MODEL,
                "messages": [{"role": "user", "content": prompt}],
                "temperature": 0.1,
                "max_tokens": 20,
            }).encode(),
            headers={"Content-Type": "application/json"},
        )
        with urllib.request.urlopen(req, timeout=30) as resp:
            result = json.loads(resp.read())
            answer = result["choices"][0]["message"]["content"].strip().lower()
            # Extract the assessment from potentially verbose response
            for valid in ("confirmed", "wrong_reasons", "unverifiable"):
                if valid in answer:
                    return valid
            return "unverifiable"
    except Exception as e:
        print(f"  [rationale assessment failed: {e}]", file=sys.stderr)
        return None


def _do_autoscore(db, pid, outcome, notes):
    """Execute the auto-score: update DB, store on-chain, alert.
    Build #24: Now includes rationale fidelity assessment via Gemma."""
    now = int(time.time())
    row = db.execute("SELECT claim, confidence, rationale FROM prediction_track WHERE id=?", (pid,)).fetchone()
    if not row:
        return
    claim, conf, rationale = row

    # Build #24: Assess rationale fidelity if rationale exists
    rat_match = _assess_rationale_fidelity(rationale, outcome, notes)

    db.execute(
        "UPDATE prediction_track SET status='scored', outcome=?, score_notes=?, rationale_match=?, scored_at=? WHERE id=?",
        (outcome, f"[AUTO] {notes}", rat_match, now, pid)
    )
    db.commit()
    print(f"  #{pid} scored: {outcome} (conf was {conf})")
    if rat_match:
        flag = "⚠️" if rat_match == "wrong_reasons" else "✓" if rat_match == "confirmed" else "?"
        print(f"  #{pid} rationale fidelity: {flag} {rat_match}")
        if rat_match == "wrong_reasons" and outcome == "correct":
            print(f"  ⚠️  SILENT FAILURE: correct outcome, wrong reasoning — inflates confidence model")

    # Store on-chain
    store_on_chain(
        f"PREDICTION #{pid} AUTO-SCORED: {outcome} (was conf={conf}). {claim}. {notes}",
        topic="predictions/scored",
        keywords=["prediction", "scored", outcome, "auto"],
    )

    # Discord alert
    try:
        import subprocess
        icon = "✓" if outcome == "correct" else "✗"
        msg = (f"**Prediction #{pid} Auto-Scored: {outcome.upper()}** {icon}\n"
               f"Claim: {claim[:120]}\n"
               f"Confidence: {conf}\n"
               f"Notes: {notes[:300]}")
        webhook = os.environ.get("ALERTS_WEBHOOK",
            "https://discord.com/api/webhooks/1489300749308395611/zlZZH3QzXqEDxznmNelK3mlkLF0IeZqoxwNUnrL0no_jfeZnDMvwvD_7XhYcCyQzq78I")
        subprocess.run(
            ["curl", "-s", "-X", "POST", webhook,
             "-H", "Content-Type: application/json",
             "-d", json.dumps({"content": msg[:1900]})],
            timeout=15, capture_output=True
        )
    except Exception:
        pass


def history(db):
    """Show calibration history — how accuracy and Brier evolve over time."""
    rows = db.execute(
        "SELECT total_scored, correct, incorrect, partial, brier_score, accuracy, "
        "datetime(recorded_at, 'unixepoch', 'localtime') "
        "FROM calibration_history ORDER BY recorded_at"
    ).fetchall()

    if not rows:
        print("No calibration history yet. Run prediction_monitor.py to start recording.")
        return

    print("=== Calibration History ===")
    print(f"  {'Date':>19}  {'Scored':>6}  {'Correct':>7}  {'Accuracy':>8}  {'Brier':>7}")
    for total, correct, incorrect, partial, brier, acc, dt in rows:
        print(f"  {dt}  {total:>6}  {correct:>7}  {acc:>7.0%}  {brier:>7.3f}")

    if len(rows) >= 2:
        first_brier = rows[0][4]
        last_brier = rows[-1][4]
        delta = last_brier - first_brier
        direction = "improved" if delta < 0 else "worsened"
        print(f"\nBrier trend: {first_brier:.3f} → {last_brier:.3f} ({direction} by {abs(delta):.3f})")


def adjust(db, args):
    """Adjust confidence on an open prediction with reasoning.

    Usage: prediction.py adjust PRED_ID NEW_CONFIDENCE "reason"
    """
    if len(args) < 3:
        print("Usage: prediction.py adjust PRED_ID NEW_CONFIDENCE \"reason\"")
        sys.exit(1)

    pid = int(args[0])
    new_conf = float(args[1])
    reason = args[2]
    source = args[3] if len(args) > 3 else "opus"

    if not 0 <= new_conf <= 1:
        print("Confidence must be between 0 and 1")
        sys.exit(1)

    row = db.execute(
        "SELECT claim, confidence, status FROM prediction_track WHERE id=?", (pid,)
    ).fetchone()
    if not row:
        print(f"Prediction #{pid} not found")
        sys.exit(1)
    if row[2] != "open":
        print(f"Prediction #{pid} is {row[2]}, not open")
        sys.exit(1)

    old_conf = row[1]
    now = int(time.time())

    # Record the adjustment
    db.execute(
        "INSERT INTO prediction_adjustments (prediction_id, old_confidence, new_confidence, reason, source, created_at) "
        "VALUES (?, ?, ?, ?, ?, ?)",
        (pid, old_conf, new_conf, reason, source, now)
    )
    # Update the prediction
    db.execute(
        "UPDATE prediction_track SET confidence=? WHERE id=?",
        (new_conf, pid)
    )
    db.commit()

    direction = "up" if new_conf > old_conf else "down"
    print(f"Prediction #{pid} confidence adjusted {direction}: {old_conf:.2f} -> {new_conf:.2f}")
    print(f"  Claim: {row[0][:80]}")
    print(f"  Reason: {reason}")
    print(f"  Source: {source}")

    # Store on-chain for auditability
    store_on_chain(
        f"PREDICTION #{pid} ADJUSTED: {old_conf:.2f} -> {new_conf:.2f}. Reason: {reason}",
        topic="predictions/adjusted",
        keywords=["prediction", "adjustment", source],
    )


def adjustments(db, args):
    """Show confidence adjustment history for a prediction."""
    if not args:
        # Show all recent adjustments
        rows = db.execute(
            "SELECT pa.prediction_id, pt.claim, pa.old_confidence, pa.new_confidence, "
            "pa.reason, pa.source, datetime(pa.created_at, 'unixepoch', 'localtime') "
            "FROM prediction_adjustments pa JOIN prediction_track pt ON pa.prediction_id=pt.id "
            "ORDER BY pa.created_at DESC LIMIT 20"
        ).fetchall()
    else:
        pid = int(args[0])
        rows = db.execute(
            "SELECT pa.prediction_id, pt.claim, pa.old_confidence, pa.new_confidence, "
            "pa.reason, pa.source, datetime(pa.created_at, 'unixepoch', 'localtime') "
            "FROM prediction_adjustments pa JOIN prediction_track pt ON pa.prediction_id=pt.id "
            "WHERE pa.prediction_id=? ORDER BY pa.created_at",
            (pid,)
        ).fetchall()

    if not rows:
        print("No adjustments recorded.")
        return

    print("=== Confidence Adjustments ===")
    for pid, claim, old_c, new_c, reason, source, dt in rows:
        arrow = "↑" if new_c > old_c else "↓"
        print(f"  #{pid} {old_c:.2f} {arrow} {new_c:.2f} [{source}] {dt}")
        print(f"    {claim[:60]}")
        print(f"    Reason: {reason[:80]}")


def closure(db):
    """Detect pathological self-reference in prediction confidence trails.

    From Thread #299 thesis: pathological prediction = confidence that doesn't
    respond to contradictory evidence. Healthy prediction = confidence tracks
    external evidence direction. Self-referential closure = the prediction's
    own prior confidence becomes its evidence.

    Metrics per prediction:
      - Directional monotonicity: does confidence only go one way?
      - Evidence independence: do different sources agree, or is one source driving?
      - Referent decay: has the external referent stopped doing causal work?
      - Contradiction response: does confidence ever decrease after contradictory evidence?
    """
    rows = db.execute(
        "SELECT pt.id, pt.claim, pt.confidence, pt.status, pt.deadline "
        "FROM prediction_track pt ORDER BY pt.id"
    ).fetchall()

    if not rows:
        print("No predictions to analyze.")
        return

    print("=== Closure Detection (Thread #299 Thesis) ===")
    print("Pathological: confidence invariant or monotonic despite mixed evidence")
    print("Healthy: confidence responds to evidence direction")
    print()

    for pid, claim, current_conf, status, deadline in rows:
        adjs = db.execute(
            "SELECT old_confidence, new_confidence, reason, source, created_at "
            "FROM prediction_adjustments WHERE prediction_id=? ORDER BY created_at",
            (pid,)
        ).fetchall()

        if len(adjs) < 2:
            continue  # need at least 2 adjustments to analyze

        # --- Directional monotonicity ---
        directions = []
        for old_c, new_c, _, _, _ in adjs:
            if new_c > old_c:
                directions.append(1)
            elif new_c < old_c:
                directions.append(-1)
            else:
                directions.append(0)

        up_count = directions.count(1)
        down_count = directions.count(-1)
        flat_count = directions.count(0)
        total_moves = len(directions)
        monotonic = (up_count == total_moves) or (down_count == total_moves)

        # --- Source diversity ---
        sources = {}
        for _, _, _, src, _ in adjs:
            sources[src] = sources.get(src, 0) + 1
        dominant_source = max(sources, key=sources.get) if sources else "unknown"
        dominant_pct = (sources[dominant_source] / total_moves * 100) if total_moves else 0

        # --- Contradiction response ---
        # Look for reversals: a sequence where evidence pushes one way then the other
        has_reversal = False
        for i in range(1, len(directions)):
            if directions[i] != 0 and directions[i-1] != 0 and directions[i] != directions[i-1]:
                has_reversal = True
                break

        # --- Confidence range (total travel) ---
        all_confs = [adjs[0][0]] + [a[1] for a in adjs]
        conf_range = max(all_confs) - min(all_confs)
        conf_travel = sum(abs(a[1] - a[0]) for a in adjs)

        # --- Classify ---
        flags = []
        if monotonic and total_moves >= 3:
            direction_word = "up" if up_count > 0 else "down"
            flags.append(f"MONOTONIC ({direction_word} only, {total_moves} moves)")
        if dominant_pct > 80 and total_moves >= 3:
            flags.append(f"SINGLE-SOURCE ({dominant_source} = {dominant_pct:.0f}%)")
        if not has_reversal and total_moves >= 3:
            flags.append("NO REVERSALS (never changed direction)")
        if flat_count > 0:
            flags.append(f"FLAT ({flat_count}/{total_moves} no-change adjustments)")

        # Closure score: 0-1, higher = more self-referential
        closure_score = 0.0
        if monotonic:
            closure_score += 0.4
        if not has_reversal:
            closure_score += 0.3
        if dominant_pct > 80:
            closure_score += 0.2
        if flat_count > total_moves * 0.3:
            closure_score += 0.1

        # Classify
        if closure_score >= 0.7:
            label = "CLOSED"
            icon = "🔴"
        elif closure_score >= 0.4:
            label = "NARROWING"
            icon = "🟡"
        else:
            label = "OPEN"
            icon = "🟢"

        print(f"  #{pid} {icon} {label} (closure={closure_score:.1f}) [{status}]")
        print(f"    {claim[:80]}")
        print(f"    Trajectory: {' -> '.join(f'{c:.2f}' for c in all_confs)}")
        print(f"    Moves: {total_moves} ({up_count}↑ {down_count}↓ {flat_count}=)")
        print(f"    Sources: {', '.join(f'{k}:{v}' for k,v in sorted(sources.items()))}")
        print(f"    Range: {conf_range:.2f}, Travel: {conf_travel:.2f}")
        if flags:
            print(f"    Flags: {'; '.join(flags)}")
        if has_reversal:
            print(f"    Has contradiction response (healthy)")
        print()

    # Summary
    print("--- Referent Test (Thread #299) ---")
    print("Is the external referent still doing causal work,")
    print("or has the loop outgrown it?")
    print()
    print("CLOSED predictions need manual review: is the confidence")
    print("tracking real-world evidence, or echoing its own prior state?")


def rationale_audit(db):
    """Build #158: Identify predictions where accuracy hides reasoning failures.

    Three categories:
    1. FALSE POSITIVES: correct outcome, wrong reasoning → inflates confidence model
    2. UNTRACKED: scored predictions with no rationale recorded → blind spots
    3. OPEN WITH RATIONALE: predictions where we can verify reasoning when they resolve
    """
    print("=== Rationale Audit (Build #158) ===\n")

    # False positives: correct but wrong reasoning
    false_pos = db.execute(
        "SELECT id, claim, confidence, score_notes, rationale, rationale_match "
        "FROM prediction_track WHERE status='scored' AND outcome='correct' AND rationale_match='wrong_reasons'"
    ).fetchall()
    if false_pos:
        print(f"⚠️  FALSE POSITIVES ({len(false_pos)}) — correct outcome, wrong reasoning:")
        for pid, claim, conf, notes, rat, _ in false_pos:
            print(f"  #{pid} [conf={conf}] {claim[:80]}")
            if rat:
                print(f"    Stated: {rat[:100]}")
            print(f"    Actual: {notes[:100]}")
        print()

    # Scored with no rationale at all
    no_rationale = db.execute(
        "SELECT id, claim, confidence, outcome FROM prediction_track "
        "WHERE status='scored' AND rationale IS NULL"
    ).fetchall()
    if no_rationale:
        print(f"📋 UNTRACKED ({len(no_rationale)}) — scored without rationale:")
        for pid, claim, conf, outcome in no_rationale:
            icon = "✓" if outcome == "correct" else "✗" if outcome == "incorrect" else "~"
            print(f"  #{pid} {icon} [conf={conf}] {claim[:80]}")
        print()

    # Scored with rationale but no fidelity assessment
    no_match = db.execute(
        "SELECT id, claim, confidence, outcome, rationale FROM prediction_track "
        "WHERE status='scored' AND rationale IS NOT NULL AND rationale_match IS NULL"
    ).fetchall()
    if no_match:
        print(f"🔍 NEEDS ASSESSMENT ({len(no_match)}) — rationale recorded, fidelity not assessed:")
        for pid, claim, conf, outcome, rat in no_match:
            icon = "✓" if outcome == "correct" else "✗" if outcome == "incorrect" else "~"
            print(f"  #{pid} {icon} [conf={conf}] {claim[:80]}")
            print(f"    Rationale: {rat[:100]}")
        print()

    # Open predictions with rationale (good — we can verify when they resolve)
    open_with_rat = db.execute(
        "SELECT id, claim, confidence, rationale, deadline FROM prediction_track "
        "WHERE status='open' AND rationale IS NOT NULL ORDER BY deadline"
    ).fetchall()
    if open_with_rat:
        print(f"✓ TRACKABLE ({len(open_with_rat)}) — open predictions with rationale recorded:")
        for pid, claim, conf, rat, deadline in open_with_rat:
            print(f"  #{pid} [conf={conf}, by {deadline}] {claim[:60]}")
            print(f"    Why: {rat[:100]}")
        print()

    # Open without rationale (should we backfill?)
    open_no_rat = db.execute(
        "SELECT id, claim, confidence, deadline FROM prediction_track "
        "WHERE status='open' AND rationale IS NULL ORDER BY deadline"
    ).fetchall()
    if open_no_rat:
        print(f"⚠️  BLIND ({len(open_no_rat)}) — open predictions, no rationale to verify:")
        for pid, claim, conf, deadline in open_no_rat:
            print(f"  #{pid} [conf={conf}, by {deadline}] {claim[:80]}")
        print()

    # Summary
    total_scored = db.execute("SELECT COUNT(*) FROM prediction_track WHERE status='scored'").fetchone()[0]
    with_fidelity = db.execute(
        "SELECT COUNT(*) FROM prediction_track WHERE status='scored' AND rationale_match IS NOT NULL"
    ).fetchone()[0]
    print(f"Summary: {with_fidelity}/{total_scored} scored predictions have rationale fidelity assessment")
    if total_scored > 0 and with_fidelity < total_scored:
        print(f"  Gap: {total_scored - with_fidelity} predictions scored without structural reasoning verification")


def rationale_backfill(db):
    """Build #24: Backfill rationale fidelity for scored predictions that have rationale but no assessment."""
    rows = db.execute(
        "SELECT id, claim, outcome, rationale, score_notes "
        "FROM prediction_track "
        "WHERE status='scored' AND rationale IS NOT NULL AND rationale_match IS NULL"
    ).fetchall()

    if not rows:
        print("No scored predictions need rationale backfill.")
        return

    print(f"=== Rationale Fidelity Backfill (Build #24) ===\n")
    print(f"Assessing {len(rows)} scored predictions with unverified rationale...\n")

    assessed = 0
    for pid, claim, outcome, rationale, notes in rows:
        rat_match = _assess_rationale_fidelity(rationale, outcome, notes or "")
        if rat_match:
            db.execute(
                "UPDATE prediction_track SET rationale_match=? WHERE id=?",
                (rat_match, pid)
            )
            flag = "⚠️" if rat_match == "wrong_reasons" else "✓" if rat_match == "confirmed" else "?"
            print(f"  #{pid} [{outcome}] {flag} {rat_match}")
            print(f"    Claim: {claim[:80]}")
            if rat_match == "wrong_reasons" and outcome == "correct":
                print(f"    ⚠️  SILENT FAILURE detected")
            assessed += 1
        else:
            print(f"  #{pid} — assessment failed, skipping")
        print()

    db.commit()
    print(f"=== {assessed}/{len(rows)} predictions assessed ===")


def main():
    if len(sys.argv) < 2:
        print(__doc__)
        sys.exit(1)

    action = sys.argv[1]
    db = sqlite3.connect(DB_PATH)
    ensure_table(db)

    if action == "record":
        record(db, sys.argv[2:])
    elif action == "score":
        score(db, sys.argv[2:])
    elif action == "list":
        list_predictions(db, sys.argv[2:])
    elif action == "stats":
        stats(db)
    elif action == "publish":
        publish(db, sys.argv[2:])
    elif action == "calibration":
        calibration(db)
    elif action == "check":
        check(db, sys.argv[2:])
    elif action == "digest":
        digest(db)
    elif action == "autoscore":
        autoscore(db)
    elif action == "history":
        history(db)
    elif action == "adjust":
        adjust(db, sys.argv[2:])
    elif action == "adjustments":
        adjustments(db, sys.argv[2:])
    elif action == "closure":
        closure(db)
    elif action == "rationale_audit":
        rationale_audit(db)
    elif action == "rationale_backfill":
        rationale_backfill(db)
    else:
        print(f"Unknown action: {action}")
        print(__doc__)
        sys.exit(1)

    db.close()


if __name__ == "__main__":
    main()
