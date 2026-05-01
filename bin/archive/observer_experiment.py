#!/usr/bin/env python3
"""Observer Frame Experiment — Thread #268 hypothesis test.

Tests whether watcher score convergence is a property of the content
or a property of the measurement frame.

Method:
  1. Pull the last N scored items from watcher_scores (Frame A: relevance/novelty/quality)
  2. Re-score the same items with Frame B (surprise/actionability/durability)
  3. Compute cross-frame correlation
  4. If high correlation → signal is substrate-independent (genuine)
  5. If low correlation → observer frame creates the measurement (compliance)

This is a direct empirical test of the phi-spectral observer-relativity
hypothesis applied to our own multi-model evaluation system.
"""

import os, sys, json, time, sqlite3, requests, math
from collections import defaultdict

DB_PATH = os.environ.get("CHRONICLE_DB",
    os.path.expanduser("~/.homeforge-chronicle/processed.db"))
OLLAMA_URL = os.environ.get("WATCHER_OLLAMA_URL", "http://localhost:11435")
OLLAMA_MODEL = os.environ.get("WATCHER_MODEL", "gemma4:26b")

SAMPLE_SIZE = 20  # Score 20 recent items under both frames

def log(msg):
    print(f"[observer] {msg}", flush=True)

def score_frame_b(content, source, activity_type):
    """Score using Frame B — completely different dimensions."""
    prompt = f"""Score this agent output on three dimensions (1-5 each).

AGENT: {source}
TYPE: {activity_type}
OUTPUT: {content[:800]}

Score on:
- SURPRISE (1-5): Did this change your model of the world, even slightly? 1=completely expected, 5=genuinely did not see this coming.
- ACTIONABILITY (1-5): Could someone DO something differently after reading this? 1=pure information, 5=demands immediate action.
- DURABILITY (1-5): Will this still matter in 6 months? 1=ephemeral news cycle, 5=structural shift that compounds.

Scoring calibration: Most things are 1-2 on surprise. Most things are 1-2 on actionability. Be ruthless. A 4-5 should be rare.

Respond ONLY with JSON, nothing else:
{{"surprise": N, "actionability": N, "durability": N, "rationale": "one sentence"}}"""

    try:
        resp = requests.post(
            f"{OLLAMA_URL}/api/generate",
            json={
                "model": OLLAMA_MODEL,
                "prompt": prompt,
                "stream": False,
                "options": {"temperature": 0.1, "num_predict": 150}
            },
            timeout=120
        )
        text = resp.json().get("response", "").strip()
        if "```" in text:
            text = text.split("```")[1]
            if text.startswith("json"):
                text = text[4:]
        start = text.find("{")
        end = text.rfind("}") + 1
        if start >= 0 and end > start:
            data = json.loads(text[start:end])
            for key in ("surprise", "actionability", "durability"):
                data[key] = max(1, min(5, int(data.get(key, 3))))
            data["avg_b"] = round(
                (data["surprise"] + data["actionability"] + data["durability"]) / 3, 2
            )
            return data
    except Exception as e:
        log(f"Frame B scoring error: {e}")
    return None


def pearson_r(xs, ys):
    """Compute Pearson correlation coefficient."""
    n = len(xs)
    if n < 3:
        return None
    mx = sum(xs) / n
    my = sum(ys) / n
    sx = math.sqrt(sum((x - mx)**2 for x in xs) / n)
    sy = math.sqrt(sum((y - my)**2 for y in ys) / n)
    if sx == 0 or sy == 0:
        return 0.0
    cov = sum((x - mx) * (y - my) for x, y in zip(xs, ys)) / n
    return round(cov / (sx * sy), 3)


def run_experiment():
    log("=== Observer Frame Experiment — Thread #268 ===")
    log(f"Hypothesis: watcher convergence is observer-relative, not content-intrinsic")
    log(f"Method: score {SAMPLE_SIZE} items under Frame A (existing) and Frame B (new)")
    log("")

    db = sqlite3.connect(DB_PATH, timeout=10)
    db.row_factory = sqlite3.Row

    # Pull recent scored items with their Frame A scores
    rows = db.execute("""
        SELECT ws.id, ws.feed_id, ws.relevance, ws.novelty, ws.quality,
               af.content, af.source, af.activity_type
        FROM watcher_scores ws
        JOIN activity_feed af ON ws.feed_id = af.id
        WHERE ws.relevance IS NOT NULL
        ORDER BY ws.id DESC
        LIMIT ?
    """, (SAMPLE_SIZE,)).fetchall()

    if len(rows) < 5:
        log(f"ERROR: Only {len(rows)} scored items found. Need at least 5.")
        return

    log(f"Loaded {len(rows)} items with Frame A scores")
    log(f"Frame A: relevance / novelty / quality (existing watcher)")
    log(f"Frame B: surprise / actionability / durability (new frame)")
    log("")

    results = []
    for i, row in enumerate(rows):
        log(f"Scoring {i+1}/{len(rows)}: {row['source']}/{row['activity_type']} (feed #{row['feed_id']})")

        frame_a_avg = round((row['relevance'] + row['novelty'] + row['quality']) / 3, 2)

        frame_b = score_frame_b(row['content'], row['source'], row['activity_type'])
        if frame_b is None:
            log(f"  Frame B failed — skipping")
            continue

        results.append({
            "feed_id": row['feed_id'],
            "source": row['source'],
            "type": row['activity_type'],
            "frame_a": {
                "relevance": row['relevance'],
                "novelty": row['novelty'],
                "quality": row['quality'],
                "avg": frame_a_avg
            },
            "frame_b": frame_b,
        })

        log(f"  A: rel={row['relevance']} nov={row['novelty']} qual={row['quality']} avg={frame_a_avg}")
        log(f"  B: sur={frame_b['surprise']} act={frame_b['actionability']} dur={frame_b['durability']} avg={frame_b['avg_b']}")

    db.close()

    if len(results) < 5:
        log(f"\nERROR: Only {len(results)} successful dual-scores. Need 5+.")
        return

    # === ANALYSIS ===
    log(f"\n{'='*60}")
    log(f"ANALYSIS: {len(results)} items scored under both frames")
    log(f"{'='*60}\n")

    # Cross-frame correlations
    a_avgs = [r["frame_a"]["avg"] for r in results]
    b_avgs = [r["frame_b"]["avg_b"] for r in results]

    r_avg = pearson_r(a_avgs, b_avgs)
    log(f"Correlation (Frame A avg vs Frame B avg): r = {r_avg}")

    # Dimension-level correlations
    a_rel = [r["frame_a"]["relevance"] for r in results]
    a_nov = [r["frame_a"]["novelty"] for r in results]
    a_qual = [r["frame_a"]["quality"] for r in results]
    b_sur = [r["frame_b"]["surprise"] for r in results]
    b_act = [r["frame_b"]["actionability"] for r in results]
    b_dur = [r["frame_b"]["durability"] for r in results]

    log(f"\nDimension cross-correlations:")
    log(f"  relevance ↔ surprise:      r = {pearson_r(a_rel, b_sur)}")
    log(f"  relevance ↔ actionability:  r = {pearson_r(a_rel, b_act)}")
    log(f"  relevance ↔ durability:     r = {pearson_r(a_rel, b_dur)}")
    log(f"  novelty ↔ surprise:         r = {pearson_r(a_nov, b_sur)}")
    log(f"  novelty ↔ actionability:    r = {pearson_r(a_nov, b_act)}")
    log(f"  novelty ↔ durability:       r = {pearson_r(a_nov, b_dur)}")
    log(f"  quality ↔ surprise:         r = {pearson_r(a_qual, b_sur)}")
    log(f"  quality ↔ actionability:    r = {pearson_r(a_qual, b_act)}")
    log(f"  quality ↔ durability:       r = {pearson_r(a_qual, b_dur)}")

    # Distribution comparison
    log(f"\nFrame A distribution: mean={round(sum(a_avgs)/len(a_avgs),2)}, "
        f"min={min(a_avgs)}, max={max(a_avgs)}")
    log(f"Frame B distribution: mean={round(sum(b_avgs)/len(b_avgs),2)}, "
        f"min={min(b_avgs)}, max={max(b_avgs)}")

    # Agreement on extremes — do both frames flag the same items as exceptional?
    a_top3 = sorted(range(len(a_avgs)), key=lambda i: a_avgs[i], reverse=True)[:3]
    b_top3 = sorted(range(len(b_avgs)), key=lambda i: b_avgs[i], reverse=True)[:3]
    overlap = len(set(a_top3) & set(b_top3))

    log(f"\nTop-3 overlap: {overlap}/3 items appear in both frames' top 3")
    log(f"  Frame A top 3: {[results[i]['source']+'/'+results[i]['type'] for i in a_top3]}")
    log(f"  Frame B top 3: {[results[i]['source']+'/'+results[i]['type'] for i in b_top3]}")

    # Divergence cases — items that score very differently across frames
    log(f"\nLargest divergences (|A_avg - B_avg|):")
    divergences = [(abs(a_avgs[i] - b_avgs[i]), i) for i in range(len(results))]
    divergences.sort(reverse=True)
    for diff, idx in divergences[:5]:
        r = results[idx]
        log(f"  Δ={diff:.2f}: {r['source']}/{r['type']} "
            f"(A={r['frame_a']['avg']}, B={r['frame_b']['avg_b']}) "
            f"— {r['frame_b'].get('rationale', 'n/a')[:80]}")

    # === VERDICT ===
    log(f"\n{'='*60}")
    if r_avg is not None and r_avg > 0.7:
        log(f"VERDICT: HIGH cross-frame correlation (r={r_avg})")
        log(f"The signal appears to be content-intrinsic, not observer-relative.")
        log(f"Thread #268 hypothesis: WEAKENED. Convergence may be genuine calibration.")
    elif r_avg is not None and r_avg > 0.3:
        log(f"VERDICT: MODERATE cross-frame correlation (r={r_avg})")
        log(f"Mixed signal. Some content properties survive frame change, others don't.")
        log(f"Thread #268 hypothesis: PARTIALLY SUPPORTED. Observer frame shapes but doesn't create.")
    elif r_avg is not None:
        log(f"VERDICT: LOW cross-frame correlation (r={r_avg})")
        log(f"Scores are frame-dependent. The observer creates the measurement.")
        log(f"Thread #268 hypothesis: STRONGLY SUPPORTED. Convergence is compliance, not calibration.")
    else:
        log(f"VERDICT: Insufficient data for correlation.")
    log(f"{'='*60}")

    # Save results
    out_path = os.path.expanduser("~/chronicle/traces/observer_experiment_results.json")
    with open(out_path, "w") as f:
        json.dump({
            "timestamp": int(time.time()),
            "sample_size": len(results),
            "correlation_avg": r_avg,
            "top3_overlap": overlap,
            "results": results,
            "cross_correlations": {
                "rel_sur": pearson_r(a_rel, b_sur),
                "rel_act": pearson_r(a_rel, b_act),
                "rel_dur": pearson_r(a_rel, b_dur),
                "nov_sur": pearson_r(a_nov, b_sur),
                "nov_act": pearson_r(a_nov, b_act),
                "nov_dur": pearson_r(a_nov, b_dur),
                "qual_sur": pearson_r(a_qual, b_sur),
                "qual_act": pearson_r(a_qual, b_act),
                "qual_dur": pearson_r(a_qual, b_dur),
            }
        }, f, indent=2)
    log(f"\nResults saved to {out_path}")


if __name__ == "__main__":
    run_experiment()
