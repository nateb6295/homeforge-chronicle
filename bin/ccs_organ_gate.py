#!/usr/bin/env python3
"""CCS Organ Gate — Phase 0: does compression output depend on HISTORY at all?

Prereg: data/ccs_organ_gate_prereg.md (thresholds committed BEFORE any run).

Compression is a two-slot recurrence:
    prompt = template.replace("{previous_state}", H).replace("{session_context}", C)
So it is directly manipulable. This script constructs prompts and calls the
engine. It performs ZERO writes to cognitive_state — the live CCS is untouched.

  ARM C (noise floor)      (H0, C0) x 6   -- pure sampling variance, temp=0.6
  ARM B (the test)         (H1..H6, C0)   -- same content, real different histories
  ARM A (positive control) (H0, C1..C6)   -- same history, different content

Resumable: existing output files are skipped.
"""
import json, os, sqlite3, sys, time
import requests

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB = os.environ.get("CHRONICLE_DB", "/mnt/hdd/chronicle-data/processed.db")
OUT = os.path.join(ROOT, "data",
                   "organ_gate_v2" if "--matched" in sys.argv else "organ_gate")
ENGINE_URL = "http://127.0.0.1:11436/api/generate"
MODEL = "chronicle-compress"
PROMPT_PATH = os.path.join(ROOT, "data", "ccs_brain_prompt_v4.md")
K = 6
PREV_BUDGET, CTX_BUDGET = 3000, 7000


def load_materials_matched(k=6):
    """V2 (Aug 24, after v1 confounded content with FORMAT and AGE).

    v1 drew H0 from today (brain-format, has SPINE/BRIDGE) and H1..H6 from late
    June (no SPINE). So ARM B varied content AND format AND age at once and the
    effect size was untrustworthy. Fix: draw ALL histories from one format
    signature within one era, so only CONTENT varies.
    """
    import re as _re
    from collections import defaultdict
    db = sqlite3.connect(DB)
    rows = db.execute("SELECT created_at, snapshot FROM cognitive_state_history "
                      "ORDER BY created_at DESC LIMIT 2000").fetchall()
    groups, seen = defaultdict(list), set()
    for ts, snap in rows:
        try:
            g = (json.loads(snap) or {}).get("semantic_gist") or ""
        except Exception:
            continue
        if len(g) < 1200 or g[:200] in seen:
            continue
        seen.add(g[:200])
        sig = tuple(sorted(set(_re.findall(r"#+\s*([A-Z][A-Z]+)", g[:PREV_BUDGET]))))
        if sig:
            groups[sig].append((ts, g[:PREV_BUDGET]))
    sig, pool = max(groups.items(), key=lambda x: len(x[1]))
    pool.sort(key=lambda x: x[0])
    step = max(1, len(pool) // (k + 1))
    hists = pool[::step][:k + 1]

    caps = db.execute(
        "SELECT date(timestamp), group_concat(restatement, ' ') FROM knowledge_capsules "
        "WHERE timestamp > '2026-08-01' AND length(restatement) > 200 "
        "GROUP BY date(timestamp) ORDER BY date(timestamp) DESC LIMIT 20").fetchall()
    db.close()
    ctxs = [(d, t[:CTX_BUDGET]) for d, t in caps if t and len(t) > 3000][:k + 1]
    print(f"V2 matched pool: signature {list(sig)}, {len(pool)} available, using {len(hists)}")
    return hists, ctxs


def load_materials():
    """Real prior states and real session contexts, drawn from our own history."""
    db = sqlite3.connect(DB)
    # HISTORIES: distinct brain-format gists spread across time
    rows = db.execute(
        "SELECT created_at, snapshot FROM cognitive_state_history "
        "ORDER BY created_at DESC LIMIT 2000").fetchall()
    hists, seen = [], set()
    for ts, snap in rows:
        try:
            g = (json.loads(snap) or {}).get("semantic_gist") or ""
        except Exception:
            continue
        if len(g) < 800:
            continue
        key = g[:200]
        if key in seen:
            continue
        seen.add(key)
        hists.append((ts, g[:PREV_BUDGET]))
        if len(hists) >= 40:
            break
    # spread them out rather than taking 7 adjacent ones
    step = max(1, len(hists) // (K + 1))
    hists = hists[::step][:K + 1]

    # CONTEXTS: real capsule text from distinct days, length-matched
    caps = db.execute(
        "SELECT date(timestamp), group_concat(restatement, ' ') FROM knowledge_capsules "
        "WHERE timestamp > '2026-08-01' AND length(restatement) > 200 "
        "GROUP BY date(timestamp) ORDER BY date(timestamp) DESC LIMIT 20").fetchall()
    db.close()
    ctxs = [(d, t[:CTX_BUDGET]) for d, t in caps if t and len(t) > 3000][:K + 1]
    return hists, ctxs


def call_engine(prompt):
    t0 = time.time()
    r = requests.post(ENGINE_URL, json={
        "model": MODEL, "prompt": prompt, "stream": False,
        "options": {"num_predict": 4096, "temperature": 0.6},
    }, timeout=300)
    r.raise_for_status()
    return r.json().get("response", ""), round(time.time() - t0, 1)


def main():
    os.makedirs(OUT, exist_ok=True)
    template = open(PROMPT_PATH).read()
    matched = "--matched" in sys.argv
    hists, ctxs = (load_materials_matched(K) if matched else load_materials())
    if len(hists) < K + 1 or len(ctxs) < K + 1:
        print(f"INSUFFICIENT MATERIAL: {len(hists)} histories, {len(ctxs)} contexts "
              f"(need {K+1} each). Aborting rather than shrinking k silently.")
        return 1

    H0, C0 = hists[0][1], ctxs[0][1]
    runs = []
    for i in range(K):
        runs.append(("C", f"C_{i}", H0, C0))
    for i in range(K):
        runs.append(("B", f"B_{i}", hists[i + 1][1], C0))
    for i in range(K):
        runs.append(("A", f"A_{i}", H0, ctxs[i + 1][1]))

    meta = {"histories": [(t, len(g)) for t, g in hists],
            "contexts": [(d, len(c)) for d, c in ctxs],
            "k": K, "temperature": 0.6, "model": MODEL}
    with open(os.path.join(OUT, "materials.json"), "w") as f:
        json.dump(meta, f, indent=2)

    # --- Kimi's slot-inertness arms (Amendment 2). ARM A validates the CONTENT
    # slot; ARM B tests the HISTORY slot. "A moves, B doesn't" is confounded
    # with SLOT POSITION unless we show the history slot can move ANYTHING.
    if "--extra" in sys.argv:
        db = sqlite3.connect(DB)
        oldest = db.execute(
            "SELECT snapshot FROM cognitive_state_history ORDER BY created_at ASC LIMIT 1"
        ).fetchone()
        db.close()
        try:
            far = (json.loads(oldest[0]) or {}).get("semantic_gist", "") if oldest else ""
        except Exception:
            far = ""
        runs = [
            # empty history slot: if THIS lands in ARM C noise, the slot is inert
            ("X", "X_empty", "", C0),
            # maximally distant real history (oldest we have)
            ("X", "X_far", (far or "no prior state")[:PREV_BUDGET], C0),
            # slot swap: history CONTENT placed in the CONTENT slot.
            # If output moves, the text is readable when saliently placed —
            # isolating placement from concept.
            ("X", "X_swap", H0, hists[1][1][:CTX_BUDGET]),
        ]
        print("EXTRA ARMS (slot inertness) — 3 runs")

    print(f"{len(runs)} runs. ZERO cognitive_state writes.")
    for arm, name, H, C in runs:
        path = os.path.join(OUT, f"{name}.txt")
        if os.path.exists(path) and os.path.getsize(path) > 200:
            print(f"  {name}: cached, skip")
            continue
        prompt = template.replace("{previous_state}", H).replace("{session_context}", C)
        try:
            out, secs = call_engine(prompt)
        except Exception as e:
            print(f"  {name}: FAILED {type(e).__name__}: {str(e)[:120]}")
            continue
        with open(path, "w") as f:
            f.write(out)
        print(f"  {name}: {len(out)} chars in {secs}s")
    print("DONE")
    return 0


if __name__ == "__main__":
    sys.exit(main())
