#!/usr/bin/env python3
"""Flow regression — tests advance-35 falsifiable claim.

Cole's brain-flows formula: source_activity × connectivity = target_activity.
Thread #318 mapping: CCS_window_salience × reader_prior = attribution.

Test: does a simple product model (attribution ~ salience × reader_prior)
explain >50% of variance? If <30%, the flow model is too simple.

Uses existing motif_probe T=0 data for groq and local backends.
"""

import csv
import re
import math
import sys
from pathlib import Path

DATA_DIR = Path(__file__).parent

# ─── Load motif probe CSVs ──────────────────────────────────────────────────

def load_probe(csv_path):
    rows = []
    with open(csv_path) as f:
        reader = csv.DictReader(f)
        for r in reader:
            ws = int(r["window_start"])
            if ws < 0:
                continue  # skip baseline
            rows.append({
                "window_start": ws,
                "window_end": int(r["window_end"]),
                "text": r["masked_text"],
                "attribution": float(r["attribution"]),
                "shift_norm": float(r["shift_norm"]),
            })
    return rows


# ─── Content salience scoring ────────────────────────────────────────────────

# Entity names that appear in the CCS (from thread #318 entity-salience band)
ENTITY_PATTERNS = [
    r"nate", r"hermes", r"gemma", r"opus", r"thread", r"keeper",
    r"canister", r"rotation", r"chronicle", r"sentinel",
    r"capture", r"directive", r"discord", r"ccs",
]

SALIENCE_NUMBER_RE = re.compile(r"\d+\.\d{1,2}")  # e.g. 0.93, 0.88


def score_salience(text):
    """Score a window's content salience.

    High salience = contains entity names, salience numbers, specific directives.
    Low salience = boilerplate, structural markers, generic text.
    """
    text_lower = text.lower()
    score = 0.0

    # Entity name hits
    for pat in ENTITY_PATTERNS:
        hits = len(re.findall(pat, text_lower))
        score += hits * 1.0

    # Numeric salience scores (e.g. "0.93", "0.88")
    nums = SALIENCE_NUMBER_RE.findall(text)
    score += len(nums) * 0.5

    # Parenthetical salience annotations like "(0.93)"
    paren_scores = re.findall(r"\(\d+\.\d+\)", text)
    score += len(paren_scores) * 1.5

    # Directive markers
    for marker in ["priority", "directive", "should", "must", "need"]:
        if marker in text_lower:
            score += 0.5

    return score


def score_speech_act(text):
    """Score a window's speech-act / directive content.

    Groq's high-attribution low-salience windows carry imperative/interrogative
    speech acts. This captures that dimension independently of entity-salience.
    """
    text_lower = text.lower()
    score = 0.0

    # Interrogative markers
    if "?" in text:
        score += 2.0
    for q in ["what", "how", "why", "when", "where", "which", "does", "can", "is there"]:
        if q in text_lower:
            score += 0.5

    # Imperative/directive markers
    for imp in ["let's", "lets", "keep", "make sure", "don't", "dont", "stop",
                "start", "continue", "advance", "fix", "build", "push", "run",
                "check", "verify", "do ", "try "]:
        hits = len(re.findall(re.escape(imp), text_lower))
        score += hits * 1.0

    # Second-person address (Nate talking to Opus)
    for addr in ["you ", "your ", "you're"]:
        hits = len(re.findall(re.escape(addr), text_lower))
        score += hits * 0.5

    # Exclamation (emotional/emphatic directives)
    score += text.count("!") * 0.3

    return score


def score_framing(text):
    """Score a window's framing/orientation content.

    Local gemma's high-attribution low-salience windows carry meta-level
    framing, focus statements, and orientation cues.
    """
    text_lower = text.lower()
    score = 0.0

    # Focus/orientation markers
    for marker in ["focus", "current", "active", "status", "state",
                    "working on", "right now", "today", "session"]:
        if marker in text_lower:
            score += 1.0

    # Meta/structural markers
    for marker in ["##", "**", "---", "===", "→", "—"]:
        score += text.count(marker) * 0.3

    # Contemplative/reflective language
    for marker in ["development", "exploration", "contemplat", "approach",
                    "philosophy", "perspective", "observe", "support"]:
        if marker in text_lower:
            score += 0.8

    # Abstract/category language
    for marker in ["level", "layer", "system", "process", "structure",
                    "pattern", "mode", "phase"]:
        if marker in text_lower:
            score += 0.3

    return score


# ─── Regression ──────────────────────────────────────────────────────────────

def pearson_r(xs, ys):
    n = len(xs)
    if n < 3:
        return 0.0
    mx = sum(xs) / n
    my = sum(ys) / n
    sxy = sum((x - mx) * (y - my) for x, y in zip(xs, ys))
    sx = math.sqrt(sum((x - mx) ** 2 for x in xs))
    sy = math.sqrt(sum((y - my) ** 2 for y in ys))
    if sx == 0 or sy == 0:
        return 0.0
    return sxy / (sx * sy)


def r_squared(xs, ys):
    r = pearson_r(xs, ys)
    return r * r


def main():
    # Load structural-order T=0 data
    groq_file = DATA_DIR / "motif_probe_groq_llama_T0_RUN1_20260416_134445.csv"
    local_file = DATA_DIR / "motif_probe_gemma_local_T0_20260416_134829.csv"

    groq = load_probe(groq_file)
    local = load_probe(local_file)

    print(f"Loaded {len(groq)} groq windows, {len(local)} local windows")

    # Also load content-ordered data
    groq_content_file = DATA_DIR / "motif_probe_groq_content_T0_20260416_141830.csv"
    local_content_file = DATA_DIR / "motif_probe_local_content_T0_20260416_141837.csv"

    groq_content = load_probe(groq_content_file) if groq_content_file.exists() else None
    local_content = load_probe(local_content_file) if local_content_file.exists() else None

    # ─── Score salience for each window ──────────────────────────────────
    for row in groq:
        row["salience"] = score_salience(row["text"])
    for row in local:
        row["salience"] = score_salience(row["text"])

    # ─── Regression: attribution ~ salience, per backend ─────────────────
    print("\n═══ STRUCTURAL ORDER ═══")

    for label, data in [("groq_llama", groq), ("gemma_local", local)]:
        sals = [r["salience"] for r in data]
        attrs = [r["attribution"] for r in data]
        r2 = r_squared(sals, attrs)
        r = pearson_r(sals, attrs)

        print(f"\n  {label}: r={r:.3f}, R²={r2:.3f} (n={len(data)})")

        # Top-5 by attribution vs top-5 by salience
        by_attr = sorted(data, key=lambda x: x["attribution"], reverse=True)[:5]
        by_sal = sorted(data, key=lambda x: x["salience"], reverse=True)[:5]

        attr_starts = {r["window_start"] for r in by_attr}
        sal_starts = {r["window_start"] for r in by_sal}
        overlap = len(attr_starts & sal_starts)

        print(f"    top-5 overlap (attribution vs salience): {overlap}/5")
        print(f"    top-5 by attribution: {sorted(attr_starts)}")
        print(f"    top-5 by salience:    {sorted(sal_starts)}")

    # ─── Cross-backend: same window, does salience predict BOTH? ─────────
    # Align by window_start
    groq_by_ws = {r["window_start"]: r for r in groq}
    local_by_ws = {r["window_start"]: r for r in local}
    shared = sorted(set(groq_by_ws) & set(local_by_ws))

    if shared:
        g_attrs = [groq_by_ws[w]["attribution"] for w in shared]
        l_attrs = [local_by_ws[w]["attribution"] for w in shared]
        sals = [groq_by_ws[w]["salience"] for w in shared]  # same CCS, same text

        # Cross-backend attribution correlation
        cross_r = pearson_r(g_attrs, l_attrs)
        cross_r2 = r_squared(g_attrs, l_attrs)
        print(f"\n  Cross-backend (groq vs local attribution): r={cross_r:.3f}, R²={cross_r2:.3f}")

        # Product model test: does salience explain mean attribution?
        mean_attrs = [(g + l) / 2 for g, l in zip(g_attrs, l_attrs)]
        product_r2 = r_squared(sals, mean_attrs)
        print(f"  Product model (salience → mean attribution): R²={product_r2:.3f}")

    # ─── Content-ordered data ────────────────────────────────────────────
    if groq_content and local_content:
        for row in groq_content:
            row["salience"] = score_salience(row["text"])
        for row in local_content:
            row["salience"] = score_salience(row["text"])

        print("\n═══ CONTENT ORDER ═══")

        for label, data in [("groq_content", groq_content), ("local_content", local_content)]:
            sals = [r["salience"] for r in data]
            attrs = [r["attribution"] for r in data]
            r2 = r_squared(sals, attrs)
            r = pearson_r(sals, attrs)
            print(f"\n  {label}: r={r:.3f}, R²={r2:.3f} (n={len(data)})")

        gc_by_ws = {r["window_start"]: r for r in groq_content}
        lc_by_ws = {r["window_start"]: r for r in local_content}
        shared_c = sorted(set(gc_by_ws) & set(lc_by_ws))

        if shared_c:
            gc_attrs = [gc_by_ws[w]["attribution"] for w in shared_c]
            lc_attrs = [lc_by_ws[w]["attribution"] for w in shared_c]
            sals_c = [gc_by_ws[w]["salience"] for w in shared_c]

            cross_r_c = pearson_r(gc_attrs, lc_attrs)
            print(f"\n  Cross-backend content-order: r={cross_r_c:.3f}, R²={cross_r_c**2:.3f}")

            mean_c = [(g + l) / 2 for g, l in zip(gc_attrs, lc_attrs)]
            prod_r2_c = r_squared(sals_c, mean_c)
            print(f"  Product model content-order: R²={prod_r2_c:.3f}")

    # ─── Verdict ─────────────────────────────────────────────────────────
    print("\n═══ ADVANCE-35 VERDICT ═══")

    # Use the structural-order product model R²
    if shared:
        if product_r2 > 0.50:
            print(f"  CONFIRMED: Product model R²={product_r2:.3f} > 0.50")
            print("  Simple flow model (salience × substrate) explains majority of variance.")
        elif product_r2 > 0.30:
            print(f"  PARTIAL: Product model R²={product_r2:.3f} (between 0.30-0.50)")
            print("  Flow model captures some structure but nonlinearities are significant.")
        else:
            print(f"  FALSIFIED: Product model R²={product_r2:.3f} < 0.30")
            print("  Simple flow model is too simple. Interaction terms or nonlinearities dominate.")

    # ─── 2-FACTOR REGRESSION (advance-36 falsifiable) ────────────────────
    print("\n═══ ADVANCE-36: 2-FACTOR REGRESSION ═══")
    print("Testing: attribution ~ salience + speech_act (groq) or salience + framing (local)")

    # Score all windows with new features
    for dataset_label, data, reader_scorer, reader_label in [
        ("groq structural", groq, score_speech_act, "speech_act"),
        ("groq content", groq_content, score_speech_act, "speech_act"),
        ("local structural", local, score_framing, "framing"),
        ("local content", local_content, score_framing, "framing"),
    ]:
        if data is None:
            continue

        for row in data:
            row["reader_feature"] = reader_scorer(row["text"])

        sals = [r["salience"] for r in data]
        reader_feats = [r["reader_feature"] for r in data]
        attrs = [r["attribution"] for r in data]

        # Individual R²
        r2_sal = r_squared(sals, attrs)
        r2_reader = r_squared(reader_feats, attrs)

        # Combined score (simple additive proxy for 2-factor without numpy)
        # Normalize each feature to [0,1] range, sum, correlate
        sal_min, sal_max = min(sals), max(sals)
        rf_min, rf_max = min(reader_feats), max(reader_feats)

        combined = []
        for s, rf in zip(sals, reader_feats):
            s_norm = (s - sal_min) / (sal_max - sal_min) if sal_max > sal_min else 0
            rf_norm = (rf - rf_min) / (rf_max - rf_min) if rf_max > rf_min else 0
            combined.append(s_norm + rf_norm)

        r2_combined = r_squared(combined, attrs)

        print(f"\n  {dataset_label}:")
        print(f"    salience alone:     R²={r2_sal:.3f}")
        print(f"    {reader_label} alone: R²={r2_reader:.3f}")
        print(f"    combined (sal+{reader_label}): R²={r2_combined:.3f}")

        # Advance-36 verdict for groq
        if "groq" in dataset_label:
            if r2_combined > 0.40:
                print(f"    → CONFIRMED: 2-factor R²={r2_combined:.3f} > 0.40 for groq")
            else:
                print(f"    → NOT MET: 2-factor R²={r2_combined:.3f} < 0.40 for groq")


if __name__ == "__main__":
    main()
