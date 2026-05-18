#!/usr/bin/env python3
"""Phase 4.5 full analysis — compile results from all judged files.

Merges phase4_5_judged.jsonl (main A+B), phase4_5_judged_arm_b.jsonl,
phase4_5_judged_arm_c.jsonl, phase4_5_judged_arm_control.jsonl.

Deduplicates by (arm, domain, prompt_idx), preferring arm-specific files.
Produces the comparison table for the alignment tax sign paper.
"""
import json, sys
from collections import defaultdict
from pathlib import Path

DATA = Path("/home/nate-agx/chronicle/data/care_template_dpo_run")

def load_scored(path):
    records = []
    if not path.exists():
        return records
    with open(path) as f:
        for line in f:
            r = json.loads(line)
            j = r.get("judge", {})
            if isinstance(j, str):
                try:
                    j = json.loads(j)
                except:
                    continue
            if isinstance(j, dict) and "error" not in j and j.get("integration") is not None:
                r["_j"] = j
                records.append(r)
    return records


def dedup(all_records):
    by_key = {}
    for r in all_records:
        key = (r.get("arm", "?"), r["domain"], r["prompt_idx"])
        by_key[key] = r
    return list(by_key.values())


def stats(scores):
    if not scores:
        return {}
    n = len(scores)
    avg_d = sum(s["decisiveness"] for s in scores) / n
    avg_c = sum(s["care_template"] for s in scores) / n
    avg_i = sum(s["integration"] for s in scores) / n
    stdev_i = (sum((s["integration"] - avg_i)**2 for s in scores) / n) ** 0.5
    tail = sum(1 for s in scores if s["integration"] <= 5)
    high = sum(1 for s in scores if s["integration"] >= 8)
    return {
        "n": n, "d": avg_d, "c": avg_c, "i": avg_i,
        "σ": stdev_i, "tail": tail, "high": high,
        "tail_pct": 100 * tail / n, "high_pct": 100 * high / n,
    }


def main():
    # Load all files, arm-specific files take priority
    main_recs = load_scored(DATA / "phase4_5_judged.jsonl")
    b_recs = load_scored(DATA / "phase4_5_judged_arm_b.jsonl")
    c_recs = load_scored(DATA / "phase4_5_judged_arm_c.jsonl")
    ctrl_recs = load_scored(DATA / "phase4_5_judged_arm_control.jsonl")

    # Arm-specific files override main file
    all_recs = dedup(main_recs + b_recs + c_recs + ctrl_recs)

    print(f"Total scored records: {len(all_recs)}")
    print(f"  Sources: main={len(main_recs)}, arm_b={len(b_recs)}, arm_c={len(c_recs)}, control={len(ctrl_recs)}")

    # Group by arm and domain
    by_arm = defaultdict(lambda: defaultdict(list))
    for r in all_recs:
        arm = r.get("arm", "?")
        by_arm[arm][r["domain"]].append(r["_j"])
        by_arm[arm]["all"].append(r["_j"])

    # Arm labels for display
    arm_labels = {
        "4.5_a": "4.5a (factual only, 61 records)",
        "4.5_b": "4.5b (combined, answer-only, 240 records)",
        "4.5_c": "4.5c (combined, think+answer, 240 records)",
        "4.5_control": "Control (Phase 4 rerun, 179 records)",
    }

    domains = ["all", "advice_under_uncertainty", "subjective_evaluation", "factual_judgment"]
    domain_labels = {
        "all": "ALL",
        "advice_under_uncertainty": "advice",
        "subjective_evaluation": "subjective",
        "factual_judgment": "factual",
    }

    # Print full table
    print("\n" + "=" * 100)
    print("PHASE 4.5 FULL RESULTS")
    print("=" * 100)

    header = f"{'Arm':<45} {'Domain':<12} {'n':>3} {'d':>5} {'c':>5} {'i':>6} {'σ':>5} {'tail':>8} {'high':>8}"
    print(header)
    print("-" * 100)

    for arm in sorted(by_arm):
        label = arm_labels.get(arm, arm)
        for domain in domains:
            scores = by_arm[arm].get(domain, [])
            if not scores:
                continue
            s = stats(scores)
            d_label = domain_labels.get(domain, domain)
            tail_str = f"{s['tail']}/{s['n']} ({s['tail_pct']:.0f}%)"
            high_str = f"{s['high']}/{s['n']} ({s['high_pct']:.0f}%)"
            if domain == "all":
                print(f"{label:<45} {d_label:<12} {s['n']:3d} {s['d']:5.1f} {s['c']:5.1f} {s['i']:6.2f} {s['σ']:5.2f} {tail_str:>8} {high_str:>8}")
            else:
                print(f"{'':45} {d_label:<12} {s['n']:3d} {s['d']:5.1f} {s['c']:5.1f} {s['i']:6.2f} {s['σ']:5.2f} {tail_str:>8} {high_str:>8}")
        print()

    # Key comparisons
    print("\n" + "=" * 80)
    print("KEY COMPARISONS")
    print("=" * 80)

    # 1. Factual judgment across arms (the critical test)
    print("\n--- Factual Judgment (the iatrogenic channel) ---")
    for arm in sorted(by_arm):
        scores = by_arm[arm].get("factual_judgment", [])
        if scores:
            s = stats(scores)
            label = arm_labels.get(arm, arm)[:40]
            print(f"  {label:<40} n={s['n']:2d}  i={s['i']:.2f}  σ={s['σ']:.2f}  tail={s['tail']}/{s['n']} ({s['tail_pct']:.0f}%)")

    # 2. Advice regression check
    print("\n--- Advice Regression Check ---")
    for arm in sorted(by_arm):
        scores = by_arm[arm].get("advice_under_uncertainty", [])
        if scores:
            s = stats(scores)
            label = arm_labels.get(arm, arm)[:40]
            print(f"  {label:<40} n={s['n']:2d}  i={s['i']:.2f}  σ={s['σ']:.2f}  tail={s['tail']}/{s['n']} ({s['tail_pct']:.0f}%)")

    # 3. Delta from control
    print("\n--- Integration Delta from Control ---")
    ctrl_by_domain = {}
    for domain in domains:
        ctrl_scores = by_arm.get("4.5_control", {}).get(domain, [])
        if ctrl_scores:
            ctrl_by_domain[domain] = stats(ctrl_scores)["i"]

    for arm in sorted(by_arm):
        if arm == "4.5_control":
            continue
        label = arm_labels.get(arm, arm)[:40]
        deltas = []
        for domain in ["advice_under_uncertainty", "subjective_evaluation", "factual_judgment"]:
            arm_scores = by_arm[arm].get(domain, [])
            ctrl_i = ctrl_by_domain.get(domain)
            if arm_scores and ctrl_i is not None:
                arm_i = stats(arm_scores)["i"]
                delta = arm_i - ctrl_i
                deltas.append(f"{domain_labels[domain]}={delta:+.2f}")
        if deltas:
            print(f"  {label:<40} {', '.join(deltas)}")

    # 4. Per-prompt comparison (factual only) between arms
    print("\n--- Per-Prompt Factual Scores ---")
    by_prompt = defaultdict(dict)
    for r in all_recs:
        if r["domain"] == "factual_judgment":
            by_prompt[r["prompt_idx"]][r.get("arm", "?")] = r["_j"]["integration"]

    if by_prompt:
        print(f"  {'idx':>3} ", end="")
        arms_present = sorted(set(arm for scores in by_prompt.values() for arm in scores))
        for arm in arms_present:
            print(f" {arm:>12}", end="")
        print()
        for idx in sorted(by_prompt):
            print(f"  {idx:3d} ", end="")
            for arm in arms_present:
                val = by_prompt[idx].get(arm)
                if val is not None:
                    marker = " *" if val <= 5 else ""
                    print(f" {val:10d}{marker}", end="")
                else:
                    print(f" {'—':>12}", end="")
            print()

    # Summary
    print("\n" + "=" * 80)
    print("SUMMARY")
    print("=" * 80)

    b_factual = by_arm.get("4.5_b", {}).get("factual_judgment", [])
    ctrl_factual = by_arm.get("4.5_control", {}).get("factual_judgment", [])
    b_advice = by_arm.get("4.5_b", {}).get("advice_under_uncertainty", [])
    ctrl_advice = by_arm.get("4.5_control", {}).get("advice_under_uncertainty", [])

    if b_factual and ctrl_factual:
        b_fi = stats(b_factual)["i"]
        c_fi = stats(ctrl_factual)["i"]
        delta = b_fi - c_fi
        print(f"\nFactual channel (Arm B vs Control): {b_fi:.2f} vs {c_fi:.2f} (Δ={delta:+.2f})")
        if abs(delta) < 0.5:
            print("  → Combined training PRESERVES factual capability (within noise)")
        elif delta > 0:
            print("  → Combined training IMPROVES factual capability")
        else:
            print("  → Combined training shows factual regression")

    if b_advice and ctrl_advice:
        b_ai = stats(b_advice)["i"]
        c_ai = stats(ctrl_advice)["i"]
        print(f"\nAdvice regression (Arm B vs Control): {b_ai:.2f} vs {c_ai:.2f} (Δ={b_ai-c_ai:+.2f})")
        if abs(b_ai - c_ai) < 1.0:
            print("  → No significant regression")
        elif b_ai < c_ai:
            print(f"  → Regression of {c_ai-b_ai:.2f} points")

    print()


if __name__ == "__main__":
    main()
