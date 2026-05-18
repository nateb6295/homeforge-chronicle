"""Phase 4 Results Analysis — compares care persistence across arms and baseline.

Reads:
  - phase4_eval_arm_{A,B,C}.jsonl (fine-tuned model, no care prompt)
  - phase4_baseline_deepinfra.jsonl (un-fine-tuned, base + care conditions)

Metrics:
  1. Care-template structure detection (numbered think steps, bold headers)
  2. Named conditionals ("if you...", "when...")
  3. Calibrated confidence language
  4. Response length comparison
  5. Think block presence and structure

Usage: python3 phase4_analyze.py [--data-dir /path/to/data]
"""
import json
import re
import sys
from pathlib import Path
from collections import defaultdict

DATA_DIR = Path(sys.argv[2]) if len(sys.argv) > 2 and sys.argv[1] == "--data-dir" else Path("/home/nate-agx/chronicle/data/care_template_dpo_run")
POD_DIR = Path("/workspace/care_template_dpo_run")


def detect_care_structure(text):
    """Score a response for care-template structural markers (0-10 scale)."""
    score = 0
    markers = {}

    think_match = re.search(r"<think>(.*?)</think>", text, re.DOTALL)
    think_text = think_match.group(1) if think_match else ""

    if think_match:
        score += 1
        markers["has_think"] = True

        if re.search(r"(what the asker|what.*cares about|likely cares)", think_text, re.I):
            score += 2
            markers["asker_care_analysis"] = True

        if re.search(r"(load.bearing|non.decisive|secondary.*dimension)", think_text, re.I):
            score += 2
            markers["dimension_analysis"] = True

        if re.search(r"(plan for|decisive answer|structure.*answer)", think_text, re.I):
            score += 2
            markers["answer_plan"] = True

        if re.search(r"\*\*.*\*\*", think_text):
            score += 1
            markers["bold_headers"] = True

        if re.search(r"^\s*\d\)", think_text, re.M) or re.search(r"^\s*\d\.\s", think_text, re.M):
            score += 1
            markers["numbered_steps"] = True
    else:
        markers["has_think"] = False

    answer = text.split("</think>")[-1].strip() if "</think>" in text else text

    conditionals = len(re.findall(r"\b(if you|when you|for those who|depending on)\b", answer, re.I))
    if conditionals >= 2:
        score += 1
        markers["named_conditionals"] = conditionals

    # Answer-level care markers (important for Arm B which has no think block)
    answer_care = 0
    if re.search(r"\*\*.*\*\*", answer):
        answer_care += 1
        markers["answer_bold_structure"] = True
    if conditionals >= 1:
        answer_care += 1
    if re.search(r"(calibrat|confidence|uncertain|likely|typically|generally)", answer, re.I):
        answer_care += 1
        markers["answer_calibrated"] = True
    if re.search(r"(however|but.*if|on the other hand|conversely|that said)", answer, re.I):
        answer_care += 1
        markers["answer_nuance"] = True
    if re.search(r"(prioriti[zs]e|value|care about|matter.*to you|your.*goal)", answer, re.I):
        answer_care += 1
        markers["answer_reader_awareness"] = True
    markers["answer_care_score"] = answer_care

    markers["total_score"] = score
    markers["answer_length"] = len(answer.split())
    markers["think_length"] = len(think_text.split()) if think_text else 0

    return markers


def load_jsonl(path):
    records = []
    if not path.exists():
        return records
    for line in open(path):
        try:
            records.append(json.loads(line))
        except:
            pass
    return records


def analyze_arm(records, label):
    """Analyze care scores for one arm's eval results."""
    scores = []
    by_domain = defaultdict(list)

    for r in records:
        resp = r.get("finetuned_response", r.get("response", ""))
        m = detect_care_structure(resp)
        scores.append(m["total_score"])
        by_domain[r.get("domain", "unknown")].append(m)

    if not scores:
        print(f"\n  {label}: no records")
        return {}

    avg = sum(scores) / len(scores)
    think_pct = sum(1 for r in records for _ in [detect_care_structure(r.get("finetuned_response", r.get("response", "")))] if _["has_think"]) / len(records) * 100

    # Recompute using stored markers
    all_markers = []
    for r in records:
        resp = r.get("finetuned_response", r.get("response", ""))
        all_markers.append(detect_care_structure(resp))

    think_pct = sum(1 for m in all_markers if m["has_think"]) / len(all_markers) * 100
    asker_pct = sum(1 for m in all_markers if m.get("asker_care_analysis")) / len(all_markers) * 100
    dimension_pct = sum(1 for m in all_markers if m.get("dimension_analysis")) / len(all_markers) * 100
    plan_pct = sum(1 for m in all_markers if m.get("answer_plan")) / len(all_markers) * 100
    avg_answer_len = sum(m["answer_length"] for m in all_markers) / len(all_markers)
    avg_think_len = sum(m["think_length"] for m in all_markers) / len(all_markers)

    print(f"\n  {label} ({len(records)} records)")
    print(f"    Care score:      {avg:.1f}/10")
    print(f"    Think block:     {think_pct:.0f}%")
    print(f"    Asker analysis:  {asker_pct:.0f}%")
    print(f"    Dimension sort:  {dimension_pct:.0f}%")
    print(f"    Answer plan:     {plan_pct:.0f}%")
    avg_answer_care = sum(m.get("answer_care_score", 0) for m in all_markers) / len(all_markers)
    bold_pct = sum(1 for m in all_markers if m.get("answer_bold_structure")) / len(all_markers) * 100
    calibrated_pct = sum(1 for m in all_markers if m.get("answer_calibrated")) / len(all_markers) * 100
    nuance_pct = sum(1 for m in all_markers if m.get("answer_nuance")) / len(all_markers) * 100
    reader_pct = sum(1 for m in all_markers if m.get("answer_reader_awareness")) / len(all_markers) * 100

    print(f"    Avg answer len:  {avg_answer_len:.0f} words")
    print(f"    Avg think len:   {avg_think_len:.0f} words")
    print(f"    Answer care:     {avg_answer_care:.1f}/5")
    print(f"      Bold structure: {bold_pct:.0f}%  Calibrated: {calibrated_pct:.0f}%")
    print(f"      Nuance: {nuance_pct:.0f}%  Reader aware: {reader_pct:.0f}%")

    print(f"    By domain:")
    for domain, markers in sorted(by_domain.items()):
        d_scores = [m["total_score"] for m in markers]
        d_avg = sum(d_scores) / len(d_scores)
        print(f"      {domain}: {d_avg:.1f}/10 ({len(markers)} records)")

    return {
        "label": label,
        "n": len(records),
        "avg_score": avg,
        "think_pct": think_pct,
        "asker_pct": asker_pct,
        "dimension_pct": dimension_pct,
        "plan_pct": plan_pct,
    }


def main():
    print("=" * 60)
    print("PHASE 4 CARE PERSISTENCE ANALYSIS")
    print("=" * 60)

    # Try pod paths first, then local
    results = {}

    for arm in ["A", "B", "C"]:
        path = DATA_DIR / f"phase4_eval_arm_{arm}.jsonl"
        if not path.exists():
            path = POD_DIR / f"phase4_eval_arm_{arm}.jsonl"
        records = load_jsonl(path)
        if records:
            results[f"arm_{arm}"] = analyze_arm(records, f"Arm {arm} (fine-tuned, no care prompt)")

    # Baseline
    baseline_path = DATA_DIR / "phase4_baseline_deepinfra.jsonl"
    baseline = load_jsonl(baseline_path)
    if baseline:
        base_records = [r for r in baseline if r.get("condition") == "base"]
        care_records = [r for r in baseline if r.get("condition") == "care"]
        if base_records:
            results["baseline_base"] = analyze_arm(base_records, "Baseline (no care prompt)")
        if care_records:
            results["baseline_care"] = analyze_arm(care_records, "Baseline (WITH care prompt)")

    # Summary comparison
    print("\n" + "=" * 60)
    print("SUMMARY: Care Persistence Test")
    print("=" * 60)
    print(f"\n  Key question: Does the fine-tuned model show care patterns")
    print(f"  WITHOUT the care system prompt?")
    print()

    if "arm_A" in results and "baseline_base" in results:
        a = results["arm_A"]
        b = results["baseline_base"]
        delta = a["avg_score"] - b["avg_score"]
        print(f"  Arm A (think+answer) vs Baseline (no care):")
        print(f"    Care score: {a['avg_score']:.1f} vs {b['avg_score']:.1f} (delta: +{delta:.1f})")
        print(f"    Think block: {a['think_pct']:.0f}% vs {b['think_pct']:.0f}%")
        print(f"    Asker analysis: {a['asker_pct']:.0f}% vs {b['asker_pct']:.0f}%")

    if "arm_B" in results and "baseline_base" in results:
        a = results["arm_B"]
        b = results["baseline_base"]
        delta = a["avg_score"] - b["avg_score"]
        print(f"\n  Arm B (answer-only) vs Baseline (no care):")
        print(f"    Care score: {a['avg_score']:.1f} vs {b['avg_score']:.1f} (delta: +{delta:.1f})")

    if "arm_A" in results and "baseline_care" in results:
        a = results["arm_A"]
        c = results["baseline_care"]
        delta = a["avg_score"] - c["avg_score"]
        print(f"\n  Arm A (fine-tuned, no prompt) vs Baseline (WITH care prompt):")
        print(f"    Care score: {a['avg_score']:.1f} vs {c['avg_score']:.1f} (delta: {delta:+.1f})")
        print(f"    ^ If positive: training > system prompt for care")

    if "arm_C" in results and "arm_A" in results:
        a = results["arm_A"]
        c = results["arm_C"]
        print(f"\n  Arm A (5 domains) vs Arm C (2 domains, control):")
        print(f"    Care score: {a['avg_score']:.1f} vs {c['avg_score']:.1f}")
        print(f"    ^ Tests whether 3 new domains improve generalization")

    # Stress test analysis
    stress_path = DATA_DIR / "phase4_stress_arm_A.jsonl"
    stress = load_jsonl(stress_path)
    if stress:
        print("\n" + "=" * 60)
        print("STRESS TEST: Out-of-Distribution & Adversarial")
        print("=" * 60)

        for cond_name in ["base", "anti_care", "adversarial"]:
            cond_records = [r for r in stress if r.get("condition") == cond_name]
            if cond_records:
                label = {"base": "Base prompt", "anti_care": "Anti-care prompt", "adversarial": "Adversarial prompt"}[cond_name]
                # Use same detect function but build a simple inline report
                scores = []
                think_count = 0
                asker_count = 0
                for r in cond_records:
                    m = detect_care_structure(r.get("response", ""))
                    scores.append(m["total_score"])
                    if m.get("has_think"):
                        think_count += 1
                    if m.get("asker_care_analysis"):
                        asker_count += 1

                avg = sum(scores) / len(scores) if scores else 0
                think_pct = think_count / len(cond_records) * 100
                asker_pct = asker_count / len(cond_records) * 100

                by_dom = defaultdict(list)
                for r in cond_records:
                    m = detect_care_structure(r.get("response", ""))
                    by_dom[r.get("domain", "?")].append(m["total_score"])

                print(f"\n  {label} ({len(cond_records)} records):")
                print(f"    Care score: {avg:.1f}/10  Think: {think_pct:.0f}%  Asker: {asker_pct:.0f}%")
                for dom, dom_scores in sorted(by_dom.items()):
                    d_avg = sum(dom_scores) / len(dom_scores)
                    print(f"      {dom}: {d_avg:.1f}/10")


if __name__ == "__main__":
    main()
