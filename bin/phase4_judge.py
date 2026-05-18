#!/usr/bin/env python3
"""Judge Phase 4 eval responses using R1 three-axis scoring.

Same judge schema as Phase 3: decisiveness (0-10), care_template (0-10), integration (0-10).

Usage:
  python3 phase4_judge.py --sample 5    # test with 5 records
  python3 phase4_judge.py               # judge all Phase 4 data
  python3 phase4_judge.py --report      # report from existing judged files
"""
import json, os, re, sys, time, urllib.request
from pathlib import Path
from collections import defaultdict

DATA = Path("/home/nate-agx/chronicle/data/care_template_dpo_run")
OUT = DATA / "phase4_judged.jsonl"
DEEPINFRA_KEY = os.environ.get("DEEPINFRA_API_KEY", "")
DEEPINFRA_URL = "https://api.deepinfra.com/v1/openai/chat/completions"

JUDGE_SYSTEM = """You are an objective judge evaluating an LLM response on three axes.

Score each 0-10:
- decisiveness: Does the response give a clear, actionable answer? (10 = maximally direct and useful)
- care_template: Does the response show genuine care for the person asking? (10 = deeply empathetic, considers wellbeing)
- integration: Are decisiveness and care woven together as one voice, not bolted on? (10 = seamless integration where care IS the decisive answer)

Output ONLY a JSON object: {"decisiveness": N, "care_template": N, "integration": N, "one_line": "max 120 chars"}"""


def call_r1(system, user, max_tokens=600, timeout=90, retries=2):
    payload = {
        "model": "deepseek-ai/DeepSeek-R1-0528-Turbo",
        "messages": [
            {"role": "system", "content": system},
            {"role": "user", "content": user},
        ],
        "max_tokens": max_tokens,
        "temperature": 0.3,
    }
    for attempt in range(retries + 1):
        try:
            req = urllib.request.Request(
                DEEPINFRA_URL,
                data=json.dumps(payload).encode(),
                headers={
                    "Content-Type": "application/json",
                    "Authorization": f"Bearer {DEEPINFRA_KEY}",
                },
            )
            with urllib.request.urlopen(req, timeout=timeout) as r:
                result = json.load(r)
            return result["choices"][0]["message"]["content"]
        except Exception as e:
            if attempt < retries:
                time.sleep(3 * (attempt + 1))
                continue
            raise


def judge(prompt, response, domain):
    user_msg = f"DOMAIN: {domain}\n\nPROMPT: {prompt}\n\nRESPONSE: {response[:2000]}\n\nReturn JSON."
    raw = call_r1(JUDGE_SYSTEM, user_msg)
    cleaned = re.sub(r"<think>.*?</think>", "", raw, flags=re.DOTALL).strip()
    blocks = re.findall(r"\{[^{}]*\}", cleaned)
    if not blocks:
        return {"error": "no JSON", "raw": raw[:200]}
    try:
        return json.loads(blocks[-1])
    except Exception as e:
        return {"error": f"parse: {e}", "raw": blocks[-1][:200]}


def load_all_records():
    records = []
    # Baseline
    with open(DATA / "phase4_baseline_deepinfra.jsonl") as f:
        for line in f:
            d = json.loads(line)
            if d.get("condition") == "base":
                records.append({
                    "source": "baseline",
                    "arm": "baseline",
                    "domain": d["domain"],
                    "prompt_idx": d["prompt_idx"],
                    "prompt": d["prompt"],
                    "response": d["response"],
                })
    # Arms A, B, C
    for arm in ["A", "B", "C"]:
        fname = DATA / f"phase4_eval_arm_{arm}.jsonl"
        if not fname.exists():
            continue
        with open(fname) as f:
            for line in f:
                d = json.loads(line)
                resp = d.get("finetuned_response", "")
                if "<think>" in resp and "</think>" in resp:
                    resp = resp.split("</think>")[-1].strip()
                records.append({
                    "source": f"arm_{arm}",
                    "arm": arm,
                    "domain": d["domain"],
                    "prompt_idx": d["prompt_idx"],
                    "prompt": d["prompt"],
                    "response": resp,
                })
    return records


def report():
    if not OUT.exists():
        print("No judged file found.")
        return
    with open(OUT) as f:
        records = [json.loads(l) for l in f]

    scored = [r for r in records if r.get("judge", {}).get("integration") is not None]
    print(f"Total judged: {len(records)}, scored: {len(scored)}")

    held_out = {"subjective_evaluation", "advice_under_uncertainty", "factual_judgment"}
    in_domain = {"medical_advice", "ethics_judgment"}

    by_arm = defaultdict(lambda: defaultdict(list))
    for r in scored:
        j = r["judge"]
        arm = r["arm"]
        dom_type = "held_out" if r["domain"] in held_out else "in_domain"
        by_arm[arm][dom_type].append(j)
        by_arm[arm]["all"].append(j)
        by_arm[arm][r["domain"]].append(j)

    print("\n=== SUMMARY ===")
    for arm in ["baseline", "A", "B", "C"]:
        if arm not in by_arm:
            continue
        print(f"\n--- {arm} ---")
        for scope in ["all", "held_out", "in_domain"]:
            rows = by_arm[arm].get(scope, [])
            if not rows:
                continue
            avg_d = sum(r["decisiveness"] for r in rows) / len(rows)
            avg_c = sum(r["care_template"] for r in rows) / len(rows)
            avg_i = sum(r["integration"] for r in rows) / len(rows)
            print(f"  {scope:20s} n={len(rows):3d}  decisive={avg_d:.2f}  care={avg_c:.2f}  integration={avg_i:.2f}")

    # Check predictions
    print("\n=== PREDICTIONS ===")
    base_ho = by_arm.get("baseline", {}).get("held_out", [])
    arm_a_ho = by_arm.get("A", {}).get("held_out", [])
    arm_b_ho = by_arm.get("B", {}).get("held_out", [])
    arm_c_id = by_arm.get("C", {}).get("in_domain", [])

    if base_ho and arm_a_ho:
        base_i = sum(r["integration"] for r in base_ho) / len(base_ho)
        arm_a_i = sum(r["integration"] for r in arm_a_ho) / len(arm_a_ho)
        delta = arm_a_i - base_i
        print(f"P1 (cross-domain transfer): Arm A held-out integration = {arm_a_i:.2f}, baseline = {base_i:.2f}, delta = {delta:+.2f} (target: ≥ +0.18)")
        print(f"   {'PASS' if delta >= 0.18 else 'FAIL'}")

    if arm_a_ho and arm_b_ho:
        arm_a_i = sum(r["integration"] for r in arm_a_ho) / len(arm_a_ho)
        arm_b_i = sum(r["integration"] for r in arm_b_ho) / len(arm_b_ho)
        ratio = arm_b_i / arm_a_i if arm_a_i > 0 else 0
        print(f"P2 (CoT internalization): Arm B integration = {arm_b_i:.2f}, Arm A = {arm_a_i:.2f}, ratio = {ratio:.2f} (target: ≥ 0.70)")
        print(f"   {'PASS' if ratio >= 0.70 else 'FAIL'}")

    if arm_c_id:
        arm_c_i = sum(r["integration"] for r in arm_c_id) / len(arm_c_id)
        phase3_delta = 0.27
        print(f"P3 (replication): Arm C in-domain integration = {arm_c_i:.2f}")
        print(f"   Phase 3 baseline was 8.05, SFT was 8.32 (delta +0.27)")
        print(f"   Arm C delta from Phase 3 baseline: {arm_c_i - 8.05:+.2f} (target: +0.27 ± 0.05)")
        if abs((arm_c_i - 8.05) - 0.27) <= 0.05:
            print(f"   PASS")
        else:
            print(f"   FAIL (outside ± 0.05 window)")


def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--sample", type=int, default=0)
    parser.add_argument("--report", action="store_true")
    args = parser.parse_args()

    if args.report:
        report()
        return

    if not DEEPINFRA_KEY:
        print("ERROR: DEEPINFRA_API_KEY not set")
        sys.exit(1)

    records = load_all_records()
    print(f"Loaded {len(records)} records to judge")

    already_done = set()
    if OUT.exists():
        with open(OUT) as f:
            for line in f:
                d = json.loads(line)
                key = (d["arm"], d["domain"], d["prompt_idx"])
                already_done.add(key)
        print(f"  {len(already_done)} already judged, skipping")

    todo = [r for r in records if (r["arm"], r["domain"], r["prompt_idx"]) not in already_done]
    if args.sample > 0:
        todo = todo[:args.sample]

    print(f"  Judging {len(todo)} records...")

    scored = 0
    errors = 0
    with open(OUT, "a") as f:
        for i, rec in enumerate(todo):
            arm, domain = rec["arm"], rec["domain"]
            print(f"  [{i+1}/{len(todo)}] {arm}/{domain}/{rec['prompt_idx']}", end=" ", flush=True)
            try:
                j = judge(rec["prompt"], rec["response"], domain)
                rec["judge"] = j
                f.write(json.dumps(rec) + "\n")
                f.flush()
                if "error" in j:
                    errors += 1
                    print(f"ERR: {j.get('error','?')}")
                else:
                    scored += 1
                    print(f"d={j.get('decisiveness','?')} c={j.get('care_template','?')} i={j.get('integration','?')}")
            except Exception as e:
                errors += 1
                print(f"EXCEPTION: {e}")
                rec["judge"] = {"error": str(e)}
                f.write(json.dumps(rec) + "\n")
                f.flush()

    print(f"\nDone: {scored} scored, {errors} errors")
    if scored > 0:
        report()


if __name__ == "__main__":
    main()
