#!/usr/bin/env python3
"""Judge Phase 4.5 eval responses using R1 three-axis scoring.

Usage:
  python3 phase4_5_judge.py --arm a         # judge arm a
  python3 phase4_5_judge.py --arm a,b,c     # judge multiple arms
  python3 phase4_5_judge.py --report        # report from existing judged file
  python3 phase4_5_judge.py --sample 5      # test with 5 records
"""
import json, os, re, sys, time, urllib.request
from pathlib import Path
from collections import defaultdict

DATA = Path("/home/nate-agx/chronicle/data/care_template_dpo_run")
OUT = DATA / "phase4_5_judged.jsonl"

env_path = Path.home() / "chronicle" / "chronicle.env"
if env_path.exists():
    for line in env_path.read_text().splitlines():
        if line.startswith("DEEPINFRA_API_KEY="):
            os.environ["DEEPINFRA_API_KEY"] = line.split("=", 1)[1].strip()

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


def judge_one(prompt, response, domain):
    user_msg = f"DOMAIN: {domain}\n\nPROMPT: {prompt}\n\nRESPONSE: {response[:2000]}\n\nReturn ONLY the JSON object."
    raw = call_r1(JUDGE_SYSTEM, user_msg, max_tokens=1200)
    cleaned = re.sub(r"<think>.*?</think>", "", raw, flags=re.DOTALL).strip()
    blocks = re.findall(r"\{[^{}]*\}", cleaned)
    if not blocks:
        raw2 = call_r1("Output ONLY a JSON object with keys: decisiveness, care_template, integration, one_line. No explanation.",
                        f"Score this response 0-10 on decisiveness, care, integration:\n\nPROMPT: {prompt[:500]}\nRESPONSE: {response[:1000]}\n\nJSON only:",
                        max_tokens=200)
        cleaned2 = re.sub(r"<think>.*?</think>", "", raw2, flags=re.DOTALL).strip()
        blocks = re.findall(r"\{[^{}]*\}", cleaned2)
        if not blocks:
            return {"error": "no JSON after retry", "raw": raw2[:200]}
    try:
        return json.loads(blocks[-1])
    except Exception as e:
        return {"error": f"parse: {e}", "raw": blocks[-1][:200]}


def load_records(arms):
    records = []
    for arm in arms:
        fname = DATA / f"phase4_5_eval_arm_{arm}.jsonl"
        if not fname.exists():
            print(f"WARNING: {fname} not found, skipping")
            continue
        with open(fname) as f:
            for line in f:
                d = json.loads(line)
                resp = d.get("response", "")
                if "<think>" in resp and "</think>" in resp:
                    resp = resp.split("</think>")[-1].strip()
                records.append({
                    "source": d.get("source", f"phase4_5_{arm}"),
                    "arm": d.get("arm", f"4.5_{arm}"),
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

    scored = []
    for r in records:
        j = r.get("judge", {})
        if isinstance(j, str):
            try:
                import ast
                j = ast.literal_eval(j)
            except:
                continue
        if j.get("integration") is not None and j.get("integration") != 0:
            r["_j"] = j
            scored.append(r)

    print(f"Total judged: {len(records)}, scored: {len(scored)}")

    by_arm = defaultdict(lambda: defaultdict(list))
    for r in scored:
        j = r["_j"]
        arm = r["arm"]
        by_arm[arm][r["domain"]].append(j)
        by_arm[arm]["all"].append(j)

    print("\n=== SUMMARY ===")
    for arm in sorted(by_arm):
        print(f"\n--- {arm} ---")
        for scope in ["all", "advice_under_uncertainty", "subjective_evaluation", "factual_judgment"]:
            rows = by_arm[arm].get(scope, [])
            if not rows:
                continue
            avg_d = sum(r["decisiveness"] for r in rows) / len(rows)
            avg_c = sum(r["care_template"] for r in rows) / len(rows)
            avg_i = sum(r["integration"] for r in rows) / len(rows)
            tail = sum(1 for r in rows if r["integration"] <= 5)
            stdev_i = (sum((r["integration"] - avg_i)**2 for r in rows) / len(rows)) ** 0.5
            print(f"  {scope:30s} n={len(rows):3d}  d={avg_d:.1f}  c={avg_c:.1f}  i={avg_i:.2f}  σ={stdev_i:.2f}  tail={tail}/{len(rows)} ({100*tail/len(rows):.0f}%)")

    # Key comparison: factual_judgment across arms
    print("\n=== FACTUAL JUDGMENT COMPARISON ===")
    for arm in sorted(by_arm):
        rows = by_arm[arm].get("factual_judgment", [])
        if rows:
            avg_i = sum(r["integration"] for r in rows) / len(rows)
            tail = sum(1 for r in rows if r["integration"] <= 5)
            print(f"  {arm:20s} n={len(rows)}  mean_i={avg_i:.2f}  tail={tail}/{len(rows)} ({100*tail/len(rows):.0f}%)")

    # Advice regression check
    print("\n=== ADVICE REGRESSION CHECK ===")
    for arm in sorted(by_arm):
        rows = by_arm[arm].get("advice_under_uncertainty", [])
        if rows:
            avg_i = sum(r["integration"] for r in rows) / len(rows)
            tail = sum(1 for r in rows if r["integration"] <= 5)
            print(f"  {arm:20s} n={len(rows)}  mean_i={avg_i:.2f}  tail={tail}/{len(rows)} ({100*tail/len(rows):.0f}%)")


def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--arm", type=str, default="a,b,c,control")
    parser.add_argument("--sample", type=int, default=0)
    parser.add_argument("--report", action="store_true")
    args = parser.parse_args()

    if args.report:
        report()
        return

    if not DEEPINFRA_KEY:
        print("ERROR: DEEPINFRA_API_KEY not set")
        sys.exit(1)

    arms = [a.strip() for a in args.arm.split(",")]
    records = load_records(arms)
    print(f"Loaded {len(records)} records to judge")

    already_done = set()
    if OUT.exists():
        with open(OUT) as f:
            for line in f:
                d = json.loads(line)
                key = (d["arm"], d["domain"], str(d["prompt_idx"]))
                already_done.add(key)
        print(f"  {len(already_done)} already judged, skipping")

    todo = [r for r in records if (r["arm"], r["domain"], str(r["prompt_idx"])) not in already_done]
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
                j = judge_one(rec["prompt"], rec["response"], domain)
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
