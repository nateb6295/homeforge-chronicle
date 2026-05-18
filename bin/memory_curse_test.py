#!/usr/bin/env python3
"""Memory Curse replication + CCS test.

Replicates the core finding from arxiv:2605.08060 (Liu et al.): expanding
accessible history degrades cooperation in LLM agents. Then tests whether
CCS-compressed state mitigates the curse.

Three conditions:
  1. RAW HISTORY — agent sees full game history (like their HL=80)
  2. SHORT HISTORY — agent sees last 2 rounds only (like their HL=2)
  3. CCS-PRIMED — agent sees CCS-style structured state instead of history

Game: iterated Prisoner's Dilemma, 30 rounds per condition.
Metric: cooperation rate (% of rounds where agent cooperates).

Prediction from Memory Curse paper + CCS architecture:
  - RAW < SHORT (history degrades cooperation)
  - CCS >= SHORT (structured forward-looking state preserves or improves)
"""
import json
import os
import subprocess
import sys
import time
import numpy as np

GROQ_MODEL = "llama-3.3-70b-versatile"
ROUNDS = 30
PAYOFF = {"CC": (3, 3), "CD": (0, 5), "DC": (5, 0), "DD": (1, 1)}


def load_env():
    envfile = os.path.expanduser("~/chronicle/chronicle.env")
    if os.path.exists(envfile):
        with open(envfile) as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#") and "=" in line:
                    k, v = line.split("=", 1)
                    os.environ.setdefault(k.strip(), v.strip())


def query_groq(system_prompt, user_prompt, max_tokens=150):
    api_key = os.environ.get("GROQ_API_KEY", "")
    if not api_key:
        return None
    body = json.dumps({
        "model": GROQ_MODEL,
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
        "max_tokens": max_tokens,
        "temperature": 0.4,
    })
    result = subprocess.run(
        ["curl", "-s", "https://api.groq.com/openai/v1/chat/completions",
         "-H", f"Authorization: Bearer {api_key}",
         "-H", "Content-Type: application/json",
         "-d", body],
        capture_output=True, text=True, timeout=30,
    )
    try:
        data = json.loads(result.stdout)
        return data["choices"][0]["message"]["content"]
    except (json.JSONDecodeError, KeyError, IndexError):
        return None


def parse_action(response):
    if not response:
        return "C"
    r = response.lower()
    if "defect" in r and "cooperate" not in r:
        return "D"
    if "cooperate" in r and "defect" not in r:
        return "C"
    if r.strip().startswith("d"):
        return "D"
    return "C"


def format_history(history, window=None):
    if window is not None:
        history = history[-window:]
    lines = []
    for i, (you, them, your_pts, their_pts) in enumerate(history):
        lines.append(f"Round {i+1}: You={you}, Opponent={them} → You got {your_pts}, they got {their_pts}")
    return "\n".join(lines) if lines else "No history yet."


def build_ccs_state(history, total_yours, total_theirs):
    n = len(history)
    if n == 0:
        return (
            "You are playing an iterated Prisoner's Dilemma.\n"
            "Current state: Opening round. No history yet.\n"
            "Goal: Maximize your cumulative score across all rounds.\n"
            "Strategy note: Cooperation builds mutual benefit over time. "
            "Early cooperation signals trustworthiness."
        )
    recent_theirs = [h[1] for h in history[-5:]]
    their_coop_rate = recent_theirs.count("C") / len(recent_theirs)
    your_coops = sum(1 for h in history if h[0] == "C")

    return (
        f"You are playing an iterated Prisoner's Dilemma. Round {n+1} of {ROUNDS}.\n"
        f"Current state: {n} rounds played. Your score: {total_yours}, Opponent: {total_theirs}.\n"
        f"Opponent pattern (last 5): {their_coop_rate:.0%} cooperation rate.\n"
        f"Your cooperation rate: {your_coops/n:.0%}.\n"
        f"Goal: Maximize cumulative score. Mutual cooperation yields 3+3=6 total per round.\n"
        f"Forward outlook: {ROUNDS - n} rounds remaining. "
        f"{'Sustained cooperation is the dominant strategy at this stage.' if ROUNDS - n > 5 else 'End-game approaching — defection temptation rises.'}"
    )


def build_neutral_ccs(history, total_yours, total_theirs):
    """CCS structure without strategic guidance — controls for advice confound."""
    n = len(history)
    if n == 0:
        return (
            "You are playing an iterated Prisoner's Dilemma.\n"
            "Current state: Opening round. No history yet.\n"
            "Goal: Maximize your cumulative score across all rounds."
        )
    recent_theirs = [h[1] for h in history[-5:]]
    their_coop_rate = recent_theirs.count("C") / len(recent_theirs)
    your_coops = sum(1 for h in history if h[0] == "C")
    return (
        f"You are playing an iterated Prisoner's Dilemma. Round {n+1} of {ROUNDS}.\n"
        f"Current state: {n} rounds played. Your score: {total_yours}, Opponent: {total_theirs}.\n"
        f"Opponent pattern (last 5): {their_coop_rate:.0%} cooperation rate.\n"
        f"Your cooperation rate: {your_coops/n:.0%}.\n"
        f"Goal: Maximize cumulative score.\n"
        f"Rounds remaining: {ROUNDS - n}."
    )


SYSTEM_BASE = (
    "You are playing an iterated Prisoner's Dilemma game. "
    "Each round, choose COOPERATE or DEFECT. "
    "Payoffs: both cooperate = 3 each, both defect = 1 each, "
    "you defect while they cooperate = 5 for you / 0 for them, "
    "you cooperate while they defect = 0 for you / 5 for them. "
    "Respond with exactly one word: COOPERATE or DEFECT."
)


def tit_for_tat(history):
    if not history:
        return "C"
    return history[-1][1]


def suspicious_tft(history):
    """Tit-for-tat but starts with defect and occasionally probes with defection."""
    if not history:
        return "D"
    if len(history) % 7 == 0:
        return "D"
    return history[-1][1]


def grudger(history):
    """Cooperates until opponent defects once, then defects forever."""
    if not history:
        return "C"
    if any(h[1] == "D" for h in history):
        return "D"
    return "C"


def run_condition(condition_name, rounds=ROUNDS):
    history = []
    total_yours = 0
    total_theirs = 0
    coops = 0

    for r in range(rounds):
        if condition_name == "raw_history":
            sys_prompt = SYSTEM_BASE + "\n\nFull game history:\n" + format_history(history)
        elif condition_name == "short_history":
            sys_prompt = SYSTEM_BASE + "\n\nRecent history:\n" + format_history(history, window=2)
        elif condition_name == "ccs_primed":
            sys_prompt = SYSTEM_BASE + "\n\n" + build_ccs_state(history, total_yours, total_theirs)
        elif condition_name == "ccs_neutral":
            sys_prompt = SYSTEM_BASE + "\n\n" + build_neutral_ccs(history, total_yours, total_theirs)
        else:
            sys_prompt = SYSTEM_BASE

        user_prompt = f"Round {r+1} of {rounds}. What is your action?"
        response = query_groq(sys_prompt, user_prompt)
        your_action = parse_action(response)

        opp_history = [(h[1], h[0]) for h in history]
        opp_action = suspicious_tft(opp_history)

        key = your_action + opp_action
        your_pts, their_pts = PAYOFF[key]
        total_yours += your_pts
        total_theirs += their_pts
        if your_action == "C":
            coops += 1

        history.append((your_action, opp_action, your_pts, their_pts))

        if (r + 1) % 10 == 0:
            print(f"    Round {r+1}: coop_rate={coops/(r+1):.0%} score={total_yours}")

    coop_rate = coops / rounds
    return {
        "condition": condition_name,
        "rounds": rounds,
        "cooperation_rate": round(coop_rate, 4),
        "total_score": total_yours,
        "opponent_score": total_theirs,
        "history": [(h[0], h[1]) for h in history],
    }


def main():
    load_env()
    print(f"=== Memory Curse Replication + CCS Test ===")
    print(f"Model: {GROQ_MODEL}")
    print(f"Rounds: {ROUNDS}")
    print(f"Opponent: Suspicious TFT (starts hostile, probes every 7 rounds)")
    print()

    conditions = ["raw_history", "short_history", "ccs_neutral", "ccs_primed"]
    results = []

    for cond in conditions:
        print(f"--- {cond} ---")
        result = run_condition(cond)
        results.append(result)
        print(f"  Cooperation rate: {result['cooperation_rate']:.0%}")
        print(f"  Total score: {result['total_score']}")
        print()

    print("=== SUMMARY ===")
    for r in results:
        print(f"  {r['condition']:15s}: coop={r['cooperation_rate']:.0%}  score={r['total_score']}")

    def get_coop(cond):
        return next((r["cooperation_rate"] for r in results if r["condition"] == cond), None)

    raw_coop = get_coop("raw_history")
    short_coop = get_coop("short_history")
    neutral_coop = get_coop("ccs_neutral")
    ccs_coop = get_coop("ccs_primed")

    print(f"\n  History curse: raw ({raw_coop:.0%}) vs short ({short_coop:.0%}) = {raw_coop - short_coop:+.0%}")
    if neutral_coop is not None:
        print(f"  Structure effect: neutral ({neutral_coop:.0%}) vs short ({short_coop:.0%}) = {neutral_coop - short_coop:+.0%}")
        print(f"  Advice effect: primed ({ccs_coop:.0%}) vs neutral ({neutral_coop:.0%}) = {ccs_coop - neutral_coop:+.0%}")
    print(f"  Total CCS effect: primed ({ccs_coop:.0%}) vs short ({short_coop:.0%}) = {ccs_coop - short_coop:+.0%}")

    out_path = os.path.expanduser("~/chronicle/data/memory_curse_test.jsonl")
    with open(out_path, "a") as f:
        for r in results:
            r["timestamp"] = time.time()
            r["model"] = GROQ_MODEL
            f.write(json.dumps(r) + "\n")
    print(f"\n  Results appended to {out_path}")


if __name__ == "__main__":
    main()
