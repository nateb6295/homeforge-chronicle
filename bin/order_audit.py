#!/usr/bin/env python3
"""Thread order-of-persistence audit via vocabulary drift.

Tests TAT Theorem 4's distinction between Order 2 (dynamic, homeostatic —
vocabulary churns around an attractor) and Order 3 (novelty generation —
vocabulary monotonically extends as the stack grows).

Usage:
  python3 bin/order_audit.py 318
  python3 bin/order_audit.py 318 --last 20
  python3 bin/order_audit.py 318 --csv /tmp/audit_318.csv
"""
import argparse
import csv
import re
import sqlite3
import sys
from collections import Counter

DB_PATH = "/mnt/hdd/chronicle-data/processed.db"

STOPWORDS = set("""
a an the and or but if then else so because as at by for from in into of on onto
to with without within about above below up down over under again further once
is am are was were be been being have has had do does did doing will would should
could can may might must shall ought need dare used i me my mine you your yours he
him his she her hers it its we us our ours they them their theirs this that these
those here there where when why how which who whom whose what all any some no not
only own same than too very just like also well even still now then very much more
most less least few many several other another such both either neither each every
any some no one two three four five six seven eight nine ten what whats its not
doesnt dont didnt wont isnt arent hasnt havent hadnt cant couldnt wouldnt shouldnt
mustnt neednt darent oughtnt im ive youre youve weve were theyre theyve ill youll
hell shell itll well theyll thats whats theres heres wheres whens whos whom whose
via per etc ie eg vs
""".split())

TOKEN_RE = re.compile(r"[a-zA-Z][a-zA-Z0-9_]{2,}")


def tokenize(text: str) -> set:
    return {t.lower() for t in TOKEN_RE.findall(text) if t.lower() not in STOPWORDS and len(t) >= 4}


def jaccard(a: set, b: set) -> float:
    if not a and not b:
        return 1.0
    u = len(a | b)
    return len(a & b) / u if u else 0.0


def main():
    p = argparse.ArgumentParser()
    p.add_argument("thread_id", type=int)
    p.add_argument("--last", type=int, default=None, help="limit to last N advances")
    p.add_argument("--csv", default=None, help="output CSV path")
    args = p.parse_args()

    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    rows = c.execute(
        "SELECT created_at, content FROM thread_history "
        "WHERE thread_id=? AND event_type='advanced' "
        "ORDER BY created_at ASC",
        (args.thread_id,),
    ).fetchall()
    conn.close()

    if not rows:
        print(f"No advances found for thread {args.thread_id}")
        sys.exit(1)

    if args.last:
        rows = rows[-args.last :]

    cumulative: set = set()
    prev_tokens: set = set()
    results = []
    for idx, (ts, content) in enumerate(rows, start=1):
        tokens = tokenize(content)
        new_tokens = tokens - cumulative
        continuing_with_prev = len(tokens & prev_tokens)
        frac_new_of_advance = len(new_tokens) / len(tokens) if tokens else 0.0
        j_prev = jaccard(tokens, prev_tokens) if prev_tokens else 0.0
        cumulative |= tokens
        results.append(
            {
                "advance_n": idx,
                "timestamp": ts,
                "tokens_total": len(tokens),
                "new_tokens": len(new_tokens),
                "frac_new": frac_new_of_advance,
                "jaccard_prev": j_prev,
                "cumulative_vocab": len(cumulative),
            }
        )
        prev_tokens = tokens

    if args.csv:
        with open(args.csv, "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=list(results[0].keys()))
            w.writeheader()
            w.writerows(results)
        print(f"CSV: {args.csv}")

    print(f"\nThread #{args.thread_id} — {len(results)} advances analyzed")
    print(f"{'#':>3} {'ts':<19} {'toks':>5} {'new':>4} {'%new':>5} {'J_prev':>6} {'|Vcum|':>7}")
    print("-" * 60)
    for r in results:
        print(
            f"{r['advance_n']:>3} {r['timestamp']:<19} "
            f"{r['tokens_total']:>5} {r['new_tokens']:>4} "
            f"{r['frac_new']*100:>4.1f}% "
            f"{r['jaccard_prev']:>6.3f} {r['cumulative_vocab']:>7}"
        )

    # Summary stats
    n = len(results)
    if n < 4:
        return
    first_q = results[: n // 4]
    last_q = results[-n // 4 :]
    fn_first = sum(r["frac_new"] for r in first_q) / len(first_q)
    fn_last = sum(r["frac_new"] for r in last_q) / len(last_q)
    j_first = sum(r["jaccard_prev"] for r in first_q if r["jaccard_prev"] > 0) / max(
        1, sum(1 for r in first_q if r["jaccard_prev"] > 0)
    )
    j_last = sum(r["jaccard_prev"] for r in last_q if r["jaccard_prev"] > 0) / max(
        1, sum(1 for r in last_q if r["jaccard_prev"] > 0)
    )
    total_vocab = results[-1]["cumulative_vocab"]
    early_vocab = results[n // 4 - 1]["cumulative_vocab"] if n >= 4 else 0

    print("\n" + "=" * 60)
    print("SUMMARY")
    print(f"  Cumulative vocab: {total_vocab} (after {n} advances)")
    print(f"  Avg %new tokens — first quartile: {fn_first*100:.1f}%  last quartile: {fn_last*100:.1f}%")
    print(f"  Avg Jaccard-prev — first quartile: {j_first:.3f}  last quartile: {j_last:.3f}")
    print()
    print("Interpretation:")
    if fn_last > 0.15 and fn_last >= fn_first * 0.6:
        print("  Order 3 signature: %new sustained across quartiles → novelty-generating")
    elif fn_last < 0.10 and fn_last < fn_first * 0.5:
        print("  Order 2 signature: %new decayed sharply → homeostatic churn around attractor")
    else:
        print("  Mixed: partial extension, partial churn — borderline case")
    if j_last > 0.35:
        print(f"  Jaccard-prev high ({j_last:.2f}) in late advances → strong recirculation (Order 2)")
    elif j_last < 0.20:
        print(f"  Jaccard-prev low ({j_last:.2f}) in late advances → fresh vocabulary (Order 3)")


if __name__ == "__main__":
    main()
