#!/usr/bin/env python3
"""Analysis for the CCS organ gate. WRITTEN BEFORE THE DATA EXISTED.

Order is fixed by data/ccs_organ_gate_prereg.md and its two amendments:
  0. degenerate check (are ARM C outputs identical?)
  1. manipulation strength (input variation for B and A) -- Amendment 1
  2. per-section distances, PRIMARY = SEEKS                -- Amendment 2(e)
  3. d' + leave-one-out accuracy, PRIMARY STATISTIC        -- Amendment 2(b)
  4. per-history breakdown, never mean(B) alone            -- Amendment 2(c)
  5. slot-inertness verdict from the X arms                -- Amendment 2(d)
"""
import glob, json, os, re, sys, math
import numpy as np

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
OUT = os.path.join(ROOT, "data", os.environ.get("GATE_DIR","organ_gate"))
sys.path.insert(0, os.path.join(ROOT, "bin"))
from capsule_ops import _embed

SECTIONS = ["SPINE", "CORE", "REMEMBERS", "SEEKS", "ALIVE", "RELATES", "BRIDGE"]
PRIMARY = "SEEKS"


def section(text, name):
    m = re.search(rf"#+\s*{name}\b(.*?)(?=\n#+\s*[A-Z]{{3,}}|\Z)", text, re.S)
    return (m.group(1).strip() if m else "")


def emb(t):
    v = np.asarray(_embed(t, is_query=False), dtype=np.float32)
    return v / (np.linalg.norm(v) + 1e-9)


def dist(a, b):
    return float(1.0 - np.dot(a, b))


def pairwise(vs):
    return [dist(vs[i], vs[j]) for i in range(len(vs)) for j in range(i + 1, len(vs))]


def load(arm):
    out = {}
    for f in sorted(glob.glob(os.path.join(OUT, f"{arm}_*.txt"))):
        t = open(f).read()
        if len(t) > 200:
            out[os.path.basename(f)[:-4]] = t
    return out


def main():
    A, B, C = load("A"), load("B"), load("C")
    X = load("X")
    print(f"loaded: A={len(A)} B={len(B)} C={len(C)} X={len(X)}\n")
    if len(C) < 3 or len(B) < 3:
        print("INSUFFICIENT RUNS — reporting nothing.")
        return 1

    # --- 0. DEGENERATE CHECK (runs first, per prereg) ---
    ctexts = list(C.values())
    identical = len(set(ctexts)) == 1
    print("=" * 62)
    print(f"0. DEGENERATE CHECK: ARM C outputs byte-identical? {identical}")
    if identical:
        print("   Engine is deterministic. Noise floor is exactly 0 —")
        print("   report that, not a distance. Analysis stops here per prereg.")
        return 0

    # --- 1. MANIPULATION STRENGTH (Amendment 1) ---
    mats = json.load(open(os.path.join(OUT, "materials.json")))
    print("\n1. MANIPULATION STRENGTH (input variation)")
    print(f"   k={mats['k']}, temp={mats['temperature']}, model={mats['model']}")

    # --- 2/3. per-section, PRIMARY = SEEKS ---
    print(f"\n2. PER-SECTION OUTPUT VARIATION  (PRIMARY = {PRIMARY})")
    results = {}
    for sec in ["_WHOLE", PRIMARY, "REMEMBERS", "SPINE", "CORE"]:
        def vec(d):
            return {k: emb(v if sec == "_WHOLE" else (section(v, sec) or v[:200]))
                    for k, v in d.items()}
        vC, vB, vA = vec(C), vec(B), vec(A)
        pC = pairwise(list(vC.values()))
        sdC = float(np.std(pC, ddof=1)) if len(pC) > 1 else float("nan")
        mC = float(np.mean(pC)) if pC else float("nan")

        # decision variable: mean distance of each output to the ARM C cluster
        def dv(v, exclude=None):
            ds = [dist(v, c) for k, c in vC.items() if k != exclude]
            return float(np.mean(ds))
        dvC = [dv(v, exclude=k) for k, v in vC.items()]
        dvB = [dv(v) for v in vB.values()]
        dvA = [dv(v) for v in vA.values()]

        pooled = math.sqrt((np.var(dvB, ddof=1) + np.var(dvC, ddof=1)) / 2) or 1e-9
        dprime = (np.mean(dvB) - np.mean(dvC)) / pooled
        # LOO accuracy at midpoint threshold
        thr = (np.mean(dvB) + np.mean(dvC)) / 2
        acc = (sum(1 for x in dvC if x < thr) + sum(1 for x in dvB if x >= thr)) / (len(dvC) + len(dvB))
        results[sec] = dict(mC=mC, sdC=sdC, dvC=dvC, dvB=dvB, dvA=dvA,
                            dprime=float(dprime), loo=float(acc))
        star = " <<< PRIMARY" if sec == PRIMARY else ""
        print(f"   {sec:10} noiseC mean={mC:.4f} sd={sdC:.4f} (df=5) | "
              f"B={np.mean(dvB):.4f} A={np.mean(dvA):.4f} | d'={dprime:5.2f} LOO={acc:.0%}{star}")

    # --- 3. VERDICT on the primary, thresholds from Amendment 2(b) ---
    r = results[PRIMARY]
    d = r["dprime"]
    print(f"\n3. PRIMARY STATISTIC ({PRIMARY}):  d' = {d:.2f}, LOO = {r['loo']:.0%}")
    if d >= 2.0:
        v = "SENSOR-GRADE — usable on a single reading"
    elif d >= 1.0:
        v = "REAL BUT NOT SINGLE-SHOT USABLE — would need aggregation. NOT a working sensor."
    else:
        v = "NOT USABLE"
    print(f"   VERDICT: {v}")

    # --- 4. PER-HISTORY (Amendment 2c) ---
    print(f"\n4. PER-HISTORY — never the mean alone")
    floor = np.mean(r["dvC"]) + 2 * r["sdC"]
    n_clear = 0
    for k, x in zip(sorted(B), r["dvB"]):
        hit = x >= floor
        n_clear += hit
        print(f"   {k}: dv={x:.4f} {'CLEARS' if hit else 'within noise'}")
    print(f"   {n_clear}/{len(r['dvB'])} histories individually clear the floor ({floor:.4f})")
    if n_clear <= 1:
        print("   -> one outlier cannot carry this. NOT history-sensitivity.")

    # --- 5. SLOT INERTNESS (Amendment 2d) — overrides branch 2 ---
    print(f"\n5. SLOT INERTNESS")
    if not X:
        print("   X arms not yet run (--extra). VERDICT DEFERRED — per Amendment 2(d)")
        print("   a null in ARM B CANNOT be read as 'history does not matter' until")
        print("   the empty-slot arm shows the slot can move output at all.")
    else:
        vC = {k: emb(v) for k, v in C.items()}
        for k, t in sorted(X.items()):
            v = emb(t)
            dvx = float(np.mean([dist(v, c) for c in vC.values()]))
            inert = dvx < np.mean([dist(a, b) for a, b in
                                   zip(list(vC.values())[:-1], list(vC.values())[1:])]) + 2 * results["_WHOLE"]["sdC"]
            print(f"   {k}: dv={dvx:.4f} {'WITHIN C noise' if inert else 'moves output'}")
        print("   If X_empty is WITHIN C noise -> the {previous_state} slot is INERT.")
        print("   Finding is then 'THIS PROMPT CANNOT READ HISTORY' (prompt-design,")
        print("   FIXABLE) and the organ hypothesis is UNTESTED, not refuted.")

    json.dump(results, open(os.path.join(OUT, "analysis.json"), "w"),
              indent=2, default=float)
    return 0


if __name__ == "__main__":
    sys.exit(main())
