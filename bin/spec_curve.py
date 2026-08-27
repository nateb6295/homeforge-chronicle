#!/usr/bin/env python3
"""Specification-curve analysis for the witness measure.
Simonsohn, Simmons & Nelson — 'Specification Curve: Descriptive and Inferential
Statistics on All Reasonable Specifications'. PDF in data/attachments/.

WHY THIS EXISTS. On 2026-08-26 I found that three defensible analyses of ONE
model with identical weights spread 0.01066, which is 2.51x the 0.00425
architecture effect I had published that morning — while every individual
bootstrap CI was tight and every p-value was "significant". I derived "report
the range over method choices" from scratch, at the cost of two retractions.
The field formalised this a decade ago and has an INFERENCE PROCEDURE, which
is the part I did not have.

The three steps, per the paper:
  1. Enumerate the analytic decisions, their reasonable alternatives, and take
     the exhaustive non-redundant combination.
  2. Estimate all of them; plot the descriptive specification curve.
  3. Joint inference by permutation, using three test statistics:
       (i)   median overall point estimate
       (ii)  share of estimates with the dominant sign
       (iii) share with the dominant sign AND individually significant
     Note from the paper: null curves are typically NOT symmetric about zero,
     so the null must be simulated rather than assumed.

CRITICAL IMPLEMENTATION DETAIL. Specifications share the same underlying data,
so one shuffle must be drawn ONCE and applied to EVERY specification before the
curve is recomputed. Shuffling per-specification would destroy the dependence
structure and understate the null spread.
"""
import argparse, itertools, json, os, sys
import numpy as np
from math import comb, sqrt

RES = os.path.expanduser("~/chronicle/spectral-demon/results")

METRICS = ["spectral_entropy", "participation_ratio", "spectral_gap"]
NORMS = ["raw", "per_ln_n"]
MASKS = ["full", "probe"]
CONTRASTS = [("directive", "absent"), ("receptive", "absent"),
             ("control", "absent"), ("witness_neg", "absent_pos")]


def load(slug, mask):
    pre = "witness_neg2x2_" if mask == "full" else "witness_posmask_"
    p = os.path.join(RES, f"{pre}{slug}.json")
    if not os.path.exists(p):
        return None
    d = json.load(open(p))
    idx = {(r["condition"], r["prompt_idx"], r["layer"]): r for r in d["raw"]}
    return {"nl": d["n_layers"], "idx": idx,
            "probes": sorted({r["prompt_idx"] for r in d["raw"]}),
            "conds": set(d["conditions"])}


def windows(nl):
    b = list(range(2, nl - 1))
    t = len(b) // 3
    return {"all": list(range(nl + 1)), "body": b, "early": b[:t],
            "mid": b[t:2 * t], "late": b[2 * t:], "deep21_31": list(range(21, min(32, nl + 1)))}


# ORIENTATION, a Step-1 validity fix made 2026-08-26 after the first run.
# All metrics must point the SAME way with respect to the construct (spectral
# spread / effective rank). entropy rises with spread, participation_ratio rises
# with spread, but spectral_gap = sigma1/sigma2 FALLS with spread. Counting an
# inverse measure as a "disagreeing specification" is a validity error, not
# evidence — Simonsohn's Step 1 says to eliminate invalid combinations. The
# first run scored gap at -0.0427 against entropy +0.006 and called it dissent.
ORIENT = {"spectral_entropy": +1.0, "participation_ratio": +1.0, "spectral_gap": -1.0}


def value(row, metric, norm):
    v = row[metric]
    if not np.isfinite(v):
        return np.nan
    v = v / np.log(row["n_tokens"]) if norm == "per_ln_n" else v
    return ORIENT[metric] * v


def matrices(D, c1, c2, metric, norm, layers):
    P = D["probes"]
    layers = [L for L in layers if (c1, P[0], L) in D["idx"]]
    if len(layers) < 2:
        return None
    A = np.array([[value(D["idx"][(c1, p, L)], metric, norm) for L in layers] for p in P])
    B = np.array([[value(D["idx"][(c2, p, L)], metric, norm) for L in layers] for p in P])
    if not (np.isfinite(A).all() and np.isfinite(B).all()):
        return None
    return A, B


def spec_effect(A, B, flip=None):
    """flip: bool array over probes. True swaps that probe's condition labels."""
    if flip is not None:
        X = np.where(flip[:, None], B, A); Y = np.where(flip[:, None], A, B)
    else:
        X, Y = A, B
    return float(np.mean(X.mean(0) - Y.mean(0)))


def build(slug):
    """Step 1: enumerate the non-redundant specification set."""
    specs = []
    cache = {m: load(slug, m) for m in MASKS}
    for mask in MASKS:
        D = cache[mask]
        if D is None:
            continue
        W = windows(D["nl"])
        for c1, c2 in CONTRASTS:
            if c1 not in D["conds"] or c2 not in D["conds"]:
                continue
            for metric, norm, (wname, layers) in itertools.product(METRICS, NORMS, W.items()):
                # spectral_gap has no meaningful ln(n) normalisation — it is a ratio.
                if metric == "spectral_gap" and norm == "per_ln_n":
                    continue
                M = matrices(D, c1, c2, metric, norm, layers)
                if M is None:
                    continue
                specs.append({"mask": mask, "contrast": f"{c1}-{c2}", "metric": metric,
                              "norm": norm, "window": wname, "A": M[0], "B": M[1]})
    return specs


def curve(specs, flip=None):
    return np.array([spec_effect(s["A"], s["B"], flip) for s in specs])


def joint_stats(eff, sig):
    dom = np.sign(np.median(eff)) or 1.0
    return (float(np.median(eff)),
            float(np.mean(np.sign(eff) == dom)),
            float(np.mean((np.sign(eff) == dom) & sig)))


def significance(specs, n_perm=500, seed=0):
    """Per-spec two-sided sign-flip p, used for joint statistic (iii)."""
    rng = np.random.default_rng(seed)
    n = len(specs[0]["A"])
    obs = curve(specs)
    null = np.array([curve(specs, rng.integers(0, 2, n).astype(bool)) for _ in range(n_perm)])
    p = (np.sum(np.abs(null) >= np.abs(obs), axis=0) + 1) / (n_perm + 1)
    return obs, p < 0.05, null





# --------------------------------------------------------------- control check
# COMPILED 2026-08-26 from two failures the same day, both found only AFTER the
# result was in hand:
#
#  1. A steering experiment whose entire claim was "is this direction different
#     from random", run against ONE random draw. The prereg said "THE CONTROL IS
#     IN THE DESIGN" in capitals. Writing that is not designing it.
#  2. Six hours spent on per-layer significance before noticing that with 10
#     probes the exact permutation floor (2/1024 = 0.00195) sits ABOVE the
#     Bonferroni threshold over 32 layers (0.05/32 = 0.00156), so no layer could
#     pass at any effect size. A design limit, discovered retrospectively.
#
# Both are the same question — what resolution can this test reach — and both are
# arithmetic, not judgement. I classified them as judgement and paid twice.
# Ask BEFORE running, not after.

def control_resolution(n_total, n_group=None, two_sided=True):
    """Smallest p this test can possibly return. n_group=None => sign-flip over n_total."""
    from math import comb
    if n_group is None:
        space = 2 ** n_total                      # paired sign-flip
    else:
        space = comb(n_total, n_group)            # group relabelling
    return (2.0 if two_sided else 1.0) / space


def check_control(name, n_control, n_total=None, n_group=None,
                  n_tests=1, alpha=0.05, quiet=False):
    """Refuse to pretend a control is adequate. Returns (ok, info); prints why not."""
    problems = []
    if n_control < 2:
        problems.append(f"n={n_control} is not a control — a single draw has no distribution")
    elif n_control < 5:
        problems.append(f"n={n_control} gives no usable tail")
    floor = None
    if n_total is not None:
        floor = control_resolution(n_total, n_group)
        thresh = alpha / max(1, n_tests)
        if floor > thresh:
            problems.append(f"FLOOR ABOVE THRESHOLD: min achievable p={floor:.5f} > "
                            f"{alpha}/{n_tests}={thresh:.5f} — nothing can pass at any effect size")
    info = {"n_control": n_control, "floor": floor, "problems": problems, "ok": not problems}
    if problems and not quiet:
        print(f"  !! CONTROL CHECK [{name}]", file=sys.stderr)
        for p_ in problems:
            print(f"     - {p_}", file=sys.stderr)
    return info["ok"], info

# ------------------------------------------------------------------ mean guard
# COMPILED 2026-08-26 from a rule that failed as prose. "Report curves, not
# numbers" was written into a memory file that morning and violated the same day,
# twice, in the same document: once when a body mean straddling a sign change
# flipped from one word (grades -> rates), and once when an entire "geometry"
# section was built on |mean_a - mean_b|, which is the gap between two averages
# and not a distance at all.
#
# The failure is NOT "the sign changes somewhere" — a curve that is 95% positive
# with one negative layer is fine. It is NEAR-CANCELLATION: the mean sits close to
# zero relative to the typical per-element magnitude, so any perturbation flips it.
#
#   fragility = |mean(v)| / mean(|v|)     in [0, 1]
#     ~1.0  every element same sign and similar size — the mean means something
#     <0.3  the mean is a small residue of large opposing parts — DO NOT REPORT IT
#
# Returns the mean regardless (so nothing silently breaks) but says so loudly.

def rank_safety(x, y, flat_frac=0.01, ratio_warn=10.0):
    """Is a Spearman correlation between these two series safe to quote?

    For each series reports `iqr_frac` = IQR / full range -- how much of the
    series' own span the middle half actually occupies. When that is tiny, the
    middle half is effectively constant, its RANKS are set by numerical noise,
    and a rank correlation involving it is meaningless however significant it
    looks. Two flags, because one absolute threshold does not generalise:

      ABSOLUTE  iqr_frac < flat_frac (default 1%) -- near-constant middle.
      RELATIVE  the two series' iqr_frac differ by >= ratio_warn (default 10x)
                -- one series is being ranked on a far finer scale than the
                other, which is the same defect stated comparatively and does
                not depend on a calibrated constant.

    COMPILED 2026-08-27 FROM A REAL FAILURE, the way `--fragility` was. The
    all-layer cross-architecture study reported `falcon_7b x gpt2, Spearman
    r = +1.0000, p = 0.0000` on 12 matched layers. I read it as a pipeline leak.
    Ox read it as monotone profiles. Both wrong. The vector:

        falcon sigma1 deformation, matched to gpt2's 12 layers
        [54.4, -69.1, -58.2, -57.1, -57.1, -56.5, -56.4, -56.2, -56.0, -55.9, -55.0, 1628.9]

    Ten of twelve points sit at about -56, spanning 14.1 against a full range of
    1698. Spearman sees ONLY RANKS, so it ordered those ten by the fourth
    significant figure and matched gpt2's real structure perfectly. r = +1.000
    was a rank correlation between structure and noise.

    THE RULE, which had failed as prose because it was never written down:
    BEFORE TRUSTING A RANK CORRELATION, CHECK THE SPREAD OF THE MIDDLE OF EACH
    SERIES. Spearman ranks noise exactly as confidently as it ranks signal.
    `--fragility` catches an ill-conditioned MEAN; this catches an
    ill-conditioned RANKING. Neither catches the third failure in that study --
    an autocorrelation-blind null. For that one: SHIFT, do not shuffle.

    NOTE ON THE THRESHOLD, since a calibrated constant is exactly what this file
    warns about elsewhere: 1% was chosen because the failure case measures
    0.0007 and the series it was wrongly matched against measures 0.044 -- two
    orders of margin below and two above. It is one calibration point. Read the
    printed iqr_frac; do not lean on the flag.
    """
    def q(v, p):
        n = len(v)
        if n == 1:
            return v[0]
        i = p * (n - 1)
        lo = int(i)
        hi = min(lo + 1, n - 1)
        return v[lo] + (v[hi] - v[lo]) * (i - lo)

    out = {}
    for name, series in (("x", x), ("y", y)):
        v = sorted(float(t) for t in series if t is not None)
        n = len(v)
        full = (v[-1] - v[0]) if n >= 2 else 0.0
        iqr = (q(v, 0.75) - q(v, 0.25)) if n >= 4 else full
        frac = (iqr / full) if full > 0 else 1.0
        out[name] = {"n": n, "full_span": full, "iqr": iqr,
                     "iqr_frac": frac, "flat": frac < flat_frac}
    fx, fy = out["x"]["iqr_frac"], out["y"]["iqr_frac"]
    lo, hi = min(fx, fy), max(fx, fy)
    out["ratio"] = (hi / lo) if lo > 0 else float("inf")
    out["mismatched"] = out["ratio"] >= ratio_warn
    out["safe"] = not (out["x"]["flat"] or out["y"]["flat"] or out["mismatched"])
    out["flat_frac"] = flat_frac
    out["ratio_warn"] = ratio_warn
    return out


def print_rank_safety(x, y, label="pair", flat_frac=0.01, ratio_warn=10.0):
    r = rank_safety(x, y, flat_frac=flat_frac, ratio_warn=ratio_warn)
    print(f"  {label}")
    for k in ("x", "y"):
        d = r[k]
        mark = "   <-- NEAR-CONSTANT MIDDLE: ranks are noise" if d["flat"] else ""
        print(f"    {k}: n={d['n']:3d}  range {d['full_span']:12.3f}  "
              f"IQR {d['iqr']:11.3f}  = {d['iqr_frac']:.4f} of range{mark}")
    print(f"    scale mismatch between the two: {r['ratio']:.1f}x"
          + ("   <-- one series ranked on a far finer scale" if r["mismatched"] else ""))
    if r["safe"]:
        print(f"    -> SAFE to quote a rank correlation for this pair.")
    else:
        print(f"    -> DO NOT QUOTE a rank correlation here. A middle half spanning "
              f"under {flat_frac:.0%} of its own range is ordered by numerical noise, "
              f"and Spearman cannot tell that from signal.")
    return r


def _wilson(k, n, z=1.959963985):
    """Wilson score interval for a binomial proportion. Chosen over Wald because
    Wald is degenerate at k=0 and k=n, which is exactly where a sign family lands
    when the direction is perfectly consistent — the case worth reporting."""
    if n <= 0:
        return (0.0, 1.0)
    ph = k / n
    d = 1 + z * z / n
    centre = (ph + z * z / (2 * n)) / d
    half = (z / d) * sqrt(ph * (1 - ph) / n + z * z / (4 * n * n))
    return (max(0.0, centre - half), min(1.0, centre + half))


def sign_score(effects, alpha=0.05):
    """Summarise a FAMILY of curves by DIRECTIONAL CONSISTENCY, not magnitude.

    Taken from Beguš, Leban & Gero, R. Soc. Open Sci. 13:250829 (2026), §3.2:
    they face the same problem — a family of response curves stratified by coda
    length, where a mean across strata is meaningless — and sum the SIGN of the
    expected effect across strata. Their bit1 scores -10 (negative in all ten),
    the others 4 / -4 / -4 / -4.

    WHY THIS EXISTS HERE. `--fragility` (below) tells me when a mean is unsafe
    to quote — |mean(v)|/mean(|v|) under 0.30 means the mean is a small residue
    of large opposing parts. It has never told me what to quote INSTEAD. The
    witness depth-curves are exactly that shape: a per-layer effect family whose
    body mean is ill-conditioned because it crosses zero inside the window.

    ONE ADDITION THE PAPER DOES NOT MAKE, and 2026-08-26 is the reason: they
    report raw sign scores with no null. A statistic without its null is a
    number. Under the hypothesis that signs are independent coin flips, the
    score is a shifted Binomial, so an exact two-sided p is available and cheap.
    Report both or neither.

    effects: iterable of per-stratum effect sizes (layers, doses, coda lengths).
    Returns score, n, and the exact two-sided p that |score| is this extreme.
    """
    v = [float(x) for x in effects if x is not None]
    nz = [x for x in v if x != 0.0]
    n = len(nz)
    if n == 0:
        return {"score": 0, "n": 0, "n_zero": len(v), "p": 1.0,
                "verdict": "no non-zero effects — nothing to score"}
    score = sum(1 if x > 0 else -1 for x in nz)
    # INDEPENDENCE GUARD — Wald-Wolfowitz runs test. Added within an hour of
    # writing this function, because the first real thing I pointed it at
    # violated its assumption. The witness per-layer effect family (Mistral-7B
    # v0.1, receptive vs absent_pos, 33 layers) has sign sequence
    #   -- +++++++++++++++++++++++++++++++
    # 31 positive, 2 negative, but only TWO RUNS where 4.8 are expected,
    # z = -4.85. Those are not 33 independent votes; they are one sign change
    # at depth. The Binomial null would have reported p ~ 2e-7 for what is
    # effectively two observations.
    # A lag-1 autocorrelation check MISSED this (r = +0.106, "independence
    # roughly holds") because the body is nearly constant and the statistic was
    # dominated by end excursions. For a SIGN statistic the right diagnostic is
    # RUN STRUCTURE, not correlation. Use the diagnostic that matches the
    # statistic.
    runs = 1 + sum(1 for i in range(1, n)
                   if (nz[i] > 0) != (nz[i - 1] > 0))
    n1 = sum(1 for x in nz if x > 0); n2 = n - n1
    z_runs = None
    if n1 and n2 and n > 1:
        exp_runs = 2 * n1 * n2 / n + 1
        var_runs = (2 * n1 * n2 * (2 * n1 * n2 - n)) / (n * n * (n - 1))
        if var_runs > 0:
            z_runs = (runs - exp_runs) / sqrt(var_runs)
    # exact two-sided: P(|S| >= |score|) with S = 2*Binom(n,0.5) - n
    k = (n + abs(score)) // 2          # successes needed for this |score|
    tail = sum(comb(n, i) for i in range(k, n + 1))
    p = min(1.0, 2.0 * tail / (2 ** n))
    frac = abs(score) / n
    # SECOND INDEPENDENCE FAILURE, and the runs test is BLIND to it.
    # Found 00:50 the same night, one command before misusing this function.
    # The runs test catches ORDERED dependence (layers, depth, time). It cannot
    # catch strata that are unordered but computed from SHARED DATA — e.g.
    # spec_curve's own 240 specifications, which are mask x metric x norm x
    # window x contrast over the SAME measurements. Their signs are correlated
    # because the data is common, not because they are adjacent.
    # This file already knew: joint_stats() computes "share dominant sign", and
    # CLAUDE.md records that its null median is 66-78%, NOT 50%. That is exactly
    # a Binomial null being wrong by construction. spec_curve handles it with a
    # JOINT PERMUTATION test (significance(), below) rather than an assumption.
    # So: the Binomial p here is valid only for strata that are independent
    # SAMPLES. Unordered is necessary and not sufficient. When strata share
    # data, permute — do not assume.
    #
    # TWO MORE LIMITS, both surfaced 2026-08-27 by writing test cases for the
    # estimand rather than by reasoning about the code:
    #
    # (1) THE RUNS TEST CAN FALSELY BLOCK UNORDERED STRATA. It reads adjacency as
    #     evidence, so a family whose order is arbitrary but which happens to
    #     ARRIVE sorted shows runs=2 and gets flagged dependent. The comment above
    #     records the converse failure (blind to unordered strata sharing data);
    #     this is the same assumption biting the other way. For unordered strata
    #     the runs number is not interpretable in either direction — permute.
    #
    # (2) WHEN ALL SIGNS AGREE THE RUNS TEST IS UNDEFINED, because it needs both
    #     signs present (n1 or n2 = 0 -> z_runs is None -> blocked is False).
    #     So the function returns independent=True for a perfectly consistent
    #     family — and that is the case where you MOST want to know whether the
    #     strata are independent, since it is also the case with the smallest p.
    #     independent=True there means UNTESTED, not confirmed. The printer now
    #     says so out loud.
    blocked = z_runs is not None and z_runs < -1.96
    # THE ESTIMAND, added 2026-08-27. This function computed the TEST and never
    # reported the QUANTITY. score and p go out; the interpretable number does not.
    #
    # p_superiority = (score/n + 1)/2 is exactly the fraction of strata in which
    # A beats B — the PROBABILITY OF SUPERIORITY. Verified against 7 known cases:
    # it equals frac_pos identically, and is unmoved by a 1000x magnitude outlier.
    # Named by arXiv 2603.06946 (Kaya/Ghasemi/Hashemi, Joint MDPs), which points
    # out that the classical MDP formalism leaves the JOINT law over counterfactual
    # actions unspecified, so "probability of superiority" has nowhere to live in
    # it. Arrived here from a Nate capture; I had built the estimator in August off
    # a phonetics paper without knowing the quantity had a name.
    #
    # WHY IT WAS MISSING IS THE SAME DEFECT THIS FUNCTION EXISTS TO FIX.
    # `--fragility` says when a mean is unsafe to quote and never says what to
    # quote instead; sign_score was written to be that replacement. Then it
    # reported a test statistic and a p-value and, one level down, again failed to
    # hand back the number you would actually put in a sentence.
    #
    # THE INTERVAL RESPECTS THE RUNS GUARD. The witness family is p_sup = 0.939 at
    # Binomial p = 1.3e-07, on TWO RUNS in 33 layers — one sign change at depth,
    # not 33 votes. The p-value is already refused there. The ESTIMATE needs the
    # same discipline, so when blocked we also report a Wilson interval at
    # n_eff = runs and label the nominal one invalid. n_eff is NOT substituted
    # silently: an honest wide interval beats a quiet narrow one.
    n_pos = sum(1 for x in nz if x > 0)
    p_sup = n_pos / n
    ci = _wilson(n_pos, n)
    ci_eff = _wilson(round(p_sup * runs), runs) if blocked and runs >= 1 else None
    return {"score": score, "n": n, "n_zero": len(v) - n, "p": p,
            "p_superiority": p_sup, "ci": ci, "n_eff": runs if blocked else n,
            "ci_effective": ci_eff,
            "consistency": frac, "runs": runs, "z_runs": z_runs,
            "independent": not blocked,
            "floor": 2.0 / (2 ** n),
            "verdict": ("SIGNS ARE BLOCKED — p IS INVALID, do not report it. "
                        "The strata are not independent (adjacent ones share "
                        "sign). This is what a smooth curve with one sign "
                        "change looks like; it is one observation, not n."
                        if blocked else
                        "DIRECTIONALLY CONSISTENT" if p < alpha else
                        "not distinguishable from coin flips")}


def print_sign_score(effects, name="effect family", alpha=0.05):
    r = sign_score(effects, alpha)
    print(f"  {name}: sign score {r['score']:+d} of {r['n']} strata "
          f"({100*r['consistency']:.0f}% one direction)")
    if r["n_zero"]:
        print(f"    {r['n_zero']} exactly-zero effects excluded")
    print(f"    exact two-sided p = {r['p']:.4f}   (floor at n={r['n']}: "
          f"{r['floor']:.5f})")
    print(f"    P(A beats B) = {r['p_superiority']:.3f}  "
          f"95% CI [{r['ci'][0]:.3f}, {r['ci'][1]:.3f}]   <- THE NUMBER TO QUOTE")
    if r.get("z_runs") is not None:
        print(f"    runs {r['runs']} (z={r['z_runs']:+.2f}) — "
              f"{'BLOCKED, strata not independent' if not r['independent'] else 'independence OK'}")
    if r.get("ci_effective"):
        print(f"    THE CI ABOVE IS INVALID — strata are blocked. At n_eff="
              f"{r['n_eff']} (runs): [{r['ci_effective'][0]:.3f}, "
              f"{r['ci_effective'][1]:.3f}]. Quote THIS one, or neither.")
    if r["n_zero"] == 0 and r["runs"] == 1 and r["z_runs"] is None:
        print("    NOTE: all signs agree, so the runs test is UNDEFINED (it needs "
              "both signs present). Independence is UNTESTED here, not confirmed.")
    print(f"    -> {r['verdict']}")
    print("    NULL VALIDITY: the Binomial p assumes strata are independent "
          "SAMPLES.\n    The runs test above only catches ORDERED dependence. If "
          "these strata are\n    computed from shared data (e.g. analytic "
          "variants of one measurement),\n    the p is invalid regardless of "
          "runs — permute instead. See significance().")
    if r["p"] >= alpha:
        print("    Do NOT report the score without this p. A sign score is a "
              "count, and counts look decisive.")
    return r


FRAGILITY_FLOOR = 0.30


def guarded_mean(v, name="mean", floor=FRAGILITY_FLOOR, quiet=False):
    """Mean of v, with a loud warning when that mean is ill-conditioned."""
    v = np.asarray(v, dtype=float)
    v = v[np.isfinite(v)]
    if v.size == 0:
        return float("nan"), {"fragility": float("nan"), "ok": False, "n": 0}
    m = float(np.mean(v))
    scale = float(np.mean(np.abs(v)))
    frag = abs(m) / scale if scale > 0 else 0.0
    pos, neg = v[v > 0].sum(), -v[v < 0].sum()
    flips = int(np.sum(np.sign(v[1:]) != np.sign(v[:-1])))
    info = {"mean": m, "fragility": frag, "pos_mass": float(pos), "neg_mass": float(neg),
            "sign_flips": flips, "n": int(v.size), "ok": frag >= floor}
    if not info["ok"] and not quiet:
        print(f"  !! FRAGILE MEAN [{name}]: {m:+.6f} but fragility {frag:.2f} < {floor}\n"
              f"     {pos:.5f} positive mass vs {neg:.5f} negative mass, {flips} sign flips "
              f"over {v.size} elements.\n"
              f"     This mean is a small residue of large opposing parts. Report the CURVE.",
              file=sys.stderr)
    return m, info

# ---------------------------------------------------------------- depth null
# LAYER-PERMUTATION TEST. Stolen from arXiv 2607.18348, which uses hierarchical
# bootstrap + layer-permutation with Holm correction.
#
# WHY IT EXISTS: every permutation test I ran on 2026-08-26 shuffled CONDITION
# labels, which asks "is there an effect". After the summary statistic was
# retired that day, the DEPTH CURVE became the thing being claimed — and nothing
# I had asked whether the curve is distinguishable from a random allocation of
# the same values across depth. I published the shape before testing it.
#
# The permutation preserves the multiset of per-layer values exactly and destroys
# only their ORDER, so it isolates the question "does depth order carry
# information" from "are these values unusual".

def depth_stats(curve):
    """Two statistics that a structured depth curve should have and a shuffled one should not."""
    sg = np.sign(curve)
    runs = 1 + int(np.sum(sg[1:] != sg[:-1]))          # fewer runs = more contiguous structure
    d = curve - curve.mean()
    denom = float(np.sum(d * d))
    ac1 = float(np.sum(d[1:] * d[:-1]) / denom) if denom > 1e-30 else 0.0
    return runs, ac1


def depth_null(slug, mask="full", c1="directive", c2="absent", n_perm=10000, seed=0):
    rng = np.random.default_rng(seed)
    D = load(slug, mask)
    if D is None:
        return None
    P, nl = D["probes"], D["nl"]
    layers = list(range(1, nl + 1))                     # L0 excluded: precedes all attention
    val = lambda c, p, L: (D["idx"][(c, p, L)]["spectral_entropy"]
                           / np.log(D["idx"][(c, p, L)]["n_tokens"]))
    curve = np.array([np.mean([val(c1, p, L) - val(c2, p, L) for p in P]) for L in layers])
    o_runs, o_ac1 = depth_stats(curve)
    nr, na = [], []
    for _ in range(n_perm):
        r, a = depth_stats(rng.permutation(curve))
        nr.append(r); na.append(a)
    nr, na = np.array(nr), np.array(na)
    return {"n_layers": len(layers), "runs": o_runs, "runs_null_med": float(np.median(nr)),
            "p_runs": float((np.sum(nr <= o_runs) + 1) / (n_perm + 1)),
            "ac1": o_ac1, "ac1_null_med": float(np.median(na)),
            "p_ac1": float((np.sum(na >= o_ac1) + 1) / (n_perm + 1))}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--slug", default="mistral7b_v01")
    ap.add_argument("--perm", type=int, default=500)
    ap.add_argument("--fragility", action="store_true",
                    help="report whether each model's body mean is safe to quote")
    ap.add_argument("--rank-safety", action="store_true",
                    help="self-test the near-constant-middle check against the "
                         "2026-08-27 falcon x gpt2 failure it was compiled from")
    ap.add_argument("--depth-null", action="store_true",
                    help="layer-permutation test: is the DEPTH ORDER informative?")
    a = ap.parse_args()

    if a.rank_safety:
        # SELFTEST against the real failure. Both cases must come out as stated
        # or the check is not doing its job -- see rank_safety.__doc__.
        print("=== rank_safety selftest: the case it was compiled from ===\n")
        falcon = [54.4, -69.1, -58.2, -57.1, -57.1, -56.5, -56.4, -56.2,
                  -56.0, -55.9, -55.0, 1628.9]
        gpt2 = [252.4, 6.9, 16.8, 21.4, 28.4, 31.7, 36.5, 41.1, 54.2, 75.7,
                161.1, 1244.3]
        r = print_rank_safety(falcon, gpt2,
                              "falcon_7b x gpt2, sigma1 deformation (reported r=+1.0000, p=0.0000)")
        assert r["x"]["flat"], "FAILED: falcon's near-constant middle was not caught"
        assert not r["y"]["flat"], "FAILED: gpt2 was wrongly flagged as flat"
        assert not r["safe"], "FAILED: the pair was called safe"
        print()
        spread = [1.0, 4.0, 9.0, 16.0, 25.0, 36.0, 49.0, 64.0, 81.0, 100.0, 121.0, 144.0]
        r2 = print_rank_safety(spread, gpt2, "control: a genuinely spread series vs gpt2")
        assert not r2["x"]["flat"], "FAILED: a well-spread series was flagged"
        assert r2["safe"], f"FAILED: a well-spread pair was called unsafe ({r2['ratio']:.1f}x)"
        print("\nselftest PASSED: flags the real failure, clears the control.")
        return

    if a.fragility:
        print(f"=== {a.slug} — is the body mean safe to quote? ===\n")
        for mask in ("full", "probe"):
            D = load(a.slug, mask)
            if D is None:
                continue
            P, nl = D["probes"], D["nl"]
            layers = list(range(2, nl - 1))
            val = lambda c, p, L: (D["idx"][(c, p, L)]["spectral_entropy"]
                                   / np.log(D["idx"][(c, p, L)]["n_tokens"]))
            curve = np.array([np.mean([val("directive", p, L) - val("absent", p, L) for p in P])
                              for L in layers])
            m, info = guarded_mean(curve, f"{a.slug}/{mask} directive-absent")
            verdict = "safe to quote" if info["ok"] else "DO NOT QUOTE — report the curve"
            print(f"  {mask:6s} mean {m:+.6f}  fragility {info['fragility']:.2f}  "
                  f"{info['sign_flips']} flips / {info['n']} layers  -> {verdict}")
        return

    if a.depth_null:
        print(f"=== {a.slug} — layer-permutation null (L1..Ln, L0 excluded) ===")
        print("  permutation preserves the values, destroys only their depth ORDER\n")
        print(f"  {'positions':<14s} {'sign runs':>10s} {'null med':>9s} {'p':>8s} "
              f"{'lag-1 ac':>9s} {'null med':>9s} {'p':>8s}")
        for mask in ("full", "probe"):
            r = depth_null(a.slug, mask)
            if r is None:
                print(f"  {mask:<14s} (no data)"); continue
            print(f"  {mask:<14s} {r['runs']:10d} {r['runs_null_med']:9.1f} {r['p_runs']:8.4f} "
                  f"{r['ac1']:+9.3f} {r['ac1_null_med']:+9.3f} {r['p_ac1']:8.4f}")
        return

    specs = build(a.slug)
    if not specs:
        sys.exit(f"no specifications for {a.slug}")
    obs, sig, null = significance(specs, a.perm)
    print(f"=== {a.slug} — {len(specs)} specifications, {a.perm} permutations ===\n")

    o_med, o_dom, o_domsig = joint_stats(obs, sig)
    print("STEP 2 — descriptive specification curve")
    print(f"  median effect        {o_med:+.6f}")
    print(f"  range                [{obs.min():+.6f}, {obs.max():+.6f}]")
    print(f"  share dominant sign  {o_dom:.1%}   ({int(o_dom*len(obs))}/{len(obs)})")
    print(f"  share individually significant {np.mean(sig):.1%}")
    print(f"  SIGN FLIPS across specifications: "
          f"{int(np.sum(obs>0))} positive / {int(np.sum(obs<0))} negative")

    # Step 3: joint inference. One shuffle applied to ALL specifications.
    rng = np.random.default_rng(1234)
    n = len(specs[0]["A"])
    ns = []
    for _ in range(a.perm):
        f = rng.integers(0, 2, n).astype(bool)
        e = curve(specs, f)
        # per-spec significance under this shuffle, reusing the null bank
        s = np.abs(e) >= np.percentile(np.abs(null), 95, axis=0)
        ns.append(joint_stats(e, s))
    ns = np.array(ns)
    print("\nSTEP 3 — joint inference (Simonsohn's three test statistics)")
    for i, (nm, o) in enumerate([("median effect", o_med),
                                 ("share dominant sign", o_dom),
                                 ("share dominant & significant", o_domsig)]):
        col = ns[:, i]
        cmp_ = np.abs(col) >= abs(o) if i == 0 else col >= o
        p = (np.sum(cmp_) + 1) / (a.perm + 1)
        print(f"  {nm:30s} observed {o:+.6f}   null median {np.median(col):+.6f}   p = {p:.4f}")

    print("\nMOST INFLUENTIAL DECISION (spread of median effect across each choice)")
    for key in ["mask", "metric", "norm", "window", "contrast"]:
        vals = sorted({s[key] for s in specs})
        meds = {v: np.median([obs[i] for i, s in enumerate(specs) if s[key] == v]) for v in vals}
        spread = max(meds.values()) - min(meds.values())
        best, worst = max(meds, key=meds.get), min(meds, key=meds.get)
        print(f"  {key:9s} spread {spread:.6f}   {worst}={meds[worst]:+.6f} .. {best}={meds[best]:+.6f}")


if __name__ == "__main__":
    main()
