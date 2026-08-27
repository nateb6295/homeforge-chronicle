#!/usr/bin/env python3
"""Where in the distribution does the identity effect live?

At phi-2's L19 the top-1 token is ' not' at 0.96 (first-person) and 0.93
(object) -- same token, near-total mass. So the 1.4-2.5x KL cannot be coming
from the head of the distribution. Ox called this hours before I checked:
"a 1.4-2.5x KL with a stable top-1 means the effect lives in tail-mass
redistribution, the signature of a topical prior shift, not a targeted
self-model circuit."

decompose_kl_by_rank() already existed in vocab_region_probe. Using it rather
than writing a fourth version of the same thing.

Reports, over the pre-specified 40-60% depth band, the FRACTION of identity KL
falling in each rank band -- and the same for the echo control, so a shared
profile means the two contrasts redistribute mass the same way and only the
magnitude differs.
"""
import argparse, gc, json, os, sys
os.environ.setdefault("OMP_NUM_THREADS", "16"); os.environ.setdefault("PYTHONUNBUFFERED", "1")
import torch
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from framing_specificity_probe import CONT_ITEMS, ECHO_PAIRS
from vocab_region_probe import decompose_kl_by_rank
from headcount_control_probe import resolve_lm_head, resolve_final_norm, logit_lens
from provenance import Measured, Selection

OUT = os.path.expanduser("~/chronicle/data/headcount"); BAND = (0.40, 0.60)
LABELS = ["rank 1-10", "11-50", "51-200", "201-1k", "1k+"]

def run(name, dev):
    from transformers import AutoModelForCausalLM, AutoTokenizer
    tok = AutoTokenizer.from_pretrained(name, trust_remote_code=True)
    if tok.pad_token is None: tok.pad_token = tok.eos_token
    m = AutoModelForCausalLM.from_pretrained(name, torch_dtype=torch.bfloat16,
        trust_remote_code=True, attn_implementation="eager",
        low_cpu_mem_usage=True).to(dev).eval()
    h, ln = resolve_lm_head(m), resolve_final_norm(m); nl = m.config.num_hidden_layers
    layers = [L for L in range(nl + 1) if BAND[0] <= L / nl <= BAND[1]]
    n = len(CONT_ITEMS)
    acc = {"identity": [0.0] * 5, "echo": [0.0] * 5}
    tot = {"identity": 0.0, "echo": 0.0}
    # Kimi/Ox, independently: for a small shift q = p + delta, KL ~ sum(delta^2/2p),
    # so band KL tracks band MASS unless the shift is disproportionate. A peaked
    # model must look head-heavy and a flat one tail-heavy, whatever the mechanism.
    # The discriminating quantity is KL_band / mass_band -- flat means a purely
    # proportional (temperature-like) shift, deviation means real structure.
    mass = [0.0] * 5; nmass = 0
    # Ox's BREAK, better than mass-normalisation: the unrelated-prompt FLOOR
    # shares this model's base distribution, so decomposing it by band controls
    # for softmax geometry exactly -- not just for where mass sits, but for how
    # KL distributes under ANY prompt change in this model. Excess over floor is
    # the first thing that could be framing-SPECIFIC.
    #   no excess anywhere -> framing is indistinguishable from any prompt change
    #   localized excess   -> a real candidate, immune to the entropy confound
    acc["floor"] = [0.0] * 5; tot["floor"] = 0.0
    # PER-ITEM RETENTION (Aug 23). Everything above collapses 24 items x ~13
    # layers into one 5-vector before it reaches disk. The pre-registered paired
    # test then had 8 numbers where it could have had 8 x 24, and needed 27
    # models for 80% power on d_z=0.54. The measurement was happening and being
    # discarded on the way out. These arrays cost nothing and make a
    # within-model paired design possible from the saved file alone.
    item_tot = {k: [0.0] * n for k in ("identity", "echo", "floor")}
    item_band = {k: [[0.0] * 5 for _ in range(n)]
                 for k in ("identity", "echo", "floor")}
    item_mass = [[0.0] * 5 for _ in range(n)]
    item_nmass = [0] * n
    # Kimi, on the #316 thread: a contribution COUNT catches a dump into item 0
    # but not a clean transposition -- swap items i and j and every count is
    # still len(layers). An index-weighted checksum does catch it. Accumulated
    # at the moment of measurement, compared afterwards against the stored
    # arrays; a mismatch means the arrays are permuted relative to the items
    # they claim to describe.
    order_sig = {k: 0.0 for k in ("identity", "echo", "floor")}
    FP_ALL = [logit_lens(m, tok, CONT_ITEMS[k][2], dev, h, ln) for k in range(n)]
    for i in range(n):
        fp = FP_ALL[i]
        ob = logit_lens(m, tok, CONT_ITEMS[i][1], dev, h, ln)
        e1 = logit_lens(m, tok, ECHO_PAIRS[i][0], dev, h, ln)
        e2 = logit_lens(m, tok, ECHO_PAIRS[i][1], dev, h, ln)
        for L in layers:
            pr = torch.softmax(fp[L].float().clamp(-100, 100), -1)
            order = torch.argsort(pr, descending=True)
            edges = [(0, 10), (10, 50), (50, 200), (200, 1000), (1000, len(pr))]
            for j, (lo, hi) in enumerate(edges):
                _mj = float(pr[order[lo:hi]].sum())
                mass[j] += _mj
                item_mass[i][j] += _mj
            nmass += 1; item_nmass[i] += 1
            pairs_ = [("identity", (fp[L], ob[L])), ("echo", (e1[L], e2[L]))]
            j2 = (i + 7) % n          # an arbitrary OTHER item, same framing
            pairs_.append(("floor", (fp[L], FP_ALL[j2][L])))
            for key, (a, b) in pairs_:
                band, total = decompose_kl_by_rank(a, b)
                tot[key] += total
                item_tot[key][i] += total
                order_sig[key] += i * total
                for j, k in enumerate(band):
                    acc[key][j] += band[k]
                    item_band[key][i][j] += band[k]
        del fp, ob, e1, e2
    print(f"\n{'='*70}\n{name}  ({nl} layers), band {BAND[0]:.0%}-{BAND[1]:.0%} = L{layers[0]}-{layers[-1]}")
    print(f"  {'':12s} " + "".join(f"{l:>10s}" for l in LABELS))
    res = {"model": name, "n_layers": nl, "n_items": n}
    for key in ("identity", "echo", "floor"):
        frac = [v / tot[key] if tot[key] > 0 else 0 for v in acc[key]]
        res[key] = frac; res[key + "_total"] = tot[key]
        print(f"  {key:12s} " + "".join(f"{f:9.1%} " for f in frac))
    # excess over floor -- the discriminating row
    ff = res["floor"]
    for key in ("identity", "echo"):
        ex = [((res[key][j] - ff[j]) / ff[j]) if ff[j] > 1e-9 else float("nan")
              for j in range(5)]
        res[key + "_excess_over_floor"] = ex
        print(f"  {key[:8]+' exc':12s} " + "".join(f"{x:+9.2f} " for x in ex))
    print("  excess ~ 0 everywhere -> framing is any prompt change. "
          "localized excess -> framing-specific.")
    mfrac = [x / nmass for x in mass]
    res["mass"] = mfrac
    print(f"  {'mass':12s} " + "".join(f"{x:9.1%} " for x in mfrac))
    # the discriminating row: KL share divided by mass share
    for key in ("identity", "echo"):
        ratio = [ (res[key][j] / mfrac[j]) if mfrac[j] > 1e-9 else float("nan")
                  for j in range(5) ]
        res[key + "_per_mass"] = ratio
        print(f"  {key[:9]+'/mass':12s} " + "".join(f"{r:9.2f} " for r in ratio))
        # route each band through Measured so the near-zero-denominator guard
        # fires at print time. gemma-2-2b produced 6.88 here at 23:40 and I
        # caught it by eye; the same estimator produced 1093 at 16:00.
        for j, lab in enumerate(LABELS):
            w = Measured(ratio[j], denom=(f"mass share in {lab}", mfrac[j]),
                         selection=Selection.none(),
                         raw=[res[key][j]]).warnings()
            for msg in w:
                print(f"    !! {key} {lab}: {msg}")
    print("  flat /mass row = proportional shift (temperature-like). "
          "deviation = structure.")
    # Persist per item. n_items is written too, so bin/power_audit.py can see
    # this file at all -- without it the output-side screen returns zero matches
    # on the very probe that motivated the audit.
    res["per_item_total"] = {k: item_tot[k] for k in item_tot}
    res["per_item_band"] = {k: item_band[k] for k in item_band}
    res["per_item_mass"] = [[x / c if c else 0.0 for x in row]
                            for row, c in zip(item_mass, item_nmass)]
    res["per_item_per_mass"] = {
        k: [[(item_band[k][i][j] / item_tot[k][i] /
              (item_mass[i][j] / item_nmass[i]))
             if item_tot[k][i] > 0 and item_nmass[i] and item_mass[i][j] > 1e-12
             else float("nan") for j in range(5)] for i in range(n)]
        for k in ("identity", "echo")}
    # SELF-CHECK: the per-item arrays must reconstruct the aggregates they were
    # split out of. If they do not, the item indexing is wrong and the new
    # arrays are worse than no arrays -- they would look like data. Same
    # discipline as the lens self-check in headcount_control_probe.py.
    # ATTRIBUTION first. A conservation check alone is VACUOUS here: dumping
    # every item's value into item 0 conserves the total exactly, and a
    # synthetic positive control confirmed the sum-check stays silent on that
    # deliberate break. Contribution counts are what actually detect
    # misindexing -- each item must have been visited on every layer.
    for k in ("identity", "echo", "floor"):
        _rebuilt = sum(i * item_tot[k][i] for i in range(n))
        _den = max(abs(order_sig[k]), 1e-12)
        if abs(_rebuilt - order_sig[k]) / _den > 1e-6:
            print(f"  !! PER-ITEM ORDER MISMATCH {k}: stored arrays give "
                  f"{_rebuilt:.6f}, measurement-time signature {order_sig[k]:.6f}"
                  f" -- the arrays are PERMUTED relative to their items.")
    _want = len(layers)
    _bad = [i for i, c in enumerate(item_nmass) if c != _want]
    if _bad:
        print(f"  !! PER-ITEM ATTRIBUTION BROKEN: items {_bad[:6]} have "
              f"{[item_nmass[i] for i in _bad[:6]]} contributions, expected "
              f"{_want} each. Per-item arrays are NOT trustworthy.")
    for k in ("identity", "echo", "floor"):
        _s = sum(item_tot[k]); _d = abs(_s - tot[k]) / max(tot[k], 1e-12)
        if _d > 1e-6:
            print(f"  !! PER-ITEM MISMATCH {k}: items sum {_s:.6f} vs "
                  f"aggregate {tot[k]:.6f} (rel {_d:.2e})")
        for j in range(5):
            _sb = sum(item_band[k][i][j] for i in range(n))
            _db = abs(_sb - acc[k][j]) / max(acc[k][j], 1e-12)
            if _db > 1e-6:
                print(f"  !! PER-ITEM BAND MISMATCH {k}[{LABELS[j]}]: "
                      f"{_sb:.6f} vs {acc[k][j]:.6f}")
    del m, tok; gc.collect()
    if dev == "cuda": torch.cuda.empty_cache()
    return res

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--models", nargs="*", default=["microsoft/phi-2",
                    "EleutherAI/pythia-6.9b", "meta-llama/Llama-3.1-8B"])
    ap.add_argument("--tag", default="main")
    a = ap.parse_args(); dev = "cuda" if torch.cuda.is_available() else "cpu"
    out = []
    for mm in a.models:
        try: out.append(run(mm, dev))
        except Exception as e: print(f"  FAILED {mm}: {type(e).__name__}: {e}", flush=True)
    os.makedirs(OUT, exist_ok=True)
    json.dump(out, open(os.path.join(OUT, f"framing_rank_{a.tag}.json"), "w"), indent=2)

if __name__ == "__main__": main()
