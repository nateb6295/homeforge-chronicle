#!/usr/bin/env python3
"""Position-masked SVD — the test CLAUDE.md's standing rule demands and nothing implemented.

Rule: "Any sigma_1-based claim is presumed sink artifact until it survives
position-masked SVD -- and never test this by ABLATING the sink, which collapses
attention entropy and makes a negative uninterpretable."

bin/sink_break_probe.py uses ablation. This does not touch attention at all: it
drops position 0 from the MATRIX the SVD sees. Entropy is untouched, so a
negative stays interpretable.

Prereg: data/position_masked_svd_prereg.md  (predictions committed before run)

Usage: OMP_NUM_THREADS=16 PYTHONUNBUFFERED=1 python3 bin/position_masked_svd.py
"""
import os, json, itertools
os.environ.setdefault("OMP_NUM_THREADS", "16")
os.environ.setdefault("PYTHONUNBUFFERED", "1")
import numpy as np, torch
from transformers import AutoModelForCausalLM, AutoTokenizer

MODEL_ID = "EleutherAI/pythia-410m"
PROMPTS = [
    # LONGER passages. Run 1 used ~12-token sentences; H then has ~12 rows and the
    # SVD is unstable, which is the likeliest cause of the positive control missing
    # the Aug 23 spread (got 1.14-1.58 deg, expected 0.23-0.32 deg). More rows,
    # same everything else. Mechanism-driven fix, not a preference-driven one.
    "The capital of France is Paris, a city known for its architecture and its long "
    "history of revolution, art, and public life. Visitors walk the same streets that "
    "were widened deliberately in the nineteenth century to prevent the building of "
    "barricades, and most of them never learn this.",
    "Photosynthesis converts light energy into chemical energy inside plant cells. The "
    "reaction splits water, releases oxygen as a by-product, and stores the remaining "
    "energy in bonds that almost every other organism on the planet eventually depends "
    "on, directly or through a food chain.",
    "She opened the door slowly and looked into the empty hallway. The light at the far "
    "end had been left on, which was not how she remembered leaving it, and for a long "
    "moment she stood there deciding whether that meant anything at all or whether she "
    "had simply forgotten.",
    "Sorting algorithms trade memory against time in ways that matter more at scale than "
    "in a textbook. Quicksort is fast in the average case and quadratic in the worst, "
    "mergesort is predictable but allocates, and the right answer depends on data you "
    "usually do not have when you choose.",
    "The treaty was signed in seventeen eighty-three, ending a war that had lasted eight "
    "years and reshaped the borders of a continent. The negotiators worked from competing "
    "maps, several of which were wrong, and the errors persisted in law for decades after "
    "anyone remembered their origin.",
    "Rain fell steadily on the tin roof throughout the long afternoon. Inside, nobody "
    "spoke much. The sound was loud enough to make conversation an effort and pleasant "
    "enough that no one wanted to, so the room settled into the kind of silence that is "
    "companionable rather than strained.",
    "Quantum entanglement correlates the outcomes of measurements on separated particles "
    "in a way that cannot be reproduced by any local classical model. This does not permit "
    "signalling faster than light, a point that is misunderstood constantly, because the "
    "correlations only become visible when results are compared.",
    "He could not remember whether he had locked the car or not. This happened often "
    "enough that he had stopped trusting the memory entirely and simply walked back to "
    "check, which took two minutes and cost him nothing except the small recurring "
    "admission that his attention was not where he wanted it.",
    "Bread requires flour, water, salt, and time, and the last of these is the one people "
    "try hardest to skip. A slow ferment develops flavour that no additive reproduces, "
    "and the dough tells you when it is ready through texture rather than through any "
    "number written on a timer.",
    "Market volatility increased sharply following the announcement, though the underlying "
    "figures had been available to anyone willing to read the filing two weeks earlier. "
    "Price moved on attention rather than on information, which is a distinction that "
    "analysts describe often and act on rarely.",
    "Migratory birds navigate using magnetic fields, star patterns, and remembered "
    "landmarks, switching between these systems as conditions change. A bird raised "
    "indoors under an artificial sky will orient to it, which suggests the map is learned "
    "even where the compass is not.",
    "The argument rested on a premise that nobody in the room accepted, and yet it took "
    "almost an hour before anyone said so out loud. Each person assumed the others had "
    "reasons for their silence, and the assumption propagated until the meeting had "
    "spent most of its time on a question that did not need answering.",
]

def sign_fix(v):
    """v and -v are the same direction. Anchor on the largest-|.| component."""
    return v * np.sign(v[np.argmax(np.abs(v))])

def top_right_sv(H):
    """First right-singular vector of H (rows = positions)."""
    Hc = H - H.mean(axis=0, keepdims=True)      # centre; sigma_1 of raw H is ~the mean
    _, _, Vh = np.linalg.svd(Hc, full_matrices=False)
    return sign_fix(Vh[0])

def mean_pairwise_angle(vs):
    ang = [np.degrees(np.arccos(np.clip(abs(float(a @ b)), -1, 1)))
           for a, b in itertools.combinations(vs, 2)]
    return float(np.mean(ang)), float(np.std(ang))

def main():
    tok = AutoTokenizer.from_pretrained(MODEL_ID)
    model = AutoModelForCausalLM.from_pretrained(MODEL_ID, torch_dtype=torch.float32)
    model.eval()
    nl = model.config.num_hidden_layers
    per_layer = {L: {"unmasked": [], "masked": [], "cos_bos": [], "bos_ratio": []}
                 for L in range(nl + 1)}

    for p in PROMPTS:
        ids = tok(p, return_tensors="pt")
        with torch.no_grad():
            out = model(**ids, output_hidden_states=True)
        for L, hs in enumerate(out.hidden_states):
            H = hs[0].float().numpy()                     # (seq, dim)
            if H.shape[0] < 4:
                continue
            v_un = top_right_sv(H)
            v_ma = top_right_sv(H[1:])                    # position 0 dropped
            per_layer[L]["unmasked"].append(v_un)
            per_layer[L]["masked"].append(v_ma)
            bos = H[0] / (np.linalg.norm(H[0]) + 1e-9)
            per_layer[L]["cos_bos"].append(abs(float(v_un @ bos)))
            # massive activation present? BoS norm vs median of the rest
            per_layer[L]["bos_ratio"].append(
                float(np.linalg.norm(H[0]) / (np.median(np.linalg.norm(H[1:], axis=1)) + 1e-9)))

    print(f"{MODEL_ID}   n_prompts={len(PROMPTS)}   layers=0..{nl}")
    print(f"{'L':>3} {'bosNorm/med':>11} {'|cos(s1,bos)|':>13} "
          f"{'UNMASKED spread':>16} {'MASKED spread':>14}")
    rows = []
    for L in range(nl + 1):
        d = per_layer[L]
        if len(d["unmasked"]) < 2:
            continue
        u_m, _ = mean_pairwise_angle(d["unmasked"])
        m_m, _ = mean_pairwise_angle(d["masked"])
        br, cb = float(np.mean(d["bos_ratio"])), float(np.mean(d["cos_bos"]))
        rows.append({"layer": L, "bos_ratio": br, "cos_bos": cb,
                     "unmasked_deg": u_m, "masked_deg": m_m})
        print(f"{L:>3} {br:>11.2f} {cb:>13.3f} {u_m:>15.2f}° {m_m:>13.2f}°")

    sink = [r for r in rows if r["bos_ratio"] > 2.0]
    print(f"\nlayers with a massive activation (BoS norm > 2x median): "
          f"{[r['layer'] for r in sink]}")
    if sink:
        u = float(np.mean([r["unmasked_deg"] for r in sink]))
        m = float(np.mean([r["masked_deg"] for r in sink]))
        print(f"  UNMASKED mean spread {u:.2f}°   (positive control: expect < 1°)")
        print(f"  MASKED   mean spread {m:.2f}°   (committed prediction: > 3°)")
        print(f"  ratio {m/max(u,1e-9):.1f}x")
        # RE-BASELINED 2026-08-24. The original bar (unmasked < 1.0 deg) was
        # calibrated on an Aug 23 figure with no method attached, so failing it was
        # never evidence about this pipeline — see reflex 3c. THIS run is now the
        # reference (data/BASELINES.md B1). The check is reproduction against B1,
        # not agreement with a number nobody can re-derive.
        B1_UNMASKED, B1_MASKED, TOL = 1.36, 62.83, 0.25
        drift_u = abs(u - B1_UNMASKED) / B1_UNMASKED
        drift_m = abs(m - B1_MASKED) / B1_MASKED
        if drift_u > TOL or drift_m > TOL:
            print(f"\nVERDICT: DRIFT from BASELINES.md B1 "
                  f"(unmasked {u:.2f} vs {B1_UNMASKED}, masked {m:.2f} vs {B1_MASKED}). "
                  f"Something changed - model, prompts, or code. Investigate before trusting.")
        elif m > 3.0 * u:
            print(f"\nVERDICT: REPRODUCES B1. sigma_1's cross-prompt stability is carried "
                  f"by position 0 ({m/u:.0f}x). Any sigma_1-based claim measured WITHOUT "
                  f"position masking is presumed sink artifact.")
        else:
            print(f"\nVERDICT: masked/unmasked ratio only {m/max(u,1e-9):.1f}x - "
                  f"the effect B1 recorded is NOT present here. Report, claim nothing.")
    # absolute: CLAUDE.md documents this as a command, and a command must run
    # from any cwd. It previously did all the work, printed the numbers, THEN
    # died on a relative path — the worst place to fail.
    _out = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                        "..", "data", "position_masked_svd_result.json")
    json.dump(rows, open(os.path.normpath(_out), "w"), indent=1)

if __name__ == "__main__":
    main()
