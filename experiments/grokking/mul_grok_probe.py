"""
What changes at mul's grok step, if not Fourier sparsification?

Track several candidate metrics through training for mul s0 (grok@3300):
  - MLP output norm at = position
  - Per-neuron phase coherence across b-values (0.995 post-grok; when does it lock?)
  - Output logit magnitude at correct answer vs wrong answers (margin)
  - Cross-neuron phase alignment (do mul neurons fire in-phase?)
"""
import sys, json
from pathlib import Path
import numpy as np
import torch

sys.path.insert(0, str(Path(__file__).parent))
from grok_v2 import GrokTransformer

BASE = Path("/home/nate-agx/chronicle/experiments/grokking/runs")
P = 97
STEPS = [500, 1000, 2000, 2500, 3000, 3300, 3500, 4000, 5000, 7000, 11000, 20000, 50000]


def primitive_root(p):
    phi = p - 1; n = phi; fs = []; d = 2
    while d*d <= n:
        if n%d==0: fs.append(d)
        while n%d==0: n//=d
        d += 1
    if n>1: fs.append(n)
    for g in range(2,p):
        if all(pow(g,phi//f,p)!=1 for f in fs): return g


def grok_step(run):
    with open(BASE/run/"metrics.jsonl") as f:
        for line in f:
            r = json.loads(line)
            if r.get("val_acc", 0) >= 0.95:
                return r["step"]


def val_acc_at(run, step):
    best = None
    with open(BASE/run/"metrics.jsonl") as f:
        for line in f:
            r = json.loads(line)
            if r["step"] <= step:
                best = r.get("val_acc", 0)
            else:
                break
    return best or 0


def probe(run, step, is_mul):
    m = GrokTransformer()
    sd = torch.load(BASE/run/"snapshots"/f"step_{step:06d}.pt",
                    map_location="cpu", weights_only=True)
    m.load_state_dict(sd); m.eval()
    g = primitive_root(P) if is_mul else None

    mlp_norms = []
    cross_b_cos_list = []
    margins = []
    phase_aligns = []

    # Sweep b across several values to test cross-b consistency
    b_values = [1, 5, 42, 71]
    post_per_b = []
    logits_per_b = []
    for b in b_values:
        tok = torch.tensor([[a, b, 97] for a in range(P)])
        with torch.no_grad():
            x0 = m.tok_emb(tok) + m.pos_emb.weight[None,:,:]
            attn_out, _ = m.attn(x0, x0, x0, need_weights=False)
            x = x0 + attn_out
            for layer in m.mlp: x = layer(x)
            mlp_out = x[:,2,:].numpy()  # (P, d)
            # Continue to logits
            logits = (x[:,2,:] @ m.tok_emb.weight.T).numpy()  # (P, vocab)
        if is_mul:
            ridx = np.array([pow(g,j,P)-1 for j in range(P-1)])
            mlp_out = mlp_out[ridx]
        post_per_b.append(mlp_out)
        logits_per_b.append((logits, b, tok[:,0].numpy()))
        mlp_norms.append(float(np.linalg.norm(mlp_out, axis=1).mean()))

    # Cross-b FFT cosine (top-10 neurons)
    fs_per_b = [np.abs(np.fft.fft(p, axis=0))**2 for p in post_per_b]
    for pw in fs_per_b:
        pw[0] = 0
    avg_power = np.mean([p.sum(axis=0) for p in fs_per_b], axis=0)
    top10 = np.argsort(avg_power)[::-1][:10]
    cosines = []
    for n in top10:
        for i in range(len(fs_per_b)):
            for j in range(i+1, len(fs_per_b)):
                a_ = fs_per_b[i][:,n]; c_ = fs_per_b[j][:,n]
                cosines.append(float(a_@c_/(np.linalg.norm(a_)*np.linalg.norm(c_)+1e-12)))
    cross_b_cos = float(np.mean(cosines))

    # Logit margin: correct - max wrong, averaged over prompts
    margins_all = []
    for logits, b, a_arr in logits_per_b:
        if is_mul:
            correct = (a_arr * b) % P
        else:
            correct = None  # skip
        if correct is not None:
            margs = []
            for i, a in enumerate(a_arr):
                correct_logit = logits[i, correct[i]]
                wrong_mask = np.ones(logits.shape[1], dtype=bool)
                wrong_mask[correct[i]] = False
                max_wrong = logits[i, wrong_mask].max()
                margs.append(correct_logit - max_wrong)
            margins_all.append(np.mean(margs))
    margin = float(np.mean(margins_all)) if margins_all else 0.0

    # Phase alignment across neurons: at the dominant frequency of each neuron,
    # compute the relative phases. If they're locked, the phasors sum coherently.
    fs_single = np.fft.fft(post_per_b[0], axis=0)  # b=1
    half = fs_single.shape[0] // 2
    # pick the neurons' dominant freq and their complex phase
    dom_freqs = np.argmax(np.abs(fs_single[:half])**2, axis=0)
    phasors = np.array([fs_single[dom_freqs[n], n] for n in range(fs_single.shape[1])])
    # Abs of sum of unit phasors / N gives coherence in [0,1]
    unit_phasors = phasors / (np.abs(phasors) + 1e-12)
    phase_align = float(np.abs(unit_phasors.sum()) / len(unit_phasors))

    return {
        "mlp_norm": float(np.mean(mlp_norms)),
        "cross_b_cos_top10": cross_b_cos,
        "logit_margin": margin,
        "phase_align": phase_align,
    }


run = "v2_mul_seed0"
gk = grok_step(run)
print(f"mul s0 (grok@{gk})\n")
print(f"{'step':<7} {'val_acc':<9} {'mlp_nm':<8} {'cross-b':<9} {'margin':<8} {'phase_aln':<10}")
print("-" * 60)
for s in STEPS:
    r = probe(run, s, True)
    va = val_acc_at(run, s)
    print(f"{s:<7} {va:<9.3f} {r['mlp_norm']:<8.3f} "
          f"{r['cross_b_cos_top10']:<9.3f} {r['logit_margin']:<8.3f} {r['phase_align']:<10.3f}")
