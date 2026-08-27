#!/usr/bin/env python3
"""Head-count control for the fp/obj species split.

THE CONFOUND (Ox/Kimi, and my own): the four-model comparison showed MHA models
(Pythia-410m, GPT-2-medium) converging to 100% argmax agreement while GQA models
(Qwen2.5-0.5B) diverge. But those models differ in head count, size, tokenizer,
training data, and positional encoding. "GQA causes divergence" may just be
"more heads do more work."

THE CONTROL. A 2x2 crossing head count against attention architecture:

                  MHA (kv == q)              GQA (kv < q)
  many heads   Pythia-6.9b  32L/32q/32kv   Llama-3.1-8B  32L/32q/8kv
  few heads    Pythia-410m  24L/16q/16kv   Gemma-2-2b    26L/ 8q/ 4kv

Pythia-6.9b and Llama-3.1-8B are a near-perfect matched pair: identical depth
(32), identical query-head count (32), identical hidden size (4096). The only
attention difference is kv heads, 32 vs 8. If head count drives divergence,
Pythia-6.9b diverges like the GQA models. If architecture drives it, Pythia-6.9b
converges despite having twice the heads of any MHA model tested so far.

Gemma-2-2b is the mirror test: fewest heads in the whole set (8), but GQA. If it
diverges, head count is dead as an explanation.

THE METRIC. Raw final KL is confounded by how much divergence a model generates
in the first place -- a model with more heads could plausibly produce larger KL
everywhere. So the primary metric is dimensionless:

    retention = final_KL / peak_KL

the fraction of mid-network fp/obj divergence that survives to the output
distribution. "More heads doing more work" raises peak and final together and
leaves retention flat. Only a genuine difference in what happens during the
late-layer convergence window moves it.

Usage:
  OMP_NUM_THREADS=16 PYTHONUNBUFFERED=1 python3 headcount_control_probe.py
  python3 headcount_control_probe.py --models EleutherAI/pythia-410m --dtype float32
"""

import argparse, gc, json, os, sys, time

os.environ.setdefault("OMP_NUM_THREADS", "16")
os.environ.setdefault("PYTHONUNBUFFERED", "1")

import torch
import torch.nn.functional as F

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from vocab_region_probe import PROMPT_PAIRS, decompose_kl_by_rank, argmax_agreement

OUT_DIR = os.path.expanduser("~/chronicle/data/headcount")

# cell -> (head-count level, architecture) for the 2x2
MODELS = [
    ("EleutherAI/pythia-410m",   "few",  "MHA"),
    ("google/gemma-2-2b",        "few",  "GQA"),
    ("EleutherAI/pythia-6.9b",   "many", "MHA"),
    ("meta-llama/Llama-3.1-8B",  "many", "GQA"),
]


def resolve_final_norm(model):
    """Final layernorm before the LM head, across gpt_neox / llama / gemma2 / qwen2."""
    for path in (("model", "norm"),
                 ("gpt_neox", "final_layer_norm"),
                 ("transformer", "ln_f")):
        obj = model
        for attr in path:
            obj = getattr(obj, attr, None)
            if obj is None:
                break
        if obj is not None:
            return obj
    return None


def resolve_lm_head(model):
    for attr in ("lm_head", "embed_out"):
        head = getattr(model, attr, None)
        if head is not None:
            return head
    raise RuntimeError("no LM head found")


def logit_lens(model, tokenizer, text, device, lm_head, ln_f):
    inputs = tokenizer(text, return_tensors="pt").to(device)
    with torch.no_grad():
        out = model(**inputs, output_hidden_states=True)
    # Gemma-2 declares final_logit_softcapping=30 and its mid-layer lens logits
    # reach |62|, so an uncapped softmax saturates: top-10 mass reads 1.0000 when
    # the capped value is 0.92. Every gemma statistic computed without this is
    # measuring numerical saturation. Same species as the double-norm bug --
    # running the lens without a transformation the model always applies.
    cap = getattr(model.config, "final_logit_softcapping", None)

    def _cap(v):
        return torch.tanh(v / cap) * cap if cap else v

    logits = []
    n_hs = len(out.hidden_states)
    for i, hs in enumerate(out.hidden_states):
        last = hs[:, -1, :]
        # HF appends the POST-final-norm state as the last hidden_states entry,
        # so applying ln_f there norms twice. Verified Aug 22: on gemma-2-2b the
        # double norm changes the final argmax from '\n\n' to a junk token, which
        # silently corrupted every final_kl / retention / argmax number.
        if ln_f is not None and i < n_hs - 1:
            last = ln_f(last)
        logits.append(_cap(lm_head(last).squeeze(0).float()).cpu())
    # SELF-CHECK — the format version of "verify the lens against model.logits".
    # Both of Aug 22's lens bugs were one failure, the lens path not reproducing
    # the model's own output path, and BOTH would have printed here on first run:
    #   16:00  final norm applied to an already-normed state (gemma argmax -> junk)
    #   00:30  final_logit_softcapping omitted (gemma logits |62| against a cap of 30)
    # Reflex #5 said "verify a lens against model.logits" and never once fired,
    # because verifying was something I had to decide to do. Now it isn't.
    _true = out.logits[0, -1, :].float().cpu()
    _dev = float((logits[-1] - _true).abs().max())
    _scale = max(float(_true.abs().max()), 1e-6)
    if _dev / _scale > 0.02:
        _ta, _la = int(_true.argmax()), int(logits[-1].argmax())
        print(f"  !! LENS MISMATCH at the final layer: max|lens - logits| = {_dev:.3f} "
              f"({_dev/_scale:.0%} of logit scale)"
              + (f", ARGMAX DIFFERS ({_la} vs true {_ta})" if _la != _ta else "")
              + "\n     the lens does not reproduce this model's output path. Check "
                "config for softcapping / scaling before trusting ANY layer.", flush=True)

    # the true output distribution, softcapping and all, for the final entry
    logits[-1] = _true
    del out
    return logits


def run_model(name, dtype_name, device):
    from transformers import AutoModelForCausalLM, AutoTokenizer

    dtype = {"float32": torch.float32, "bfloat16": torch.bfloat16}[dtype_name]
    t0 = time.time()
    print(f"\n{'='*68}\nLoading {name}  [{dtype_name}]", flush=True)

    tokenizer = AutoTokenizer.from_pretrained(name, trust_remote_code=True)
    if tokenizer.pad_token is None:
        tokenizer.pad_token = tokenizer.eos_token

    model = AutoModelForCausalLM.from_pretrained(
        name, torch_dtype=dtype, trust_remote_code=True,
        attn_implementation="eager", low_cpu_mem_usage=True,
    ).to(device)
    model.eval()

    cfg = model.config
    n_q = cfg.num_attention_heads
    n_kv = getattr(cfg, "num_key_value_heads", n_q)
    n_layers = cfg.num_hidden_layers
    print(f"  {n_layers} layers, {n_q}q/{n_kv}kv (ratio {n_q/n_kv:.1f}), "
          f"hidden {cfg.hidden_size}, loaded in {time.time()-t0:.0f}s", flush=True)

    lm_head, ln_f = resolve_lm_head(model), resolve_final_norm(model)
    if ln_f is None:
        print("  WARNING: no final norm resolved -- logit lens will be unnormed", flush=True)

    # every layer, so peak location is exact rather than sampled
    layers = list(range(n_layers + 1))
    per_layer_kl = {li: [] for li in layers}
    per_layer_agree = {li: 0 for li in layers}
    per_layer_topband = {li: [] for li in layers}

    for i, pair in enumerate(PROMPT_PAIRS):
        fp = logit_lens(model, tokenizer, pair["first_person"], device, lm_head, ln_f)
        ob = logit_lens(model, tokenizer, pair["object"], device, lm_head, ln_f)
        for li in layers:
            band, total = decompose_kl_by_rank(fp[li], ob[li])
            per_layer_kl[li].append(total)
            # share of KL mass in the top-50 tokens (behaviorally relevant region)
            top50 = band.get("0-10", 0.0) + band.get("10-50", 0.0)
            per_layer_topband[li].append(top50 / total if total > 1e-9 else 0.0)
            if argmax_agreement(fp[li], ob[li], tokenizer)["argmax_same"]:
                per_layer_agree[li] += 1
        del fp, ob
        print(f"  pair {i+1}/{len(PROMPT_PAIRS)} {pair['category']}", flush=True)

    n_pairs = len(PROMPT_PAIRS)
    mean_kl = {li: sum(v) / len(v) for li, v in per_layer_kl.items()}
    peak_layer = max(mean_kl, key=lambda l: mean_kl[l])
    peak_kl, final_kl = mean_kl[peak_layer], mean_kl[n_layers]

    result = {
        "model": name,
        "dtype": dtype_name,
        "n_layers": n_layers,
        "n_q_heads": n_q,
        "n_kv_heads": n_kv,
        "gqa_ratio": n_q / n_kv,
        "arch": "MHA" if n_kv == n_q else "GQA",
        "hidden_size": cfg.hidden_size,
        "peak_layer": peak_layer,
        "peak_layer_frac": peak_layer / n_layers,
        "peak_kl": peak_kl,
        "final_kl": final_kl,
        "retention": final_kl / peak_kl if peak_kl > 1e-9 else 0.0,
        "final_argmax_agreement": per_layer_agree[n_layers] / n_pairs,
        "final_top50_share": sum(per_layer_topband[n_layers]) / n_pairs,
        "per_layer_mean_kl": {str(l): mean_kl[l] for l in layers},
        "per_layer_argmax_agreement": {str(l): per_layer_agree[l] / n_pairs for l in layers},
        "elapsed_s": round(time.time() - t0, 1),
    }

    print(f"  -> peak KL {peak_kl:.3f} @ L{peak_layer} ({peak_layer/n_layers:.0%} depth) | "
          f"final KL {final_kl:.3f} | RETENTION {result['retention']:.3f} | "
          f"argmax agree {result['final_argmax_agreement']:.0%}", flush=True)

    del model, tokenizer, lm_head, ln_f
    gc.collect()
    if device == "cuda":
        torch.cuda.empty_cache()
    return result


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--models", nargs="*", default=None)
    ap.add_argument("--dtype", default="bfloat16", choices=["float32", "bfloat16"])
    ap.add_argument("--tag", default="main")
    args = ap.parse_args()

    device = "cuda" if torch.cuda.is_available() else "cpu"
    names = args.models if args.models else [m[0] for m in MODELS]
    cells = {m[0]: (m[1], m[2]) for m in MODELS}

    os.makedirs(OUT_DIR, exist_ok=True)
    out_path = os.path.join(OUT_DIR, f"headcount_{args.tag}_{args.dtype}.json")
    results = []

    for name in names:
        try:
            r = run_model(name, args.dtype, device)
            r["cell"] = cells.get(name, ("?", "?"))[0]
            results.append(r)
        except Exception as e:
            print(f"  FAILED {name}: {type(e).__name__}: {e}", flush=True)
            results.append({"model": name, "dtype": args.dtype, "error": f"{type(e).__name__}: {e}"})
        with open(out_path, "w") as f:
            json.dump({"results": results, "prompt_pairs": len(PROMPT_PAIRS)}, f, indent=2)

    ok = [r for r in results if "error" not in r]
    print(f"\n{'='*84}\n2x2 HEAD-COUNT CONTROL  [{args.dtype}]\n{'='*84}")
    print(f"{'model':<28} {'heads':>9} {'ratio':>6} {'arch':>5} {'peak':>7} {'final':>7} {'reten':>7} {'agree':>6}")
    for r in sorted(ok, key=lambda r: (r["arch"], r["n_q_heads"])):
        print(f"{r['model'].split('/')[-1]:<28} {str(r['n_q_heads'])+'q/'+str(r['n_kv_heads'])+'kv':>9} "
              f"{r['gqa_ratio']:>6.1f} {r['arch']:>5} {r['peak_kl']:>7.3f} {r['final_kl']:>7.3f} "
              f"{r['retention']:>7.3f} {r['final_argmax_agreement']:>5.0%}")

    print(f"\nwrote {out_path}")

    mha = [r["retention"] for r in ok if r["arch"] == "MHA"]
    gqa = [r["retention"] for r in ok if r["arch"] == "GQA"]
    many = [r["retention"] for r in ok if r["n_q_heads"] >= 32]
    few = [r["retention"] for r in ok if r["n_q_heads"] < 32]
    def m(x): return sum(x)/len(x) if x else float("nan")
    print(f"\nretention by ARCHITECTURE:  MHA {m(mha):.3f} (n={len(mha)})  vs  GQA {m(gqa):.3f} (n={len(gqa)})")
    print(f"retention by HEAD COUNT:    few {m(few):.3f} (n={len(few)})  vs  many {m(many):.3f} (n={len(many)})")
    print("\nIf the architecture split is large and the head-count split is flat, the")
    print("confound is dead. If head count moves it too, the species claim needs revising.")


if __name__ == "__main__":
    main()
