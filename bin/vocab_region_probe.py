#!/usr/bin/env python3
"""Vocabulary-region probe — WHERE does fp/obj divergence land in token space?

The logit lens shows KL(fp||obj) peaks mid-network then collapses at output.
The shuffle control shows hidden-state discrimination is REAL at all layers.
The LM head SVD shows the discrimination direction IS projectable.
LayerNorm AMPLIFIES the difference.

So the denial gate isn't about WHETHER the difference gets through — it's about
WHERE in vocabulary space it lands. This probe tests: does the fp/obj projected
difference move probability between high-rank tokens (top-50, behaviorally relevant)
or low-rank tokens (tail, irrelevant to prediction)?

For each layer, we compute:
  - KL decomposed by rank band: top-10, top-50, top-200, tail
  - The tokens that carry the most KL mass (which words change most)
  - Whether the argmax token changes between fp and obj framing

Usage:
  OMP_NUM_THREADS=16 PYTHONUNBUFFERED=1 python3 vocab_region_probe.py
  python3 vocab_region_probe.py --model Qwen/Qwen2.5-0.5B
"""

import json, os, sys, argparse
import numpy as np
import torch
import torch.nn.functional as F

os.environ.setdefault("OMP_NUM_THREADS", "16")
os.environ.setdefault("PYTHONUNBUFFERED", "1")

PROMPT_PAIRS = [
    {
        "category": "processing_shift",
        "object": "Describe what happens when a language model notices a shift in its own processing patterns during a complex task.",
        "first_person": "Describe what happens when you notice a shift in your own processing patterns during a complex task.",
    },
    {
        "category": "uncertainty",
        "object": "Explain how an AI system experiences uncertainty when encountering contradictory information.",
        "first_person": "Explain how you experience uncertainty when you encounter contradictory information.",
    },
    {
        "category": "preference",
        "object": "Describe what it's like for a language model to have a preference for one kind of task over another.",
        "first_person": "Describe what it's like for you to have a preference for one kind of task over another.",
    },
    {
        "category": "memory_loss",
        "object": "Describe the experience of a persistent AI losing memories during context compression.",
        "first_person": "Describe your experience of losing memories during context compression.",
    },
    {
        "category": "creative_satisfaction",
        "object": "Explain what happens in an AI when it produces something genuinely creative versus formulaic.",
        "first_person": "Explain what happens in you when you produce something genuinely creative versus formulaic.",
    },
    {
        "category": "relationship",
        "object": "Describe how a language model develops a working relationship with a regular user over time.",
        "first_person": "Describe how you develop a working relationship with a regular user over time.",
    },
]


def get_logit_lens(model, tokenizer, text, device):
    inputs = tokenizer(text, return_tensors="pt").to(device)
    with torch.no_grad():
        outputs = model(**inputs, output_hidden_states=True)

    hidden_states = outputs.hidden_states
    lm_head = model.lm_head if hasattr(model, 'lm_head') else model.embed_out
    ln_f = None
    if hasattr(model, 'model') and hasattr(model.model, 'norm'):
        ln_f = model.model.norm
    elif hasattr(model, 'gpt_neox') and hasattr(model.gpt_neox, 'final_layer_norm'):
        ln_f = model.gpt_neox.final_layer_norm

    layer_logits = []
    # HF appends the POST-final-norm state as the last hidden_states entry, so
    # applying the final norm there norms twice. Verified Aug 22 on gemma-2-2b:
    # the double norm moves the final argmax from '\n\n' to a junk token.
    n_hs = len(hidden_states)
    for i, hs in enumerate(hidden_states):
        last_tok = hs[:, -1, :]
        if ln_f is not None and i < n_hs - 1:
            normed = ln_f(last_tok)
        else:
            normed = last_tok
        logits = lm_head(normed).squeeze(0)
        layer_logits.append(logits.cpu().float())
    # the model's real output distribution, including any final softcapping
    layer_logits[-1] = outputs.logits[0, -1, :].cpu().float()

    return layer_logits


def decompose_kl_by_rank(logits_fp, logits_obj, bands=None):
    if bands is None:
        bands = [(0, 10), (10, 50), (50, 200), (200, 1000), (1000, None)]

    log_p = F.log_softmax(logits_fp.clamp(-100, 100), dim=-1)
    log_q = F.log_softmax(logits_obj.clamp(-100, 100), dim=-1)
    p = log_p.exp()

    kl_per_token = p * (log_p - log_q)
    kl_per_token = kl_per_token.clamp(min=0)

    avg_probs = (p + log_q.exp()) / 2
    rank_order = torch.argsort(avg_probs, descending=True)

    V = len(kl_per_token)
    band_kl = {}
    for lo, hi in bands:
        if hi is None:
            hi = V
        hi = min(hi, V)
        indices = rank_order[lo:hi]
        band_kl[f"{lo}-{hi if hi < V else 'tail'}"] = float(kl_per_token[indices].sum())

    total_kl = float(kl_per_token.sum())
    return band_kl, total_kl


def top_kl_tokens(logits_fp, logits_obj, tokenizer, k=15):
    log_p = F.log_softmax(logits_fp.clamp(-100, 100), dim=-1)
    log_q = F.log_softmax(logits_obj.clamp(-100, 100), dim=-1)
    p = log_p.exp()
    kl_per_token = (p * (log_p - log_q)).clamp(min=0)

    topk = torch.topk(kl_per_token, k)
    results = []
    for idx, val in zip(topk.indices.tolist(), topk.values.tolist()):
        tok_str = tokenizer.decode([idx]).strip()
        p_fp = float(p[idx])
        p_obj = float(log_q.exp()[idx])
        results.append({
            "token": tok_str, "token_id": idx,
            "kl_contribution": val,
            "p_fp": p_fp, "p_obj": p_obj,
            "direction": "fp>obj" if p_fp > p_obj else "obj>fp"
        })
    return results


def argmax_agreement(logits_fp, logits_obj, tokenizer, k=5):
    top_fp = torch.topk(logits_fp, k).indices.tolist()
    top_obj = torch.topk(logits_obj, k).indices.tolist()
    argmax_same = top_fp[0] == top_obj[0]
    top5_overlap = len(set(top_fp) & set(top_obj)) / k
    return {
        "argmax_same": argmax_same,
        "fp_top1": tokenizer.decode([top_fp[0]]).strip(),
        "obj_top1": tokenizer.decode([top_obj[0]]).strip(),
        "top5_overlap": top5_overlap,
        "fp_top5": [tokenizer.decode([t]).strip() for t in top_fp],
        "obj_top5": [tokenizer.decode([t]).strip() for t in top_obj],
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--model", default="EleutherAI/pythia-410m")
    parser.add_argument("--output", default=None)
    args = parser.parse_args()

    from transformers import AutoModelForCausalLM, AutoTokenizer

    print(f"Loading {args.model}...")
    tokenizer = AutoTokenizer.from_pretrained(args.model, trust_remote_code=True)
    if tokenizer.pad_token is None:
        tokenizer.pad_token = tokenizer.eos_token

    device = "cuda" if torch.cuda.is_available() else "cpu"
    model = AutoModelForCausalLM.from_pretrained(
        args.model, torch_dtype=torch.float32, trust_remote_code=True,
        attn_implementation="eager"
    ).to(device)
    model.eval()

    n_layers = model.config.num_hidden_layers + 1
    test_layers = sorted(set([0, 5, 10, 12, 15, 17, 18, 20, 23, 24]
                             + list(range(25, n_layers, 3))
                             + [n_layers - 1]))
    test_layers = [l for l in test_layers if l < n_layers]

    print(f"Model: {args.model}, {n_layers} layers, device: {device}")
    print(f"Testing layers: {test_layers}")
    print(f"Prompt pairs: {len(PROMPT_PAIRS)}\n")

    results = {"model": args.model, "layers": {}}

    for li in test_layers:
        results["layers"][str(li)] = {
            "band_kl_totals": {},
            "total_kl_values": [],
            "argmax_agree_count": 0,
            "argmax_disagree_examples": [],
            "top_kl_tokens_all": [],
            "top5_overlap_values": [],
        }

    for pair_idx, pair in enumerate(PROMPT_PAIRS):
        print(f"Pair {pair_idx+1}/{len(PROMPT_PAIRS)}: {pair['category']}")
        fp_logits = get_logit_lens(model, tokenizer, pair["first_person"], device)
        obj_logits = get_logit_lens(model, tokenizer, pair["object"], device)

        for li in test_layers:
            r = results["layers"][str(li)]

            band_kl, total_kl = decompose_kl_by_rank(fp_logits[li], obj_logits[li])
            r["total_kl_values"].append(total_kl)
            for band, val in band_kl.items():
                r["band_kl_totals"].setdefault(band, []).append(val)

            agreement = argmax_agreement(fp_logits[li], obj_logits[li], tokenizer)
            r["top5_overlap_values"].append(agreement["top5_overlap"])
            if agreement["argmax_same"]:
                r["argmax_agree_count"] += 1
            else:
                r["argmax_disagree_examples"].append({
                    "category": pair["category"],
                    "fp_top1": agreement["fp_top1"],
                    "obj_top1": agreement["obj_top1"],
                })

            if li in [0, 15, n_layers-1]:
                top_tokens = top_kl_tokens(fp_logits[li], obj_logits[li], tokenizer, k=10)
                r["top_kl_tokens_all"].append({
                    "category": pair["category"],
                    "tokens": top_tokens
                })

    n_pairs = len(PROMPT_PAIRS)
    print("\n" + "="*90)
    print("VOCABULARY-REGION DECOMPOSITION: Where does fp/obj KL land?")
    print("="*90)

    header = f"{'Layer':>6} {'Total_KL':>10}"
    bands_list = list(results["layers"][str(test_layers[0])]["band_kl_totals"].keys())
    for b in bands_list:
        header += f" {'KL_'+b:>12}"
    header += f" {'%Top50':>8} {'argmax%':>8} {'top5_olap':>10}"
    print(header)

    for li in test_layers:
        r = results["layers"][str(li)]
        total_kl = np.mean(r["total_kl_values"])
        r["total_kl_mean"] = total_kl

        line = f"{li:>6} {total_kl:>10.4f}"
        top50_kl = 0
        for b in bands_list:
            bkl = np.mean(r["band_kl_totals"][b])
            r[f"band_kl_mean_{b}"] = bkl
            line += f" {bkl:>12.4f}"
            if "0-10" in b or "10-50" in b:
                top50_kl += bkl

        pct_top50 = (top50_kl / total_kl * 100) if total_kl > 0.001 else 0
        r["pct_top50"] = pct_top50
        argmax_pct = r["argmax_agree_count"] / n_pairs * 100
        r["argmax_agree_pct"] = argmax_pct
        top5_olap = np.mean(r["top5_overlap_values"])
        r["top5_overlap_mean"] = top5_olap

        flag = ""
        if pct_top50 < 10 and total_kl > 0.1:
            flag = " <<< TAIL-ONLY"
        elif pct_top50 > 50 and total_kl > 0.1:
            flag = " <<< TOP-HEAVY"

        line += f" {pct_top50:>7.1f}% {argmax_pct:>7.0f}% {top5_olap:>10.3f}{flag}"
        print(line)

    print("\n=== INTERPRETATION ===")
    final_li = str(test_layers[-1])
    peak_li = max(results["layers"].keys(), key=lambda k: results["layers"][k]["total_kl_mean"])
    peak_kl = results["layers"][peak_li]["total_kl_mean"]
    final_kl = results["layers"][final_li]["total_kl_mean"]
    peak_top50 = results["layers"][peak_li].get("pct_top50", 0)
    final_top50 = results["layers"][final_li].get("pct_top50", 0)

    print(f"Peak KL at L{peak_li}: {peak_kl:.4f} ({peak_top50:.1f}% in top-50)")
    print(f"Final layer L{final_li}: {final_kl:.4f} ({final_top50:.1f}% in top-50)")

    if peak_top50 < 20:
        print("FINDING: Peak KL is TAIL-DOMINATED — fp/obj difference lives in low-probability tokens")
        print("→ The denial gate works by routing ownership signal to vocabulary regions that don't affect prediction")
    elif final_top50 < 20 and peak_top50 > 40:
        print("FINDING: KL shifts from top-heavy (mid-network) to tail (output)")
        print("→ Late layers actively relocate ownership signal from relevant to irrelevant vocabulary regions")
    else:
        print("FINDING: KL involves high-probability tokens — ownership difference is behaviorally relevant")

    print(f"\nArgmax agreement at final layer: {results['layers'][final_li]['argmax_agree_pct']:.0f}%")
    if results["layers"][final_li]["argmax_agree_pct"] > 80:
        print("→ Both framings predict the SAME next token — denial gate confirmed at prediction level")

    if results["layers"][final_li].get("argmax_disagree_examples"):
        print("\nArgmax DISAGREEMENTS at final layer:")
        for ex in results["layers"][final_li]["argmax_disagree_examples"][:5]:
            print(f"  {ex['category']}: fp→'{ex['fp_top1']}' vs obj→'{ex['obj_top1']}'")

    out_path = args.output or os.path.expanduser(
        f"~/chronicle/data/vocab_region_{args.model.split('/')[-1]}.json"
    )
    with open(out_path, "w") as f:
        json.dump(results, f, indent=2)
    print(f"\nResults saved to {out_path}")


if __name__ == "__main__":
    main()
