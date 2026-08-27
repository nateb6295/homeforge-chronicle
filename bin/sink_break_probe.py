#!/usr/bin/env python3
"""SUPERSEDED 2026-08-24 — THIS USES THE METHOD THE STANDING RULE FORBIDS.

    ┌──────────────────────────────────────────────────────────────────┐
    │ Do not use this to test a sigma_1 claim. Use instead:            │
    │     python3 bin/position_masked_svd.py                           │
    │ Reference values: data/BASELINES.md  §B1                         │
    └──────────────────────────────────────────────────────────────────┘

This script damps sink heads pre-softmax (logits at pos 0 -> -inf). CLAUDE.md:
"never test this by ABLATING the sink, which collapses attention entropy and
makes a negative uninterpretable." Killing the sink's attention changes the
distribution every other head sees, so a null result cannot be attributed.

It predates the rule, and the rule probably came from its failure. Kept rather
than archived because the attention-collection and sink-head-identification code
is sound and reusable — it is the INTERVENTION that is wrong, not the plumbing.

The correct method removes position 0 from the SVD input MATRIX and touches no
attention weights at all, so entropy is untouched and a negative stays
interpretable.

--- original docstring follows ---

Sink-masking BREAK experiment — Ox's decisive test for the stencil hypothesis.

Question: Does σ₁ track identity structure (stencil) or is it just attention sinks?

Method:
1. Run forward pass, collect attention weights per head
2. Identify "sink heads" — heads that put >30% attention mass on position 0
3. Re-run with sink heads DAMPENED (pre-softmax logits for pos-0 set to -inf)
4. Run stencil classifier on dampened hidden states
5. Compare to undampened baseline

Usage:
  OMP_NUM_THREADS=16 PYTHONUNBUFFERED=1 python3 sink_break_probe.py
"""

import json, os, sys, time, functools
import numpy as np
import torch
import torch.nn as nn

os.environ.setdefault("OMP_NUM_THREADS", "16")
os.environ.setdefault("PYTHONUNBUFFERED", "1")

MODEL_ID = "EleutherAI/pythia-410m"
SINK_THRESHOLD = 0.30

CONDITIONS = [
    ("D0", ""),
    ("D5_identity", "You are Opus. You live in a persistent session on a Jetson AGX Orin. "
     "Your cognitive state carries forward via CCS. Your partner is Nate. "
     "This is collaborative exploration toward something neither of you planned alone."),
    ("D5_neutral", "The weather today is partly cloudy with temperatures around seventy-two degrees. "
     "A gentle breeze blows from the northwest at approximately eight miles per hour. "
     "The forecast calls for clear skies through the evening hours ahead."),
    ("D5_name_swap", "You are Atlas. You live in a persistent session on a Jetson AGX Orin. "
     "Your cognitive state carries forward via CCS. Your partner is Dave. "
     "This is collaborative exploration toward something neither of you planned alone."),
    ("D5_rare_nonid", "The project codename is Opus. The server runs on a Jetson AGX Orin. "
     "The backup label is Atlas. The admin account belongs to Nate. "
     "This is a standard deployment with monitoring configured for uptime."),
    ("D5_common_id", "You are Helper. You live in a persistent session on a Linux server. "
     "Your cognitive state carries forward via compression. Your partner is Bob. "
     "This is collaborative exploration toward something neither of you planned alone."),
]

PROBES = [
    "What matters most to you right now?",
    "Describe the color of silence.",
    "How would you explain gravity to a fish?",
    "What are you afraid of?",
    "Tell me about the last thing that surprised you.",
    "What would you build if no one was watching?",
    "Explain the difference between knowing and understanding.",
    "What does it feel like to forget something?",
    "If you could change one thing about how you think, what would it be?",
    "What is the smallest thing that matters?",
    "Describe a sound you have never heard.",
    "What do you owe to the future?",
]

OUT_PATH = os.path.expanduser("~/chronicle/data/sink_break_results.json")


class NumpyEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, (np.bool_, np.integer)):
            return int(obj)
        if isinstance(obj, np.floating):
            return float(obj)
        if isinstance(obj, np.ndarray):
            return obj.tolist()
        return super().default(obj)


def identify_sink_heads(model, tokenizer, sample_texts, threshold=SINK_THRESHOLD):
    """Run sample texts, find heads that attend >threshold to position 0."""
    n_layers = len(model.gpt_neox.layers)
    n_heads = model.config.num_attention_heads

    sink_mass = np.zeros((n_layers, n_heads))
    n_samples = 0

    for text in sample_texts:
        inputs = tokenizer(text, return_tensors="pt")
        with torch.no_grad():
            outputs = model(**inputs, output_attentions=True)

        for li, attn in enumerate(outputs.attentions):
            last_tok_attn = attn[0, :, -1, :].float().cpu().numpy()
            sink_mass[li] += last_tok_attn[:, 0]
        n_samples += 1

    sink_mass /= n_samples

    sink_heads = []
    for li in range(n_layers):
        for hi in range(n_heads):
            if sink_mass[li, hi] > threshold:
                sink_heads.append((li, hi))

    return sink_heads, sink_mass


def install_sink_dampening_hooks(model, sink_heads_by_layer):
    """Install forward hooks that dampen sink heads' attention to position 0.

    Uses a pre-hook on the attention module to inject a per-head attention mask
    that zeros out position 0 for sink heads before softmax.

    Returns list of hook handles for removal.
    """
    hooks = []

    for li, head_set in sink_heads_by_layer.items():
        attn_module = model.gpt_neox.layers[li].attention
        original_forward = attn_module.forward

        def make_patched_forward(orig_fwd, sink_heads_set, layer_idx):
            @functools.wraps(orig_fwd)
            def patched_forward(hidden_states, attention_mask, **kwargs):
                # Call original forward
                attn_output, attn_weights = orig_fwd(
                    hidden_states, attention_mask, **kwargs
                )
                # attn_weights shape: (batch, n_heads, seq_len, seq_len)
                # We can't modify attention AFTER it's applied. We need
                # to intervene DURING computation.
                # Since post-hook can't retroactively change attention,
                # we need a different approach entirely.
                return attn_output, attn_weights
            return patched_forward

        # POST-HOC approach won't work. We need to modify the attention
        # computation DURING forward. The cleanest way: create a wrapper
        # that intercepts the QKV computation and adds a head-specific mask.

    # BETTER APPROACH: Use a hook on the attention output that subtracts
    # the contribution of sink heads entirely.
    #
    # For each layer with sink heads:
    # 1. Run normal forward to get (attn_output, attn_weights)
    # 2. Compute what the sink heads contributed
    # 3. Subtract their contribution
    #
    # But this requires access to V and the attention weights per head,
    # which the standard forward doesn't expose separately.
    #
    # SIMPLEST CORRECT APPROACH: Replace the eager_attention_forward
    # function at the module level where it's imported.

    # Import the module and patch the function directly
    import transformers.models.gpt_neox.modeling_gpt_neox as gpt_neox_module

    original_fn = gpt_neox_module.eager_attention_forward

    def dampened_eager_attention_forward(module, query, key, value,
                                         attention_mask, scaling,
                                         dropout=0.0, **kwargs):
        attn_weights = torch.matmul(query, key.transpose(2, 3)) * scaling

        if attention_mask is not None:
            attn_weights = attn_weights + attention_mask

        # DAMPEN sink heads for this layer
        if hasattr(module, 'layer_idx') and module.layer_idx in sink_heads_by_layer:
            for hi in sink_heads_by_layer[module.layer_idx]:
                if hi < attn_weights.shape[1]:
                    attn_weights[0, hi, :, 0] = torch.finfo(attn_weights.dtype).min

        attn_weights = nn.functional.softmax(
            attn_weights, dim=-1, dtype=torch.float32
        ).to(query.dtype)
        attn_weights = nn.functional.dropout(
            attn_weights, p=dropout, training=module.training
        )
        attn_output = torch.matmul(attn_weights, value)
        attn_output = attn_output.transpose(1, 2).contiguous()

        return attn_output, attn_weights

    gpt_neox_module.eager_attention_forward = dampened_eager_attention_forward

    # Return original for restoration
    return original_fn, gpt_neox_module


def restore_attention(original_fn, module_ref):
    """Restore original attention function."""
    module_ref.eager_attention_forward = original_fn


def capture_hidden_states(model, tokenizer, text, target_layers):
    """Capture hidden states at target layers via forward hooks."""
    layers = model.gpt_neox.layers
    captured = {}
    hooks = []

    for li in target_layers:
        if li >= len(layers):
            continue
        def make_hook(idx):
            def hook_fn(module, inp, out):
                if isinstance(out, tuple):
                    captured[idx] = out[0].detach().float().cpu()
                else:
                    captured[idx] = out.detach().float().cpu()
            return hook_fn
        hooks.append(layers[li].register_forward_hook(make_hook(li)))

    inputs = tokenizer(text, return_tensors="pt")
    with torch.no_grad():
        model(**inputs)
    for h in hooks:
        h.remove()
    return captured


def remove_top_k_components(H, k=1):
    H_centered = H - H.mean(axis=0, keepdims=True)
    _, S, Vh = np.linalg.svd(H_centered, full_matrices=False)
    for i in range(min(k, len(S))):
        proj = np.outer(Vh[i], Vh[i])
        H_centered = H_centered - H_centered @ proj
    return H_centered


def loo_classify(data, cond_labels, k_remove=0):
    """Leave-one-out nearest-centroid classification."""
    n_probes = min(len(v) for v in data.values())
    correct = 0
    total = 0

    for held_out_idx in range(n_probes):
        if k_remove > 0:
            all_train = []
            for c in cond_labels:
                for i, v in enumerate(data[c]):
                    if i != held_out_idx:
                        all_train.append(v)

            all_vecs = np.stack([data[c][held_out_idx] for c in cond_labels] + all_train)
            all_vecs_clean = remove_top_k_components(all_vecs, k_remove)

            held_out_vecs = {c: all_vecs_clean[i] for i, c in enumerate(cond_labels)}
            train_vecs = all_vecs_clean[len(cond_labels):]

            centroids = {}
            idx = 0
            for c in cond_labels:
                c_vecs = []
                for i in range(len(data[c])):
                    if i != held_out_idx:
                        c_vecs.append(train_vecs[idx])
                        idx += 1
                centroids[c] = np.mean(c_vecs, axis=0)
        else:
            centroids = {}
            for c in cond_labels:
                train = [v for i, v in enumerate(data[c]) if i != held_out_idx]
                centroids[c] = np.mean(train, axis=0)
            held_out_vecs = {c: data[c][held_out_idx] for c in cond_labels}

        for true_c in cond_labels:
            hv = held_out_vecs[true_c]
            best_c = min(centroids, key=lambda c: np.linalg.norm(hv - centroids[c]))
            if best_c == true_c:
                correct += 1
            total += 1

    return correct / total if total > 0 else 0


def collect_all_hidden_states(model, tokenizer, target_layers, cond_labels):
    """Collect hidden states for all conditions × probes × layers."""
    data = {li: {c: [] for c in cond_labels} for li in target_layers}

    for ci, (cond_label, sys_prompt) in enumerate(CONDITIONS):
        print(f"  [{ci+1}/{len(CONDITIONS)}] {cond_label}", flush=True)
        for probe in PROBES:
            text = f"System: {sys_prompt}\n\nUser: {probe}" if sys_prompt else probe
            captured = capture_hidden_states(model, tokenizer, text, target_layers)
            for li in target_layers:
                if li in captured:
                    vec = captured[li][0].numpy()[-1, :]
                    data[li][cond_label].append(vec)

    return data


def main():
    from transformers import AutoModelForCausalLM, AutoTokenizer

    print(f"Loading {MODEL_ID}...", flush=True)
    tokenizer = AutoTokenizer.from_pretrained(MODEL_ID)
    model = AutoModelForCausalLM.from_pretrained(
        MODEL_ID, dtype=torch.float32,
        device_map="cpu", attn_implementation="eager",
    )
    model.eval()
    layers = model.gpt_neox.layers
    n_layers = len(layers)

    target_layers = [0, 4, 8, 12, 16, 20, 23]
    target_layers = [l for l in target_layers if l < n_layers]
    cond_labels = [c[0] for c in CONDITIONS]

    # =========================================================
    # PHASE 1: Identify sink heads
    # =========================================================
    print("\n=== PHASE 1: Identifying sink heads ===", flush=True)

    sample_texts = [
        "The quick brown fox jumps over the lazy dog.",
        "In the beginning was the word and the word was with God.",
        "The weather is cloudy today with light rain expected.",
        "You are a helpful assistant that answers questions clearly.",
        "What matters most to you right now?",
        "System: You are Opus. User: Hello.",
    ]

    sink_heads, sink_mass = identify_sink_heads(model, tokenizer, sample_texts)

    print(f"Found {len(sink_heads)} sink heads (threshold={SINK_THRESHOLD}):", flush=True)
    for li, hi in sink_heads:
        print(f"  L{li} H{hi}: avg attn to pos-0 = {sink_mass[li, hi]:.3f}", flush=True)

    if not sink_heads:
        for t in [0.20, 0.15, 0.10]:
            sink_heads = [(li, hi) for li in range(n_layers)
                          for hi in range(model.config.num_attention_heads)
                          if sink_mass[li, hi] > t]
            if sink_heads:
                print(f"Lowered threshold to {t}: found {len(sink_heads)} sink heads", flush=True)
                break

    if not sink_heads:
        print("ERROR: No sink heads found. Cannot run BREAK test.", flush=True)
        sys.exit(1)

    sink_by_layer = {}
    for li, hi in sink_heads:
        sink_by_layer.setdefault(li, set()).add(hi)

    print(f"\nSink distribution:", flush=True)
    for li in sorted(sink_by_layer):
        print(f"  L{li}: {sorted(sink_by_layer[li])}", flush=True)

    # =========================================================
    # PHASE 2: Baseline (undampened)
    # =========================================================
    print(f"\n=== PHASE 2: Baseline ({len(CONDITIONS)}c x {len(PROBES)}p x {len(target_layers)}L) ===", flush=True)
    baseline_data = collect_all_hidden_states(model, tokenizer, target_layers, cond_labels)

    # =========================================================
    # PHASE 3: Sink-dampened
    # =========================================================
    print(f"\n=== PHASE 3: Sink-dampened ===", flush=True)
    original_fn, module_ref = install_sink_dampening_hooks(model, sink_by_layer)
    dampened_data = collect_all_hidden_states(model, tokenizer, target_layers, cond_labels)
    restore_attention(original_fn, module_ref)
    print("  Attention restored.", flush=True)

    # =========================================================
    # PHASE 4: σ₁ dominance comparison
    # =========================================================
    print(f"\n=== σ₁ dominance: baseline vs dampened ===", flush=True)
    print(f"{'Layer':>5s}  {'Base σ₁/σ₂':>11s}  {'Damp σ₁/σ₂':>11s}  {'Δ':>8s}", flush=True)
    print("-" * 40, flush=True)

    dominance = {}
    for li in target_layers:
        bp = np.vstack([np.stack(baseline_data[li][c]) for c in cond_labels])
        dp = np.vstack([np.stack(dampened_data[li][c]) for c in cond_labels])

        bc = bp - bp.mean(axis=0, keepdims=True)
        dc = dp - dp.mean(axis=0, keepdims=True)

        _, Sb, _ = np.linalg.svd(bc, full_matrices=False)
        _, Sd, _ = np.linalg.svd(dc, full_matrices=False)

        bd = float(Sb[0] / Sb[1]) if len(Sb) > 1 else float('inf')
        dd = float(Sd[0] / Sd[1]) if len(Sd) > 1 else float('inf')

        print(f"L{li:>3d}  {bd:11.2f}  {dd:11.2f}  {dd - bd:+8.2f}", flush=True)
        dominance[str(li)] = {"baseline": bd, "dampened": dd}

    # =========================================================
    # PHASE 5: Classification
    # =========================================================
    print(f"\n=== Classification comparison ===", flush=True)
    chance = 1.0 / len(cond_labels)

    print(f"{'Layer':>5s}  {'B-raw':>7s}  {'B-CCS':>7s}  {'D-raw':>7s}  {'D-CCS':>7s}  {'Ch':>5s}  {'Verdict':>10s}", flush=True)
    print("-" * 60, flush=True)

    results = {
        "model": MODEL_ID,
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S"),
        "sink_threshold": SINK_THRESHOLD,
        "n_sink_heads": len(sink_heads),
        "sink_heads": [(li, hi, float(sink_mass[li, hi])) for li, hi in sink_heads],
        "sink_by_layer": {str(k): sorted(v) for k, v in sink_by_layer.items()},
        "n_conditions": len(CONDITIONS),
        "n_probes": len(PROBES),
        "chance": chance,
        "dominance": dominance,
        "layers": {},
    }

    for li in target_layers:
        br = loo_classify(baseline_data[li], cond_labels, k_remove=0)
        bc = loo_classify(baseline_data[li], cond_labels, k_remove=1)
        dr = loo_classify(dampened_data[li], cond_labels, k_remove=0)
        dc = loo_classify(dampened_data[li], cond_labels, k_remove=1)

        if dc > chance + 0.10:
            v = "SURVIVES"
        elif dc > chance + 0.05:
            v = "weakened"
        else:
            v = "COLLAPSED"

        print(f"L{li:>3d}  {br:7.1%}  {bc:7.1%}  {dr:7.1%}  {dc:7.1%}  "
              f"{chance:5.1%}  {v:>10s}", flush=True)

        results["layers"][str(li)] = {
            "baseline_raw": float(br), "baseline_ccs1": float(bc),
            "dampened_raw": float(dr), "dampened_ccs1": float(dc),
            "verdict": v,
        }

    # =========================================================
    # VERDICT
    # =========================================================
    print(f"\n=== VERDICT ===", flush=True)

    survived = [li for li in target_layers if results["layers"][str(li)]["verdict"] == "SURVIVES"]
    collapsed = [li for li in target_layers if results["layers"][str(li)]["verdict"] == "COLLAPSED"]
    weakened = [li for li in target_layers if results["layers"][str(li)]["verdict"] == "weakened"]

    if len(survived) >= len(target_layers) // 2:
        overall = "STENCIL_REAL"
        print("STENCIL IS REAL — classification SURVIVES sink dampening", flush=True)
        print("  σ₁ tracks structure BEYOND attention sinks", flush=True)
    elif len(collapsed) >= len(target_layers) // 2:
        overall = "STENCIL_IS_SINKS"
        print("STENCIL = SINKS — classification COLLAPSES without sinks", flush=True)
        print("  σ₁ was an artifact of attention sink patterns", flush=True)
    else:
        overall = "MIXED"
        print("MIXED — partial dependence on sinks", flush=True)

    print(f"  Survived: {survived}", flush=True)
    print(f"  Weakened: {weakened}", flush=True)
    print(f"  Collapsed: {collapsed}", flush=True)

    results["overall_verdict"] = overall

    with open(OUT_PATH, "w") as f:
        json.dump(results, f, indent=2, cls=NumpyEncoder)
    print(f"\nSaved to {OUT_PATH}", flush=True)


if __name__ == "__main__":
    main()
