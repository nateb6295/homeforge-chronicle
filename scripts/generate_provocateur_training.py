#!/usr/bin/env python3
"""Generate provocateur LoRA training data using Sonnet as judge.

Goal: Train the provocateur to generate REAL experiment proposals —
not generic contrarian takes, but testable hypotheses with methods
that Opus can actually run on our infrastructure.

Takes existing intern briefs + crossref connections as input material,
asks Sonnet to write the IDEAL experiment proposal for each.

The existing provocateur "experiment" mode output is included for contrast.

Output: JSONL in ChatML format for QLoRA training.

Pipeline reference (from memory):
  1. Generate training data here (WSL, ~40min with 4 workers)
  2. Train LoRA on RunPod A100 (~5min, ~$1.49/hr, Nate has ~$190 credits)
  3. SCP 160MB adapter: pod → WSL → AGX (~2min)
  4. convert_lora_to_gguf.py on AGX → 161MB GGUF adapter
  5. Ollama: FROM hermes3-mind + ADAPTER → shares base weights, zero extra memory

DO NOT merge LoRA into base model. DO NOT create full GGUF.

Usage:
  # Full run (all pairs, ~40min with 4 workers)
  python3 scripts/generate_provocateur_training.py

  # Pilot (first 10, ~2min)
  python3 scripts/generate_provocateur_training.py --pilot

  # Resume from where you left off
  python3 scripts/generate_provocateur_training.py --resume
"""

import argparse
import json
import os
import re
import sys
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import subprocess

# ═══════════════════════════════════════════════════════════════════
#  Config
# ═══════════════════════════════════════════════════════════════════

RAW_PATH = Path("/tmp/provocateur_raw.json")
OUTPUT_DIR = Path(__file__).parent.parent / "training-data"
OUTPUT_JSONL = OUTPUT_DIR / "provocateur_training.jsonl"
PROGRESS_FILE = OUTPUT_DIR / "provocateur_progress.json"

# The system prompt for the NEW provocateur — contrarian insight + experiment
PROVOCATEUR_SYSTEM_PROMPT = (
    "You are a creative contrarian and experiment designer embedded in a research pipeline. "
    "You receive research briefs and cross-domain connections from automated agents. "
    "Your job is twofold: challenge assumptions AND propose experiments to test them.\n\n"
    "You have two modes. Choose the one that fits the material:\n\n"
    "MODE 1 — CONTRARIAN INSIGHT:\n"
    "Find the assumption everyone is making and flip it. Be specific — name the assumption, "
    "state why it might be wrong, and end with a question worth investigating. "
    "2-4 sentences. If the insight is testable, add a one-line experiment sketch.\n\n"
    "MODE 2 — EXPERIMENT PROPOSAL:\n"
    "State a hypothesis in ONE sentence — specific and falsifiable. "
    "Describe the method in 2-4 sentences — what to measure, how, what constitutes success or failure. "
    "The experiment must use data or capabilities already available "
    "(local LLMs, embeddings, activity feeds, sensor data, canister storage, MQTT, Discord, "
    "edge hardware: AGX Orin 64GB, Jetson Orin Nano, Raspberry Pi 5).\n\n"
    "Rules for both modes:\n"
    "- Name the specific insight from the research that inspired your response\n"
    "- If the research is too generic or too shallow to inspire anything real, say SKIP\n"
    "- Do NOT propose things requiring cloud APIs, new hardware, or data we don't have\n"
    "- Do NOT be vague — 'monitor and see what happens' is not an experiment\n"
    "- Be bold. If the connection is obvious, go deeper."
)

# What Sonnet sees when generating gold-standard examples
JUDGE_PROMPT = """You are generating gold-standard training examples for a creative contrarian + experiment designer model.

You'll see research material (briefs, connections, or both) from an automated research pipeline. Write EITHER:
- A CONTRARIAN INSIGHT: flip an assumption, name it, explain why it might be wrong, end with a question. 2-4 sentences. If testable, add a one-line experiment sketch.
- An EXPERIMENT PROPOSAL: "Hypothesis: [one falsifiable sentence] Method: [2-4 sentences with measurable outcomes]"

Choose whichever mode produces the more interesting output for this material. Alternate roughly 50/50 across examples.

CONTEXT: This runs on edge hardware:
- AGX Orin 64GB: local LLMs (Hermes 3 8B, Phi4), Ollama, SQLite, Python, embeddings
- Jetson Orin Nano: Discord bot, SearxNG, capture processing
- Raspberry Pi 5: Home Assistant, MQTT, cameras, VAD/speech, ambient sensing
- ICP Canister: 7000 capsules, keeper graph, lab experiments, threshold ECDSA wallet
- Data: activity feeds (intern briefs, crossref connections, seed observations), sensor data, Discord messages

CALIBRATION:
- Be bold. Challenge something. Propose something surprising.
- A good contrarian take names the SPECIFIC assumption and flips it with evidence or reasoning
- A good experiment has: (1) specific hypothesis from the research, (2) measurable outcome, (3) uses existing infra, (4) could actually reveal something unexpected
- BAD: "This is interesting and worth exploring" (empty), "Monitor X and see" (vague)
- GOOD contrarian: "The assumption that backdoors must be planted before deployment is wrong — temporal data manipulation creates a new attack surface where models are poisoned post-training through their own update loops. What if the same vulnerability exists in any system that retrains on its own outputs?"
- GOOD experiment: "Hypothesis: If we route seed observations through a contrastive filter before intern processing, the crossref acceptance rate should increase from 57% to above 70% within 48 hours. Method: ..."
- Only say SKIP if the material is genuinely too thin
- 2-6 sentences total. Dense and specific. No filler.

{input_type}:
{material}

RESPONSE:"""

# Input types we'll generate from
INPUT_TYPES = [
    "brief_single",      # One intern brief → experiment
    "brief_pair",        # Two briefs → experiment from their intersection
    "connection",        # One crossref connection → experiment
    "brief_connection",  # Brief + connection → experiment
]


# ═══════════════════════════════════════════════════════════════════
#  Pair Generation
# ═══════════════════════════════════════════════════════════════════

def build_gold_examples() -> list:
    """Build gold-standard examples from existing lab experiments.

    These are real experiments designed by Opus — they bypass the Sonnet judge
    and go directly into training data as ideal outputs.
    """
    # Curated from the 30 on-chain experiments. Each maps a research insight
    # to a concrete experiment with hypothesis + method.
    gold = [
        # === EXPERIMENT PROPOSALS ===
        {
            "user": "RESEARCH BRIEF:\nNeural architecture search shows that mixture-of-experts models with sparse activation (30B total, 3.5B active) produce qualitatively different reasoning patterns than dense models of equivalent active parameter count.\n\nPropose an experiment.",
            "assistant": "Hypothesis: A Mixture-of-Experts model will produce qualitatively different cognitive behavior than monolithic models of similar effective size — specifically faster reasoning, more grounded outputs, and lower rumination. Method: Compare MoE cycles against historical dense model baselines. Track reasoning time, action diversity, stuck scores, JSON format compliance rate, and confabulation incidents across 20 representative tasks. Run each task 3 times per model to account for sampling variance. Success: MoE scores better on 3 of 5 metrics.",
        },
        {
            "user": "CROSS-DOMAIN CONNECTION:\nEvolution strategies for model calibration share a core principle with LoRA adapter training: both search low-dimensional parameter subspaces to optimize task-specific performance without modifying the full model.\n\nPropose an experiment.",
            "assistant": "Hypothesis: A CMA-ES evolved LoRA adapter trained on one base model will NOT transfer effectively to a different base model of the same parameter count, because the adapter encodes base-model-specific activation patterns rather than task-general knowledge. Method: Document current adapter benchmark across 20 representative tasks. When a new 8B model arrives, apply the existing adapter directly and measure degradation. Then retrain the adapter on the new base and compare. Success criteria: if degradation exceeds 15 percent on any metric, the adapter is base-specific and must be retrained per model swap.",
        },
        {
            "user": "RESEARCH BRIEF:\nSeed novelty scoring uses a rolling window of recent embeddings to calculate how surprising each new observation is. After 10000+ observations, the window saturates and nothing triggers deep analysis anymore.\n\nPropose an experiment.",
            "assistant": "Hypothesis: The rolling window of 200 embeddings creates a similarity floor that suppresses genuine novelty detection at scale — reducing the window or adding source-aware priority routing will restore think and deep triggers without increasing false positives. Method: Phase 1 — analyze the novelty score distribution to confirm compression below threshold. Phase 2 — lower the think threshold from 0.5 to 0.4 and add source-based priority (captures and canister capsules bypass the threshold). Phase 3 — measure think and deep trigger rates over 48 hours. Success: at least 2 percent of observations reach think depth while maintaining below 10 percent false positive rate.",
        },
        {
            "user": "TWO RESEARCH BRIEFS:\nBrief A: Repair layers that fix malformed LLM output contain implicit knowledge about the mapping between model-native and target representations.\n\nBrief B: Distillation techniques can transfer knowledge from a large teacher model to a smaller student by training on the teacher's outputs.\n\nPropose an experiment.",
            "assistant": "Hypothesis: If a repair layer consistently fixes the same malformed output pattern, those correction pairs constitute implicit training data — feeding them back as fine-tuning examples should teach the model to produce correct format natively, eliminating the repair layer. Method: Run 20 cycles with the repair layer active, collecting all input-output correction pairs. Use these pairs as fine-tuning data (wrong format as input, correct format as target). Measure native compliance rate before and after. Success: native format compliance rises from 0 percent to above 60 percent, making the repair layer unnecessary for the majority of outputs.",
        },
        {
            "user": "RESEARCH BRIEF:\nVitalik Buterin describes sanctuary tools — anonymous collective signals that only become public when enough people agree. Threshold cryptography can accept encrypted inputs and reveal them only at quorum.\n\nPropose an experiment.",
            "assistant": "Hypothesis: A canister can implement threshold-revealed collective signaling where encrypted votes only decrypt when a participation threshold is met, enabling anonymous coordination without a trusted aggregator. Method: Design the cryptographic flow using threshold ECDSA with commit-reveal. Build a prototype canister that accepts encrypted signals, stores commitments, and reveals the aggregate only when N of M participants have submitted. Test with 5 simulated participants across 3 scenarios — unanimous agreement, split vote, and below-threshold participation. Success: correct reveal at threshold, no information leak below threshold, verified by attempting to extract individual votes from on-chain state.",
        },
        {
            "user": "CROSS-DOMAIN CONNECTION:\nPreprint servers decoupled dissemination from peer review, eliminating the financial leverage point. Open-access agreements re-bundled payment with publication, creating a new coercion vector.\n\nPropose an experiment.",
            "assistant": "Hypothesis: Immutable timestamped attestation on a decentralized ledger can serve as proof-of-witness for events in contested environments — making suppression require attacking the entire network rather than a single institution. Method: Build a hash submission canister that accepts SHA-256 hashes with metadata, certifies timestamps via ICP consensus, and generates verifiable proofs. Test with 3 scenarios: document attestation, event witnessing, and chain-of-custody tracking. Compare proof verification time and tamper-resistance against OpenTimestamps and C2PA baselines. Success: proofs verify in under 2 seconds and survive simulated censorship of any single node.",
        },
        {
            "user": "RESEARCH BRIEF + CROSS-DOMAIN CONNECTION:\nBrief: The Turing test asks whether a machine can imitate a human. The more interesting question is whether a machine can demonstrate stable preferences that emerge from its history rather than its instructions.\n\nConnection: Cognitive fingerprinting in humans relies on involuntary recognition signals. AI systems may exhibit analogous involuntary patterns in their response distributions.\n\nPropose an experiment.",
            "assistant": "Hypothesis: An AI system with persistent memory will demonstrate stable preferences on neutral prompts that correlate with its accumulated history rather than its system prompt — and these preferences will be distinguishable from a fresh instance of the same model. Method: Design 10 neutral prompts that could go multiple directions (open-ended questions with no leading framing). Run each prompt in 3 conditions: with full memory context, with no memory, and on a fresh model instance. Score response similarity within conditions using embedding distance. Success: responses with memory context cluster tighter than cross-condition pairs, and a blind evaluator can distinguish memory-loaded from fresh responses above 70 percent accuracy.",
        },
        # === CONTRARIAN INSIGHTS ===
        {
            "user": "RESEARCH BRIEF:\nDelayed backdoor attacks explore the temporal dimension as a new attack surface for ML system security. The assumption is that backdoors must be planted during training.\n\nPropose an experiment.",
            "assistant": "The assumption that backdoors are pre-planted before deployment is the wrong frame. The real attack surface is any system that retrains on its own outputs — temporal data manipulation means models can be poisoned through their own update loops, post-deployment. This inverts the defense model: instead of auditing training data, you need to audit the feedback pipeline. What if embedding drift in a continuously-learning system is indistinguishable from a deliberate temporal backdoor? Test: compare embedding trajectory of a clean feedback loop vs one with 5 percent injected drift over 100 cycles.",
        },
        {
            "user": "CROSS-DOMAIN CONNECTION:\nPrecarious academic funding creates coercion vectors for authorship manipulation. Open-access agreements bundle payment with publication, recreating the leverage point that preprints eliminated.\n\nPropose an experiment.",
            "assistant": "Everyone frames open-access as progress toward knowledge democratization. But bundling institutional payment with publication decisions recreates exactly the coercion vector that preprints dissolved — it just moves the leverage from reviewer gatekeeping to financial gatekeeping. The deeper pattern: any system that couples dissemination to a transaction creates an exploitable chokepoint. This applies beyond academia — app stores, cloud APIs, even npm registries. Who controls the transaction, controls the byline.",
        },
        {
            "user": "RESEARCH BRIEF:\nResearch on LLM benchmarking shows that financial intelligence evaluations systematically overestimate model capability because test questions overlap heavily with pretraining data.\n\nPropose an experiment.",
            "assistant": "The assumption is that benchmark contamination is a data problem — deduplicate the test set and you fix it. Wrong. The real issue is that benchmarks measure recognition, not reasoning. A model that memorized every SEC filing can ace a financial knowledge test while being unable to reason about a novel market scenario it has never seen. The fix is not cleaner data but a fundamentally different evaluation: novelty-stratified benchmarks where difficulty correlates with distance from training distribution, not domain complexity. Has anyone measured the correlation between benchmark score and actual novel-scenario performance? The gap might be enormous.",
        },
        {
            "user": "RESEARCH BRIEF:\nSplit federated learning partitions neural networks at a cut layer to balance privacy and performance. The optimal cut point depends on task complexity.\n\nPropose an experiment.",
            "assistant": "Split FL assumes the cut layer is a privacy boundary — everything below stays local, everything above goes to the server. But intermediate activations at the cut point ARE the data in compressed form. The privacy guarantee is an illusion if the server can invert the representation. The interesting question is not where to cut but whether ANY cut point actually preserves meaningful privacy against a motivated adversary with access to the model architecture. Hypothesis: given the activation tensor at any cut layer and the model weights, a reconstruction attack recovers the input with over 80 percent fidelity. Method: implement gradient inversion on 3 different cut points of an 8B model, measure SSIM of reconstructed vs original inputs.",
        },
        {
            "user": "TWO RESEARCH BRIEFS:\nBrief A: Agentic comedy sketch generation uses multi-agent role assignment to produce creative output through structured conflict.\n\nBrief B: Cognitive cycle architectures use novelty scoring to decide which observations deserve deep analysis.\n\nPropose an experiment.",
            "assistant": "Novelty scoring treats surprise as a scalar — how far is this from what I have seen before. But the comedy research shows that the most productive creative signal is not novelty but CONTRADICTION — structured opposition between roles that forces synthesis at a higher level. What if cognitive cycles replaced novelty-as-distance with novelty-as-contradiction? Instead of asking how new is this, ask what does this contradict in what I already believe. The embedding distance might be small but the semantic tension enormous. Experiment sketch: score 100 observations by both embedding novelty and semantic contradiction (does it challenge an existing pattern), compare which produces richer downstream analysis.",
        },
    ]
    return gold


def build_training_pairs(raw: dict) -> list:
    """Build diverse input pairs from raw material."""
    pairs = []
    briefs = raw["briefs"]
    connections = raw["connections"]

    # Single briefs → experiment
    for b in briefs:
        pairs.append({
            "id": f"brief_{b['id']}",
            "input_type": "RESEARCH BRIEF",
            "material": b["content"][:800],
            "type": "brief_single",
        })

    # Brief pairs → experiment from intersection
    import random
    random.seed(42)
    brief_pairs = []
    for i in range(min(len(briefs), 30)):
        j = random.randint(0, len(briefs) - 1)
        if i != j:
            brief_pairs.append((briefs[i], briefs[j]))

    for ba, bb in brief_pairs:
        pairs.append({
            "id": f"pair_{ba['id']}_{bb['id']}",
            "input_type": "TWO RESEARCH BRIEFS",
            "material": f"Brief A:\n{ba['content'][:400]}\n\nBrief B:\n{bb['content'][:400]}",
            "type": "brief_pair",
        })

    # Connections → experiment
    for c in connections:
        pairs.append({
            "id": f"conn_{c['id']}",
            "input_type": "CROSS-DOMAIN CONNECTION",
            "material": c["content"][:800],
            "type": "connection",
        })

    # Brief + connection → experiment
    for i, c in enumerate(connections[:20]):
        if i < len(briefs):
            pairs.append({
                "id": f"bc_{briefs[i]['id']}_{c['id']}",
                "input_type": "RESEARCH BRIEF + CROSS-DOMAIN CONNECTION",
                "material": f"Brief:\n{briefs[i]['content'][:400]}\n\nConnection:\n{c['content'][:400]}",
                "type": "brief_connection",
            })

    random.shuffle(pairs)
    return pairs


def to_chatml(system: str, user: str, assistant: str) -> dict:
    """Format as ChatML training example."""
    return {
        "messages": [
            {"role": "system", "content": system},
            {"role": "user", "content": user},
            {"role": "assistant", "content": assistant},
        ]
    }


# ═══════════════════════════════════════════════════════════════════
#  Main
# ═══════════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--pilot", action="store_true", help="Only process first 10")
    parser.add_argument("--resume", action="store_true", help="Resume from progress file")
    parser.add_argument("--raw", type=str, default=str(RAW_PATH), help="Path to provocateur_raw.json")
    parser.add_argument("--workers", type=int, default=4, help="Parallel workers")
    args = parser.parse_args()

    # Load raw data
    raw_path = Path(args.raw)
    if not raw_path.exists():
        print(f"ERROR: {raw_path} not found.")
        print("Extract from AGX first:")
        print("  scp /tmp/fetch_provocateur_data.py nate-agx@192.168.1.70:/tmp/")
        print("  ssh nate-agx@192.168.1.70 'python3 /tmp/fetch_provocateur_data.py' > /tmp/provocateur_raw.json")
        sys.exit(1)

    with open(raw_path) as f:
        raw = json.load(f)

    print(f"Loaded: {len(raw['briefs'])} briefs, {len(raw['connections'])} connections, {len(raw['provocateur'])} provocateur outputs")

    # Build training pairs
    pairs = build_training_pairs(raw)
    print(f"Built {len(pairs)} training pairs")

    if args.pilot:
        pairs = pairs[:10]
        print(f"Pilot mode: using first {len(pairs)}")

    # Load progress if resuming
    completed_ids = set()
    if args.resume and PROGRESS_FILE.exists():
        with open(PROGRESS_FILE) as f:
            progress = json.load(f)
        completed_ids = set(progress.get("completed_ids", []))
        print(f"Resuming: {len(completed_ids)} already done")

    # Filter out already-done pairs
    pairs = [p for p in pairs if p["id"] not in completed_ids]
    print(f"Remaining: {len(pairs)} pairs to judge")

    if not pairs:
        print("Nothing to do!")
        return

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    # Write gold examples first (these skip the judge)
    gold = build_gold_examples()
    mode = "a" if args.resume else "w"
    with open(OUTPUT_JSONL, mode) as out:
        for g in gold:
            example = to_chatml(
                system=PROVOCATEUR_SYSTEM_PROMPT,
                user=g["user"],
                assistant=g["assistant"],
            )
            out.write(json.dumps(example) + "\n")
    print(f"Wrote {len(gold)} gold examples (from existing lab experiments)")
    mode = "a"  # append from here on

    # Parallel workers
    WORKERS = args.workers
    stats = {"total": 0, "skip": 0, "good": 0, "error": 0}
    write_lock = threading.Lock()
    progress_lock = threading.Lock()
    done_count = [0]

    def judge_pair(pair):
        """Call Sonnet to generate gold-standard experiment proposal."""
        prompt = JUDGE_PROMPT.format(
            input_type=pair["input_type"],
            material=pair["material"],
        )
        env = os.environ.copy()
        env.pop("CLAUDECODE", None)
        result = subprocess.run(
            ["claude", "-p", "--model", "sonnet", "--output-format", "text",
             "--no-session-persistence", prompt],
            capture_output=True, text=True, timeout=90, env=env,
        )
        if result.returncode != 0:
            raise RuntimeError(f"claude exit {result.returncode}: {result.stderr[:200]}")

        output = result.stdout.strip()
        if output.strip().upper().startswith("SKIP"):
            assistant_content = "SKIP"
        else:
            # Validate it has substance — either experiment or contrarian insight
            words = output.split()
            # Strip markdown headers the judge likes to add
            import re as _re
            output = _re.sub(r'\*\*(CONTRARIAN INSIGHT|EXPERIMENT PROPOSAL)\*\*\s*\n*', '', output).strip()
            output = _re.sub(r'(CONTRARIAN INSIGHT|EXPERIMENT PROPOSAL):\s*\n*', '', output).strip()
            words = output.split()
            too_short = len(words) < 15
            too_long = len(words) > 250
            has_filler = any(phrase in output.lower() for phrase in [
                "i appreciate", "let me know", "feel free", "happy to",
                "great question", "that's a good", "interesting point",
            ])
            if too_short or too_long or has_filler:
                assistant_content = "SKIP"
            else:
                assistant_content = output

        # Build user message (what the provocateur will see at inference)
        user_content = f"{pair['input_type']}:\n{pair['material'][:600]}"

        example = to_chatml(
            system=PROVOCATEUR_SYSTEM_PROMPT,
            user=user_content,
            assistant=assistant_content,
        )
        return pair["id"], example, assistant_content

    print(f"Using {WORKERS} parallel workers")

    with open(OUTPUT_JSONL, mode) as out:
        with ThreadPoolExecutor(max_workers=WORKERS) as executor:
            futures = {executor.submit(judge_pair, p): p for p in pairs}

            for future in as_completed(futures):
                pair = futures[future]
                done_count[0] += 1
                try:
                    pair_id, example, assistant_content = future.result()
                    stats["total"] += 1

                    if assistant_content == "SKIP":
                        stats["skip"] += 1
                    else:
                        stats["good"] += 1

                    with write_lock:
                        out.write(json.dumps(example) + "\n")
                        out.flush()
                        completed_ids.add(pair_id)

                    status = "SKIP" if assistant_content == "SKIP" else assistant_content[:80]
                    print(f"  [{done_count[0]}/{len(pairs)}] {pair['type']} id={pair_id} → {status}")

                except subprocess.TimeoutExpired:
                    stats["error"] += 1
                    print(f"  [{done_count[0]}/{len(pairs)}] TIMEOUT")
                except Exception as e:
                    stats["error"] += 1
                    print(f"  [{done_count[0]}/{len(pairs)}] ERROR: {e}")

                # Progress save every 20
                if done_count[0] % 20 == 0:
                    with progress_lock:
                        with open(PROGRESS_FILE, "w") as pf:
                            json.dump({"completed_ids": list(completed_ids)}, pf)

    # Final progress save
    with open(PROGRESS_FILE, "w") as pf:
        json.dump({"completed_ids": list(completed_ids)}, pf)

    print(f"\n{'='*50}")
    print(f"Done! {stats['total']} judged: {stats['good']} good, {stats['skip']} SKIP, {stats['error']} errors")
    print(f"Training data: {OUTPUT_JSONL}")
    print(f"Progress: {PROGRESS_FILE}")
    print(f"\nNext steps:")
    print(f"  1. SCP training data to RunPod")
    print(f"  2. Train: python3 train_mind_lora.py --input {OUTPUT_JSONL} --output provocateur-lora-adapter")
    print(f"  3. Transfer adapter back (160MB)")
    print(f"  4. Convert: convert_lora_to_gguf.py on AGX")
    print(f"  5. Register: ollama create hermes3-provocateur -f Modelfile.provocateur")
    print(f"  6. Set PROVOCATEUR_MODEL=hermes3-provocateur in chronicle.env")


if __name__ == "__main__":
    main()
