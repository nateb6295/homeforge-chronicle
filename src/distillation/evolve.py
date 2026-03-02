#!/usr/bin/env python3
"""
Evolutionary Model Forge — CMA-ES Evolution Loop.

Search space: 12 dimensions — 4 layer segments (8 layers each) x 3 parent
weights per segment. Softmax per segment normalizes to valid distribution.

Per-candidate pipeline (~20 min each):
  1. CMA-ES generates weight vector → mergekit YAML config
  2. mergekit merge (CPU, lazy tensors) → merged safetensors
  3. llama-quantize → Q4_K_M GGUF (~4.5GB)
  4. ollama create forge-candidate → load into VRAM
  5. eval_harness.evaluate() → 24 prompts, 6 dimensions, fitness score
  6. ollama rm forge-candidate → cleanup
  7. Score feeds back to CMA-ES

Usage:
    # Pilot: 2 gens x 4 pop = 8 evals (~3h)
    python3 evolve.py --pilot

    # Full: 20 gens x 8 pop = 160 evals (~53h)
    python3 evolve.py

    # Resume from checkpoint
    python3 evolve.py --resume

    # Custom params
    python3 evolve.py --generations 5 --population 6
"""

import argparse
import json
import math
import os
import pickle
import shutil
import subprocess
import sys
import tempfile
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from forge_config import (
    FORGE_ROOT, MERGED_DIR, CANDIDATE_DIR, GGUF_DIR, EVOLVE_DIR,
    CAPSULE_MERGED, ACTION_MERGED, DOMAIN_MERGED,
    OLLAMA_URL, CANDIDATE_MODEL_NAME, LLAMA_QUANTIZE, QUANT_TYPE,
    POPULATION_SIZE, NUM_GENERATIONS, PILOT_POPULATION, PILOT_GENERATIONS,
    SEARCH_DIMS, NUM_LAYER_SEGMENTS, NUM_PARENTS, LAYERS_PER_SEGMENT,
    EVOLVE_LOG, EVOLVE_CHECKPOINT, EVAL_NUM_CTX,
)


def parse_args():
    parser = argparse.ArgumentParser(description="CMA-ES evolutionary model merging")
    parser.add_argument("--pilot", action="store_true",
                        help=f"Pilot mode: {PILOT_GENERATIONS} gens x {PILOT_POPULATION} pop")
    parser.add_argument("--generations", type=int, default=None,
                        help=f"Number of generations (default: {NUM_GENERATIONS})")
    parser.add_argument("--population", type=int, default=None,
                        help=f"Population size (default: {POPULATION_SIZE})")
    parser.add_argument("--resume", action="store_true",
                        help="Resume from CMA-ES checkpoint")
    parser.add_argument("--ollama-url", default=OLLAMA_URL)
    parser.add_argument("--dry-run", action="store_true",
                        help="Print configs without running merges/evals")
    return parser.parse_args()


# =============================================================================
# Weight vector → mergekit config
# =============================================================================

def softmax(x):
    """Compute softmax over a list of values."""
    max_x = max(x)
    exp_x = [math.exp(v - max_x) for v in x]
    sum_exp = sum(exp_x)
    return [v / sum_exp for v in exp_x]


def vector_to_layer_weights(raw_vector):
    """Convert 12-dim raw vector to per-segment normalized weights.

    raw_vector: [s0_capsule, s0_action, s0_domain,
                 s1_capsule, s1_action, s1_domain,
                 s2_capsule, s2_action, s2_domain,
                 s3_capsule, s3_action, s3_domain]

    Returns: list of 4 dicts, each {capsule: w, action: w, domain: w}
    """
    segments = []
    for seg in range(NUM_LAYER_SEGMENTS):
        offset = seg * NUM_PARENTS
        raw = raw_vector[offset:offset + NUM_PARENTS]
        normalized = softmax(raw)
        segments.append({
            "capsule": normalized[0],
            "action": normalized[1],
            "domain": normalized[2],
        })
    return segments


def generate_mergekit_config(segments):
    """Generate a mergekit YAML config for linear merge with per-layer weights.

    The config uses the 'linear' merge method with slice-based weight assignment
    for 4 layer segments of 8 layers each.
    """
    # Build the YAML config
    # mergekit linear merge: each model gets a weight, optionally per-layer
    config = {
        "merge_method": "linear",
        "dtype": "float16",
        "models": [
            {
                "model": CAPSULE_MERGED,
                "parameters": {
                    "weight": _build_layer_weight_list(segments, "capsule"),
                },
            },
            {
                "model": ACTION_MERGED,
                "parameters": {
                    "weight": _build_layer_weight_list(segments, "action"),
                },
            },
            {
                "model": DOMAIN_MERGED,
                "parameters": {
                    "weight": _build_layer_weight_list(segments, "domain"),
                },
            },
        ],
    }
    return config


def _build_layer_weight_list(segments, parent_name):
    """Build mergekit per-layer weight spec as a list of {filter, value} dicts.

    Hermes 3 8B (Llama 3.1) has 32 transformer layers.
    We split into 4 segments of 8 layers each.
    """
    weight_list = []

    for seg_idx, seg_weights in enumerate(segments):
        start_layer = seg_idx * LAYERS_PER_SEGMENT
        end_layer = start_layer + LAYERS_PER_SEGMENT
        weight = seg_weights[parent_name]

        # Apply weight to each layer in this segment
        for layer_idx in range(start_layer, end_layer):
            weight_list.append({
                "filter": f"model.layers.{layer_idx}",
                "value": round(weight, 6),
            })

    # Embedding and output layers — use average across all segments
    avg_weight = sum(seg[parent_name] for seg in segments) / len(segments)
    weight_list.insert(0, {
        "filter": "model.embed_tokens",
        "value": round(avg_weight, 6),
    })
    weight_list.append({
        "filter": "model.norm",
        "value": round(avg_weight, 6),
    })
    weight_list.append({
        "filter": "lm_head",
        "value": round(avg_weight, 6),
    })

    return weight_list


# =============================================================================
# Candidate pipeline
# =============================================================================

def write_mergekit_config(config, output_path):
    """Write mergekit config as YAML."""
    import yaml
    with open(output_path, "w") as f:
        yaml.dump(config, f, default_flow_style=False)


def run_mergekit_merge(config_path, output_dir):
    """Run mergekit merge via CLI (avoids pydantic v2 compatibility issues)."""
    # Clear output dir
    if os.path.exists(output_dir):
        shutil.rmtree(output_dir)
    os.makedirs(output_dir, exist_ok=True)

    try:
        import yaml
        with open(config_path) as f:
            config_dict = yaml.safe_load(f)

        from mergekit.config import MergeConfiguration
        from mergekit.merge import MergeOptions, run_merge

        merge_config = MergeConfiguration.model_validate(config_dict)
        run_merge(
            merge_config,
            out_path=output_dir,
            options=MergeOptions(
                lazy_unpickle=True,
                low_cpu_memory=True,
            ),
        )
        return True
    except Exception as e:
        print(f"  [ERROR] mergekit merge failed: {e}")
        return False


def quantize_to_gguf(model_dir, gguf_path, quant_type=QUANT_TYPE):
    """Convert safetensors model to GGUF using llama-quantize.

    Two-step: convert to f16 GGUF first, then quantize.
    """
    # Step 1: Convert safetensors → f16 GGUF
    f16_gguf = gguf_path.replace(".gguf", "-f16.gguf")

    # Use llama.cpp's convert_hf_to_gguf.py
    convert_script = os.path.expanduser("~/llama.cpp/convert_hf_to_gguf.py")
    if not os.path.exists(convert_script):
        print(f"  [ERROR] convert_hf_to_gguf.py not found at {convert_script}")
        return False

    result = subprocess.run(
        ["python3", convert_script, model_dir, "--outfile", f16_gguf, "--outtype", "f16"],
        capture_output=True, text=True, timeout=600,
    )
    if result.returncode != 0:
        print(f"  [ERROR] GGUF conversion failed: {result.stderr[-500:]}")
        return False

    # Step 2: Quantize f16 → Q4_K_M
    if not os.path.exists(LLAMA_QUANTIZE):
        print(f"  [ERROR] llama-quantize not found: {LLAMA_QUANTIZE}")
        return False

    result = subprocess.run(
        [LLAMA_QUANTIZE, f16_gguf, gguf_path, quant_type],
        capture_output=True, text=True, timeout=600,
    )

    # Clean up f16 intermediate
    if os.path.exists(f16_gguf):
        os.remove(f16_gguf)

    if result.returncode != 0:
        print(f"  [ERROR] Quantization failed: {result.stderr[-500:]}")
        return False

    return True


def ollama_create_model(model_name, gguf_path, ollama_url=OLLAMA_URL):
    """Create an Ollama model from a GGUF file."""
    # Write a Modelfile
    modelfile_path = os.path.join(CANDIDATE_DIR, "Modelfile")
    with open(modelfile_path, "w") as f:
        f.write(f"FROM {gguf_path}\n")
        f.write(f"PARAMETER num_ctx {EVAL_NUM_CTX}\n")

    result = subprocess.run(
        ["ollama", "create", model_name, "-f", modelfile_path],
        capture_output=True, text=True, timeout=300,
    )
    if result.returncode != 0:
        print(f"  [ERROR] ollama create failed: {result.stderr[-500:]}")
        return False
    return True


def ollama_remove_model(model_name):
    """Remove an Ollama model."""
    subprocess.run(
        ["ollama", "rm", model_name],
        capture_output=True, text=True, timeout=60,
    )


def evaluate_candidate(model_name, ollama_url):
    """Run eval harness on a candidate model."""
    from eval_harness import evaluate
    result = evaluate(
        model=model_name,
        ollama_url=ollama_url,
        timeout=120,
        verbose=False,
    )
    return result


def run_candidate_pipeline(raw_vector, candidate_idx, generation, ollama_url, dry_run=False):
    """Full pipeline for one candidate: merge → quantize → eval → cleanup.

    Returns (fitness, details) tuple.
    """
    start = time.time()
    segments = vector_to_layer_weights(raw_vector)

    print(f"\n--- Gen {generation}, Candidate {candidate_idx} ---")
    print(f"  Weights per segment:")
    for i, seg in enumerate(segments):
        print(f"    Seg {i} (L{i*8}-{i*8+7}): "
              f"capsule={seg['capsule']:.3f} action={seg['action']:.3f} domain={seg['domain']:.3f}")

    if dry_run:
        return 0.5, {"dry_run": True}

    # Step 1: Generate mergekit config
    config = generate_mergekit_config(segments)
    config_path = os.path.join(CANDIDATE_DIR, "merge_config.yaml")

    try:
        import yaml
    except ImportError:
        print("  [ERROR] PyYAML not installed")
        return 0.0, {"error": "no yaml"}

    write_mergekit_config(config, config_path)

    # Step 2: Run merge
    merge_output = os.path.join(CANDIDATE_DIR, "merged")
    print(f"  Merging models...")
    if not run_mergekit_merge(config_path, merge_output):
        return 0.0, {"error": "merge failed"}

    # Step 3: Quantize to GGUF
    candidate_gguf = os.path.join(CANDIDATE_DIR, "candidate.gguf")
    print(f"  Quantizing to {QUANT_TYPE}...")
    if not quantize_to_gguf(merge_output, candidate_gguf):
        _cleanup_candidate(merge_output, candidate_gguf)
        return 0.0, {"error": "quantize failed"}

    # Step 4: Create Ollama model
    print(f"  Creating Ollama model...")
    if not ollama_create_model(CANDIDATE_MODEL_NAME, candidate_gguf, ollama_url):
        _cleanup_candidate(merge_output, candidate_gguf)
        return 0.0, {"error": "ollama create failed"}

    # Step 5: Evaluate
    print(f"  Evaluating...")
    try:
        eval_result = evaluate_candidate(CANDIDATE_MODEL_NAME, ollama_url)
        fitness = eval_result["fitness"]
    except Exception as e:
        print(f"  [ERROR] Evaluation failed: {e}")
        fitness = 0.0
        eval_result = {"error": str(e)}

    # Step 6: Cleanup
    print(f"  Cleaning up...")
    ollama_remove_model(CANDIDATE_MODEL_NAME)
    _cleanup_candidate(merge_output, candidate_gguf)

    elapsed = time.time() - start
    print(f"  Fitness: {fitness:.4f} ({elapsed:.0f}s)")

    details = {
        "generation": generation,
        "candidate": candidate_idx,
        "raw_vector": list(raw_vector),
        "segments": segments,
        "fitness": fitness,
        "elapsed": elapsed,
        "dim_averages": eval_result.get("dim_averages", {}),
    }

    return fitness, details


def _cleanup_candidate(merge_output, candidate_gguf):
    """Clean up candidate artifacts."""
    if os.path.exists(merge_output):
        shutil.rmtree(merge_output, ignore_errors=True)
    if os.path.exists(candidate_gguf):
        os.remove(candidate_gguf)


# =============================================================================
# Evolution loop
# =============================================================================

def save_checkpoint(es, generation, log, best_fitness, best_vector):
    """Save CMA-ES checkpoint for resume."""
    checkpoint = {
        "es_state": es,
        "generation": generation,
        "log": log,
        "best_fitness": best_fitness,
        "best_vector": best_vector,
    }
    with open(EVOLVE_CHECKPOINT, "wb") as f:
        pickle.dump(checkpoint, f)


def load_checkpoint():
    """Load CMA-ES checkpoint."""
    if not os.path.exists(EVOLVE_CHECKPOINT):
        return None
    with open(EVOLVE_CHECKPOINT, "rb") as f:
        return pickle.load(f)


def save_winner(best_vector, best_fitness, segments):
    """Save the winning weight vector and merged model."""
    winner_info = {
        "vector": list(best_vector),
        "fitness": best_fitness,
        "segments": segments,
    }
    winner_info_path = os.path.join(GGUF_DIR, "winner_info.json")
    with open(winner_info_path, "w") as f:
        json.dump(winner_info, f, indent=2)
    print(f"\n[OK] Winner info saved to {winner_info_path}")


def main():
    args = parse_args()

    # Determine population and generations
    if args.pilot:
        pop_size = PILOT_POPULATION
        num_gens = PILOT_GENERATIONS
    else:
        pop_size = args.population or POPULATION_SIZE
        num_gens = args.generations or NUM_GENERATIONS

    total_evals = pop_size * num_gens

    print(f"=== Evolutionary Model Forge ===")
    print(f"Population: {pop_size}")
    print(f"Generations: {num_gens}")
    print(f"Total evaluations: {total_evals}")
    print(f"Search dimensions: {SEARCH_DIMS}")
    print(f"Estimated time: ~{total_evals * 20 / 60:.1f} hours")

    # Verify prerequisites
    for name, path in [("capsule", CAPSULE_MERGED), ("action", ACTION_MERGED), ("domain", DOMAIN_MERGED)]:
        if not os.path.isdir(path):
            print(f"[ERROR] Merged model not found: {name} at {path}")
            print("Run merge_loras_to_base.py first.")
            sys.exit(1)

    os.makedirs(EVOLVE_DIR, exist_ok=True)
    os.makedirs(CANDIDATE_DIR, exist_ok=True)
    os.makedirs(GGUF_DIR, exist_ok=True)

    # Initialize CMA-ES
    import cma

    if args.resume:
        checkpoint = load_checkpoint()
        if checkpoint:
            es = checkpoint["es_state"]
            start_gen = checkpoint["generation"] + 1
            log = checkpoint["log"]
            best_fitness = checkpoint["best_fitness"]
            best_vector = checkpoint["best_vector"]
            print(f"Resumed from generation {start_gen}, best fitness: {best_fitness:.4f}")
        else:
            print("[WARN] No checkpoint found, starting fresh")
            args.resume = False

    if not args.resume:
        # Initial point: equal weights (softmax of [0,0,0] = [0.33, 0.33, 0.33])
        x0 = [0.0] * SEARCH_DIMS
        sigma0 = 1.0  # initial step size

        es = cma.CMAEvolutionStrategy(x0, sigma0, {
            "popsize": pop_size,
            "maxiter": num_gens,
            "seed": 42,
            "verbose": -1,  # we do our own logging
        })
        start_gen = 0
        log = []
        best_fitness = -1.0
        best_vector = None

    # Evolution loop
    for gen in range(start_gen, num_gens):
        gen_start = time.time()
        print(f"\n{'='*60}")
        print(f"Generation {gen + 1}/{num_gens}")
        print(f"{'='*60}")

        # Ask CMA-ES for candidate vectors
        solutions = es.ask()

        # Evaluate each candidate
        # CMA-ES minimizes, so we negate fitness (we want to maximize)
        fitnesses = []
        gen_log = []

        for i, x in enumerate(solutions):
            fitness, details = run_candidate_pipeline(
                raw_vector=x,
                candidate_idx=i,
                generation=gen + 1,
                ollama_url=args.ollama_url,
                dry_run=args.dry_run,
            )

            # CMA-ES minimizes, so negate
            fitnesses.append(-fitness)
            gen_log.append(details)

            # Track best
            if fitness > best_fitness:
                best_fitness = fitness
                best_vector = list(x)
                print(f"  *** New best: {best_fitness:.4f} ***")

        # Tell CMA-ES the results
        if not args.dry_run:
            es.tell(solutions, fitnesses)

        gen_elapsed = time.time() - gen_start

        # Generation summary
        gen_fitnesses = [-f for f in fitnesses]  # un-negate for display
        gen_best = max(gen_fitnesses)
        gen_avg = sum(gen_fitnesses) / len(gen_fitnesses)

        print(f"\nGeneration {gen + 1} summary:")
        print(f"  Best this gen: {gen_best:.4f}")
        print(f"  Avg this gen:  {gen_avg:.4f}")
        print(f"  Best overall:  {best_fitness:.4f}")
        print(f"  Time: {gen_elapsed:.0f}s")

        # Save to log
        gen_entry = {
            "generation": gen + 1,
            "best_fitness": gen_best,
            "avg_fitness": gen_avg,
            "best_overall": best_fitness,
            "elapsed": gen_elapsed,
            "candidates": gen_log,
        }
        log.append(gen_entry)

        # Save checkpoint
        if not args.dry_run:
            save_checkpoint(es, gen, log, best_fitness, best_vector)

            # Save running log
            with open(EVOLVE_LOG, "w") as f:
                json.dump(log, f, indent=2)

    # Final report
    print(f"\n{'='*60}")
    print(f"Evolution Complete")
    print(f"{'='*60}")
    print(f"Best fitness: {best_fitness:.4f}")

    if best_vector:
        segments = vector_to_layer_weights(best_vector)
        print(f"\nWinning blend:")
        for i, seg in enumerate(segments):
            print(f"  Segment {i} (L{i*8}-{i*8+7}): "
                  f"capsule={seg['capsule']:.3f} action={seg['action']:.3f} domain={seg['domain']:.3f}")

        if not args.dry_run:
            # Create the final winner model
            print(f"\nBuilding winner model...")
            config = generate_mergekit_config(segments)
            config_path = os.path.join(GGUF_DIR, "winner_config.yaml")

            import yaml
            write_mergekit_config(config, config_path)

            # Merge
            winner_merged = os.path.join(GGUF_DIR, "winner-merged")
            if run_mergekit_merge(config_path, winner_merged):
                # Quantize
                winner_gguf = os.path.join(GGUF_DIR, "winner.gguf")
                if quantize_to_gguf(winner_merged, winner_gguf):
                    print(f"\n[OK] Winner GGUF: {winner_gguf}")
                    save_winner(best_vector, best_fitness, segments)

                    # Cleanup merged safetensors (keep GGUF)
                    shutil.rmtree(winner_merged, ignore_errors=True)
                else:
                    print(f"[ERROR] Failed to quantize winner")
            else:
                print(f"[ERROR] Failed to merge winner")

    print(f"\nLog: {EVOLVE_LOG}")
    print(f"Checkpoint: {EVOLVE_CHECKPOINT}")
    print(f"\nNext: Run deploy_winner.sh to deploy the winning model")


if __name__ == "__main__":
    main()
