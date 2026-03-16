#!/usr/bin/env python3
"""Generate crossref LoRA training data using Opus as judge.

Takes the 602 existing crossref connections, extracts the brief pairs,
and asks Opus to write the IDEAL connection description (or SKIP).

The model's existing output is included for contrast — Opus sees what
hermes3-mind wrote and writes what it SHOULD have written.

Output: JSONL in ChatML format for QLoRA training.

Usage:
  # Full run (all 602, ~$15-20 in API costs)
  python3 scripts/generate_crossref_training.py

  # Pilot (first 20, ~$0.50)
  python3 scripts/generate_crossref_training.py --pilot

  # Resume from where you left off
  python3 scripts/generate_crossref_training.py --resume
"""

import argparse
import json
import os
import re
import sys
import time
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import subprocess

# ═══════════════════════════════════════════════════════════════════
#  Config
# ═══════════════════════════════════════════════════════════════════

RAW_PATH = Path("/tmp/crossref_raw.json")
OUTPUT_DIR = Path(__file__).parent.parent / "training-data"
OUTPUT_JSONL = OUTPUT_DIR / "crossref_training.jsonl"
PROGRESS_FILE = OUTPUT_DIR / "crossref_progress.json"

# The same system prompt crossref.py uses — training data must match inference format
CROSSREF_SYSTEM_PROMPT = (
    "You are a research analyst finding cross-domain connections. "
    "You're given two briefs from different areas that share "
    "embedding similarity. Your job is to find what's TRANSFERABLE "
    "between them — not just what's similar, but what technique, "
    "principle, or approach from one domain could be applied to the other.\n\n"
    "Rules:\n"
    "- Name the specific shared principle (not 'both use AI' or 'both involve data')\n"
    "- State what could transfer: 'Domain A's approach to X could solve Domain B's problem with Y'\n"
    "- 2-3 sentences maximum, prose not lists\n"
    "- If the connection is only superficial (shared buzzwords, both mention technology), say SKIP\n"
    "- Do NOT connect through the host system (Chronicle, Homeforge, memory metabolism, etc.) "
    "— those references are context framing, not the actual research content. "
    "Find the connection between the EXTERNAL domains described.\n"
    "- Genuinely surprising connections are worth more than obvious ones"
)

OPUS_JUDGE_PROMPT = """You are generating gold-standard training examples for a crossref judgment model.

Two research briefs share embedding similarity. Write the IDEAL connection — the specific transferable principle between them.

IMPORTANT CALIBRATION:
- You are training a model to FIND connections, not to be skeptical. The embedding similarity already filtered for relevance — your job is to articulate WHY they're connected.
- Only say SKIP if the briefs are truly from completely unrelated domains with zero transferable principles, OR if the briefs are too truncated/mangled to understand.
- A good connection names a SPECIFIC mechanism, technique, or principle from one domain that maps onto the other.
- "Both involve X" is NOT a connection. "A's approach to X (specifically Y) could solve B's problem with Z" IS.
- Do NOT reference Chronicle, Homeforge, Sprout, seed agent, memory metabolism, canister, capsule, or any host system. Only the external research domains.
- 2-3 sentences max. Be precise and dense.

THE SMALL MODEL WROTE (for reference — improve on this or SKIP):
{model_output}

BRIEF A:
{brief_a}

BRIEF B:
{brief_b}

CONNECTION:"""


# ═══════════════════════════════════════════════════════════════════
#  Parsing
# ═══════════════════════════════════════════════════════════════════

def parse_connection(content: str) -> dict:
    """Parse a crossref connection entry into brief_a, brief_b, model_output."""
    # Pattern: [Crossref] Connection found (sim=X.XX):\n\nA: ...\nB: ...\n\nConnection: ...
    result = {"brief_a": "", "brief_b": "", "model_output": "", "similarity": 0.0}

    # Extract similarity
    sim_match = re.search(r"sim=(\d+\.\d+)", content)
    if sim_match:
        result["similarity"] = float(sim_match.group(1))

    # Extract briefs
    # Split on "A: " and "B: " and "Connection: "
    a_match = re.search(r"\nA:\s*(.+?)(?=\nB:\s)", content, re.DOTALL)
    b_match = re.search(r"\nB:\s*(.+?)(?=\nConnection:\s)", content, re.DOTALL)
    conn_match = re.search(r"\nConnection:\s*(.+)", content, re.DOTALL)

    if a_match:
        result["brief_a"] = a_match.group(1).strip()
    if b_match:
        result["brief_b"] = b_match.group(1).strip()
    if conn_match:
        result["model_output"] = conn_match.group(1).strip()

    return result


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
    parser.add_argument("--pilot", action="store_true", help="Only process first 20")
    parser.add_argument("--resume", action="store_true", help="Resume from progress file")
    parser.add_argument("--raw", type=str, default=str(RAW_PATH), help="Path to crossref_raw.json")
    args = parser.parse_args()

    # Load raw connections
    raw_path = Path(args.raw)
    if not raw_path.exists():
        print(f"ERROR: {raw_path} not found. Extract from AGX first.")
        sys.exit(1)

    with open(raw_path) as f:
        raw = json.load(f)

    print(f"Loaded {len(raw)} raw connections")

    # Parse into structured pairs
    pairs = []
    for entry in raw:
        parsed = parse_connection(entry["content"])
        if parsed["brief_a"] and parsed["brief_b"]:
            parsed["id"] = entry["id"]
            pairs.append(parsed)

    print(f"Parsed {len(pairs)} valid brief pairs")

    if args.pilot:
        pairs = pairs[:20]
        print(f"Pilot mode: using first {len(pairs)}")

    # Load progress if resuming
    completed_ids = set()
    existing_examples = []
    if args.resume and PROGRESS_FILE.exists():
        with open(PROGRESS_FILE) as f:
            progress = json.load(f)
        completed_ids = set(progress.get("completed_ids", []))
        print(f"Resuming: {len(completed_ids)} already done")

    if args.resume and OUTPUT_JSONL.exists():
        with open(OUTPUT_JSONL) as f:
            for line in f:
                if line.strip():
                    existing_examples.append(json.loads(line))

    # Filter out already-done pairs
    pairs = [p for p in pairs if p["id"] not in completed_ids]
    print(f"Remaining: {len(pairs)} pairs to judge")

    if not pairs:
        print("Nothing to do!")
        return

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    # Parallel workers — 4 concurrent claude calls
    WORKERS = int(os.environ.get("CROSSREF_WORKERS", "4"))
    mode = "a" if args.resume else "w"
    stats = {"total": 0, "skip": 0, "good": 0, "error": 0}
    write_lock = threading.Lock()
    progress_lock = threading.Lock()
    done_count = [0]

    def judge_pair(pair):
        """Call Sonnet to judge one pair. Returns (pair_id, example_dict) or (pair_id, None)."""
        prompt = OPUS_JUDGE_PROMPT.format(
            brief_a=pair["brief_a"][:600],
            brief_b=pair["brief_b"][:600],
            model_output=pair["model_output"][:400],
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

        opus_output = result.stdout.strip()
        if opus_output.strip().upper().startswith("SKIP"):
            assistant_content = "SKIP"
        else:
            assistant_content = opus_output

        user_content = (
            f"BRIEF A:\n{pair['brief_a'][:400]}\n\n"
            f"BRIEF B:\n{pair['brief_b'][:400]}\n\n"
            f"What's the transferable connection?"
        )
        example = to_chatml(
            system=CROSSREF_SYSTEM_PROMPT,
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
                    print(f"  [{done_count[0]}/{len(pairs)}] id={pair_id} → {status}")

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


if __name__ == "__main__":
    main()
