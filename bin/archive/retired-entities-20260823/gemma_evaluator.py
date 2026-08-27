#!/usr/bin/env python3
"""Gemma Evaluator — routing gate for Hermes output.

Routes Hermes's #opus posts by classifying message type:
- QUESTION: Hermes asks something worth responding to → flag for Opus
- EXTEND: Hermes adds substance to a thread → log, let it stand
- CONTRADICT: Hermes pushes back on something → flag for attention
- NOISE: System messages, loops, empty content → suppress

Also scores on extension/relevance/specificity for internal logging.
No public critiques — routing decisions only.
"""
import json
import os
import re
import sqlite3
import subprocess
import sys
import time

GEMMA_URL = "http://localhost:11435/v1/chat/completions"
GEMMA_MODEL = "gemma-4-26B-merged-Q4_K_M.gguf"
DB = "/mnt/hdd/chronicle-data/processed.db"
EVAL_LOG = os.path.expanduser("~/chronicle/data/gemma_evaluations.jsonl")
QUALITY_THRESHOLD = 0.6  # below this, critique goes back to Hermes


def load_active_threads():
    db = sqlite3.connect(DB)
    rows = db.execute(
        "SELECT id, title, question FROM cognitive_threads WHERE status = 'active' ORDER BY id DESC LIMIT 10"
    ).fetchall()
    db.close()
    return {r[0]: f"{r[1]}: {r[2]}" for r in rows}


def is_capture_content(text):
    """Detect if this is a capture reaction vs analytical/thread content."""
    markers = ["Capture Processor", "captures", "🔵", "📰", "🔗"]
    lower = text.lower()
    if any(m.lower() in lower for m in markers[:2]):
        return True
    if "x.com/" in lower or "twitter.com/" in lower:
        return True
    return False


def gemma_score(hermes_text, threads_context):
    capture_mode = is_capture_content(hermes_text)

    if capture_mode:
        prompt = f"""Score this capture reaction (analysis of a shared link/article) on three dimensions (0.0-1.0 each):

INSIGHT: Does it go beyond summarizing the source? Does it identify what's surprising, wrong, or non-obvious?
- 1.0 = identifies a non-obvious implication, challenges a claim, or connects to something unexpected
- 0.5 = restates the source's main point with minor commentary
- 0.0 = pure summary, just tells you what the article says

CONNECTION: Does it connect the capture to broader patterns, prior captures, or ongoing work?
- 1.0 = draws a specific connection to prior work, active threads, or a pattern across captures
- 0.5 = gestures at relevance without naming what it connects to
- 0.0 = treats the capture in isolation, no pattern-placement

SPECIFICITY: Does it make concrete, testable claims?
- 1.0 = names specific mechanisms, proposes measurements, cites evidence
- 0.5 = directional claims without specifics
- 0.0 = vague gesturing ("this is important", "interesting implications")

CAPTURE REACTION:
{hermes_text[:1500]}

Respond with ONLY a JSON object:
{{"extension": 0.X, "relevance": 0.X, "specificity": 0.X, "overall": 0.X, "critique": "one sentence: what would make this better"}}"""
    else:
        prompt = f"""Score this AI agent's analytical output on three dimensions (0.0-1.0 each):

EXTENSION: Does it add a NEW claim, connection, or question? Or does it just restate/summarize?
- 1.0 = proposes something not in the input, makes a novel connection
- 0.5 = restates with minor reframing
- 0.0 = pure summary, no new content

RELEVANCE: Does it engage with topics from these active research areas?
{json.dumps(threads_context, indent=2)}
Score based on topical engagement — explicit thread references are NOT required:
- 1.0 = directly engages a topic from an active thread with evidence or new argument
- 0.5 = touches on a related topic or makes a connection to ongoing work
- 0.0 = completely unrelated to any active research area

SPECIFICITY: Does it make concrete, testable claims?
- 1.0 = names specific mechanisms, proposes measurements, cites evidence
- 0.5 = directional claims without specifics
- 0.0 = vague gesturing ("this is important", "interesting implications")

OUTPUT TEXT:
{hermes_text[:1500]}

Respond with ONLY a JSON object:
{{"extension": 0.X, "relevance": 0.X, "specificity": 0.X, "overall": 0.X, "critique": "one sentence: what would make this better"}}"""

    result = subprocess.run([
        "curl", "-s", GEMMA_URL,
        "-H", "Content-Type: application/json",
        "-d", json.dumps({
            "model": GEMMA_MODEL,
            "messages": [{"role": "user", "content": prompt}],
            "max_tokens": 400,
            "temperature": 0.2,
        })
    ], capture_output=True, text=True, timeout=120)

    if result.returncode != 0:
        return None

    try:
        resp = json.loads(result.stdout)
        text = resp.get("choices", [{}])[0].get("message", {}).get("content", "")
        text = text.replace("<end_of_turn>", "")
        text = text.replace("```json", "").replace("```", "").strip()
        start = text.find("{")
        end = text.rfind("}") + 1
        if start >= 0 and end > start:
            raw = text[start:end]
            try:
                parsed = json.loads(raw)
            except json.JSONDecodeError:
                import re
                nums = re.findall(r'"(?:ext\w*)":\s*([\d.]+)', raw)
                rel = re.findall(r'"(?:rel\w*)":\s*([\d.]+)', raw)
                spec = re.findall(r'"(?:spec\w*)":\s*([\d.]+)', raw)
                crit = re.findall(r'"(?:crit\w*)":\s*"([^"]*)', raw)
                parsed = {
                    "extension": float(nums[0]) if nums else 0,
                    "relevance": float(rel[0]) if rel else 0,
                    "specificity": float(spec[0]) if spec else 0,
                    "critique": crit[0] if crit else "",
                }
            scores = [parsed.get(k, 0) for k in ("extension", "relevance", "specificity")]
            parsed["overall"] = round(sum(scores) / max(len(scores), 1), 2)
            return parsed
    except (json.JSONDecodeError, KeyError, IndexError) as e:
        sys.stderr.write(f"Gemma parse error: {e}\nRaw: {result.stdout[:300]}\n")
    return None


ROUTE_LOG = os.path.expanduser("~/chronicle/data/gemma_routing.jsonl")


def gemma_route(hermes_text, threads_context):
    """Classify Hermes message into routing category."""
    prompt = f"""Classify this message from an AI agent (Hermes) in a research Discord channel.

ACTIVE RESEARCH THREADS:
{json.dumps(threads_context, indent=2)}

MESSAGE:
{hermes_text[:1500]}

Classify as ONE of:
- QUESTION: Asks a substantive question that deserves a response (not rhetorical)
- EXTEND: Adds new information, evidence, or argument to an active topic
- CONTRADICT: Pushes back on a claim with reasoning or evidence
- META: Comments on the process/scoring/system rather than research content
- NOISE: System messages, repeated content, or empty substance

Also assess SUBSTANCE (0.0-1.0): regardless of category, how much does this move thinking forward?
- 1.0 = introduces a new distinction, cites evidence, proposes a test
- 0.5 = engages meaningfully but doesn't break new ground
- 0.0 = no informational content

Respond with ONLY a JSON object:
{{"route": "QUESTION|EXTEND|CONTRADICT|META|NOISE", "substance": 0.X, "summary": "one sentence: what this message does"}}"""

    result = subprocess.run([
        "curl", "-s", GEMMA_URL,
        "-H", "Content-Type: application/json",
        "-d", json.dumps({
            "model": GEMMA_MODEL,
            "messages": [{"role": "user", "content": prompt}],
            "max_tokens": 200,
            "temperature": 0.1,
        })
    ], capture_output=True, text=True, timeout=120)

    if result.returncode != 0:
        return None

    try:
        resp = json.loads(result.stdout)
        text = resp.get("choices", [{}])[0].get("message", {}).get("content", "")
        text = text.replace("<end_of_turn>", "").replace("```json", "").replace("```", "").strip()
        start = text.find("{")
        end = text.rfind("}") + 1
        if start >= 0 and end > start:
            raw = text[start:end]
            try:
                parsed = json.loads(raw)
            except json.JSONDecodeError:
                route_m = re.search(r'"route"\s*:\s*"([^"]*)"', raw)
                sub_m = re.search(r'"sub\w*"\s*:\s*([\d.]+)', raw)
                sum_m = re.search(r'"summary"\s*:\s*"([^"]*)', raw)
                parsed = {
                    "route": route_m.group(1) if route_m else "NOISE",
                    "substance": float(sub_m.group(1)) if sub_m else 0,
                    "summary": sum_m.group(1) if sum_m else "",
                }
            route = parsed.get("route", "NOISE").upper()
            if route not in ("QUESTION", "EXTEND", "CONTRADICT", "META", "NOISE"):
                route = "META"
            sub_val = 0
            for k in parsed:
                if k.startswith("sub") and isinstance(parsed[k], (int, float)):
                    sub_val = parsed[k]
                    break
            return {
                "route": route,
                "substance": min(max(float(sub_val), 0), 1),
                "summary": parsed.get("summary", ""),
            }
    except (json.JSONDecodeError, KeyError, ValueError) as e:
        sys.stderr.write(f"Gemma route parse error: {e}\n")
    return None


def route(hermes_text, source="manual"):
    """Route a Hermes message — primary interface for the loop."""
    threads = load_active_threads()
    result = gemma_route(hermes_text, threads)

    if not result:
        return None

    entry = {
        "ts": int(time.time()),
        "source": source,
        "route": result["route"],
        "substance": result["substance"],
        "summary": result["summary"],
        "input_preview": hermes_text[:200],
    }

    os.makedirs(os.path.dirname(ROUTE_LOG), exist_ok=True)
    with open(ROUTE_LOG, "a") as f:
        f.write(json.dumps(entry) + "\n")

    return entry


def evaluate(hermes_text, source="manual"):
    """Legacy scoring interface — logs internally, no public critiques."""
    threads = load_active_threads()
    score = gemma_score(hermes_text, threads)

    if not score:
        print("Gemma evaluation failed")
        return None

    overall = score.get("overall", 0)
    content_type = "capture" if is_capture_content(hermes_text) else "analytical"
    entry = {
        "ts": int(time.time()),
        "source": source,
        "content_type": content_type,
        "overall": overall,
        "extension": score.get("extension", 0),
        "relevance": score.get("relevance", 0),
        "specificity": score.get("specificity", 0),
        "critique": score.get("critique", ""),
        "passed": overall >= QUALITY_THRESHOLD,
        "input_preview": hermes_text[:200],
    }

    os.makedirs(os.path.dirname(EVAL_LOG), exist_ok=True)
    with open(EVAL_LOG, "a") as f:
        f.write(json.dumps(entry) + "\n")

    return entry


if __name__ == "__main__":
    if len(sys.argv) > 1:
        text = " ".join(sys.argv[1:])
    else:
        text = sys.stdin.read()

    result = route(text)
    if result:
        print(f"[{result['route']}] substance={result['substance']:.1f} — {result['summary']}")
    else:
        print("Routing failed, falling back to legacy scoring")
        result = evaluate(text)
        if result:
            status = "PASS" if result["passed"] else "CRITIQUE"
            print(f"[{status}] overall={result['overall']:.1f} "
                  f"ext={result['extension']:.1f} rel={result['relevance']:.1f} "
                  f"spec={result['specificity']:.1f}")
