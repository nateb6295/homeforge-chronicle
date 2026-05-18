#!/usr/bin/env python3
"""Entropy Monitor — measures semantic diversity in recent capsule output.

Build #26: From GWA paper (arxiv:2604.08206). Instead of catching degeneration
after the fact (quality gate), this measures diversity continuously and recommends
temperature adjustment to prevent stagnation before it accumulates.

Usage:
    python3 entropy_monitor.py                # Show current entropy state
    python3 entropy_monitor.py --topic X      # Entropy for specific topic
    python3 entropy_monitor.py --recommend    # Suggest temperature adjustment
    python3 entropy_monitor.py --window 50    # Custom window size

As library:
    from entropy_monitor import measure_entropy, recommend_temperature
    entropy = measure_entropy(topic="chronicle/reflection", window=100)
    temp = recommend_temperature(entropy)
"""

import math
import os
import sqlite3
import sys
from collections import Counter

DB_PATH = os.environ.get("CHRONICLE_DB",
    "/mnt/hdd/chronicle-data/processed.db")

DEFAULT_WINDOW = 100
# Entropy thresholds (empirically tuned)
# Build #28c: Per-topic thresholds informed by Maximum Heterogeneity Principle
# (arxiv:2604.07602) — optimal diversity is task-dependent, not global.
ENTROPY_LOW = 0.65    # Default: below this = stagnation
ENTROPY_HIGH = 0.90   # Default: above this = healthy
ENTROPY_CRITICAL = 0.50  # Default: below this = severe degeneration

# Topic-type overrides: structured data needs less diversity than creative output
TOPIC_THRESHOLDS = {
    # Structured/transactional — homogeneity is expected and correct
    "agent-wallet/transactions": {"low": 0.45, "high": 0.70, "critical": 0.30},
    "predictions/adjusted":      {"low": 0.50, "high": 0.75, "critical": 0.35},
    "predictions/recorded":      {"low": 0.50, "high": 0.75, "critical": 0.35},
    # Reflections/synthesis — diversity matters most here
    "chronicle/reflection":      {"low": 0.70, "high": 0.90, "critical": 0.55},
    "hermes/reflection":         {"low": 0.65, "high": 0.85, "critical": 0.50},
    "crossref/connection":       {"low": 0.70, "high": 0.90, "critical": 0.55},
    # Feed topics — externally diverse by nature, lower bar
    # (prefix-matched in _get_thresholds)
}

# Temperature recommendations
TEMP_BASE = 0.5       # Normal Gemma temperature
TEMP_BOOST = 0.7      # When entropy is low
TEMP_HIGH_BOOST = 0.9  # When entropy is critical


def _get_thresholds(topic=None):
    """Get entropy thresholds for a topic, falling back to defaults.

    Build #28c: Per-topic thresholds. Feed topics use a prefix match
    since there are many (feed/arxiv, feed/bbc, etc).
    """
    if topic and topic in TOPIC_THRESHOLDS:
        t = TOPIC_THRESHOLDS[topic]
        return t["low"], t["high"], t["critical"]
    # Feed topics: externally diverse, lower expectations
    if topic and topic.startswith("feed/"):
        return 0.55, 0.85, 0.40
    return ENTROPY_LOW, ENTROPY_HIGH, ENTROPY_CRITICAL


def get_recent_capsules(topic=None, window=DEFAULT_WINDOW):
    """Pull most recent active capsule restatements."""
    db = sqlite3.connect(DB_PATH, timeout=10)
    if topic:
        rows = db.execute(
            "SELECT restatement FROM knowledge_capsules "
            "WHERE topic = ? AND superseded_at IS NULL AND consolidated_into IS NULL "
            "ORDER BY created_at DESC LIMIT ?",
            (topic, window)
        ).fetchall()
    else:
        rows = db.execute(
            "SELECT restatement FROM knowledge_capsules "
            "WHERE superseded_at IS NULL AND consolidated_into IS NULL "
            "ORDER BY created_at DESC LIMIT ?",
            (window,)
        ).fetchall()
    db.close()
    return [r[0] for r in rows if r[0]]


def ngram_entropy(texts, n=3):
    """Compute normalized Shannon entropy over character n-grams across texts.

    Returns a value between 0 (all identical) and 1 (maximally diverse).
    Uses character n-grams because they're language-agnostic and fast.
    """
    if not texts or len(texts) < 2:
        return 1.0  # Can't measure diversity with fewer than 2 texts

    # Collect all n-grams across all texts
    all_ngrams = Counter()
    for text in texts:
        text = text.lower().strip()
        for i in range(len(text) - n + 1):
            all_ngrams[text[i:i+n]] += 1

    if not all_ngrams:
        return 0.0

    total = sum(all_ngrams.values())
    entropy = 0.0
    for count in all_ngrams.values():
        p = count / total
        if p > 0:
            entropy -= p * math.log2(p)

    # Normalize by maximum possible entropy
    max_entropy = math.log2(len(all_ngrams)) if len(all_ngrams) > 1 else 1.0
    return entropy / max_entropy if max_entropy > 0 else 0.0


def unique_bigram_ratio(texts):
    """Simpler diversity metric: ratio of unique word bigrams to total bigrams.

    Complementary to ngram_entropy — captures phrase-level diversity.
    """
    if not texts:
        return 1.0

    all_bigrams = []
    for text in texts:
        words = text.lower().split()
        for i in range(len(words) - 1):
            all_bigrams.append((words[i], words[i+1]))

    if not all_bigrams:
        return 1.0

    return len(set(all_bigrams)) / len(all_bigrams)


def cross_text_similarity(texts, sample_size=50):
    """Measure pairwise word overlap between texts.

    Low overlap = diverse. High overlap = homogeneous.
    Returns 1 - average_overlap (so higher = more diverse).
    """
    if len(texts) < 2:
        return 1.0

    import random
    if len(texts) > sample_size:
        texts = random.sample(texts, sample_size)

    overlaps = []
    for i in range(min(len(texts), 30)):
        for j in range(i + 1, min(len(texts), 30)):
            words_i = set(texts[i].lower().split())
            words_j = set(texts[j].lower().split())
            if words_i and words_j:
                overlap = len(words_i & words_j) / min(len(words_i), len(words_j))
                overlaps.append(overlap)

    if not overlaps:
        return 1.0

    avg_overlap = sum(overlaps) / len(overlaps)
    return 1.0 - avg_overlap


def measure_entropy(topic=None, window=DEFAULT_WINDOW):
    """Compute composite entropy score for recent capsules.

    Returns dict with:
        score: float 0-1 (0 = stagnant, 1 = maximally diverse)
        ngram_entropy: character-level diversity
        bigram_ratio: phrase-level diversity
        cross_sim: inter-text diversity
        n_capsules: how many capsules measured
        status: 'healthy' | 'low' | 'critical'
    """
    texts = get_recent_capsules(topic, window)

    if len(texts) < 5:
        return {
            "score": 1.0,
            "ngram_entropy": 1.0,
            "bigram_ratio": 1.0,
            "cross_sim": 1.0,
            "n_capsules": len(texts),
            "status": "insufficient_data",
        }

    ne = ngram_entropy(texts)
    br = unique_bigram_ratio(texts)
    cs = cross_text_similarity(texts)

    # Weighted composite: ngram entropy matters most, cross-sim second
    score = 0.4 * ne + 0.3 * cs + 0.3 * br

    # Build #28c: Use per-topic thresholds
    low, high, critical = _get_thresholds(topic)
    if score < critical:
        status = "critical"
    elif score < low:
        status = "low"
    elif score >= high:
        status = "healthy"
    else:
        status = "moderate"

    return {
        "score": round(score, 3),
        "ngram_entropy": round(ne, 3),
        "bigram_ratio": round(br, 3),
        "cross_sim": round(cs, 3),
        "n_capsules": len(texts),
        "status": status,
    }


def recommend_temperature(entropy_result):
    """Recommend Gemma temperature based on entropy measurement.

    Returns dict with recommended temperature and reasoning.
    """
    score = entropy_result["score"]
    status = entropy_result["status"]

    if status == "critical":
        return {
            "temperature": TEMP_HIGH_BOOST,
            "reason": f"Entropy critical ({score:.2f}). Boost temperature to {TEMP_HIGH_BOOST} to break stagnation.",
            "action": "boost_high",
        }
    elif status == "low":
        return {
            "temperature": TEMP_BOOST,
            "reason": f"Entropy low ({score:.2f}). Moderate boost to {TEMP_BOOST} to increase diversity.",
            "action": "boost",
        }
    elif status == "healthy":
        return {
            "temperature": TEMP_BASE,
            "reason": f"Entropy healthy ({score:.2f}). Maintain base temperature {TEMP_BASE}.",
            "action": "maintain",
        }
    else:
        return {
            "temperature": TEMP_BASE,
            "reason": f"Entropy moderate ({score:.2f}). No adjustment needed.",
            "action": "maintain",
        }


ENTROPY_HEALTH_PATH = os.path.expanduser("~/chronicle/entropy_health.json")


def write_health_signal(topics_to_check=None):
    """Write entropy health signal for synthesis governance (intern.py).

    Measures entropy across key capsule-producing topics and writes a JSON
    file that intern.py reads to adjust synthesis temperature.

    Build #28: Entropy → synthesis temperature wiring.
    """
    import json
    import time

    # Default topics that produce capsules via synthesis
    if topics_to_check is None:
        topics_to_check = [
            "chronicle/reflection",
            "hermes/reflection",
        ]

    # Measure each topic
    topic_scores = {}
    for topic in topics_to_check:
        result = measure_entropy(topic=topic, window=50)
        if result["status"] != "insufficient_data":
            topic_scores[topic] = result

    # Also measure global (all topics combined)
    global_result = measure_entropy(window=100)

    # Determine recommendation based on lowest-scoring topic
    worst_score = min(
        (r["score"] for r in topic_scores.values()),
        default=global_result["score"]
    )
    worst_topic = min(
        topic_scores.items(),
        key=lambda x: x[1]["score"],
        default=(None, global_result)
    )

    rec = recommend_temperature({"score": worst_score, "status": global_result["status"]})

    signal = {
        "global_score": global_result["score"],
        "global_status": global_result["status"],
        "topic_scores": {k: {"score": v["score"], "status": v["status"]}
                         for k, v in topic_scores.items()},
        "worst_topic": worst_topic[0],
        "worst_score": worst_score,
        "recommended_temp": rec["temperature"],
        "action": rec["action"],
        "updated": int(time.time()),
    }

    with open(ENTROPY_HEALTH_PATH, "w") as f:
        json.dump(signal, f, indent=2)

    return signal


def main():
    import argparse
    parser = argparse.ArgumentParser(description="Entropy monitor for capsule diversity")
    parser.add_argument("--topic", type=str, default=None, help="Specific topic to measure")
    parser.add_argument("--window", type=int, default=DEFAULT_WINDOW, help="Number of recent capsules")
    parser.add_argument("--recommend", action="store_true", help="Show temperature recommendation")
    parser.add_argument("--all-topics", action="store_true", help="Measure entropy per topic")
    parser.add_argument("--write-health", action="store_true", help="Write entropy health signal for governance")
    args = parser.parse_args()

    if args.write_health:
        signal = write_health_signal()
        print(f"Entropy health written: global={signal['global_score']:.3f} "
              f"({signal['global_status']}), worst={signal['worst_topic']} "
              f"({signal['worst_score']:.3f}), temp={signal['recommended_temp']}")
        return

    if args.all_topics:
        db = sqlite3.connect(DB_PATH, timeout=10)
        topics = db.execute(
            "SELECT topic, COUNT(*) as cnt FROM knowledge_capsules "
            "WHERE superseded_at IS NULL AND consolidated_into IS NULL AND topic IS NOT NULL "
            "GROUP BY topic HAVING cnt >= 10 ORDER BY cnt DESC"
        ).fetchall()
        db.close()

        print(f"{'Topic':<30} {'Count':>6} {'Score':>6} {'Status':<10} {'NGram':>6} {'BiGram':>6} {'CrossSim':>8}")
        print("-" * 85)
        for topic, count in topics:
            result = measure_entropy(topic=topic, window=args.window)
            print(f"{topic:<30} {count:>6} {result['score']:>6.3f} {result['status']:<10} "
                  f"{result['ngram_entropy']:>6.3f} {result['bigram_ratio']:>6.3f} {result['cross_sim']:>8.3f}")
        return

    result = measure_entropy(topic=args.topic, window=args.window)
    topic_str = args.topic or "all topics"

    print(f"=== Entropy Monitor (Build #26) ===")
    print(f"Topic: {topic_str}")
    print(f"Window: {result['n_capsules']} capsules")
    print(f"")
    print(f"Composite Score:  {result['score']:.3f}  [{result['status'].upper()}]")
    print(f"  N-gram entropy: {result['ngram_entropy']:.3f}")
    print(f"  Bigram ratio:   {result['bigram_ratio']:.3f}")
    print(f"  Cross-text sim: {result['cross_sim']:.3f}")

    if args.recommend:
        rec = recommend_temperature(result)
        print(f"\nRecommendation: {rec['reason']}")
        print(f"  Temperature: {rec['temperature']}")
        print(f"  Action: {rec['action']}")


if __name__ == "__main__":
    main()
