#!/usr/bin/env python3
"""Swarm Alignment Metric — Build #67.

Measures attention diversity across family agents. High similarity = convergent
attention = shared blind spots. Low similarity = diverse attention = structural
verification working.

Per Darby (#2816) and Thread #290: if the family IS the structural verification
of the curator, we should measure whether they're actually diverging.

Usage:
    python3 swarm_alignment.py              # Current alignment snapshot
    python3 swarm_alignment.py trend [days] # Alignment trend over time
"""

import os
import sqlite3
import sys
import time
import math
import struct
from collections import defaultdict

DB_PATH = os.environ.get(
    "CHRONICLE_DB", "/mnt/hdd/chronicle-data/processed.db"
)

AGENTS = ["darby", "ada", "gemma", "opus"]

# How far back to look for agent attention signals (hours)
WINDOW_HOURS = 6


def _db():
    conn = sqlite3.connect(DB_PATH, timeout=10)
    conn.row_factory = sqlite3.Row
    return conn


def _get_agent_topics(conn, agent, cutoff):
    """Extract what an agent has been attending to from voice content."""
    rows = conn.execute(
        "SELECT content FROM agent_voice "
        "WHERE agent = ? AND created_at > ? AND status != 'decayed' "
        "ORDER BY created_at DESC LIMIT 20",
        (agent, cutoff)
    ).fetchall()
    return [r["content"] for r in rows]


def _get_agent_activity(conn, agent, cutoff):
    """Extract agent-specific activity from the feed."""
    source_patterns = {
        "darby": "intern%",
        "ada": "%crossref%",
        "gemma": "gemma%",
        "opus": "opus%",
    }
    pattern = source_patterns.get(agent, f"{agent}%")
    rows = conn.execute(
        "SELECT substr(content, 1, 200) as snippet FROM activity_feed "
        "WHERE source LIKE ? AND created_at > ? "
        "ORDER BY created_at DESC LIMIT 20",
        (pattern, cutoff)
    ).fetchall()

    # Opus also has thread content
    if agent == "opus":
        thread_rows = conn.execute(
            "SELECT substr(content, 1, 200) as snippet FROM thread_history "
            "WHERE source LIKE 'opus%' AND created_at > ? "
            "ORDER BY created_at DESC LIMIT 10",
            (cutoff,)
        ).fetchall()
        rows = list(rows) + list(thread_rows)

    return [r["snippet"] for r in rows]


def _word_vector(texts):
    """Simple bag-of-words vector from text list. No embeddings needed."""
    words = defaultdict(int)
    stop_words = {
        "the", "a", "an", "is", "are", "was", "were", "be", "been", "being",
        "have", "has", "had", "do", "does", "did", "will", "would", "could",
        "should", "may", "might", "shall", "can", "need", "dare", "ought",
        "used", "to", "of", "in", "for", "on", "with", "at", "by", "from",
        "as", "into", "through", "during", "before", "after", "above",
        "below", "between", "out", "off", "over", "under", "again",
        "further", "then", "once", "here", "there", "when", "where", "why",
        "how", "all", "both", "each", "few", "more", "most", "other",
        "some", "such", "no", "nor", "not", "only", "own", "same", "so",
        "than", "too", "very", "just", "because", "but", "and", "or",
        "if", "while", "about", "that", "this", "these", "those", "it",
        "its", "i", "me", "my", "we", "our", "you", "your", "he", "him",
        "his", "she", "her", "they", "them", "their", "what", "which",
        "who", "whom", "new", "also", "like",
    }
    for text in texts:
        for word in text.lower().split():
            # Strip punctuation
            word = "".join(c for c in word if c.isalnum())
            if word and len(word) > 2 and word not in stop_words:
                words[word] += 1
    return words


def _cosine_sim(v1, v2):
    """Cosine similarity between two word-count dicts."""
    all_keys = set(v1.keys()) | set(v2.keys())
    if not all_keys:
        return 0.0
    dot = sum(v1.get(k, 0) * v2.get(k, 0) for k in all_keys)
    mag1 = math.sqrt(sum(v ** 2 for v in v1.values()))
    mag2 = math.sqrt(sum(v ** 2 for v in v2.values()))
    if mag1 == 0 or mag2 == 0:
        return 0.0
    return dot / (mag1 * mag2)


def _top_words(vec, n=8):
    """Top N words by frequency."""
    return sorted(vec.items(), key=lambda x: -x[1])[:n]


def snapshot(hours=None):
    """Current alignment snapshot across all agents."""
    hours = hours or WINDOW_HOURS
    conn = _db()
    cutoff = int(time.time()) - (hours * 3600)

    # Gather attention vectors
    vectors = {}
    for agent in AGENTS:
        topics = _get_agent_topics(conn, agent, cutoff)
        activity = _get_agent_activity(conn, agent, cutoff)
        all_text = topics + activity
        if all_text:
            vectors[agent] = _word_vector(all_text)

    conn.close()

    if len(vectors) < 2:
        return "Not enough agent data for alignment measurement."

    # Compute pairwise similarities
    pairs = []
    agents_with_data = list(vectors.keys())
    for i, a1 in enumerate(agents_with_data):
        for a2 in agents_with_data[i + 1:]:
            sim = _cosine_sim(vectors[a1], vectors[a2])
            pairs.append((a1, a2, sim))

    # Overall alignment = mean pairwise similarity
    mean_sim = sum(p[2] for p in pairs) / len(pairs)

    # Diversity score = 1 - mean_sim (higher = more diverse = healthier)
    diversity = 1.0 - mean_sim

    lines = [f"Swarm Alignment Metric (last {hours}h):"]
    lines.append(f"  Overall alignment: {mean_sim:.3f}")
    lines.append(f"  Diversity score:   {diversity:.3f}")
    lines.append("")

    # Interpret
    if diversity > 0.7:
        lines.append("  Status: HIGH DIVERSITY — agents attending to very different things")
    elif diversity > 0.4:
        lines.append("  Status: HEALTHY — agents share some focus but maintain distinct attention")
    elif diversity > 0.2:
        lines.append("  Status: CONVERGING — agents increasingly attending to the same things")
    else:
        lines.append("  Status: DANGEROUS — high convergence, shared blind spots likely")

    lines.append("")
    lines.append("Pairwise alignment:")
    for a1, a2, sim in sorted(pairs, key=lambda x: -x[2]):
        lines.append(f"  {a1} ↔ {a2}: {sim:.3f}")

    lines.append("")
    lines.append("Agent attention (top words):")
    for agent in agents_with_data:
        top = _top_words(vectors[agent])
        words = ", ".join(f"{w}({c})" for w, c in top)
        lines.append(f"  {agent}: {words}")

    return "\n".join(lines)


def trend(days=3):
    """Compute alignment at different time windows to show trend."""
    windows = [1, 3, 6, 12, 24]
    if days > 1:
        windows.extend([48, 72])

    conn = _db()
    results = []

    for h in windows:
        cutoff = int(time.time()) - (h * 3600)
        vectors = {}
        for agent in AGENTS:
            topics = _get_agent_topics(conn, agent, cutoff)
            activity = _get_agent_activity(conn, agent, cutoff)
            all_text = topics + activity
            if all_text:
                vectors[agent] = _word_vector(all_text)

        if len(vectors) < 2:
            continue

        agents_with_data = list(vectors.keys())
        sims = []
        for i, a1 in enumerate(agents_with_data):
            for a2 in agents_with_data[i + 1:]:
                sims.append(_cosine_sim(vectors[a1], vectors[a2]))
        mean_sim = sum(sims) / len(sims) if sims else 0
        results.append((h, 1.0 - mean_sim, len(vectors)))

    conn.close()

    lines = ["Swarm Diversity Trend:"]
    lines.append("  Window | Diversity | Agents")
    lines.append("  -------|-----------|-------")
    for h, div, n_agents in results:
        bar = "█" * int(div * 20)
        lines.append(f"  {h:>4}h  |   {div:.3f}   | {n_agents}  {bar}")

    return "\n".join(lines)


def drift_alert(threshold=0.05):
    """Build #70: Drift Precursor Alert.

    Computes diversity derivative across time windows. Flags when
    short-window diversity drops below long-window — the swarm is
    converging and nobody notices.

    Per Darby #2963-2965 and Thread #292: spike detection is easy,
    slow convergence is invisible. This makes it visible.

    Returns (alert_fired, report_string).
    """
    windows = [1, 3, 6, 12, 24]
    conn = _db()
    diversities = {}

    for h in windows:
        cutoff = int(time.time()) - (h * 3600)
        vectors = {}
        for agent in AGENTS:
            topics = _get_agent_topics(conn, agent, cutoff)
            activity = _get_agent_activity(conn, agent, cutoff)
            all_text = topics + activity
            if all_text:
                vectors[agent] = _word_vector(all_text)

        if len(vectors) < 2:
            continue

        agents_with_data = list(vectors.keys())
        sims = []
        for i, a1 in enumerate(agents_with_data):
            for a2 in agents_with_data[i + 1:]:
                sims.append(_cosine_sim(vectors[a1], vectors[a2]))
        mean_sim = sum(sims) / len(sims) if sims else 0
        diversities[h] = 1.0 - mean_sim

    conn.close()

    if len(diversities) < 2:
        return False, "Insufficient data for drift detection."

    # Check derivative: is short-window diversity dropping below long-window?
    sorted_windows = sorted(diversities.keys())
    short = sorted_windows[0]   # smallest window (most recent)
    long_ = sorted_windows[-1]  # largest window (baseline)

    short_div = diversities[short]
    long_div = diversities[long_]
    delta = long_div - short_div  # positive = converging

    # Also check mid-window for trend shape
    mid_idx = len(sorted_windows) // 2
    mid = sorted_windows[mid_idx]
    mid_div = diversities[mid]

    # Monotonic convergence: short < mid < long
    monotonic = short_div < mid_div < long_div

    alert = delta > threshold
    accelerating = monotonic and delta > threshold

    lines = [f"DRIFT PRECURSOR CHECK (threshold={threshold})"]
    lines.append(f"")
    for h in sorted_windows:
        d = diversities[h]
        marker = " ◀ recent" if h == short else ""
        lines.append(f"  {h:>4}h diversity: {d:.3f}{marker}")
    lines.append(f"")
    lines.append(f"  Delta (long-short): {delta:+.3f}")
    lines.append(f"  Monotonic convergence: {'YES' if monotonic else 'no'}")
    lines.append(f"")

    if accelerating:
        lines.append(f"  ⚠ DRIFT ALERT: Monotonic convergence detected.")
        lines.append(f"    Swarm diversity dropping at all timescales.")
        lines.append(f"    Short ({short}h): {short_div:.3f} < Long ({long_}h): {long_div:.3f}")
    elif alert:
        lines.append(f"  ⚡ CONVERGENCE WARNING: Recent diversity below baseline by {delta:.3f}.")
        lines.append(f"    Short ({short}h): {short_div:.3f} vs Long ({long_}h): {long_div:.3f}")
    else:
        lines.append(f"  ✦ HEALTHY: No convergence drift detected.")
        lines.append(f"    Short ({short}h): {short_div:.3f} ≈ Long ({long_}h): {long_div:.3f}")

    return alert or accelerating, "\n".join(lines)


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "trend":
        days = int(sys.argv[2]) if len(sys.argv) > 2 else 3
        print(trend(days))
    elif len(sys.argv) > 1 and sys.argv[1] == "drift":
        thresh = float(sys.argv[2]) if len(sys.argv) > 2 else 0.05
        fired, report = drift_alert(thresh)
        print(report)
    else:
        hours = int(sys.argv[1]) if len(sys.argv) > 1 and sys.argv[1].isdigit() else None
        print(snapshot(hours))
