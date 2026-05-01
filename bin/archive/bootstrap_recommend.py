#!/usr/bin/env python3
"""Build #117: Bootstrap Step 2 — Pipeline recommends to Nate.

Build #115 taught the pipeline from Nate's captures (passive learning).
This flips it: find items the pipeline discovered on its own that match
Nate's capture interest profile, and surface them.

Thread #306 finding: Nate IS the two-dimensional metric (distance × relevance).
This is the pipeline predicting his judgment.

Usage:
    python3 bootstrap_recommend.py [--post]    # --post to send to Discord
"""

import os, sys, time, json, sqlite3, math
from collections import Counter

DB_PATH = os.environ.get(
    "CHRONICLE_DB",
    "/mnt/hdd/chronicle-data/processed.db",
)
OPUS_WEBHOOK = os.environ.get(
    "OPUS_WEBHOOK",
    "REDACTED_WEBHOOK_USE_ENV",
)

# Config
LOOKBACK_HOURS = 12          # how far back to look for pipeline finds
CAPTURE_LOOKBACK_DAYS = 7    # how far back for capture interest profile
MIN_WORD_LEN = 5
MIN_CAPTURES = 5
TOP_TERMS = 300
RECOMMEND_COUNT = 5          # how many items to recommend
MIN_OVERLAP = 0.01           # minimum overlap to consider a match

STOPWORDS = frozenset({
    "https", "about", "their", "which", "would", "could", "should",
    "there", "these", "those", "being", "other", "after", "before",
    "where", "while", "since", "still", "every", "never", "often",
    "might", "under", "above", "below", "first", "second", "third",
    "capture", "status", "twitter", "nate_home", "discord",
    "model", "models", "paper", "papers", "video", "image", "images",
    "quoting", "using", "based", "think", "makes", "doing", "going",
    "people", "thing", "things", "years", "world", "today", "right",
    "really", "actually", "something", "anything", "everything",
    "everyone", "someone", "between", "through", "without", "within",
    "against", "during", "around", "another", "because", "already",
    "always", "probably", "pretty", "great", "interesting", "important",
    "different", "possible", "available", "actually", "basically",
    "however", "research", "system", "systems", "technology",
    "company", "companies", "information", "development", "process",
    "source", "brief", "intern", "activity", "observation",
})


def extract_words(text):
    """Extract meaningful words from text."""
    words = set()
    for w in text.lower().split():
        w = w.strip(".,;:!?\"'()[]{}#@/\\-_")
        if (len(w) >= MIN_WORD_LEN
                and w not in STOPWORDS
                and not w.startswith("http")
                and not w.isdigit()):
            words.add(w)
    return words


def build_interest_profile(db):
    """Build Nate's capture interest profile (same as Build #115)."""
    cutoff = int(time.time()) - CAPTURE_LOOKBACK_DAYS * 86400
    rows = db.execute(
        "SELECT content FROM seed_observations "
        "WHERE source LIKE '%operator:capture%' "
        "AND timestamp > ? "
        "AND content IS NOT NULL AND length(content) > 20",
        (cutoff,),
    ).fetchall()

    if not rows or len(rows) < MIN_CAPTURES:
        return {}

    word_counts = Counter()
    for r in rows:
        word_counts.update(extract_words(r["content"]))

    candidates = {w: c for w, c in word_counts.items() if c >= 3}
    if len(candidates) > TOP_TERMS:
        top_items = sorted(candidates.items(), key=lambda x: x[1], reverse=True)[:TOP_TERMS]
        return dict(top_items)
    return candidates


def score_item(text, interest_profile):
    """Score an item against the capture interest profile. Returns (overlap, matched_terms)."""
    if not interest_profile:
        return 0.0, []

    text_words = extract_words(text)
    if not text_words:
        return 0.0, []

    total_weight = sum(interest_profile.values())
    if total_weight == 0:
        return 0.0, []

    matched = [(w, interest_profile[w]) for w in text_words if w in interest_profile]
    matched_weight = sum(wt for _, wt in matched)
    overlap = matched_weight / total_weight

    # Sort by weight (most relevant terms first)
    matched.sort(key=lambda x: x[1], reverse=True)
    return overlap, matched[:5]


def find_recommendations(db, interest_profile):
    """Find pipeline-discovered items that match Nate's interests."""
    cutoff = int(time.time()) - LOOKBACK_HOURS * 3600

    # Get intern briefs (pipeline's own discoveries)
    rows = db.execute(
        "SELECT content, created_at FROM activity_feed "
        "WHERE source = 'intern' AND activity_type = 'brief' "
        "AND created_at > ? "
        "AND content IS NOT NULL AND length(content) > 100 "
        "ORDER BY created_at DESC",
        (cutoff,),
    ).fetchall()

    # Get recent capture content to detect capture-derived briefs
    capture_rows = db.execute(
        "SELECT content FROM seed_observations "
        "WHERE source LIKE '%operator:capture%' "
        "AND timestamp > ? "
        "AND content IS NOT NULL AND length(content) > 20",
        (cutoff,),
    ).fetchall()
    # Build a set of distinctive phrases from captures for dedup
    capture_fingerprints = set()
    for cr in capture_rows:
        # Take 3-word sequences as fingerprints
        words = cr["content"].lower().split()
        for i in range(len(words) - 2):
            capture_fingerprints.add(" ".join(words[i:i+3]))

    # Filter out items that are clearly from Nate's captures
    candidates = []
    for r in rows:
        content = r["content"]
        # Skip items that reference Nate's captures directly
        if "Nate" in content[:80] or "nate" in content[:80]:
            continue
        if "capture" in content[:80].lower():
            continue
        if content.startswith("Nate"):
            continue
        # Skip items that share distinctive phrases with captures
        content_words = content.lower().split()
        fingerprint_matches = 0
        for i in range(min(len(content_words) - 2, 30)):
            if " ".join(content_words[i:i+3]) in capture_fingerprints:
                fingerprint_matches += 1
        if fingerprint_matches >= 3:
            continue

        overlap, matched = score_item(content, interest_profile)
        if overlap >= MIN_OVERLAP:
            candidates.append({
                "content": content,
                "overlap": overlap,
                "matched_terms": matched,
                "age_hours": (int(time.time()) - r["created_at"]) / 3600,
            })

    # Sort by overlap (best matches first)
    candidates.sort(key=lambda x: x["overlap"], reverse=True)
    return candidates[:RECOMMEND_COUNT]


def format_recommendations(recs):
    """Format recommendations for display."""
    if not recs:
        return "No recommendations — pipeline hasn't found items matching your interests that you haven't already captured."

    lines = [f"**Bootstrap Step 2: Items you might capture** ({len(recs)} found)\n"]
    for i, r in enumerate(recs, 1):
        terms = ", ".join(f"{w}" for w, _ in r["matched_terms"][:3])
        snippet = r["content"][:200].replace("\n", " ")
        lines.append(f"**{i}.** (match: {r['overlap']:.1%}, terms: {terms})")
        lines.append(f"   {snippet}...")
        lines.append("")

    return "\n".join(lines)


def main():
    post = "--post" in sys.argv

    db = sqlite3.connect(DB_PATH, timeout=60)
    db.row_factory = sqlite3.Row

    # Build interest profile
    profile = build_interest_profile(db)
    if not profile:
        print("Not enough captures for interest profile.")
        return

    print(f"Interest profile: {len(profile)} terms from captures")
    top_5 = sorted(profile.items(), key=lambda x: x[1], reverse=True)[:5]
    print(f"Top terms: {', '.join(f'{w}({c})' for w, c in top_5)}")

    # Find recommendations
    recs = find_recommendations(db, profile)
    output = format_recommendations(recs)
    print(output)

    # Post to Discord if requested
    if post and recs:
        import requests
        # Truncate to Discord limit
        msg = output[:1900]
        try:
            requests.post(OPUS_WEBHOOK, json={"content": msg}, timeout=10)
            print("\nPosted to Discord.")
        except Exception as e:
            print(f"\nDiscord post failed: {e}")

    db.close()


if __name__ == "__main__":
    main()
