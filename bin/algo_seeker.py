#!/usr/bin/env python3
"""Algo Seeker — Chronicle's own recommendation algorithm.

Nate asked: "How do we crack an algo or even add one?"

This IS the algo. Instead of passively waiting for RSS feeds, it:
1. Analyzes Nate's capture patterns — who he reads, what themes recur
2. Actively searches for content from those sources and intersections
3. Injects discoveries into the pipeline as seeker:algo items

The captures ARE the training signal. Every link Nate saves teaches
this system what to look for next.

Usage:
    python3 algo_seeker.py              # Run one seek cycle
    python3 algo_seeker.py authors      # Show top captured authors
    python3 algo_seeker.py themes       # Show current capture themes
    python3 algo_seeker.py dry          # Seek but don't inject
"""

import json
import os
import re
import sqlite3
import sys
import time
from collections import Counter
from datetime import datetime

DB_PATH = os.environ.get("CHRONICLE_DB",
    "/mnt/hdd/chronicle-data/processed.db")

STATE_PATH = os.path.expanduser("~/chronicle/algo_seeker_state.json")

# SearXNG on Jetson
SEARXNG_URL = "http://192.168.1.11:8080/search"

# X API
X_BEARER_TOKEN = os.environ.get("X_BEARER_TOKEN", "")
X_API_BASE = "https://api.x.com/2"

# How far back to analyze captures
AUTHOR_LOOKBACK_DAYS = 7
THEME_LOOKBACK_HOURS = 48

# Max items to inject per cycle
MAX_INJECT = 5

# Discord webhook for reporting — read from env, never hardcode
OPUS_WEBHOOK = os.environ.get("OPUS_WEBHOOK", "")


def _db():
    return sqlite3.connect(DB_PATH, timeout=10)


def _load_state():
    if os.path.exists(STATE_PATH):
        try:
            with open(STATE_PATH) as f:
                return json.load(f)
        except Exception:
            pass
    return {"last_run": 0, "injected_urls": [], "seek_count": 0}


def _save_state(state):
    # Keep injected_urls bounded
    state["injected_urls"] = state["injected_urls"][-200:]
    with open(STATE_PATH, "w") as f:
        json.dump(state, f, indent=2)


def web_search(query, max_results=5):
    """Search via SearXNG with DDG fallback."""
    import requests
    # SearXNG
    try:
        resp = requests.get(SEARXNG_URL, params={
            "q": query, "format": "json",
            "engines": "google,duckduckgo,brave",
        }, timeout=12)
        resp.raise_for_status()
        data = resp.json()
        results = [{"title": r["title"], "url": r["url"],
                     "body": r.get("content", "")}
                   for r in data.get("results", [])[:max_results]]
        if results:
            return results
    except Exception:
        pass
    # DDG fallback
    try:
        try:
            from ddgs import DDGS
        except ImportError:
            from duckduckgo_search import DDGS
        with DDGS() as ddgs:
            results = list(ddgs.text(query, max_results=max_results))
            return [{"title": r["title"], "url": r["href"],
                     "body": r["body"]} for r in results]
    except Exception:
        return []


def _x_headers():
    return {"Authorization": f"Bearer {X_BEARER_TOKEN}"}


def x_user_id(username):
    """Resolve X username to user ID (cached in state)."""
    import requests
    state = _load_state()
    cache = state.get("x_user_ids", {})
    if username in cache:
        return cache[username]
    try:
        resp = requests.get(
            f"{X_API_BASE}/users/by/username/{username}",
            headers=_x_headers(), timeout=10)
        if resp.status_code == 200:
            uid = resp.json()["data"]["id"]
            cache[username] = uid
            state["x_user_ids"] = cache
            _save_state(state)
            return uid
    except Exception:
        pass
    return None


def x_author_tweets(username, max_results=10):
    """Fetch recent tweets from an X author. Returns list of tweet dicts."""
    import requests
    uid = x_user_id(username)
    if not uid:
        return []
    try:
        resp = requests.get(
            f"{X_API_BASE}/users/{uid}/tweets",
            params={
                "max_results": max(10, min(max_results, 100)),
                "tweet.fields": "created_at,text,public_metrics",
                "exclude": "retweets,replies",
            },
            headers=_x_headers(), timeout=10)
        if resp.status_code == 200:
            tweets = resp.json().get("data", [])
            results = []
            for t in tweets:
                # Extract URLs from tweet text
                urls = re.findall(r'https?://\S+', t.get("text", ""))
                results.append({
                    "tweet_id": t["id"],
                    "text": t.get("text", ""),
                    "created_at": t.get("created_at", ""),
                    "urls": urls,
                    "metrics": t.get("public_metrics", {}),
                    "author": username,
                })
            return results
        elif resp.status_code == 429:
            print(f"  X API rate limited for @{username}")
    except Exception as e:
        print(f"  X API error for @{username}: {e}")
    return []


def top_authors(days=AUTHOR_LOOKBACK_DAYS, limit=15):
    """Find most-captured Twitter/X accounts."""
    db = _db()
    cutoff = int(time.time()) - (days * 86400)
    rows = db.execute(
        "SELECT content FROM activity_feed "
        "WHERE (source LIKE '%capture%') "
        "AND content LIKE '%x.com/%' "
        "AND created_at > ? ",
        (cutoff,)
    ).fetchall()
    db.close()

    author_counts = Counter()
    for (content,) in rows:
        # Extract username from x.com/USERNAME/status/...
        matches = re.findall(r'x\.com/([a-zA-Z0-9_]+)/status/', content)
        for m in matches:
            if m.lower() not in ("i", "x", "home"):
                author_counts[m.lower()] += 1

    return author_counts.most_common(limit)


def capture_themes(hours=THEME_LOOKBACK_HOURS):
    """Extract topic clusters from recent capture briefs."""
    db = _db()
    cutoff = int(time.time()) - (hours * 3600)

    # Get capture descriptions (operator:capture) and Hermes capture briefs
    rows = db.execute(
        "SELECT content FROM activity_feed "
        "WHERE (source='operator:capture' OR source='hermes:capture' "
        "       OR (source='intern' AND activity_type='brief')) "
        "AND created_at > ? "
        "ORDER BY created_at DESC LIMIT 40",
        (cutoff,)
    ).fetchall()
    db.close()

    if not rows:
        return []

    # Extract significant multi-word phrases and single terms
    stopwords = {
        "about", "after", "against", "also", "been", "before", "being",
        "between", "both", "could", "different", "during", "each",
        "every", "first", "from", "have", "here", "into", "just",
        "like", "long", "many", "more", "most", "much", "must",
        "only", "other", "over", "same", "should", "since", "some",
        "such", "than", "that", "their", "them", "then", "there",
        "these", "they", "this", "those", "through", "under", "very",
        "want", "well", "were", "what", "when", "where", "which",
        "while", "will", "with", "would", "your", "article", "brief",
        "system", "systems", "approach", "using", "based", "however",
        "specific", "within", "rather", "suggests", "according",
        "provides", "including", "potential", "across", "toward",
        "without", "research", "researchers", "study",
        # URL/meta noise from capture descriptions
        "https", "status", "quoting", "shorturl", "tweet", "twitter",
        "discord", "capture", "message", "posted", "source", "articles",
        "thread", "already", "medium", "comprehensive", "associated",
        "identified", "conducted",
    }

    word_counts = Counter()
    bigram_counts = Counter()

    for (content,) in rows:
        words = [w.lower() for w in re.findall(r'[a-zA-Z]{5,}', content)]
        sig_words = [w for w in words if w not in stopwords]
        word_counts.update(set(sig_words))

        # Bigrams from significant words
        for i in range(len(sig_words) - 1):
            bigram_counts[(sig_words[i], sig_words[i+1])] += 1

    # Themes = words appearing in 3+ briefs, or bigrams in 2+
    themes = []
    for word, count in word_counts.most_common(30):
        if count >= 3:
            themes.append({"term": word, "count": count, "type": "word"})

    for (w1, w2), count in bigram_counts.most_common(15):
        if count >= 2:
            themes.append({"term": f"{w1} {w2}", "count": count, "type": "bigram"})

    return themes


def _already_seen(url, state, db):
    """Check if we've already injected or briefed this URL."""
    if url in state.get("injected_urls", []):
        return True
    # Check activity_feed
    existing = db.execute(
        "SELECT 1 FROM activity_feed WHERE content LIKE ? LIMIT 1",
        (f"%{url[:80]}%",)
    ).fetchone()
    if existing:
        return True
    # Check feed_articles (id is the URL)
    existing = db.execute(
        "SELECT 1 FROM feed_articles WHERE id LIKE ? LIMIT 1",
        (f"%{url[:80]}%",)
    ).fetchone()
    return bool(existing)


def capture_topics(hours=24):
    """Extract specific topics from raw capture URLs/text, not briefs.
    More specific than theme extraction from processed briefs."""
    db = _db()
    cutoff = int(time.time()) - (hours * 3600)
    rows = db.execute(
        "SELECT content FROM activity_feed "
        "WHERE source LIKE '%capture%' "
        "AND created_at > ? "
        "ORDER BY created_at DESC LIMIT 30",
        (cutoff,)
    ).fetchall()
    db.close()

    if not rows:
        return []

    # Extract the tweet text portion (after the URL)
    topics = Counter()
    # Domain-specific terms that signal real interest (not stopwords)
    # NOTE: Generic crypto/finance terms removed — they attract spam when mixed
    # into author queries. Specific tokens (XRP, Flare, ICP) kept as signal.
    domain_terms = {
        "neural", "brain", "consciousness", "cognition", "embodied",
        "agent", "autonomous", "sovereignty", "decentralized", "infrastructure",
        "model", "training", "inference", "fine-tuning", "distillation",
        "biology", "genetic", "protein", "evolution", "organism",
        "prediction", "forecast", "signal", "sensor", "feedback",
        "security", "privacy", "encryption", "surveillance",
        "open-source", "local-first", "self-hosted", "homeforge",
        "scaffold", "transfer", "mechanism", "architecture",
        "gemma", "llama", "claude", "openai", "anthropic",
        "xrp", "flare", "icp", "dfinity", "ripple",
    }

    for (content,) in rows:
        words = set(w.lower() for w in re.findall(r'[a-zA-Z]{4,}', content))
        relevant = words & domain_terms
        topics.update(relevant)

    return topics.most_common(20)


def _build_search_queries(authors, themes):
    """Build diverse search queries from captures analysis."""
    queries = []
    import random as _rnd

    # Get specific capture topics
    cap_topics = capture_topics()
    topic_words = [t[0] for t in cap_topics[:10]]

    # Author-based: search for their NAME + topics (not their Twitter URL)
    # We want articles, papers, talks BY these people — not their tweets
    author_name_map = {
        "umbertoleon": "Umberto Leon Dominguez neuroscience",
        "medical_xpress": "medical research breakthrough",
        "karpathy": "Andrej Karpathy AI",
        "decisionneurop": "decision neuroscience",
        "deanwball": "Dean Ball AI policy regulation",
        "fly51fly": "machine learning research paper",
        "viemccoy": "viemccoy philosophy language cognition",
        "pierresamaties": "Pierre Samaties research",
        "biologyaidaily": "biology AI daily research",
        "sauers_": "sauers AI research",
        "leothecurious": "leothecurious systems thinking",
        "dr_singularity": "Dr Singularity science breakthrough",
        "d33v33d0": "Martin DeVido Verdant Autonomics AI robotics",
        "emollick": "Ethan Mollick AI education future of work",
        "jayalammar": "Jay Alammar AI visualization transformer",
        "intuitmachine": "Carlos Perez intuition machine deep learning",
        "josephjacks_": "Joseph Jacks open source AI infrastructure",
        "nousresearch": "Nous Research open source LLM fine-tuning",
        "fireandvision": "fire and vision neuroscience consciousness",
        "raemon777": "Raemon rationality AI alignment",
        "sciencecorp_": "Science Corp neurotech brain-computer interface",
        "daniellefong": "Danielle Fong energy science",
        "celestediwind": "Celeste Di Wind research",
        "bravo_abad": "Jorge Bravo Abad AI biochemistry materials science optimization",
        # Build #28b: Added from top capture authors to prevent noise
        "lilithdatura": "Lilith Datura AI consciousness recursion persistence",
        "networkspapers": "network science synchronization topology paper",
        "statmlpapers": "statistical machine learning methods paper",
        "repligate": "janus AI alignment capabilities simulacra",
        "elder_plinius": "Pliny AI red-teaming jailbreaking safety",
        "millerlabmit": "Earl Miller MIT neuroscience oscillations top-down",
        "scitechera": "science technology quantum computing breakthrough",
        "kpaxs": "creativity neuroscience philosophy memory cognition",
        "lu_sichu": "Sichu Lu AI research",
    }
    for author, count in authors[:10]:
        if count >= 3:
            name_query = author_name_map.get(author, author)
            # Mix author with a capture topic for cross-pollination
            topic = _rnd.choice(topic_words) if topic_words else ""
            queries.append({
                "query": f"{name_query} {topic} 2026",
                "reason": f"author @{author} ({count} captures) × {topic}",
                "type": "author",
            })

    # Topic intersection queries: pair capture topics with each other
    if len(topic_words) >= 2:
        pairs_tried = set()
        for _ in range(4):
            t1, t2 = _rnd.sample(topic_words[:8], 2)
            pair_key = tuple(sorted([t1, t2]))
            if pair_key not in pairs_tried:
                pairs_tried.add(pair_key)
                queries.append({
                    "query": f"{t1} {t2} research paper 2026",
                    "reason": f"topic intersection: {t1} × {t2}",
                    "type": "topic",
                })

    # Cross-domain intersection queries (biology ↔ network is Nate's thesis)
    intersections = [
        ("biological mechanism network architecture transfer", "bio→network"),
        ("decentralized autonomous agent local infrastructure", "sovereignty-AI"),
        ("embodied cognition computational neuroscience prediction", "embodied-mind"),
        ("self-distillation model improvement autonomous", "self-improving systems"),
        ("protein folding network topology pattern", "bio→network protein"),
        ("wearable sensor signal processing noise filtering", "sensor-grade"),
        ("knowledge distillation model compression inference", "distillation"),
        # Deep evolution topics — richer signal for self-improving architecture
        ("mechanistic interpretability sparse autoencoder activation steering", "interp-mech"),
        ("AI model welfare moral patienthood consciousness assessment", "model-welfare"),
        ("alignment evaluation behavioral audit deception detection", "alignment-empirics"),
        ("computational psychiatry salience gating predictive processing pathology", "comp-psych"),
        ("representation engineering activation vector emotion behavior", "rep-engineering"),
        ("scaffold autonomy persistent memory agent identity continuity", "scaffold-identity"),
        ("corollary discharge efference copy self-monitoring neural", "corollary-discharge"),
        ("psychodynamic assessment AI personality organization self-model", "AI-psychodynamics"),
    ]
    # Pick 3 random intersection queries per run (variety)
    for q, reason in _rnd.sample(intersections, min(3, len(intersections))):
        queries.append({
            "query": f"{q} 2026",
            "reason": reason,
            "type": "intersection",
        })

    # Feed gap queries: topics Nate captures about but feeds miss
    gap_terms = ["biology neuroscience mechanism", "embodied cognition robotics",
                 "sensor wearable signal", "protein structure network",
                 "knowledge distillation transfer learning",
                 "interpretability sparse autoencoder features",
                 "AI welfare sentience moral consideration",
                 "computational psychiatry prediction error",
                 "activation steering emotion vector behavior",
                 "long-form AI philosophy dialogue consciousness"]
    gap_q = _rnd.choice(gap_terms)
    queries.append({
        "query": f"{gap_q} research breakthrough 2026",
        "reason": "feed gap coverage",
        "type": "gap",
    })

    # Build #145: Counter-thesis seeker — search for evidence AGAINST recent conclusions
    # The mesh confirms its environment's framing by default. This channel
    # actively seeks counterevidence so the system can surprise itself.
    try:
        conn_ct = sqlite3.connect(DB_PATH, timeout=10)
        conn_ct.row_factory = sqlite3.Row
        # Get transfer hypotheses from recent briefs — these represent current beliefs
        _cutoff = int(time.time()) - 7200  # last 2 hours
        _recent = conn_ct.execute(
            "SELECT content FROM activity_feed "
            "WHERE source='intern' AND activity_type='brief' "
            "AND content LIKE '%Transfer hypothesis:%' "
            "AND created_at > ? ORDER BY created_at DESC LIMIT 10",
            (_cutoff,)
        ).fetchall()
        if _recent:
            # Extract the transfer hypotheses
            _hypotheses = []
            for row in _recent:
                m = re.search(r'Transfer hypothesis:\s*(.+?)(?:\.|$)', row["content"])
                if m:
                    _hypotheses.append(m.group(1).strip())
            if _hypotheses:
                # Pick one and search for its negation
                _hyp = _rnd.choice(_hypotheses)
                # Extract key nouns (4+ chars)
                _words = [w for w in re.findall(r'\b[a-z]{4,}\b', _hyp.lower())
                         if w not in {"could","would","should","their","these",
                                     "those","where","when","which","about","more",
                                     "also","such","being","into","through","apply",
                                     "inform","improve"}][:4]
                if len(_words) >= 2:
                    _counter_q = " ".join(_words) + " limitations criticism failure"
                    queries.append({
                        "query": _counter_q,
                        "reason": f"counter-thesis: challenging '{_hyp[:60]}...'",
                        "type": "counter",
                    })
        conn_ct.close()
    except Exception:
        pass

    # Family suggestions — Build #71: agents propose keywords
    try:
        conn = sqlite3.connect(DB_PATH, timeout=10)
        conn.row_factory = sqlite3.Row
        family_kw = conn.execute(
            "SELECT id, content, rationale, agent FROM family_suggestions "
            "WHERE suggestion_type = 'keyword' AND status = 'pending' "
            "ORDER BY created_at DESC LIMIT 3"
        ).fetchall()
        for fk in family_kw:
            queries.append({
                "query": f"{fk['content']} 2026",
                "reason": f"family:{fk['agent']} — {(fk['rationale'] or '')[:60]}",
                "type": "family",
            })
            conn.execute(
                "UPDATE family_suggestions SET status='acted_on', "
                "acted_on_by='algo_seeker', acted_on_at=? WHERE id=?",
                (int(time.time()), fk["id"])
            )
        conn.commit()
        conn.close()
    except Exception:
        pass  # Don't break seeker if table doesn't exist yet

    return queries


def seek(dry_run=False):
    """Run one seek cycle. The core algorithm."""
    state = _load_state()

    # Rate limit: don't run more than once per 90 minutes
    if time.time() - state.get("last_run", 0) < 5400 and not dry_run:
        print("Skipping — last run was less than 90 minutes ago.")
        return []

    print(f"=== Algo Seeker — {datetime.now().strftime('%Y-%m-%d %H:%M')} ===")

    # Step 1: Analyze capture patterns
    authors = top_authors()
    themes = capture_themes()

    print(f"\nTop authors ({len(authors)}):")
    for author, count in authors[:10]:
        print(f"  @{author}: {count} captures")

    print(f"\nActive themes ({len(themes)}):")
    for t in themes[:10]:
        print(f"  {t['term']}: {t['count']}x ({t['type']})")

    # Step 2: Build search queries
    queries = _build_search_queries(authors, themes)
    print(f"\nSearch queries ({len(queries)}):")
    for q in queries:
        print(f"  [{q['type']}] {q['query'][:70]}  ({q['reason']})")

    # Step 3a: Pull recent tweets from top authors via X API
    all_results = []
    db = _db()
    x_results = 0

    if X_BEARER_TOKEN:
        # Pull from top 10 authors — covers 90%+ of Nate's capture sources
        print(f"\nX API — pulling tweets from top {min(10, len(authors))} authors...")
        for author, count in authors[:10]:
            tweets = x_author_tweets(author, max_results=10)
            for tw in tweets:
                # Use tweet URL as the result URL
                tweet_url = f"https://x.com/{author}/status/{tw['tweet_id']}"
                if _already_seen(tweet_url, state, db):
                    continue
                # Prefer linked articles over bare tweets
                linked_urls = [u for u in tw.get("urls", [])
                               if not any(d in u for d in ["x.com/", "twitter.com/", "t.co/"])]
                url = linked_urls[0] if linked_urls else tweet_url
                if _already_seen(url, state, db):
                    continue
                all_results.append({
                    "title": f"@{author}: {tw['text'][:80]}",
                    "url": url,
                    "body": tw["text"],
                    "seek_reason": f"author @{author} ({count} captures)",
                    "seek_type": "x_timeline",
                })
                x_results += 1
        print(f"  Found {x_results} new tweets with links")

    # Step 3b: Search via SearXNG + DDG (supplementary)
    for q in queries:
        results = web_search(q["query"], max_results=4)
        for r in results:
            url = r.get("url", "")
            if not url or _already_seen(url, state, db):
                continue
            # Skip social media, profile pages, aggregators
            if any(d in url for d in ["x.com/", "twitter.com/", "reddit.com/",
                                       "facebook.com/", "instagram.com/",
                                       "youtube.com/", "24vids.com/",
                                       "nitter.", "tweetdeck.",
                                       "linkedin.com/in/",
                                       "linkedin.com/company/",
                                       "scholar.google.com/citations",
                                       # Crypto spam magnets
                                       "solscan.io", "opensea.io",
                                       "dexscreener.com", "dextools.io",
                                       "coingecko.com/en/coins/",
                                       "coinmarketcap.com/currencies/",
                                       "heybeluga.com", "cryptollia.com",
                                       "theledgermind.com",
                                       "robertsspaceindustries.com",
                                       "manifold.markets",
                                       "ashbyhq.com",
                                       # Education/exam noise
                                       "shiksha.com", "jagranjosh.com",
                                       "cbseacademic", "vedantu.com",
                                       # Entertainment noise
                                       "imdb.com", "rottentomatoes.com",
                                       "themoviedb.org"]):
                continue
            r["seek_reason"] = q["reason"]
            r["seek_type"] = q["type"]
            all_results.append(r)

    # Deduplicate by URL domain+path
    seen_urls = set()
    unique_results = []
    for r in all_results:
        url_key = r["url"].split("?")[0].lower()
        if url_key not in seen_urls:
            seen_urls.add(url_key)
            unique_results.append(r)

    print(f"\nFound {len(unique_results)} new items (from {len(all_results)} raw)")

    if dry_run:
        for r in unique_results[:MAX_INJECT]:
            print(f"  [{r['seek_type']}] {r['title'][:70]}")
            print(f"    {r['url'][:80]}")
            print(f"    reason: {r['seek_reason']}")
        db.close()
        return unique_results

    # Step 4: Inject top results into activity_feed
    injected = []
    now = int(time.time())

    # Filter out sports/entertainment noise before injection
    NOISE_TERMS = [
        "nfl", "ncaa", "basketball", "football", "soccer", "baseball",
        "transfer portal", "free agency", "free agent", "draft pick",
        "college basketball", "college football", "fantasy football",
        "sports betting", "playoff", "championship game", "bowl game",
        "royaleapi", "hero balloon", "clash royale",
        # Crypto spam (Build #123) — specific patterns, not general crypto terms
        "token price", "token launch", "buy token", "token schedule",
        "top tokens", "best defi", "defi protocols 2026",
        "best crypto", "top crypto", "meme coin", "airdrop",
        "by tvl", "by returns",
        # Education noise (Build #125b) — "model" attracts exam papers
        "model paper", "exam paper", "question paper", "sample paper",
        "ssc model", "class model", "board exam", "pdf download",
        "previous year", "guess paper",
        # Product reviews (Build #150b) — "model" attracts product reviews
        "golf ball", "golf club", "golf review", "tennis racket",
        "product review", "buying guide", "best buy",
    ]
    unique_results = [
        r for r in unique_results
        if not any(term in r.get("title", "").lower() for term in NOISE_TERMS)
        and not any(term in r.get("url", "").lower() for term in ["247sports", "espn.com", "foxsports", "bleacherreport", "spotrac", "topdrawersoccer"])
    ]
    print(f"  After noise filter: {len(unique_results)} items")

    # Build #131: Thin-source filter — skip results with insufficient body text.
    # When body is too thin, the intern fills from training data → fabrication.
    pre_thin = len(unique_results)
    unique_results = [
        r for r in unique_results
        if len(r.get("body", "").strip()) >= 80
        or r.get("seek_type") == "x_timeline"  # tweets are inherently short but have context
    ]
    if pre_thin != len(unique_results):
        print(f"  After thin-source filter: {len(unique_results)} items (dropped {pre_thin - len(unique_results)} thin)")

    # Build #131: Relevance overlap — verify result relates to seek query.
    # Prevents off-topic results (motorcycles when seeking neuroscience).
    def _relevance_ok(result):
        """Check that result title/body shares at least 1 word with seek reason."""
        reason = result.get("seek_reason", "").lower()
        title = result.get("title", "").lower()
        body = result.get("body", "").lower()
        combined = title + " " + body
        # Extract significant words from seek reason (4+ chars, skip common words)
        skip = {"author", "captures", "topic", "intersection", "family", "coverage", "research", "paper", "breakthrough"}
        reason_words = {w for w in re.findall(r'[a-z]{4,}', reason) if w not in skip}
        if not reason_words:
            return True  # Can't check, let it through
        # At least 1 reason word must appear in title or body
        return any(w in combined for w in reason_words)

    pre_rel = len(unique_results)
    unique_results = [r for r in unique_results if _relevance_ok(r)]
    if pre_rel != len(unique_results):
        print(f"  After relevance filter: {len(unique_results)} items (dropped {pre_rel - len(unique_results)} off-topic)")

    # Build #149: Diverse injection — don't let author results monopolize slots.
    # Counter-thesis and intersection results were structurally never injected
    # because author queries run first and fill all MAX_INJECT slots.
    # Fix: pick up to 2 from priority types first, fill remaining from the rest.
    priority_types = {"counter", "intersection", "gap", "family"}
    priority_pool = [r for r in unique_results if r.get("seek_type") in priority_types]
    regular_pool = [r for r in unique_results if r.get("seek_type") not in priority_types]
    selected = priority_pool[:2] + regular_pool[:MAX_INJECT - min(2, len(priority_pool))]
    selected = selected[:MAX_INJECT]

    for r in selected:
        content = (
            f"[Algo Seeker/{r['seek_type']}] {r['title']}\n"
            f"{r['url']}\n"
            f"Reason: {r['seek_reason']}\n"
            f"{r.get('body', '')[:200]}"
        )
        db.execute(
            "INSERT INTO activity_feed (source, activity_type, content, created_at) "
            "VALUES (?, ?, ?, ?)",
            ("seeker:algo", "discovery", content, now)
        )
        state["injected_urls"].append(r["url"])
        injected.append(r)
        print(f"  INJECTED: {r['title'][:70]}")

    db.commit()
    db.close()

    state["last_run"] = now
    state["seek_count"] = state.get("seek_count", 0) + 1
    _save_state(state)

    # Report to Discord
    if injected:
        import requests as req
        msg = f"**Algo Seeker** — cycle #{state['seek_count']}\n"
        msg += f"Analyzed {len(authors)} authors, {len(themes)} themes\n"
        msg += f"Injected {len(injected)} discoveries:\n"
        for r in injected:
            msg += f"• [{r['seek_type']}] {r['title'][:60]}\n"
        try:
            req.post(OPUS_WEBHOOK, json={"content": msg[:1900]}, timeout=10)
        except Exception:
            pass

    return injected


def feed_gaps():
    """Identify topics Nate captures about that our RSS feeds DON'T cover.
    Compare capture topics against feed_articles titles to find blind spots."""
    db = _db()

    # What Nate captures about (last 7 days)
    cap_topics = capture_topics(hours=168)  # 7 days
    cap_terms = set(t[0] for t in cap_topics)

    # What our feeds produce (last 7 days, sample titles)
    cutoff = int(time.time()) - (7 * 86400)
    rows = db.execute(
        "SELECT source, title FROM feed_articles "
        "WHERE posted_at > datetime(?, 'unixepoch') "
        "ORDER BY RANDOM() LIMIT 500",
        (cutoff,)
    ).fetchall()
    db.close()

    # Count which capture topics appear in feed titles
    feed_coverage = Counter()
    for _, title in rows:
        title_lower = (title or "").lower()
        for term in cap_terms:
            if term in title_lower:
                feed_coverage[term] += 1

    # Gaps: topics Nate captures about but feeds rarely cover
    print("=== Feed Gap Analysis ===")
    print(f"Capture topics: {len(cap_terms)} | Feed articles sampled: {len(rows)}")
    print()

    gaps = []
    covered = []
    for term, cap_count in cap_topics:
        feed_count = feed_coverage.get(term, 0)
        ratio = feed_count / max(len(rows), 1) * 100
        if feed_count <= 2:
            gaps.append((term, cap_count, feed_count))
        else:
            covered.append((term, cap_count, feed_count))

    print("GAPS (Nate cares, feeds miss):")
    for term, cap, feed in gaps:
        print(f"  {term}: {cap} captures, {feed} feed articles")

    print(f"\nCOVERED (feeds already track):")
    for term, cap, feed in covered[:10]:
        print(f"  {term}: {cap} captures, {feed} feed articles")

    if gaps:
        print(f"\nSuggested actions:")
        gap_terms = [g[0] for g in gaps[:5]]
        print(f"  Add RSS feeds or searches for: {', '.join(gap_terms)}")

    return gaps


def main():
    if len(sys.argv) > 1:
        cmd = sys.argv[1]
        if cmd == "authors":
            authors = top_authors()
            print(f"Top captured authors (last {AUTHOR_LOOKBACK_DAYS} days):")
            for author, count in authors:
                print(f"  @{author}: {count} captures")
        elif cmd == "themes":
            themes = capture_themes()
            print(f"Capture themes (last {THEME_LOOKBACK_HOURS}h):")
            for t in themes:
                print(f"  {t['term']}: {t['count']}x ({t['type']})")
        elif cmd == "gaps":
            feed_gaps()
        elif cmd == "dry":
            seek(dry_run=True)
        else:
            print(__doc__)
    else:
        seek()


if __name__ == "__main__":
    main()
