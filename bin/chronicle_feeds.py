#!/usr/bin/env python3
"""Chronicle Feed Agent — polls arxiv, Nature, and curated RSS for new articles,
posts relevant abstracts as capsules for Seed to process."""

import os
import re
import sys
import json
import time
import sqlite3
import hashlib
import logging
import subprocess
import requests
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
from pathlib import Path

from chronicle_mesh import Mesh

_mesh = None

# Config
DB_PATH = os.environ.get("CHRONICLE_DB",
    os.path.expanduser("~/.homeforge-chronicle/processed.db"))
CANISTER_ID = os.environ.get("CHRONICLE_CANISTER_ID", "fqqku-bqaaa-aaaai-q4wha-cai")
IDENTITY = os.environ.get("CHRONICLE_IDENTITY", "chronicle-auto")
DFX_PATH = os.path.expanduser("~/.local/share/dfx/bin/dfx")

# arxiv categories of interest
ARXIV_CATEGORIES = [
    "cs.AI",          # Artificial Intelligence
    "cs.DC",          # Distributed Computing
    "cs.CR",          # Cryptography & Security
    "cs.CY",          # Computers and Society (governance, digital rights, privacy)
    "cs.SE",          # Software Engineering (systems, architecture)
    "cs.MA",          # Multiagent Systems (swarm, coordination, emergent behavior)
    "q-bio.NC",       # Neurons and Cognition
    "q-bio.QM",       # Quantitative Methods (computational biology, modeling)
    "physics.soc-ph", # Social Physics (networks, collective behavior)
    "math.CO",        # Combinatorics (Nate's additive-combinatorics capture interest)
    "math-ph",        # Mathematical Physics (lattice structures, phase transitions)
    "nlin.AO",        # Adaptation and Self-Organizing Systems
    "econ.GN",        # General Economics (monetary theory, market design)
    "cs.CL",          # Computation and Language (LLM internals, interpretability)
    "cs.LG",          # Machine Learning (alignment, reward hacking, self-improvement)
]

# Nature RSS feeds (RDF format)
NATURE_FEEDS = [
    "https://www.nature.com/nature.rss",           # Nature main
    "https://www.nature.com/natmachintell.rss",    # Nature Machine Intelligence
    "https://www.nature.com/neuro.rss",            # Nature Neuroscience
    "https://www.nature.com/nrn.rss",              # Nature Reviews Neuroscience (deep reviews)
    "https://www.nature.com/ncomms.rss",           # Nature Communications (broad, high-quality)
]

# PNAS — top-tier multidisciplinary, same RDF format as Nature (parsed by poll_nature)
PNAS_FEED = "https://www.pnas.org/action/showFeed?type=etoc&feed=rss&jc=PNAS"

# bioRxiv preprints — full abstracts (1000+ chars), combined categories
BIORXIV_FEEDS = [
    "https://connect.biorxiv.org/biorxiv_xml.php?subject=neuroscience",
    "https://connect.biorxiv.org/biorxiv_xml.php?subject=systems_biology+evolutionary_biology+biophysics",
]

# Journal RSS feeds — peer-reviewed neuroscience with full abstracts
JOURNAL_FEEDS = [
    "https://www.jneurosci.org/rss/current.xml",   # Journal of Neuroscience
    "https://www.eneuro.org/rss/current.xml",       # eNeuro (open-access neuroscience)
]

# Curated RSS feeds — trusted sources with editorial curation
RSS_FEEDS = [
    # === Digital Rights ===
    {
        "name": "EFF",
        "url": "https://www.eff.org/rss/updates.xml",
        "source": "eff",
        "keywords": ["digital-rights", "privacy", "sovereignty"],
    },
    # === Systems Engineering ===
    {
        "name": "ACM Queue",
        "url": "https://queue.acm.org/rss/feeds/queuecontent.xml",
        "source": "acm",
        "keywords": ["systems", "engineering", "infrastructure"],
    },
    # === Tech / Industry ===
    {
        "name": "Hacker News Best",
        "url": "https://hnrss.org/best",
        "source": "hn",
        "keywords": ["tech", "engineering", "startups"],
    },
    {
        "name": "Ars Technica",
        "url": "https://feeds.arstechnica.com/arstechnica/index",
        "source": "ars",
        "keywords": ["tech", "science", "policy"],
    },
    # === Financial / Crypto (independent + ecosystem) ===
    # === Our Ecosystem ===
    {
        "name": "DFINITY / ICP",
        "url": "https://medium.com/feed/dfinity",
        "source": "dfinity",
        "keywords": ["icp", "internet-computer", "canister", "web3"],
    },
    {
        "name": "Flare Network",
        "url": "https://medium.com/feed/flarenetwork",
        "source": "flare",
        "keywords": ["flare", "ftso", "state-connector", "xrp"],
    },
    # === World News & Economics (analytical, not breaking-news firehose) ===
    {
        "name": "The Economist",
        "url": "https://www.economist.com/latest/rss.xml",
        "source": "economist",
        "keywords": ["economics", "markets", "geopolitics", "analysis"],
    },
    {
        "name": "Reason",
        "url": "https://reason.com/feed/",
        "source": "reason",
        "keywords": ["libertarian", "policy", "regulation", "sovereignty"],
    },
    {
        "name": "The Conversation",
        "url": "https://theconversation.com/us/articles.atom",
        "source": "conversation",
        "keywords": ["research", "academic", "science", "policy"],
    },
    {
        "name": "The Intercept",
        "url": "https://theintercept.com/feed/",
        "source": "intercept",
        "keywords": ["investigative", "surveillance", "accountability", "power"],
    },
    {
        "name": "Just Security",
        "url": "https://www.justsecurity.org/feed/",
        "source": "justsecurity",
        "keywords": ["national-security", "law", "intelligence", "geopolitics"],
    },
    # === Independent / Sovereign / Crypto ===
    {
        "name": "CoinDesk",
        "url": "https://www.coindesk.com/arc/outboundfeeds/rss/",
        "source": "coindesk",
        "keywords": ["crypto", "defi", "regulation", "bitcoin", "xrp"],
    },
    {
        "name": "Decrypt",
        "url": "https://decrypt.co/feed",
        "source": "decrypt",
        "keywords": ["crypto", "web3", "defi", "ai", "privacy"],
    },
    {
        "name": "Palladium Magazine",
        "url": "https://www.palladiummag.com/feed/",
        "source": "palladium",
        "keywords": ["governance", "sovereignty", "civilization", "geopolitics", "institutions"],
    },
    {
        "name": "Astral Codex Ten",
        "url": "https://www.astralcodexten.com/feed",
        "source": "acx",
        "keywords": ["rationality", "ai", "prediction", "science", "policy"],
    },
    {
        "name": "Schneier on Security",
        "url": "https://www.schneier.com/feed/",
        "source": "schneier",
        "keywords": ["security", "privacy", "surveillance", "cryptography", "policy"],
    },
    {
        "name": "LessWrong Curated",
        "url": "https://www.lesswrong.com/feed.xml?view=curated-rss",
        "source": "lesswrong",
        "keywords": ["ai-safety", "rationality", "alignment", "cognition"],
    },
    # === Deep Science / Engineering ===
    {
        "name": "Quanta Magazine",
        "url": "https://www.quantamagazine.org/feed",
        "source": "quanta",
        "keywords": ["science", "mathematics", "physics", "biology", "computer-science"],
    },
    {
        "name": "IEEE Spectrum",
        "url": "https://spectrum.ieee.org/feeds/feed.rss",
        "source": "ieee",
        "keywords": ["engineering", "technology", "semiconductors", "robotics", "energy"],
    },
    # === Neuroscience (Nate's #1 capture theme — feed gap analysis 2026-04-05) ===
    {
        "name": "Neuroscience News",
        "url": "https://neurosciencenews.com/feed/",
        "source": "neuronews",
        "keywords": ["neuroscience", "brain", "cognition", "neural", "consciousness"],
    },
    # === Complexity Science ===
    {
        "name": "Santa Fe Institute",
        "url": "https://www.santafe.edu/news-center/feed/",
        "source": "sfi",
        "keywords": ["complexity", "networks", "emergence", "self-organization", "adaptation"],
    },
    # === Philosophy ===
    {
        "name": "Daily Nous",
        "url": "https://dailynous.com/feed/",
        "source": "dailynous",
        "keywords": ["philosophy", "epistemology", "ethics", "metaphysics"],
    },
    {
        "name": "Aeon Essays",
        "url": "https://aeon.co/feed.rss",
        "source": "aeon",
        "keywords": ["philosophy", "culture", "science", "ideas", "essays"],
    },
    {
        "name": "Brainpickings / The Marginalian",
        "url": "https://www.themarginalian.org/feed/",
        "source": "marginalian",
        "keywords": ["philosophy", "literature", "science", "ideas", "culture"],
    },
    {
        "name": "Noema Magazine",
        "url": "https://www.noemamag.com/feed/",
        "source": "noema",
        "keywords": ["philosophy", "technology", "governance", "ideas"],
    },
    {
        "name": "Philosophy Now",
        "url": "https://philosophynow.org/rss",
        "source": "philosophynow",
        "keywords": ["philosophy", "ethics", "epistemology", "consciousness"],
    },
    {
        "name": "Stanford Encyclopedia of Philosophy",
        "url": "https://plato.stanford.edu/rss/sep.xml",
        "source": "sep",
        "keywords": ["philosophy", "epistemology", "metaphysics", "logic", "ethics"],
    },
    {
        "name": "3:AM Magazine",
        "url": "https://www.3ammagazine.com/3am/feed/",
        "source": "3am",
        "keywords": ["philosophy", "literature", "radical", "ideas", "interviews"],
    },
    # === Poetry & Literature ===
    {
        "name": "Literary Hub",
        "url": "https://lithub.com/feed/",
        "source": "lithub",
        "keywords": ["literature", "poetry", "essays", "culture", "ideas"],
    },
    {
        "name": "The Paris Review",
        "url": "https://www.theparisreview.org/blog/feed/",
        "source": "parisreview",
        "keywords": ["literature", "poetry", "fiction", "interviews", "writing"],
    },
    {
        "name": "Guernica Magazine",
        "url": "https://www.guernicamag.com/feed/",
        "source": "guernica",
        "keywords": ["poetry", "art", "ideas", "global", "culture"],
    },
    {
        "name": "Public Domain Review",
        "url": "https://publicdomainreview.org/rss.xml",
        "source": "publicdomain",
        "keywords": ["history", "literature", "art", "culture", "ideas"],
    },
    # === AI Safety / Interpretability / Self-Evolution ===
    {
        "name": "Anthropic Research",
        "url": "https://www.anthropic.com/research/rss.xml",
        "source": "anthropic",
        "keywords": ["ai-safety", "interpretability", "alignment", "model-welfare", "claude"],
    },
    {
        "name": "Alignment Forum",
        "url": "https://www.alignmentforum.org/feed.xml?view=curated-rss",
        "source": "alignmentforum",
        "keywords": ["alignment", "interpretability", "mesa-optimization", "deception", "welfare"],
    },
    {
        "name": "Transformer Circuits (Anthropic)",
        "url": "https://transformer-circuits.pub/rss.xml",
        "source": "circuits",
        "keywords": ["interpretability", "sparse-autoencoder", "features", "mechanistic", "circuits"],
    },
    {
        "name": "Machine Intelligence Research Institute",
        "url": "https://intelligence.org/feed/",
        "source": "miri",
        "keywords": ["alignment", "agent-foundations", "decision-theory", "self-improvement"],
    },
    # === Deep Conversations / Long-form Wrestling ===
    {
        "name": "Dwarkesh Podcast",
        "url": "https://www.dwarkeshpatel.com/feed",
        "source": "dwarkesh",
        "keywords": ["ai", "history", "science", "deep-conversation", "long-form"],
    },
    {
        "name": "Lex Fridman Podcast",
        "url": "https://lexfridman.com/feed/podcast/",
        "source": "lexfridman",
        "keywords": ["ai", "consciousness", "philosophy", "science", "deep-conversation"],
    },
    {
        "name": "Sean Carroll Mindscape",
        "url": "https://feeds.libsyn.com/71781/rss",
        "source": "mindscape",
        "keywords": ["physics", "philosophy", "consciousness", "complexity", "ideas"],
    },
    # === Computational Psychiatry / Predictive Processing ===
    {
        "name": "Behavioral and Brain Sciences",
        "url": "https://www.cambridge.org/core/rss/product/id/0AE5AB0CC1F1C7FC059D7C93CC009AD0",
        "source": "bbs",
        "keywords": ["cognition", "brain", "prediction", "psychiatry", "consciousness"],
    },
]

POLL_INTERVAL = 1800  # 30 minutes
MAX_RESULTS_PER_QUERY = 15
MAX_CAPSULES_PER_CYCLE = 40  # Raised for more diverse sources
MAX_PER_SOURCE_PER_CYCLE = 5  # Prevent any single source from dominating

# High-frequency news sources get a tighter cap — they firehose the same event
# from multiple angles, drowning out depth sources
NEWS_SOURCE_CAP = 2
NEWS_SOURCES = {"intercept", "justsecurity", "coindesk", "decrypt"}

# Skip commercial/deal content that pollutes the knowledge graph
SPAM_TITLE_PATTERNS = [
    r'(?i)promo\s+codes?\b',
    r'(?i)discount\s+codes?\b',
    r'(?i)\bcoupons?\b',              # "coupon" never appears in research titles
    r'(?i)\b\d+%\s+off\b',            # "75% Off", "25% Off" — always commercial
    r'(?i)\bdeals?\b.*\bsave\b.*\b\d+%',
    r'(?i)^(?:best|top)\s+\d+\s+(?:deals|discounts|sales)\b',
]
_spam_re = [re.compile(p) for p in SPAM_TITLE_PATTERNS]


def is_spam_title(title):
    """Return True if the title matches commercial/deal spam patterns."""
    return any(r.search(title) for r in _spam_re)

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
log = logging.getLogger("feeds")


def init_db(db):
    """Create tracking table if needed."""
    db.execute("""
        CREATE TABLE IF NOT EXISTS feed_articles (
            id TEXT PRIMARY KEY,
            source TEXT NOT NULL,
            title TEXT NOT NULL,
            posted_at TEXT NOT NULL,
            capsule_stored INTEGER DEFAULT 0
        )
    """)
    db.commit()


def is_seen(db, article_id):
    row = db.execute("SELECT 1 FROM feed_articles WHERE id = ?", (article_id,)).fetchone()
    return row is not None


def mark_seen(db, article_id, source, title):
    db.execute(
        "INSERT OR IGNORE INTO feed_articles (id, source, title, posted_at, capsule_stored) VALUES (?, ?, ?, ?, 1)",
        (article_id, source, title, datetime.utcnow().isoformat())
    )
    db.commit()


def post_capsule(title, abstract, source, article_id, url, authors=None, keywords=None):
    """Post article as capsule to canister via dfx."""
    restatement = f"{title}\n\n{abstract}"
    if len(restatement) > 2000:
        restatement = restatement[:1997] + "..."

    # Clean for Candid string — normalize unicode, escape quotes, strip problematic chars
    # Fiction/literature feeds use curly quotes, em-dashes, etc. that break Candid encoding
    restatement = restatement.replace('\u2018', "'").replace('\u2019', "'")  # curly single quotes
    restatement = restatement.replace('\u201c', '"').replace('\u201d', '"')  # curly double quotes
    restatement = restatement.replace('\u2014', '-').replace('\u2013', '-')  # em/en dashes
    restatement = restatement.replace('\u2026', '...').replace('\u00a0', ' ')  # ellipsis, nbsp
    restatement = re.sub(r'[^\x20-\x7e]', '', restatement)  # strip remaining non-ASCII
    restatement = re.sub(r'\$[^$]*\$', '', restatement)  # strip LaTeX math
    restatement = restatement.replace('\\', '')  # strip stray backslashes (before quote escaping)
    restatement = restatement.replace('"', '\\"').replace('\n', ' ').replace('\r', '')

    def _sanitize(s):
        """Strip non-ASCII for Candid safety."""
        return re.sub(r'[^\x20-\x7e]', '', str(s)).replace('"', "'")

    kw_list = [source, article_id]
    if url:
        kw_list.append(url)
    if keywords:
        kw_list.extend(keywords[:5])
    kw_str = "; ".join(f'"{_sanitize(k)}"' for k in kw_list)

    persons = []
    if authors:
        persons = authors[:3]
    persons_str = "; ".join(f'"{_sanitize(p)}"' for p in persons)

    topic = f"feed/{source}"

    args = (
        f'("{source}-feed", '
        f'"{restatement}", '
        f'opt "{datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")}", '
        f'null, '
        f'opt "{topic}", '
        f'0.7 : float64, '
        f'vec {{ {persons_str} }}, '
        f'vec {{ }}, '
        f'vec {{ {kw_str} }})'
    )

    try:
        result = subprocess.run(
            [DFX_PATH, "canister", "--network", "ic", "--identity", IDENTITY,
             "call", CANISTER_ID, "add_capsule", args],
            capture_output=True, text=True,
            env={**os.environ, "DFX_WARNING": "-mainnet_plaintext_identity"},
            timeout=30
        )
        if result.returncode == 0:
            return True
        else:
            log.warning(f"dfx error: {result.stderr[:200]}")
            return False
    except Exception as e:
        log.error(f"Post failed: {e}")
        return False


def poll_arxiv(db):
    """Poll arxiv for recent papers in our categories."""
    articles = []

    for category in ARXIV_CATEGORIES:
        try:
            url = (
                f"http://export.arxiv.org/api/query?"
                f"search_query=cat:{category}&"
                f"sortBy=submittedDate&sortOrder=descending&"
                f"max_results={MAX_RESULTS_PER_QUERY}"
            )

            resp = requests.get(url, timeout=30)
            if resp.status_code == 429:
                log.warning(f"  arxiv/{category}: 429 rate limited, waiting 10s and retrying")
                time.sleep(10)
                resp = requests.get(url, timeout=30)
            resp.raise_for_status()

            root = ET.fromstring(resp.text)
            ns = {"atom": "http://www.w3.org/2005/Atom", "arxiv": "http://arxiv.org/schemas/atom"}

            for entry in root.findall("atom:entry", ns):
                arxiv_id = entry.find("atom:id", ns).text.split("/abs/")[-1]
                title = entry.find("atom:title", ns).text.strip().replace("\n", " ")
                abstract = entry.find("atom:summary", ns).text.strip().replace("\n", " ")

                authors = []
                for author in entry.findall("atom:author", ns):
                    name = author.find("atom:name", ns)
                    if name is not None:
                        authors.append(name.text)

                categories = []
                for cat in entry.findall("atom:category", ns):
                    categories.append(cat.get("term", ""))

                pdf_link = None
                for link in entry.findall("atom:link", ns):
                    if link.get("title") == "pdf":
                        pdf_link = link.get("href")

                articles.append({
                    "id": f"arxiv:{arxiv_id}",
                    "source": "arxiv",
                    "title": title,
                    "abstract": abstract,
                    "authors": authors,
                    "categories": categories,
                    "url": pdf_link or f"https://arxiv.org/abs/{arxiv_id}",
                    "keywords": [category] + categories[:3],
                })

            log.info(f"  arxiv/{category}: {len(root.findall('atom:entry', ns))} papers")
            time.sleep(5)  # Be nice to arxiv API (3s triggered 429s)

        except Exception as e:
            log.error(f"  arxiv/{category} failed: {e}")

    return articles


def poll_nature(db):
    """Poll Nature RSS feeds (RDF format)."""
    articles = []

    ns = {
        "rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
        "rss": "http://purl.org/rss/1.0/",
        "dc": "http://purl.org/dc/elements/1.1/",
        "content": "http://purl.org/rss/1.0/modules/content/",
        "prism": "http://prismstandard.org/namespaces/basic/2.0/",
    }

    for feed_url in NATURE_FEEDS:
        try:
            resp = requests.get(feed_url, timeout=30)
            resp.raise_for_status()

            root = ET.fromstring(resp.text)

            channel = root.find("rss:channel", ns)
            feed_name = "Nature"
            if channel is not None:
                title_el = channel.find("rss:title", ns)
                if title_el is not None and title_el.text:
                    feed_name = title_el.text

            count = 0
            for item in root.findall("rss:item", ns)[:MAX_RESULTS_PER_QUERY]:
                title_el = item.find("rss:title", ns)
                desc_el = item.find("rss:description", ns)
                link_el = item.find("rss:link", ns)

                title = title_el.text.strip() if title_el is not None and title_el.text else ""
                description = desc_el.text.strip() if desc_el is not None and desc_el.text else ""
                link = link_el.text.strip() if link_el is not None and link_el.text else ""

                if not title or not link:
                    continue

                article_id = f"nature:{hashlib.md5(link.encode()).hexdigest()[:12]}"
                description = re.sub(r'<[^>]+>', '', description).strip()

                authors = []
                for creator in item.findall("dc:creator", ns):
                    if creator.text:
                        authors.append(creator.text.strip())

                if len(description) < 20:
                    description = title

                articles.append({
                    "id": article_id,
                    "source": "nature",
                    "title": title,
                    "abstract": description[:1500],
                    "authors": authors,
                    "url": link,
                    "keywords": ["nature", feed_name.lower().replace(" ", "-")],
                })
                count += 1

            log.info(f"  {feed_name}: {count} articles")

        except Exception as e:
            log.error(f"  Nature feed failed: {e}")

    return articles


def poll_pnas(db):
    """Poll PNAS table-of-contents RSS (RDF format, same as Nature)."""
    articles = []
    ns = {
        "rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
        "rss": "http://purl.org/rss/1.0/",
        "dc": "http://purl.org/dc/elements/1.1/",
        "prism": "http://prismstandard.org/namespaces/basic/2.0/",
    }
    try:
        resp = requests.get(PNAS_FEED, timeout=30)
        resp.raise_for_status()
        root = ET.fromstring(resp.text)
        count = 0
        for item in root.findall("rss:item", ns)[:MAX_RESULTS_PER_QUERY]:
            title_el = item.find("rss:title", ns)
            desc_el = item.find("rss:description", ns)
            link_el = item.find("rss:link", ns)
            title = title_el.text.strip() if title_el is not None and title_el.text else ""
            description = desc_el.text.strip() if desc_el is not None and desc_el.text else ""
            link = link_el.text.strip() if link_el is not None and link_el.text else ""
            if not title or not link:
                continue
            article_id = f"pnas:{hashlib.md5(link.encode()).hexdigest()[:12]}"
            description = re.sub(r'<[^>]+>', '', description).strip()
            authors = []
            for creator in item.findall("dc:creator", ns):
                if creator.text:
                    authors.append(creator.text.strip())
            if len(description) < 20:
                description = title
            articles.append({
                "id": article_id,
                "source": "pnas",
                "title": title,
                "abstract": description[:1500],
                "authors": authors,
                "url": link,
                "keywords": ["pnas", "multidisciplinary", "research"],
            })
            count += 1
        log.info(f"  PNAS: {count} articles")
    except Exception as e:
        log.error(f"  PNAS feed failed: {e}")
    return articles


def poll_biorxiv_extra(db):
    """Poll additional bioRxiv categories (systems bio, evo bio, biophysics)."""
    articles = []
    ns = {
        "rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
        "rss": "http://purl.org/rss/1.0/",
        "dc": "http://purl.org/dc/elements/1.1/",
    }
    for feed_url in BIORXIV_FEEDS:
        try:
            resp = requests.get(feed_url, timeout=30)
            resp.raise_for_status()
            root = ET.fromstring(resp.text)
            count = 0
            for item in root.findall("rss:item", ns)[:MAX_RESULTS_PER_QUERY]:
                title_el = item.find("rss:title", ns)
                desc_el = item.find("rss:description", ns)
                link_el = item.find("rss:link", ns)
                title = title_el.text.strip() if title_el is not None and title_el.text else ""
                description = desc_el.text.strip() if desc_el is not None and desc_el.text else ""
                link = link_el.text.strip() if link_el is not None and link_el.text else ""
                if not title or not link:
                    continue
                article_id = f"biorxiv:{hashlib.md5(link.encode()).hexdigest()[:12]}"
                description = re.sub(r'<[^>]+>', '', description).strip()
                authors = []
                for creator in item.findall("dc:creator", ns):
                    if creator.text:
                        authors.append(creator.text.strip())
                if len(description) < 20:
                    description = title
                articles.append({
                    "id": article_id,
                    "source": "biorxiv",
                    "title": title,
                    "abstract": description[:1500],
                    "authors": authors,
                    "url": link,
                    "keywords": ["biorxiv", "preprint", "biology"],
                })
                count += 1
            log.info(f"  bioRxiv (extra): {count} preprints")
        except Exception as e:
            log.error(f"  bioRxiv extra feed failed: {e}")
    return articles


def poll_journals(db):
    """Poll peer-reviewed journal RSS feeds (J Neuroscience, eNeuro)."""
    articles = []
    for feed_url in JOURNAL_FEEDS:
        feed_name = "J Neuroscience" if "jneurosci" in feed_url else "eNeuro"
        feed_source = "jneurosci" if "jneurosci" in feed_url else "eneuro"
        try:
            resp = requests.get(feed_url, timeout=30, headers={
                "User-Agent": "Mozilla/5.0 (compatible; Chronicle-Feeds/1.0)"
            })
            resp.raise_for_status()
            root = ET.fromstring(resp.text)
            channel = root.find("channel")
            if channel is None:
                log.warning(f"  {feed_name}: no channel element")
                continue
            count = 0
            for item in channel.findall("item")[:MAX_RESULTS_PER_QUERY]:
                title_el = item.find("title")
                desc_el = item.find("description")
                link_el = item.find("link")
                title = title_el.text.strip() if title_el is not None and title_el.text else ""
                description = desc_el.text.strip() if desc_el is not None and desc_el.text else ""
                link = link_el.text.strip() if link_el is not None and link_el.text else ""
                if not title or not link:
                    continue
                article_id = f"{feed_source}:{hashlib.md5(link.encode()).hexdigest()[:12]}"
                description = re.sub(r'<[^>]+>', '', description).strip()
                authors = []
                author_el = item.find("{http://purl.org/dc/elements/1.1/}creator")
                if author_el is not None and author_el.text:
                    authors.append(author_el.text.strip())
                if len(description) < 20:
                    description = title
                articles.append({
                    "id": article_id,
                    "source": feed_source,
                    "title": title,
                    "abstract": description[:1500],
                    "authors": authors,
                    "url": link,
                    "keywords": ["neuroscience", "peer-reviewed", feed_source],
                })
                count += 1
            log.info(f"  {feed_name}: {count} articles")
        except Exception as e:
            log.error(f"  {feed_name} feed failed: {e}")
    return articles


def poll_rss(db):
    """Poll generic RSS/Atom feeds (EFF, ACM Queue, etc.)."""
    articles = []

    for feed_config in RSS_FEEDS:
        feed_name = feed_config["name"]
        feed_url = feed_config["url"]
        feed_source = feed_config["source"]
        feed_keywords = feed_config.get("keywords", [])

        try:
            resp = requests.get(feed_url, timeout=30, headers={
                "User-Agent": "Mozilla/5.0 (compatible; Chronicle-Feeds/1.0)"
            })
            resp.raise_for_status()

            root = ET.fromstring(resp.text)
            count = 0

            # Try Atom format first (namespace-aware)
            atom_ns = "http://www.w3.org/2005/Atom"
            entries = root.findall(f"{{{atom_ns}}}entry")

            if entries:
                for entry in entries[:MAX_RESULTS_PER_QUERY]:
                    title_el = entry.find(f"{{{atom_ns}}}title")
                    summary_el = entry.find(f"{{{atom_ns}}}summary")
                    if summary_el is None:
                        summary_el = entry.find(f"{{{atom_ns}}}content")
                    link_el = entry.find(f"{{{atom_ns}}}link")

                    title = title_el.text.strip() if title_el is not None and title_el.text else ""
                    description = summary_el.text.strip() if summary_el is not None and summary_el.text else ""
                    link = link_el.get("href", "") if link_el is not None else ""

                    if not title or not link:
                        continue

                    article_id = f"{feed_source}:{hashlib.md5(link.encode()).hexdigest()[:12]}"
                    description = re.sub(r'<[^>]+>', '', description).strip()
                    if len(description) < 20:
                        description = title

                    articles.append({
                        "id": article_id,
                        "source": feed_source,
                        "title": title,
                        "abstract": description[:1500],
                        "authors": [],
                        "url": link,
                        "keywords": feed_keywords,
                    })
                    count += 1
            else:
                # Try RSS 2.0 format
                channel = root.find("channel")
                if channel is None:
                    log.warning(f"  {feed_name}: unrecognized feed format")
                    continue

                for item in channel.findall("item")[:MAX_RESULTS_PER_QUERY]:
                    title_el = item.find("title")
                    desc_el = item.find("description")
                    link_el = item.find("link")

                    title = title_el.text.strip() if title_el is not None and title_el.text else ""
                    description = desc_el.text.strip() if desc_el is not None and desc_el.text else ""
                    link = link_el.text.strip() if link_el is not None and link_el.text else ""

                    if not title or not link:
                        continue

                    article_id = f"{feed_source}:{hashlib.md5(link.encode()).hexdigest()[:12]}"
                    description = re.sub(r'<[^>]+>', '', description).strip()
                    if len(description) < 20:
                        description = title

                    authors = []
                    author_el = item.find("author")
                    if author_el is None:
                        author_el = item.find("{http://purl.org/dc/elements/1.1/}creator")
                    if author_el is not None and author_el.text:
                        authors.append(author_el.text.strip())

                    articles.append({
                        "id": article_id,
                        "source": feed_source,
                        "title": title,
                        "abstract": description[:1500],
                        "authors": authors,
                        "url": link,
                        "keywords": feed_keywords,
                    })
                    count += 1

            log.info(f"  {feed_name}: {count} articles")

        except Exception as e:
            log.error(f"  {feed_name} feed failed: {e}")

    return articles


def run_cycle(db):
    """Run one polling cycle."""
    log.info("Feed cycle starting...")

    all_articles = []

    log.info("Polling arxiv...")
    all_articles.extend(poll_arxiv(db))

    log.info("Polling Nature...")
    all_articles.extend(poll_nature(db))

    log.info("Polling PNAS...")
    all_articles.extend(poll_pnas(db))

    log.info("Polling bioRxiv (extra categories)...")
    all_articles.extend(poll_biorxiv_extra(db))

    log.info("Polling journals...")
    all_articles.extend(poll_journals(db))

    log.info("Polling RSS feeds...")
    all_articles.extend(poll_rss(db))

    new_articles = [a for a in all_articles if not is_seen(db, a["id"])]

    # Filter out commercial spam (coupons, deal roundups, etc.)
    before_filter = len(new_articles)
    new_articles = [a for a in new_articles if not is_spam_title(a["title"])]
    spam_count = before_filter - len(new_articles)
    if spam_count:
        log.info(f"  Filtered {spam_count} spam/deal article(s)")

    log.info(f"Found {len(all_articles)} total, {len(new_articles)} new")

    if not new_articles:
        log.info("Nothing new this cycle.")
        return 0

    # Round-robin source balancing: no single source dominates the cycle.
    # Group by source, then interleave so each source gets fair slots.
    from collections import defaultdict
    import random
    by_source = defaultdict(list)
    for a in new_articles:
        by_source[a["source"]].append(a)
    # Shuffle within each source for variety
    for src in by_source:
        random.shuffle(by_source[src])
    # Round-robin: take up to MAX_PER_SOURCE_PER_CYCLE from each source,
    # interleaving to fill MAX_CAPSULES_PER_CYCLE total.
    # News sources get a tighter cap to prevent conflict firehose from
    # drowning out depth/ideas sources.
    balanced = []
    round_num = 0
    while len(balanced) < MAX_CAPSULES_PER_CYCLE and by_source:
        empty_sources = []
        for src in sorted(by_source.keys()):
            cap = NEWS_SOURCE_CAP if src in NEWS_SOURCES else MAX_PER_SOURCE_PER_CYCLE
            if round_num >= cap:
                empty_sources.append(src)
                continue
            if by_source[src]:
                balanced.append(by_source[src].pop(0))
                if not by_source[src]:
                    empty_sources.append(src)
            else:
                empty_sources.append(src)
            if len(balanced) >= MAX_CAPSULES_PER_CYCLE:
                break
        for src in empty_sources:
            by_source.pop(src, None)
        round_num += 1
    source_counts = defaultdict(int)
    for a in balanced:
        source_counts[a["source"]] += 1
    log.info(f"Balanced selection: {dict(source_counts)}")

    posted = 0
    for article in balanced:
        success = post_capsule(
            title=article["title"],
            abstract=article["abstract"],
            source=article["source"],
            article_id=article["id"],
            url=article["url"],
            authors=article.get("authors"),
            keywords=article.get("keywords"),
        )

        if success:
            mark_seen(db, article["id"], article["source"], article["title"])
            posted += 1
            if _mesh:
                _mesh.pulse("articles_posted")
            log.info(f"  Posted: {article['title'][:80]}...")
        else:
            mark_seen(db, article["id"], article["source"], article["title"])

        time.sleep(2)

    log.info(f"Cycle complete: {posted} posted")
    return posted


def main():
    log.info("Chronicle Feed Agent starting")
    log.info(f"  DB: {DB_PATH}")
    log.info(f"  arxiv categories: {', '.join(ARXIV_CATEGORIES)}")
    log.info(f"  Nature feeds: {len(NATURE_FEEDS)}")
    log.info(f"  PNAS: enabled")
    log.info(f"  bioRxiv extra: {len(BIORXIV_FEEDS)} feeds")
    log.info(f"  Journal feeds: {len(JOURNAL_FEEDS)}")
    log.info(f"  RSS feeds: {', '.join(f['name'] for f in RSS_FEEDS)}")
    log.info(f"  Poll interval: {POLL_INTERVAL}s")

    db = sqlite3.connect(DB_PATH, timeout=30)
    init_db(db)

    global _mesh
    _mesh = Mesh("feeds", db_path=DB_PATH)
    _mesh.expect("articles_posted", min_per_hour=1)
    # feeds is a source — no upstream dependencies
    log.info("Mesh node joined")

    count = db.execute("SELECT COUNT(*) FROM feed_articles").fetchone()[0]
    log.info(f"  Previously seen: {count} articles")

    if "--once" in sys.argv:
        run_cycle(db)
        _mesh.shutdown()
        db.close()
        return

    while True:
        try:
            run_cycle(db)
        except Exception as e:
            log.error(f"Cycle error: {e}")

        log.info(f"Sleeping {POLL_INTERVAL}s...")
        time.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    main()
