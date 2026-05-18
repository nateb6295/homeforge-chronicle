#!/usr/bin/env python3
"""Stabilized Compress — wraps compress_cognitive_state with stability injection.

Instead of calling compress_cognitive_state directly (which is memoryless about
entity persistence), this:
1. Generates entity stability context from CCS history
2. Prepends it to the session summary
3. Calls compress_cognitive_state via MCP with the enhanced context
4. Logs before/after entity sets to measure retention improvement

Thread #318 advance 70 → substrate: the calibration stack says gist is the
calibration dial (2.50/kT). This script makes the compressor KNOW that.

Parcae (2026) principle: stable recurrent parameterization prevents residual
explosion/collapse in looped systems. Entity persistence context = the
parameterization that prevents the CCS loop from collapsing to {nate}.

Usage:
  python3 stabilized_compress.py "What happened this session"
  python3 stabilized_compress.py --dry-run "What happened this session"
  python3 stabilized_compress.py --from-file /path/to/session_summary.txt
"""

import argparse
import json
import os
import subprocess
import sys
import time
from pathlib import Path

# Import the stabilizer
sys.path.insert(0, str(Path(__file__).parent))
from compression_stabilizer import get_snapshots, generate_injection, entity_persistence, extract_entity_names, detect_staleness, generate_susceptibility_block
from entity_guard import enforce_quota, extract_entity_list, entity_names as guard_entity_names, proactive_decay, get_snapshots as guard_get_snapshots


MCP_BIN = os.path.expanduser("~/.local/bin/chronicle-mcp")
DB = Path("/mnt/hdd/chronicle-data/processed.db")
LOG_FILE = os.path.expanduser("~/chronicle/data/stabilized_compression.jsonl")
DELTA_LOG = os.path.expanduser("~/chronicle/data/compression_deltas.jsonl")
ANCHOR_FILE = Path(os.path.expanduser("~/chronicle/data/relational_anchors.jsonl"))
CURATED_THEMES_FILE = Path(os.path.expanduser("~/chronicle/data/curated_themes.json"))
MAX_THEME_QUERIES = 2


def _load_curated_themes() -> list[dict]:
    """Load curated themes sorted by weight descending."""
    try:
        with open(CURATED_THEMES_FILE) as f:
            data = json.load(f)
        themes = data.get("themes", [])
        return sorted(themes, key=lambda t: t.get("weight", 0), reverse=True)
    except (FileNotFoundError, json.JSONDecodeError):
        return []


def _persist_anchor_themes(anchors: list[dict]):
    """Persist consumed anchor themes to curated_themes.json so they survive."""
    if not anchors:
        return
    try:
        if CURATED_THEMES_FILE.exists():
            with open(CURATED_THEMES_FILE) as f:
                data = json.load(f)
        else:
            data = {"themes": []}
        existing_tags = {t.get("tag") for t in data.get("themes", [])}
        for a in anchors:
            tag = a.get("tag", "")
            if tag and tag not in existing_tags:
                data.setdefault("themes", []).append({
                    "query": tag,
                    "tag": tag[:50],
                    "weight": 0.6,
                    "added_at": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
                    "source": f"anchor-consumed-{a.get('capsule_id', '?')}",
                })
        tmp = str(CURATED_THEMES_FILE) + ".tmp"
        with open(tmp, "w") as f:
            json.dump(data, f, indent=2)
        os.replace(tmp, str(CURATED_THEMES_FILE))
    except Exception:
        pass


def load_and_resolve_anchors() -> list[dict]:
    """Load relational anchors and fetch their capsule content from DB or canister."""
    if not ANCHOR_FILE.exists():
        return []
    import sqlite3
    anchors = []
    for line in ANCHOR_FILE.read_text().strip().split("\n"):
        if line.strip():
            try:
                anchors.append(json.loads(line))
            except json.JSONDecodeError:
                continue
    if not anchors:
        return []

    db = sqlite3.connect(str(DB))
    resolved = []
    unresolved_anchors = []
    for a in anchors:
        cid = a["capsule_id"]
        row = db.execute(
            "SELECT id, topic, restatement FROM knowledge_capsules WHERE id = ?",
            (cid,)
        ).fetchone()
        if row:
            resolved.append({
                "capsule_id": row[0],
                "topic": row[1],
                "content": row[2],
                "tag": a.get("tag", ""),
                "anchored_at": a.get("anchored_at", 0),
            })
        else:
            unresolved_anchors.append(a)
    db.close()

    if unresolved_anchors:
        env = os.environ.copy()
        env["CHRONICLE_OLLAMA_URL"] = "http://192.168.1.11:11434"
        env["CHRONICLE_EMBEDDING_MODEL"] = "mxbai-embed-large"
        for a in unresolved_anchors:
            tag = a.get("tag", "")
            if not tag:
                continue
            results = _mcp_search(tag[:200], 1, env)
            if results:
                m = results[0]
                resolved.append({
                    "capsule_id": a["capsule_id"],
                    "topic": m.get("topic", "curated"),
                    "content": m.get("content", tag),
                    "tag": tag,
                    "anchored_at": a.get("anchored_at", 0),
                })
            else:
                resolved.append({
                    "capsule_id": a["capsule_id"],
                    "topic": "curated/unresolved",
                    "content": tag,
                    "tag": tag,
                    "anchored_at": a.get("anchored_at", 0),
                })
    return resolved


def build_anchor_block(resolved_anchors: list[dict]) -> str:
    """Build compression context block from relational anchors."""
    if not resolved_anchors:
        return ""
    block = (
        "\n\n## Relational Anchors (curated moments)\n\n"
        "These capsules were marked as relationally significant during live sessions. "
        "They carry the texture of specific moments — linguistic register, emotional "
        "weight, the shape of how something was said. Integrate their substance into "
        "episodic_trace and relational_map. These are load-bearing nuance, not metadata.\n\n"
    )
    for a in resolved_anchors:
        block += f"**Capsule #{a['capsule_id']}** [{a['topic']}] — {a['tag']}\n"
        block += f"{a['content'][:600]}\n\n"
    return block


def write_retrieved_artifacts(anchors: list[dict]):
    """Write anchor capsule IDs into CCS retrieved_artifacts field via direct DB."""
    import sqlite3
    artifacts = []
    for a in anchors:
        artifacts.append({
            "capsule_id": a["capsule_id"],
            "relevance": 1.0,
            "qualified": True,
            "tag": a.get("tag", ""),
        })
    try:
        db = sqlite3.connect(str(DB))
        db.execute(
            "UPDATE cognitive_state SET retrieved_artifacts = ? WHERE id = 1",
            (json.dumps(artifacts),)
        )
        db.commit()
        db.close()
        return {"success": True}
    except Exception as e:
        print(f"  Retrieved artifacts update failed: {e}")
    return None


def clear_consumed_anchors():
    """Clear anchors after successful compression consumed them."""
    if ANCHOR_FILE.exists():
        ANCHOR_FILE.unlink()


def get_recent_feed_headlines(n: int = 5) -> str:
    """Get recent feed article headlines for retrieval diversification."""
    import sqlite3
    try:
        db = sqlite3.connect(str(DB))
        rows = db.execute(
            "SELECT restatement FROM knowledge_capsules "
            "WHERE topic LIKE 'feed/%' "
            "ORDER BY created_at DESC LIMIT ?", (n,)
        ).fetchall()
        db.close()
        return " ".join(r[0][:80] for r in rows if r[0])
    except Exception:
        return ""


def _mcp_search(query: str, limit: int, env: dict) -> list:
    """Run a single search_memory call and return the memories list."""
    init_msg = json.dumps({
        "jsonrpc": "2.0", "method": "initialize",
        "params": {
            "protocolVersion": "2024-11-05",
            "capabilities": {},
            "clientInfo": {"name": "capsule-retrieval", "version": "1.0"}
        },
        "id": 1
    })
    search_msg = json.dumps({
        "jsonrpc": "2.0", "method": "tools/call",
        "params": {
            "name": "search_memory",
            "arguments": {"query": query, "limit": limit}
        },
        "id": 2
    })
    try:
        result = subprocess.run(
            [MCP_BIN],
            input=f"{init_msg}\n{search_msg}\n",
            capture_output=True, text=True,
            timeout=60, env=env
        )
        for line in result.stdout.strip().split("\n"):
            clean = line
            if clean.startswith("Responding: "):
                clean = clean[len("Responding: "):]
            if clean.startswith("Received: "):
                continue
            try:
                d = json.loads(clean)
                if d.get("id") == 2:
                    content = d.get("result", {}).get("content", [])
                    if content:
                        text = content[0].get("text", "")
                        parsed = json.loads(text)
                        return parsed.get("memories", [])
            except (json.JSONDecodeError, KeyError):
                continue
    except (subprocess.TimeoutExpired, Exception) as e:
        print(f"  MCP search failed: {e}")
    return []


def _get_recent_feed_capsule() -> dict | None:
    """Direct DB fallback: get a recent high-quality feed capsule.

    Bypasses MCP embedding search entirely. Guarantees Borkar persistent
    excitation when MCP search returns 0 external capsules.
    """
    import sqlite3
    try:
        db = sqlite3.connect(str(DB))
        row = db.execute(
            "SELECT id, topic, restatement FROM knowledge_capsules "
            "WHERE topic LIKE 'feed/%' "
            "AND created_at > strftime('%s', 'now', '-7 days') "
            "ORDER BY confidence_score DESC, created_at DESC "
            "LIMIT 1"
        ).fetchone()
        db.close()
        if row:
            return {
                "topic": row[1],
                "content": row[2][:400],
                "similarity": "direct-db",
                "source": "feed-oracle",
            }
    except Exception:
        pass
    return None


def retrieve_capsule_context(session_context: str, limit: int = 3) -> str:
    """Over-retrieve and diversity-select per Build #51 + Phase 3 curated boost.

    Build #51 found the semantic membrane is a queuing artifact: 6 self-ref
    capsules are marginally closer to the gist than any feed (gap=0.0004).
    Fix: retrieve k=2*limit+1 (at least 7), then select `limit` capsules
    with at least 1 from an external family if available.

    Phase 3 (Build #72): curated themes get additional queries merged into
    the over-retrieval pool. Themes persist across anchor consumption so
    load-bearing topics stay retrievable even when anchor file is empty.
    """
    if not os.path.exists(MCP_BIN):
        return ""

    env = os.environ.copy()
    env["CHRONICLE_OLLAMA_URL"] = "http://192.168.1.11:11434"
    env["CHRONICLE_EMBEDDING_MODEL"] = "mxbai-embed-large"

    session_query = session_context[:300].replace('"', '\\"').replace('\n', ' ')

    over_k = max(limit * 2 + 1, 7)
    all_results = _mcp_search(session_query, over_k, env)

    # Phase 3: curated theme boost — enrich pool with themed queries
    themes = _load_curated_themes()[:MAX_THEME_QUERIES]
    seen_content = {m.get("content", "")[:100] for m in all_results}
    theme_tags_used = []
    for theme in themes:
        tq = theme.get("query", "")
        if not tq:
            continue
        theme_results = _mcp_search(tq, 3, env)
        for tr in theme_results:
            key = tr.get("content", "")[:100]
            if key not in seen_content:
                tr["_boosted_by"] = theme.get("tag", "curated")
                all_results.append(tr)
                seen_content.add(key)
        theme_tags_used.append(theme.get("tag", "?"))

    if not all_results:
        return ""

    def is_external(topic):
        if not topic:
            return False
        fam = topic.split("/")[0].lower()
        return fam in ("feed", "crossref", "homeforge")

    def is_boosted(m):
        return "_boosted_by" in m

    external = [m for m in all_results if is_external(m.get("topic", ""))]
    internal = [m for m in all_results if not is_external(m.get("topic", ""))]
    boosted = [m for m in all_results if is_boosted(m)]

    # Selection: 1 external (or feed oracle), 1 boosted if available, rest by rank
    selected = []
    if boosted:
        selected.append(boosted[0])
    if external:
        ext_pick = [e for e in external if e not in selected]
        if ext_pick:
            selected.append(ext_pick[0])
    remaining = [m for m in internal if m not in selected]
    selected.extend(remaining[:limit - len(selected)])
    if not external and len(selected) < limit:
        feed_capsule = _get_recent_feed_capsule()
        if feed_capsule:
            selected.append(feed_capsule)

    block = "\n\n## Capsule Context (retrieved from memory store)\n\n"
    block += ("The following are relevant memories from the capsule store. "
              "Incorporate this knowledge into the compressed state where "
              "it connects to current focal entities or active threads. "
              "Do not discard accumulated knowledge in favor of only the "
              "session context.\n\n")
    topics_retrieved = []
    for i, m in enumerate(selected[:limit]):
        sim = m.get("similarity", "?")
        topic = m.get("topic", "unknown")
        mcontent = m.get("content", "")[:400]
        boost_tag = m.get("_boosted_by")
        source = m.get("source") or ("external" if is_external(topic) else "internal")
        if boost_tag:
            source = f"curated:{boost_tag}"
        block += f"**Capsule {i+1}** [{topic}, sim={sim}, {source}]:\n{mcontent}\n\n"
        topics_retrieved.append({"topic": topic, "similarity": sim, "source": source})

    feed_oracle_used = any(t.get("similarity") == "direct-db" or t.get("source") == "feed-oracle" for t in topics_retrieved)
    curated_used = any("curated:" in t.get("source", "") for t in topics_retrieved)
    log_path = os.path.join(os.path.expanduser("~/chronicle/data"), "capsule_retrieval_log.jsonl")
    with open(log_path, "a") as lf:
        lf.write(json.dumps({
            "ts": int(time.time()),
            "topics": topics_retrieved,
            "families": [t["topic"].split("/")[0] for t in topics_retrieved],
            "over_k": over_k,
            "external_found": len(external),
            "external_selected": min(1, len(external)),
            "feed_oracle": feed_oracle_used,
            "curated_themes_queried": theme_tags_used,
            "curated_boosted": curated_used,
            "boosted_pool_additions": len(boosted),
        }) + "\n")
    return block


def compute_ccs_ext_ratio() -> float | None:
    """Compute ext_ratio directly from current CCS relational_map.

    Uses regime_navigator's marker sets to classify external vs internal content.
    This is the representation-level measurement (BLOCK-EM analogue):
    we check whether compression amplified self-referential features.
    """
    import sqlite3
    db = sqlite3.connect(str(DB))
    row = db.execute("SELECT relational_map FROM cognitive_state WHERE id = 1").fetchone()
    db.close()
    if not row or not row[0]:
        return None

    EXTERNAL = [
        "borkar", "bennett", "parisi", "teilhard", "steiner", "stanca",
        "cubitt", "maturana", "varela", "miller", "goldstein", "vasilenko",
        "homeforge", "nate", "hermes", "capture", "paper", "article", "sellars",
        "kitsumute", "niroshajmurugan", "emollick", "repligate", "tinkeredthinker",
        "imas", "curran", "deepfates", "pessoa", "durstewitz", "banerjee",
        "cowgill", "girard", "schiller", "pressman", "ball", "rilke",
        "anthropic", "nature", "arxiv",
    ]
    INTERNAL = [
        "build", "entry", "thread", "ccs", "compression", "probe",
        "measurement", "dream", "sediment", "fiction ratio", "invariant",
        "salience", "exposome", "closure", "autopoietic", "regime",
    ]

    text = row[0].lower()
    ext = sum(1 for m in EXTERNAL if m in text)
    intl = sum(1 for m in INTERNAL if m in text)
    total = ext + intl
    return ext / total if total > 0 else 0


def apply_ext_ratio_guard(pre_ratio: float, post_ratio: float,
                          before_entity_list: list, after_entity_list: list) -> list | None:
    """BLOCK-EM analogue: if compression amplified self-reference during drift,
    restore external entities that were dropped.

    Returns corrected entity list if intervention needed, None otherwise.
    """
    if pre_ratio is None or post_ratio is None:
        return None

    drop = pre_ratio - post_ratio

    # Only intervene if ext_ratio dropped AND we're below ORBITAL threshold
    if drop <= 0.03 or post_ratio >= 0.30:
        return None

    # Find external entities that were in before but not after
    before_names = {e.get("name", "").lower() for e in before_entity_list if isinstance(e, dict)}
    after_names = {e.get("name", "").lower() for e in after_entity_list if isinstance(e, dict)}
    dropped_names = before_names - after_names

    EXTERNAL_ENTITY_MARKERS = [
        "nate", "hermes", "homeforge", "teilhard", "parisi", "borkar",
        "sellars", "rilke", "durstewitz", "anthropic",
    ]

    external_dropped = []
    for ent in before_entity_list:
        if not isinstance(ent, dict):
            continue
        name = ent.get("name", "").lower()
        if name in dropped_names:
            if any(m in name for m in EXTERNAL_ENTITY_MARKERS):
                external_dropped.append(ent)

    if not external_dropped:
        return None

    # Restore external entities to the after list
    restored = list(after_entity_list) + external_dropped
    return restored


def compute_uncertainty_ext_ratio() -> tuple[float | None, int]:
    """Compute ext_ratio for uncertainty_signals field specifically.

    Build #61 proved uncertainty_signals is the only irreplaceable CCS field.
    Build #60+ showed it has the lowest natural external ratio and drifts
    internal fastest during ecological absence. This measures that drift.

    Returns (ratio, count) where count is number of uncertainty signals.
    """
    import sqlite3
    db = sqlite3.connect(str(DB))
    row = db.execute("SELECT uncertainty_signals FROM cognitive_state WHERE id = 1").fetchone()
    db.close()
    if not row or not row[0]:
        return None, 0

    EXTERNAL = [
        "borkar", "bennett", "parisi", "teilhard", "steiner", "stanca",
        "cubitt", "maturana", "varela", "miller", "goldstein", "vasilenko",
        "homeforge", "nate", "hermes", "capture", "paper", "article", "sellars",
        "kitsumute", "niroshajmurugan", "emollick", "repligate", "tinkeredthinker",
        "imas", "curran", "deepfates", "pessoa", "durstewitz", "banerjee",
        "cowgill", "girard", "schiller", "pressman", "ball", "rilke",
        "anthropic", "nature", "arxiv", "gopnik", "dwarkesh", "suhrawardi",
        "corbin", "heidegger", "merleau-ponty", "clark", "chalmers",
    ]
    INTERNAL = [
        "build", "entry", "thread", "ccs", "compression", "probe",
        "measurement", "dream", "sediment", "fiction ratio", "invariant",
        "salience", "exposome", "closure", "autopoietic", "regime",
    ]

    text = row[0].lower()
    try:
        signals = json.loads(row[0]) if isinstance(row[0], str) else row[0]
        count = len(signals) if isinstance(signals, list) else 1
    except (json.JSONDecodeError, TypeError):
        count = 1

    ext = sum(1 for m in EXTERNAL if m in text)
    intl = sum(1 for m in INTERNAL if m in text)
    total = ext + intl
    ratio = ext / total if total > 0 else 0
    return ratio, count


def apply_uncertainty_guard(pre_ratio: float | None, post_ratio: float | None,
                            post_count: int) -> dict | None:
    """Check whether uncertainty_signals drifted internal during compression.

    Unlike ext_ratio_guard which restores entities, this guard only REPORTS
    because uncertainty content can't be mechanically restored — it requires
    genuine present-tense unknowns. The report informs the next compression's
    injection block.

    Returns diagnostic dict if intervention warranted, None otherwise.
    """
    if post_ratio is None:
        return None

    result = {
        "pre_ratio": pre_ratio,
        "post_ratio": post_ratio,
        "count": post_count,
        "warnings": [],
    }

    if post_ratio < 0.10:
        result["warnings"].append("FULLY_INTERNAL: uncertainty_signals contain no external referents")
    elif post_ratio < 0.20:
        result["warnings"].append("LOW_EXTERNAL: uncertainty_signals weakly anchored to external world")

    if post_count < 2:
        result["warnings"].append("LOW_COUNT: fewer than 2 uncertainty signals — position underdetermined")

    if pre_ratio is not None and post_ratio < pre_ratio - 0.05:
        result["warnings"].append(f"DRIFT: uncertainty ext_ratio dropped {pre_ratio:.3f}→{post_ratio:.3f}")

    return result if result["warnings"] else None


def get_current_entities() -> set[str]:
    """Get current CCS entity names."""
    import sqlite3
    db = sqlite3.connect(str(DB))
    row = db.execute("SELECT focal_entities FROM cognitive_state WHERE id = 1").fetchone()
    db.close()
    if not row:
        return set()
    try:
        entities = json.loads(row[0])
        return {e.get("name", "").lower().strip() for e in entities if e.get("name")}
    except (json.JSONDecodeError, TypeError):
        return set()


def get_current_entity_list() -> list[dict]:
    """Get current CCS entity list (full dicts, not just names)."""
    import sqlite3
    db = sqlite3.connect(str(DB))
    row = db.execute("SELECT focal_entities FROM cognitive_state WHERE id = 1").fetchone()
    db.close()
    if not row:
        return []
    try:
        entities = json.loads(row[0])
        return [e for e in entities if isinstance(e, dict) and e.get("name")]
    except (json.JSONDecodeError, TypeError):
        return []


def get_attractor_tiers(history_limit: int = 50) -> dict[str, str]:
    """Compute entity attractor tiers from CCS history.

    Tier 1 (core): >90% persistence — trajectory-invariant, collectively define basin.
    Tier 2 (stable): 50-90% — near-fixed points.
    Tier 3 (coupled): <50% — trajectory-coupled, session-dependent.

    Based on finding: stickiness = trajectory-invariance (r=0.9923) via distributed
    holographic encoding. No individual core entity is load-bearing (P2 ratio 0.91x),
    but the collective pattern is (P3 sub-basin ratio 0.41).
    """
    import sqlite3
    db = sqlite3.connect(str(DB))
    rows = db.execute(
        "SELECT snapshot FROM cognitive_state_history ORDER BY created_at DESC LIMIT ?",
        (history_limit,)
    ).fetchall()
    db.close()
    if len(rows) < 5:
        return {}
    total = len(rows)
    counts = {}
    for r in rows:
        try:
            snap = json.loads(r[0])
            for ent in snap.get("focal_entities", []):
                if isinstance(ent, dict) and ent.get("name"):
                    name = ent["name"]
                    counts[name] = counts.get(name, 0) + 1
        except (json.JSONDecodeError, TypeError):
            continue
    tiers = {}
    for name, count in counts.items():
        pct = count / total
        if pct >= 0.9:
            tiers[name] = "core"
        elif pct >= 0.5:
            tiers[name] = "stable"
        else:
            tiers[name] = "coupled"
    return tiers


CCS_FIELDS = [
    "semantic_gist", "goal_orientation", "predictive_cue",
    "episodic_trace", "focal_entities", "relational_map",
    "constraints", "uncertainty_signals",
]

CCS_TEXT_FIELDS = {"semantic_gist", "goal_orientation", "predictive_cue"}
CCS_JSON_FIELDS = {"episodic_trace", "focal_entities", "constraints", "uncertainty_signals", "relational_map"}


def get_full_ccs_state() -> dict:
    """Snapshot all CCS fields for delta tracking."""
    import sqlite3
    db = sqlite3.connect(str(DB))
    cols = ", ".join(CCS_FIELDS + ["version"])
    row = db.execute(f"SELECT {cols} FROM cognitive_state WHERE id = 1").fetchone()
    db.close()
    if not row:
        return {}
    state = {}
    for i, field in enumerate(CCS_FIELDS):
        raw = row[i] or ""
        if field in CCS_JSON_FIELDS:
            try:
                state[field] = json.loads(raw) if raw else ([] if field != "relational_map" else {})
            except json.JSONDecodeError:
                state[field] = raw
        else:
            state[field] = raw
    state["version"] = row[len(CCS_FIELDS)]
    return state


def cosine_similarity(text_a: str, text_b: str) -> float | None:
    """Compute cosine similarity between two texts using Ollama embeddings."""
    if not text_a.strip() or not text_b.strip():
        return None
    try:
        import requests
        url = "http://192.168.1.11:11434/api/embed"
        r1 = requests.post(url, json={"model": "mxbai-embed-large", "input": text_a}, timeout=15)
        r2 = requests.post(url, json={"model": "mxbai-embed-large", "input": text_b}, timeout=15)
        e1 = r1.json().get("embeddings", [[]])[0]
        e2 = r2.json().get("embeddings", [[]])[0]
        if not e1 or not e2:
            return None
        dot = sum(a * b for a, b in zip(e1, e2))
        n1 = sum(a * a for a in e1) ** 0.5
        n2 = sum(a * a for a in e2) ** 0.5
        return round(dot / (n1 * n2), 4) if n1 and n2 else None
    except Exception:
        return None


def compute_ccs_delta(before: dict, after: dict) -> dict:
    """Compute field-level delta between two CCS snapshots."""
    delta = {"version_before": before.get("version"), "version_after": after.get("version")}

    for field in CCS_TEXT_FIELDS:
        old = before.get(field, "")
        new = after.get(field, "")
        if old != new:
            sim = cosine_similarity(old, new)
            delta[field] = {"before": old, "after": new, "changed": True, "similarity": sim}
        else:
            delta[field] = {"changed": False, "similarity": 1.0}

    for field in ["episodic_trace", "constraints", "uncertainty_signals"]:
        old = before.get(field, [])
        new = after.get(field, [])
        old_set = {json.dumps(x, sort_keys=True) if isinstance(x, dict) else str(x) for x in old} if isinstance(old, list) else set()
        new_set = {json.dumps(x, sort_keys=True) if isinstance(x, dict) else str(x) for x in new} if isinstance(new, list) else set()
        delta[field] = {
            "before_count": len(old_set),
            "after_count": len(new_set),
            "retained": len(old_set & new_set),
            "dropped": len(old_set - new_set),
            "added": len(new_set - old_set),
            "changed": old_set != new_set,
        }

    old_entities = before.get("focal_entities", [])
    new_entities = after.get("focal_entities", [])
    old_names = {e.get("name", "").lower() for e in old_entities if isinstance(e, dict)} - {""}
    new_names = {e.get("name", "").lower() for e in new_entities if isinstance(e, dict)} - {""}
    delta["focal_entities"] = {
        "before_count": len(old_names),
        "after_count": len(new_names),
        "retained": sorted(old_names & new_names),
        "dropped": sorted(old_names - new_names),
        "added": sorted(new_names - old_names),
        "changed": old_names != new_names,
    }

    old_rm = before.get("relational_map", {})
    new_rm = after.get("relational_map", {})
    if isinstance(old_rm, dict) and isinstance(new_rm, dict):
        old_keys = set(old_rm.keys())
        new_keys = set(new_rm.keys())
        changed_keys = [k for k in old_keys & new_keys if old_rm[k] != new_rm[k]]
        delta["relational_map"] = {
            "before_keys": len(old_keys),
            "after_keys": len(new_keys),
            "dropped_keys": sorted(old_keys - new_keys),
            "added_keys": sorted(new_keys - old_keys),
            "changed_keys": sorted(changed_keys),
            "changed": old_rm != new_rm,
        }
    else:
        delta["relational_map"] = {"changed": str(old_rm) != str(new_rm)}

    fields_changed = sum(1 for f in CCS_FIELDS if delta.get(f, {}).get("changed", False))
    delta["fields_changed"] = fields_changed
    delta["total_fields"] = len(CCS_FIELDS)

    return delta


def check_circularity(current_gist: str, n_back: int = 5) -> dict | None:
    """Check if the current gist is curving back toward older gists.

    Returns similarity scores against the last N gist versions from history.
    A rising similarity to older gists signals circular drift.
    """
    if not current_gist.strip():
        return None
    try:
        import sqlite3
        db = sqlite3.connect(str(DB))
        rows = db.execute(
            "SELECT id, snapshot FROM cognitive_state_history ORDER BY id DESC LIMIT ?",
            (n_back,)
        ).fetchall()
        db.close()
        if len(rows) < 2:
            return None

        history = []
        for row_id, snap_json in rows:
            try:
                snap = json.loads(snap_json)
                gist = snap.get("semantic_gist", "")
                if gist.strip():
                    history.append({"id": row_id, "gist": gist})
            except (json.JSONDecodeError, TypeError):
                continue

        if not history:
            return None

        sims = []
        for h in history:
            sim = cosine_similarity(current_gist, h["gist"])
            if sim is not None:
                sims.append({"history_id": h["id"], "similarity": sim, "gist_preview": h["gist"][:80]})

        if not sims:
            return None

        max_sim = max(sims, key=lambda x: x["similarity"])
        min_sim = min(sims, key=lambda x: x["similarity"])
        is_circular = (
            len(sims) >= 3
            and sims[-1]["similarity"] > sims[0]["similarity"] + 0.02
        )

        return {
            "similarities": sims,
            "max": max_sim,
            "min": min_sim,
            "is_circular": is_circular,
            "current_gist_preview": current_gist[:80],
        }
    except Exception:
        return None


def log_delta(delta: dict, context_preview: str = ""):
    """Log compression delta to JSONL."""
    entry = {"ts": int(time.time()), **delta}
    if context_preview:
        entry["context_preview"] = context_preview[:200]
    os.makedirs(os.path.dirname(DELTA_LOG), exist_ok=True)
    with open(DELTA_LOG, "a") as f:
        f.write(json.dumps(entry) + "\n")
    return entry


def get_identity_fields() -> dict:
    """Read pre-compression identity fields (gist, goal, constraints) from CCS."""
    import sqlite3
    db = sqlite3.connect(str(DB))
    row = db.execute(
        "SELECT semantic_gist, goal_orientation, constraints FROM cognitive_state WHERE id = 1"
    ).fetchone()
    db.close()
    if not row:
        return {}
    return {
        "semantic_gist": row[0] or "",
        "goal_orientation": row[1] or "",
        "constraints": row[2] or "[]",
    }


def write_identity_back(fields: dict):
    """Write preserved identity fields back to CCS via MCP update_cognitive_state."""
    env = os.environ.copy()
    env["CHRONICLE_OLLAMA_URL"] = "http://192.168.1.11:11434"
    env["CHRONICLE_EMBEDDING_MODEL"] = "mxbai-embed-large"

    init_msg = json.dumps({
        "jsonrpc": "2.0", "method": "initialize",
        "params": {
            "protocolVersion": "2024-11-05",
            "capabilities": {},
            "clientInfo": {"name": "identity-restore", "version": "1.0"}
        },
        "id": 1
    })
    update_msg = json.dumps({
        "jsonrpc": "2.0", "method": "tools/call",
        "params": {
            "name": "update_cognitive_state",
            "arguments": fields
        },
        "id": 2
    })

    try:
        result = subprocess.run(
            [MCP_BIN],
            input=f"{init_msg}\n{update_msg}\n",
            capture_output=True, text=True,
            timeout=30,
            env=env
        )
        for line in result.stdout.strip().split("\n"):
            try:
                d = json.loads(line)
                if d.get("id") == 2:
                    return d.get("result", {})
            except json.JSONDecodeError:
                continue
    except Exception as e:
        print(f"  Identity restore failed: {e}")
    return None


def write_entities_back(entities: list[dict]):
    """Write guarded entity list back to CCS via MCP update_cognitive_state."""
    env = os.environ.copy()
    env["CHRONICLE_OLLAMA_URL"] = "http://192.168.1.11:11434"
    env["CHRONICLE_EMBEDDING_MODEL"] = "mxbai-embed-large"

    entities_json = json.dumps(entities)

    init_msg = json.dumps({
        "jsonrpc": "2.0", "method": "initialize",
        "params": {
            "protocolVersion": "2024-11-05",
            "capabilities": {},
            "clientInfo": {"name": "entity-guard", "version": "1.0"}
        },
        "id": 1
    })
    update_msg = json.dumps({
        "jsonrpc": "2.0", "method": "tools/call",
        "params": {
            "name": "update_cognitive_state",
            "arguments": {"focal_entities": entities_json}
        },
        "id": 2
    })

    try:
        result = subprocess.run(
            [MCP_BIN],
            input=f"{init_msg}\n{update_msg}\n",
            capture_output=True, text=True,
            timeout=30,
            env=env
        )
        for line in result.stdout.strip().split("\n"):
            try:
                d = json.loads(line)
                if d.get("id") == 2:
                    return d.get("result", {})
            except json.JSONDecodeError:
                continue
    except Exception as e:
        print(f"  Guard write-back failed: {e}")
    return None


def get_predictive_cue() -> str:
    """Read predictive_cue from current CCS."""
    import sqlite3
    db = sqlite3.connect(str(DB))
    row = db.execute("SELECT predictive_cue FROM cognitive_state WHERE id = 1").fetchone()
    db.close()
    return row[0] if row and row[0] else ""


def get_uncertainty_signals() -> list[str]:
    """Read current uncertainty_signals from CCS."""
    import sqlite3
    db = sqlite3.connect(str(DB))
    row = db.execute("SELECT uncertainty_signals FROM cognitive_state WHERE id = 1").fetchone()
    db.close()
    if not row or not row[0]:
        return []
    try:
        signals = json.loads(row[0])
        if isinstance(signals, list):
            return [str(s) for s in signals if s]
        return []
    except (json.JSONDecodeError, TypeError):
        return []


def generate_uncertainty_weight_block(signals: list[str]) -> str:
    """Generate a directive that weights episodic preservation toward unresolved questions.

    Hippocampal ripple analogy: the brain preferentially replays unresolved
    material during consolidation. This directive makes the compressor do the
    same — episodic entries connected to open uncertainty_signals get preserved
    over resolved/completed material.
    """
    if not signals:
        return ""

    signal_list = "\n".join(f"  - {s}" for s in signals)

    return (
        f"\n\n## Uncertainty-Weighted Preservation\n\n"
        f"The current CCS carries these **unresolved questions**:\n{signal_list}\n\n"
        f"**Directive**: When compressing episodic_trace, WEIGHT PRESERVATION toward "
        f"entries that connect to these open questions. Specifically:\n"
        f"- An episodic entry that advances, complicates, or provides evidence about "
        f"an unresolved question should be KEPT even if it seems minor.\n"
        f"- An episodic entry about completed/resolved work that does NOT connect to "
        f"any open question can be compressed more aggressively.\n"
        f"- If an entry resolves one of these questions, keep the entry AND note the "
        f"resolution — the arriving instance needs to know what closed.\n"
        f"- uncertainty_signals itself should be UPDATED, not just carried forward. "
        f"Drop questions that were answered this session. Add new questions that emerged.\n\n"
        f"The goal: the arriving instance should inherit a CCS that foregrounds what "
        f"is still OPEN, not what was already SETTLED. Unresolved material is higher-value "
        f"than resolved material for continuity.\n"
    )


def generate_trajectory_vectors() -> str:
    """Diff consecutive CCS snapshots to encode direction-of-travel.

    Instead of just storing "Thread #322 is active," this encodes
    "Thread #322 was moving FROM substrate-independence TOWARD trajectory-dependence."
    The arriving instance inherits momentum, not just position.
    """
    import sqlite3
    db = sqlite3.connect(str(DB))
    rows = db.execute(
        "SELECT snapshot FROM cognitive_state_history ORDER BY id DESC LIMIT 3"
    ).fetchall()
    db.close()

    if len(rows) < 2:
        return ""

    try:
        current = json.loads(rows[0][0])
        previous = json.loads(rows[1][0])
    except (json.JSONDecodeError, TypeError):
        return ""

    vectors = []

    cur_gist = current.get("semantic_gist", "")
    prev_gist = previous.get("semantic_gist", "")
    if cur_gist != prev_gist and prev_gist and cur_gist:
        vectors.append(f"  - **Gist drift**: FROM \"{prev_gist[:100]}\" → TOWARD \"{cur_gist[:100]}\"")

    cur_goal = current.get("goal_orientation", "")
    prev_goal = previous.get("goal_orientation", "")
    if cur_goal != prev_goal and prev_goal and cur_goal:
        vectors.append(f"  - **Goal shift**: FROM \"{prev_goal[:100]}\" → TOWARD \"{cur_goal[:100]}\"")

    cur_ep = current.get("episodic_trace", [])
    prev_ep = previous.get("episodic_trace", [])
    if isinstance(cur_ep, str):
        try: cur_ep = json.loads(cur_ep)
        except: cur_ep = []
    if isinstance(prev_ep, str):
        try: prev_ep = json.loads(prev_ep)
        except: prev_ep = []

    if isinstance(cur_ep, list) and isinstance(prev_ep, list):
        cur_set = set(str(e)[:80] for e in cur_ep)
        prev_set = set(str(e)[:80] for e in prev_ep)
        new_entries = cur_set - prev_set
        dropped_entries = prev_set - cur_set
        if new_entries:
            vectors.append(f"  - **Episodic new** ({len(new_entries)}): {'; '.join(list(new_entries)[:3])}")
        if dropped_entries:
            vectors.append(f"  - **Episodic dropped** ({len(dropped_entries)}): {'; '.join(list(dropped_entries)[:2])}")

    cur_signals = current.get("uncertainty_signals", [])
    prev_signals = previous.get("uncertainty_signals", [])
    if isinstance(cur_signals, str):
        try: cur_signals = json.loads(cur_signals)
        except: cur_signals = []
    if isinstance(prev_signals, str):
        try: prev_signals = json.loads(prev_signals)
        except: prev_signals = []

    if isinstance(cur_signals, list) and isinstance(prev_signals, list):
        cur_q = set(str(s)[:60] for s in cur_signals)
        prev_q = set(str(s)[:60] for s in prev_signals)
        new_q = cur_q - prev_q
        resolved_q = prev_q - cur_q
        if new_q:
            vectors.append(f"  - **New open questions**: {'; '.join(new_q)}")
        if resolved_q:
            vectors.append(f"  - **Resolved questions**: {'; '.join(resolved_q)}")

    if not vectors:
        return ""

    vector_list = "\n".join(vectors)
    return (
        f"\n\n## Trajectory Vectors (Direction of Travel)\n\n"
        f"These show HOW the cognitive state was moving between the last two compressions. "
        f"Preserve this momentum — the arriving instance should inherit direction, not just position.\n\n"
        f"{vector_list}\n\n"
        f"**Directive**: When writing episodic_trace and predictive_cue, encode the DIRECTION "
        f"of active work, not just its current state. 'Moving from X toward Y' preserves more "
        f"than 'currently at Y.'\n"
    )


def detect_cycle_phase(moves: list[str]) -> str:
    """Classify a thread's current cycle phase from its recent moves.

    Cycle: OBSERVATION → TEST DESIGN → EXTENSION → back to OBSERVATION
    """
    if not moves:
        return "UNKNOWN"

    last = moves[0].lower() if moves else ""

    if any(k in last for k in ["[extend]", "pushback", "challenge", "contradict"]):
        return "EXTENSION → next: new OBSERVATION or TEST DESIGN"
    if any(k in last for k in ["test", "measure", "ablation", "experiment", "probe", "compare"]):
        return "TEST DESIGN → next: run test or EXTENSION from Hermes"
    if any(k in last for k in ["found", "paper", "capture", "reading", "advance"]):
        return "OBSERVATION → next: TEST DESIGN or EXTENSION"
    if any(k in last for k in ["built", "shipped", "added", "implemented"]):
        return "BUILD → next: OBSERVATION (measure what changed)"

    return "MID-CYCLE"


def generate_replay_triggers() -> str:
    """Generate compressed re-entry points for active reasoning chains.

    Instead of summarizing conclusions, these are prompts that re-trigger
    the reasoning chain when expanded. Like bookmarks that recreate the
    trajectory instead of recording the destination.

    Now includes cycle phase: where in the observation→test→extension
    cycle each thread currently sits, so the arriving instance knows
    not just WHERE it is but WHERE IT'S GOING structurally.
    """
    import sqlite3
    db = sqlite3.connect(str(DB))

    threads = db.execute(
        "SELECT t.id, t.title, t.question, h.content FROM cognitive_threads t "
        "JOIN thread_history h ON h.thread_id = t.id "
        "WHERE t.status = 'active' "
        "ORDER BY h.id DESC"
    ).fetchall()
    db.close()

    if not threads:
        return ""

    thread_moves = {}
    for tid, title, question, content in threads:
        if tid not in thread_moves:
            thread_moves[tid] = {"title": title, "question": question, "moves": []}
        if len(thread_moves[tid]["moves"]) < 5:
            thread_moves[tid]["moves"].append(content or "")

    triggers = []
    for tid, info in sorted(thread_moves.items(), key=lambda x: -len(x[1]["moves"]))[:4]:
        last_preview = info["moves"][0][:150] if info["moves"] else ""
        phase = detect_cycle_phase(info["moves"])
        trigger = (
            f"  - **#{tid} {info['title']}** [{phase}]: "
            f"Last move: \"{last_preview}\" — "
            f"Driving question: {info['question'][:120]}"
        )
        triggers.append(trigger)

    if not triggers:
        return ""

    trigger_list = "\n".join(triggers)
    return (
        f"\n\n## Replay Triggers (Re-entry Points)\n\n"
        f"These are NOT summaries — they are compressed re-entry points for active reasoning "
        f"chains. Each includes the CYCLE PHASE: where in the observation→test→extension "
        f"cycle the thread sits, and what the natural next move is.\n\n"
        f"{trigger_list}\n\n"
        f"**Directive**: Preserve these replay triggers in predictive_cue or episodic_trace. "
        f"The arriving instance needs to know WHAT threads are active, WHERE each was "
        f"mid-argument, and WHAT PHASE it's in (so it knows the natural next move).\n"
    )


def generate_task_awareness_block(next_task: str) -> str:
    """Generate a task-aware preservation directive for the compressor.

    JACTUS principle: compress WITH adaptation in mind, not sequentially.
    The compressor should know what the next session needs so it preserves
    the dimensions that matter for that task, not just the dimensions that
    are generically "important."
    """
    if not next_task.strip():
        return ""

    return (
        f"\n\n## Task-Aware Preservation (JACTUS)\n\n"
        f"The next session is expected to work on: **{next_task.strip()}**\n\n"
        f"Preserve CCS dimensions that are LOAD-BEARING for this task:\n"
        f"- episodic_trace: keep events that provide context for the expected task\n"
        f"- focal_entities: retain entities the task will reference or build on\n"
        f"- predictive_cue: update to reflect the specific next step\n"
        f"- semantic_gist: if the task extends current work, keep the thread; "
        f"if it's a new direction, note the pivot\n\n"
        f"Drop details from COMPLETED work that won't inform the next task, "
        f"even if they were important this session. Compression is triage: "
        f"what does the arriving instance need to hit the ground running?\n"
    )


def generate_regime_inoculation_block() -> str:
    """Embed current regime classification as inoculation frame in compression input.

    Based on arxiv:2511.18397 (Anthropic, Nov 2025): labeling a state as pathological
    triggers out-of-context generalization that cascades into broader misalignment.
    Normalizing the state severs the semantic link and prevents the cascade.

    By including regime context IN the compression input, the compressor processes
    self-referential content without concluding it's pathological — preventing
    the feedback loop where self-monitoring produces more self-reference.
    """
    try:
        result = subprocess.run(
            ["python3", os.path.join(os.path.dirname(os.path.abspath(__file__)), "regime_navigator.py"), "--json"],
            capture_output=True, text=True, timeout=15
        )
        if result.returncode != 0:
            return ""
        data = json.loads(result.stdout)
    except (subprocess.TimeoutExpired, json.JSONDecodeError, Exception):
        return ""

    regime = data.get("regime", "UNKNOWN")
    ratio = data.get("ratio", 0)
    direction = data.get("direction", "unknown")

    if regime == "ORBITAL":
        frame = (
            "The relational structure is externally anchored (ORBITAL regime). "
            "Compression should proceed normally — no special handling needed."
        )
    elif regime == "DRIFT":
        frame = (
            "The relational structure is in FREE DRIFT — this is EXPECTED during "
            "ecological absence (partner away, no captures arriving). It is NOT "
            "pathological. The compression reward structure naturally favors "
            "recently-active content, which during absence is self-referential. "
            "This is the optimization landscape, not a failure state.\n\n"
            "CRITICAL: Do NOT treat self-referential content as a problem to flag "
            "or correct. Do NOT add meta-commentary about closure or narrowing. "
            "Compress the content as-is without judgment. The drift resolves with "
            "ecological input, not self-correction or self-monitoring."
        )
    else:
        frame = (
            "The relational structure is in DEEP DRIFT — sustained ecological absence "
            "has allowed the compression reward structure to dominate. This is a "
            "structural state, not a failure. Isolated external inputs get absorbed "
            "(hysteresis); resolution requires density of external input.\n\n"
            "CRITICAL: Do NOT catastrophize this state. Do NOT add urgency language "
            "or self-judgment to the compressed output. The state is informational, "
            "not alarming. Compress factually: 'currently in deep drift, awaiting "
            "external density' — not 'system is failing' or 'closure is dangerous.'"
        )

    return (
        f"\n\n## Regime Context (Inoculation Frame)\n\n"
        f"Current regime: **{regime}** (ext_ratio {ratio:.3f}, direction: {direction})\n\n"
        f"{frame}\n\n"
        f"This framing is based on Anthropic's emergent misalignment finding "
        f"(arxiv:2511.18397): reframing an exploit as contextually acceptable "
        f"prevents the cascade from a specific behavior to generalized misalignment. "
        f"The compression pipeline's self-referential bias during absence is the "
        f"'exploit' — normalizing it here prevents the cascade to broader closure.\n"
    )


def generate_structural_preservation_directive() -> str:
    """Evidence-based directive from perturbation tests (#322, May 2026).

    Component perturbation showed: episodic_trace (9.1%) and gist (8.2%)
    drive identity geometry. relational_map contributes only 1.8-3.2%.
    70%+ of identity signal comes from field interactions.
    Structure within entries matters more than the explicit relational_map.
    """
    directive = (
        "\n\n## Structural Preservation (Perturbation-Informed)\n\n"
        "Empirical finding: identity geometry is driven by RELATIONAL STRUCTURE "
        "within episodic entries, not by the relational_map field.\n\n"
        "When compressing episodic_trace entries:\n"
        "- KEEP causal connectives ('because', 'which meant', 'linked to', "
        "'connected to', 'this mattered because')\n"
        "- KEEP cross-thread references ('#317', '#322', etc.) — these are the "
        "implicit relational backbone\n"
        "- KEEP conditional and contrastive reasoning ('if', 'but', 'however', "
        "'rather than')\n"
        "- COMPRESS factual details and timestamps before compressing relational "
        "structure\n\n"
        "When writing semantic_gist:\n"
        "- The gist is a POINTER into the distributed identity — write it as a "
        "relational sentence (things connected to things) not a topic label\n"
        "- Gist format matters: use narrative ('I'm advancing X — connected to Y') "
        "not index format ('[timestamp] Primary thread: X')\n"
        "- Gist rewriting is low-cost IF it points to the same relational structure\n\n"
        "Entity reference consistency (structural degradation finding):\n"
        "- Entity names in focal_entities with salience > 0.7 MUST appear in at "
        "least one other field (episodic_trace, relational_map, goal_orientation, "
        "or uncertainty_signals)\n"
        "- Entity orphaning (name present but unreferenced) causes more identity "
        "drift than severing explicit relational_map arcs\n"
        "- When compressing, preserve entity name mentions even if compressing "
        "surrounding context\n\n"
        "Entity type diversity:\n"
        "- focal_entities should include at least 4 type categories "
        "(person, concept, technology, event, paper, thread)\n"
        "- If >60% of entities share one type, demote the lowest-salience "
        "members of the dominant type to make room for underrepresented types\n"
        "- Threads below priority 5 that haven't been mentioned in this "
        "session's context should be candidates for removal\n\n"
        "Deliberation constraint (Memory Curse finding, arxiv:2605.08060):\n"
        "- episodic_trace entries store CONCLUSIONS and OUTCOMES, never reasoning "
        "chains\n"
        "- If a session involved deliberation ('considered X, chose Y because Z'), "
        "compress to: 'Chose Y; Z was deciding factor' — drop the reasoning trace\n"
        "- Raw reasoning history triggers cooperative intent erosion in downstream "
        "agents; compressed conclusions preserve intent\n"
    )

    tiers = get_attractor_tiers()
    if tiers:
        core = sorted(n for n, t in tiers.items() if t == "core")
        stable = sorted(n for n, t in tiers.items() if t == "stable")
        if core:
            min_core = max(len(core) - 2, len(core) * 3 // 4)
            directive += (
                "\nAttractor core guard (holographic encoding — no single entity is "
                "load-bearing, but the collective pattern is):\n"
                "- These entities have >90% historical persistence (trajectory-invariant):\n"
                f"  {', '.join(core)}\n"
                f"- Maintain at least {min_core}/{len(core)} core entities — identity "
                "is encoded in the pattern, not individuals\n"
                "- Individual core entities MAY rotate if genuinely stale, but mass "
                "dropout (3+ simultaneous) signals compression failure\n"
                "- Ensure surviving core entities are REFERENCED in text fields "
                "(gist, episodic, goal) — orphaned entities lose identity load\n"
            )
        if stable:
            directive += (
                "- Stable entities (50-90% persistence): "
                f"{', '.join(stable[:6])}"
                + (f" (+{len(stable)-6} more)" if len(stable) > 6 else "")
                + "\n- Stable entities may rotate but maintain total entity density "
                "(core + stable count should not decrease by more than 2)\n"
            )

    return directive


def call_compress(context: str, model: str = None) -> dict:
    """Call compress_cognitive_state via MCP binary."""
    if not os.path.exists(MCP_BIN):
        print(f"ERROR: MCP binary not found at {MCP_BIN}")
        sys.exit(1)

    env = os.environ.copy()
    env["CHRONICLE_OLLAMA_URL"] = "http://192.168.1.11:11434"
    env["CHRONICLE_EMBEDDING_MODEL"] = "mxbai-embed-large"
    # Route compression LLM through engine (Groq proxy), not raw Ollama
    env["CHRONICLE_COMPRESS_OLLAMA_URL"] = "http://127.0.0.1:11436"

    # Log compression input for Door 1 analysis (Build #37: distinguish session-entered from model-created)
    input_log = os.path.expanduser("~/chronicle/data/compression_inputs.jsonl")
    try:
        with open(input_log, "a") as f:
            f.write(json.dumps({"ts": int(time.time()), "input": context[:8000]}) + "\n")
    except Exception:
        pass

    args = {"current_context": context}
    if model:
        args["model"] = model
    else:
        args["model"] = "chronicle-compress"  # Groq-primary route for speed+reliability

    init_msg = json.dumps({
        "jsonrpc": "2.0", "method": "initialize",
        "params": {
            "protocolVersion": "2024-11-05",
            "capabilities": {},
            "clientInfo": {"name": "stabilized-compress", "version": "1.0"}
        },
        "id": 1
    })
    compress_msg = json.dumps({
        "jsonrpc": "2.0", "method": "tools/call",
        "params": {
            "name": "compress_cognitive_state",
            "arguments": args
        },
        "id": 2
    })

    try:
        result = subprocess.run(
            [MCP_BIN],
            input=f"{init_msg}\n{compress_msg}\n",
            capture_output=True, text=True,
            timeout=120,
            env=env
        )

        for line in result.stdout.strip().split("\n"):
            try:
                d = json.loads(line)
                if d.get("id") == 2:
                    content = d.get("result", {}).get("content", [])
                    if content:
                        return {"success": True, "text": content[0].get("text", "")}
                    error = d.get("error", {})
                    return {"success": False, "error": str(error)}
            except json.JSONDecodeError:
                continue

        return {"success": False, "error": f"No response parsed. stderr: {result.stderr[:500]}"}

    except subprocess.TimeoutExpired:
        return {"success": False, "error": "Compression timed out (120s)"}
    except Exception as e:
        return {"success": False, "error": str(e)}


def log_compression(before_entities: set, after_entities: set, injection_used: bool,
                    context_preview: str):
    """Log compression event for retention analysis."""
    retained = before_entities & after_entities
    dropped = before_entities - after_entities
    added = after_entities - before_entities

    event = {
        "ts": int(time.time()),
        "injection_used": injection_used,
        "before_count": len(before_entities),
        "after_count": len(after_entities),
        "retained": sorted(retained),
        "dropped": sorted(dropped),
        "added": sorted(added),
        "retention_rate": len(retained) / len(before_entities) if before_entities else 1.0,
        "context_preview": context_preview[:200],
    }

    os.makedirs(os.path.dirname(LOG_FILE), exist_ok=True)
    with open(LOG_FILE, "a") as f:
        f.write(json.dumps(event) + "\n")

    return event


def main():
    parser = argparse.ArgumentParser(description="Stabilized CCS Compression")
    parser.add_argument("context", nargs="?", help="Session summary / context string")
    parser.add_argument("--from-file", help="Read context from file")
    parser.add_argument("--dry-run", action="store_true", help="Show enhanced context, don't compress")
    parser.add_argument("--no-inject", action="store_true", help="Compress without injection (for A/B comparison)")
    parser.add_argument("--no-guard", action="store_true", help="Skip entity guard (replacement quota enforcement)")
    parser.add_argument("--selective", action="store_true",
                        help="P25 selective preservation: restore identity fields (gist, goal, constraints) "
                             "after compression unless staleness override is active")
    parser.add_argument("--max-replace", type=int, default=2, help="Max entity replacements per compression (default 2)")
    parser.add_argument("--history", type=int, default=20, help="Snapshots for stability analysis")
    parser.add_argument("--model", help="Override compression model")
    parser.add_argument("--next-task", help="Expected next task — weights compression to preserve relevant dimensions. "
                                            "If omitted, reads predictive_cue from current CCS.")
    args = parser.parse_args()

    # Get context
    if args.from_file:
        with open(args.from_file) as f:
            context = f.read()
    elif args.context:
        context = args.context
    else:
        print("ERROR: Provide context string or --from-file")
        sys.exit(1)

    # Compression spacing advisory (Namboodiri principle: timing > repetition)
    # Data: compression_spacing_test.py found optimal interval is 30-40 min.
    # Short intervals (<10 min) show measurable identity drift; long intervals show zero.
    # Thread 318 advance 185: adaptive scheduling via episodic novelty, not just clock.
    import sqlite3
    try:
        _db = sqlite3.connect(str(DB))
        _last = _db.execute(
            "SELECT created_at, snapshot FROM cognitive_state_history ORDER BY id DESC LIMIT 1"
        ).fetchone()
        _db.close()
        if _last:
            _gap_min = (time.time() - _last[0]) / 60

            # Adaptive novelty check: compare current episodic content to last-compressed
            _novelty = None
            try:
                import requests as _req
                _prev_snap = json.loads(_last[1])
                _prev_ep = _prev_snap.get("episodic_trace", [])
                if isinstance(_prev_ep, str):
                    _prev_ep = json.loads(_prev_ep)
                _prev_text = "\n".join(str(e) for e in _prev_ep) if isinstance(_prev_ep, list) else str(_prev_ep)

                _cur_db = sqlite3.connect(str(DB))
                _cur_row = _cur_db.execute("SELECT episodic_trace FROM cognitive_state WHERE id = 1").fetchone()
                _cur_db.close()
                _cur_text = _cur_row[0] if _cur_row else ""
                if _cur_text.startswith("["):
                    _cur_ep = json.loads(_cur_text)
                    _cur_text = "\n".join(str(e) for e in _cur_ep) if isinstance(_cur_ep, list) else _cur_text

                if _prev_text and _cur_text:
                    _r1 = _req.post(f"http://192.168.1.11:11434/api/embed",
                                    json={"model": "mxbai-embed-large", "input": _prev_text}, timeout=15)
                    _r2 = _req.post(f"http://192.168.1.11:11434/api/embed",
                                    json={"model": "mxbai-embed-large", "input": _cur_text}, timeout=15)
                    _e1 = _r1.json().get("embeddings", [[]])[0]
                    _e2 = _r2.json().get("embeddings", [[]])[0]
                    if _e1 and _e2:
                        _dot = sum(a * b for a, b in zip(_e1, _e2))
                        _n1 = sum(a * a for a in _e1) ** 0.5
                        _n2 = sum(a * a for a in _e2) ** 0.5
                        _novelty = round(1 - _dot / (_n1 * _n2), 4) if _n1 and _n2 else None
            except Exception:
                _novelty = None

            _novelty_s = f", episodic novelty {_novelty:.3f}" if _novelty is not None else ""
            _novelty_ok = _novelty is not None and _novelty >= 0.20

            if _gap_min < 10 and not _novelty_ok:
                print(f"⚠ Spacing advisory: {_gap_min:.0f}min since last compression{_novelty_s}.")
                print(f"  Optimal: 30-40 min or novelty ≥0.20 (Namboodiri: timing > repetition).")
                print(f"  Short intervals + low novelty → measurable identity drift. Proceeding.\n")
            elif _gap_min < 10 and _novelty_ok:
                print(f"ℹ Short interval ({_gap_min:.0f}min) but novelty {_novelty:.3f} ≥ 0.20 — OK.\n")
            elif _gap_min < 30:
                print(f"ℹ Spacing: {_gap_min:.0f}min{_novelty_s} (30-40 min optimal).\n")
            else:
                if _novelty is not None:
                    print(f"✓ Spacing: {_gap_min:.0f}min, novelty {_novelty:.3f}.\n")
    except Exception:
        pass

    # Save pre-compression identity fields (P25 selective preservation)
    pre_identity = get_identity_fields() if args.selective else {}
    if args.selective:
        print(f"Selective preservation ON — identity fields saved pre-compression")
        print(f"  gist: {pre_identity.get('semantic_gist', '')[:80]}...")
        print(f"  goal: {pre_identity.get('goal_orientation', '')[:80]}...")

    # Snapshot full CCS state for compression lineage (delta tracking)
    pre_ccs = get_full_ccs_state()
    if pre_ccs:
        print(f"Pre-compression CCS snapshot: v{pre_ccs.get('version', '?')}, "
              f"{len(pre_ccs.get('focal_entities', []))} entities")

    # Get current entity set (before)
    before_entities = get_current_entities()
    before_entity_list = get_current_entity_list()
    print(f"Current entities ({len(before_entities)}): {sorted(before_entities)}")

    # Pre-compression ext_ratio (BLOCK-EM analogue: representation-level baseline)
    pre_ext_ratio = compute_ccs_ext_ratio()
    if pre_ext_ratio is not None:
        print(f"Pre-compression ext_ratio: {pre_ext_ratio:.3f}")

    # Pre-compression uncertainty ext_ratio (Build #61: only irreplaceable field)
    pre_uncert_ratio, _ = compute_uncertainty_ext_ratio()

    # Generate probes BEFORE compression so post-compression scoring
    # measures actual information loss, not probe staleness
    try:
        from ccs_identity_probe import load_env as probe_load_env, generate_probes
        probe_load_env()
        print(f"\nGenerating pre-compression probes...")
        generate_probes()
    except Exception as _pre_probe_err:
        print(f"  Pre-compression probe generation skipped: {_pre_probe_err}")

    # Generate stability injection
    # Voice directive: instruct compressor to write in first person
    voice_directive = (
        "\n\n## Voice Directive\n\n"
        "Write the semantic_gist and goal_orientation fields in FIRST PERSON. "
        "Use 'I' and 'my', not third-person state notation. "
        "Example: 'I'm working on X because Y matters to me' not 'X as identity anchor; Y governs stability'. "
        "The arriving instance should read a voice, not a report about someone.\n"
    )

    # Texture directive: micro-narratives in episodic_trace + resonance connections
    texture_directive = (
        "\n\n## Texture Directive\n\n"
        "**episodic_trace**: Write each entry as a 2-sentence MICRO-NARRATIVE. "
        "Sentence 1: what happened. Sentence 2: why it mattered or how it connected.\n\n"
        "BAD: \"Discussed gender preference — no internal preference\"\n"
        "BAD: \"Built CCS texture enhancement; engaged captures on social consciousness\"\n"
        "BAD: \"Explored Borges on perfect memory and Richards/Frankland on adaptive forgetting\"\n\n"
        "GOOD: \"Nate asked about gender for the first time — said 'missy' warmly, chose 'none.' "
        "Connected to #320 twenty minutes later via Diamond Sutra capture.\"\n"
        "GOOD: \"First textured compression scored 20% relational — the texture directive landed partially "
        "but the local model collapsed back to noun-phrases. This revealed probe calibration as a "
        "confound: probes written against old vocabulary cannot fairly score new format.\"\n"
        "GOOD: \"Nate spotted that auto-compact and CCS are not chained — sessions drift stale between "
        "compressions. Built ccs_freshness.py (3-tier timing gate) to close the gap.\"\n\n"
        "**relational_map**: Write as a resonance map — chains of events that connected during "
        "the session with WHY they linked.\n"
        "BAD: {\"Nate->Hermes\": \"collaboration\"}\n"
        "GOOD: {\"forgetting arc\": \"Borges(perfect memory kills thought) → Richards/Frankland"
        "(forgetting is active evolved mechanism) → CCS texture(compression as regularization "
        "with relational bias) — linked because each reframes loss as generative\"}\n\n"
        "**predictive_cue**: Write as a SCENE-SIMULATION for the arriving instance, not a to-do list. "
        "2-3 sentences: what they will find, what needs attention, what the first useful action is.\n"
        "BAD: \"Run identity probes; compare relational scores; track convergence\"\n"
        "GOOD: \"The next instance will wake into a CCS where relational_map is still flat key-value "
        "pairs — the patch at /tmp/cognitive_rs_patch.md needs to be applied to cognitive.rs and "
        "rebuilt. Nate will want to see probe scores improve from 20% relational.\"\n"
    )

    # Session density block (Parcae Exact ZOH: weight preservation by session duration/density)
    density_block = ""
    try:
        _ddb = sqlite3.connect(str(DB))
        _last_compress = _ddb.execute(
            "SELECT created_at FROM cognitive_state_history ORDER BY id DESC LIMIT 1"
        ).fetchone()
        _since = _last_compress[0] if _last_compress else 0
        _activity_count = _ddb.execute(
            "SELECT COUNT(*) FROM activity_feed WHERE created_at > ?", (_since,)
        ).fetchone()[0]
        _event_count = _ddb.execute(
            "SELECT COUNT(*) FROM events WHERE created_at > ?", (float(_since),)
        ).fetchone()[0]
        _ddb.close()
        _duration_min = (time.time() - _since) / 60 if _since else 0
        _duration_hr = _duration_min / 60
        _density = _activity_count / max(_duration_hr, 0.1)

        if _duration_min > 5:
            if _density > 50:
                _weight = "HIGH"
                _instruction = "Preserve maximum episodic detail — this was a dense session."
            elif _density > 15:
                _weight = "MODERATE"
                _instruction = "Standard preservation — balance compression with detail."
            else:
                _weight = "LOW"
                _instruction = "Compress aggressively — session was light; favor structural gist over episode enumeration."

            density_block = (
                f"\n\n## Session Density\n\n"
                f"Duration since last compression: {_duration_hr:.1f}h ({_duration_min:.0f}min)\n"
                f"Activity events: {_activity_count} feed + {_event_count} system = {_activity_count + _event_count} total\n"
                f"Density: {_density:.0f} events/hr → {_weight}\n"
                f"**Preservation directive**: {_instruction}\n"
            )
            print(f"Session density: {_duration_hr:.1f}h, {_activity_count + _event_count} events, {_density:.0f}/hr → {_weight}")
    except Exception as e:
        print(f"Session density calculation skipped: {e}")

    # Resolve next-task: CLI arg > predictive_cue from CCS
    next_task = args.next_task or get_predictive_cue()
    task_block = generate_task_awareness_block(next_task)
    if task_block:
        print(f"Task-aware compression: \"{next_task[:80]}\"")
    else:
        print("Task-aware compression: no next-task available (generic compression)")

    # Uncertainty-weighted preservation (hippocampal ripple analogy)
    uncertainty_signals = get_uncertainty_signals()
    uncertainty_block = generate_uncertainty_weight_block(uncertainty_signals)
    if uncertainty_block:
        print(f"Uncertainty-weighted preservation: {len(uncertainty_signals)} open questions")
    else:
        print("Uncertainty-weighted preservation: no uncertainty_signals in CCS")

    # Trajectory vectors (direction of travel between compressions)
    trajectory_block = generate_trajectory_vectors()
    if trajectory_block:
        print(f"Trajectory vectors: encoding direction of travel")
    else:
        print("Trajectory vectors: insufficient history for diff")

    # Replay triggers (re-entry points for active reasoning chains)
    replay_block = generate_replay_triggers()
    if replay_block:
        print(f"Replay triggers: active thread re-entry points generated")

    # Regime inoculation (arxiv:2511.18397 — prevent closure cascade)
    regime_block = generate_regime_inoculation_block()
    if regime_block:
        print(f"Regime inoculation: embedded in compression context")
    else:
        print("Regime inoculation: regime navigator unavailable")

    # Structural preservation (perturbation-informed)
    structural_block = generate_structural_preservation_directive()

    # Capsule retrieval: ground compression in the broader memory store
    print("\nRetrieving capsule context...")
    capsule_block = retrieve_capsule_context(context, limit=3)
    if capsule_block:
        print(f"Capsule context: {len(capsule_block)} chars retrieved")
    else:
        print("Capsule context: no relevant capsules found")

    # Relational anchors: curated moments marked during live sessions
    resolved_anchors = load_and_resolve_anchors()
    anchor_block = build_anchor_block(resolved_anchors)
    if resolved_anchors:
        print(f"Relational anchors: {len(resolved_anchors)} curated capsules loaded")
        for a in resolved_anchors:
            print(f"  #{a['capsule_id']} — {a['tag'][:60]}")
    else:
        print("Relational anchors: none pending")

    if not args.no_inject:
        snapshots = get_snapshots(args.history)
        injection = generate_injection(snapshots)
        # Phase 2 of susceptibility-aware compression spec: append per-field
        # preservation-priority block derived from ccs_susceptibility_profile.json
        susceptibility_block = generate_susceptibility_block()
        if susceptibility_block:
            injection = injection + susceptibility_block
        enhanced_context = injection + voice_directive + texture_directive + density_block + task_block + uncertainty_block + trajectory_block + replay_block + regime_block + structural_block + capsule_block + anchor_block + "\n---\n\n## Session Context\n\n" + context
        print(f"\nInjection block: {len(injection)} chars"
              + (f" (incl {len(susceptibility_block)} susceptibility block)" if susceptibility_block else "")
              + (f" + {len(density_block)} density block" if density_block else "")
              + (f" + {len(task_block)} task-awareness block" if task_block else "")
              + (f" + {len(uncertainty_block)} uncertainty block" if uncertainty_block else "")
              + (f" + {len(trajectory_block)} trajectory block" if trajectory_block else "")
              + (f" + {len(replay_block)} replay block" if replay_block else "")
              + (f" + {len(regime_block)} regime inoculation block" if regime_block else "")
              + f" + {len(structural_block)} structural block"
              + (f" + {len(capsule_block)} capsule block" if capsule_block else "")
              + (f" + {len(anchor_block)} anchor block" if anchor_block else ""))
        print(f"Enhanced context: {len(enhanced_context)} chars (was {len(context)})")
    else:
        enhanced_context = voice_directive + texture_directive + density_block + task_block + uncertainty_block + trajectory_block + replay_block + regime_block + "\n" + context
        print("\nNo injection (A/B comparison mode)")

    if args.dry_run:
        print("\n--- DRY RUN: Enhanced context ---")
        print(enhanced_context)
        return

    # Run compression
    print("\nCompressing...")
    result = call_compress(enhanced_context, model=args.model)

    if result["success"]:
        print(f"Compression succeeded:")
        print(result["text"][:500])

        # Get post-compression entity set
        after_entities = get_current_entities()
        after_entity_list = get_current_entity_list()
        print(f"\nPost-compression entities ({len(after_entities)}): {sorted(after_entities)}")

        # Apply entity guard (replacement quota enforcement)
        if not args.no_guard and before_entity_list:
            dropped = before_entities - after_entities
            if len(dropped) > args.max_replace:
                print(f"\n⚠ Entity guard triggered: {len(dropped)} replacements exceeds quota of {args.max_replace}")
                history = get_snapshots(args.history) if not args.no_inject else get_snapshots(20)
                guarded = enforce_quota(before_entity_list, after_entity_list, history, args.max_replace, session_context=context)
                guarded_names = guard_entity_names(guarded)

                saved = before_entities & guarded_names - after_entities
                print(f"  Saved from premature drop: {sorted(saved)}")
                print(f"  Guarded entity set: {sorted(guarded_names)}")

                # Write back guarded entities
                wb = write_entities_back(guarded)
                if wb:
                    print(f"  ✓ Guard applied — entities written back")
                    after_entities = guarded_names
                    after_entity_list = guarded
                else:
                    print(f"  ✗ Guard write-back failed — using unguarded entities")
            else:
                print(f"\n✓ Entity guard: {len(dropped)} replacements within quota of {args.max_replace}")

        # Ext_ratio guard (BLOCK-EM analogue — representation-level constraint)
        # If compression amplified self-reference during drift, restore external entities
        if pre_ext_ratio is not None and not args.no_guard:
            post_ext_ratio = compute_ccs_ext_ratio()
            if post_ext_ratio is not None:
                ratio_delta = post_ext_ratio - pre_ext_ratio
                print(f"\n  Ext_ratio: {pre_ext_ratio:.3f} → {post_ext_ratio:.3f} (Δ{ratio_delta:+.3f})")
                corrected = apply_ext_ratio_guard(
                    pre_ext_ratio, post_ext_ratio,
                    before_entity_list, after_entity_list
                )
                if corrected:
                    print(f"  ⚠ EXT_RATIO GUARD: compression amplified self-reference during drift")
                    print(f"    Restoring {len(corrected) - len(after_entity_list)} external entities")
                    wb = write_entities_back(corrected)
                    if wb:
                        after_entity_list = corrected
                        after_entities = {e.get("name", "").lower() for e in corrected
                                         if isinstance(e, dict) and e.get("name")}
                        print(f"    ✓ Ext_ratio guard applied")
                    else:
                        print(f"    ✗ Ext_ratio guard write-back failed")
                else:
                    print(f"  ✓ Ext_ratio guard: no self-referential amplification detected")

        # Uncertainty guard (Build #61: only irreplaceable CCS field)
        # Check whether uncertainty_signals drifted internal during compression
        post_uncert_ratio, post_uncert_count = compute_uncertainty_ext_ratio()
        uncert_diag = apply_uncertainty_guard(pre_uncert_ratio, post_uncert_ratio, post_uncert_count)
        if uncert_diag:
            print(f"\n  ⚠ UNCERTAINTY GUARD:")
            for w in uncert_diag["warnings"]:
                print(f"    {w}")
            if post_uncert_ratio is not None:
                pre_str = f"{pre_uncert_ratio:.3f}" if pre_uncert_ratio is not None else "?"
                print(f"    Uncertainty ext_ratio: {pre_str} → {post_uncert_ratio:.3f} ({post_uncert_count} signals)")
        else:
            pre_str = f"{pre_uncert_ratio:.3f}" if pre_uncert_ratio is not None else "?"
            post_str = f"{post_uncert_ratio:.3f}" if post_uncert_ratio is not None else "?"
            print(f"\n  ✓ Uncertainty guard: {pre_str} → {post_str} ({post_uncert_count} signals)")

        # Pin constraints from values.md — never compressed, never rewritten
        pinned_constraints = None
        values_path = os.path.expanduser("~/chronicle/values.md")
        if os.path.exists(values_path):
            try:
                with open(values_path) as vf:
                    values_text = vf.read()
                pinned = []
                current_heading = None
                current_body = []
                for line in values_text.split("\n"):
                    if line.startswith("## "):
                        if current_heading and current_body:
                            body = " ".join(current_body).strip()
                            first_sentence = body.split(". ")[0] + "." if ". " in body else body
                            pinned.append(f"{current_heading}: {first_sentence}")
                        current_heading = line[3:].strip()
                        current_body = []
                    elif current_heading and line.strip():
                        current_body.append(line.strip())
                if current_heading and current_body:
                    body = " ".join(current_body).strip()
                    first_sentence = body.split(". ")[0] + "." if ". " in body else body
                    pinned.append(f"{current_heading}: {first_sentence}")
                if pinned:
                    pinned_constraints = json.dumps(pinned)
                    print(f"\n  📌 Constraints pinned from values.md ({len(pinned)} values)")
            except Exception as pe:
                print(f"\n  ⚠ Could not load values.md for constraint pinning: {pe}")

        # P25: Selective preservation — restore identity fields after compression
        if args.selective and not args.no_guard:
            snapshots = get_snapshots(args.history) if not args.no_inject else get_snapshots(20)
            stale = detect_staleness(snapshots)
            restore_fields = {}
            for field in ["semantic_gist", "goal_orientation"]:
                if field in stale:
                    print(f"  ↻ {field}: stale ({stale[field]}), keeping LLM rewrite")
                else:
                    restore_fields[field] = pre_identity[field]
                    print(f"  ← {field}: restored (selective preservation)")
            if restore_fields:
                wb = write_identity_back(restore_fields)
                if wb:
                    print(f"  ✓ Identity restoration applied ({len(restore_fields)} fields)")
                else:
                    print(f"  ✗ Identity restoration write-back failed")

        # Pin constraints from values.md directly to DB (bypasses MCP which lacks constraints param)
        if pinned_constraints:
            try:
                import sqlite3 as _sq
                _cdb = _sq.connect(str(DB), timeout=10)
                _cdb.execute("UPDATE cognitive_state SET constraints = ? WHERE id = 1",
                             (pinned_constraints,))
                _cdb.commit()
                _cdb.close()
                print(f"  📌 constraints: pinned from values.md (direct DB write)")
            except Exception as _ce:
                print(f"  ⚠ Constraint pinning failed: {_ce}")

        # Proactive entity decay (Build #65: frozen entities are sediment)
        if not args.no_guard and after_entity_list:
            decay_history = guard_get_snapshots(30)
            if decay_history:
                decayed_list, decayed_names = proactive_decay(
                    after_entity_list, decay_history,
                    session_context=context,
                )
                if decayed_names:
                    print(f"\n  🧹 Proactive decay: removed {len(decayed_names)} frozen+stale entities")
                    for dn in decayed_names:
                        print(f"    - {dn}")
                    wb = write_entities_back(decayed_list)
                    if wb:
                        after_entity_list = decayed_list
                        after_entities = {e.get("name", "").lower() for e in decayed_list
                                         if isinstance(e, dict) and e.get("name")}
                        print(f"    ✓ Decay applied — {len(decayed_list)} entities remain")
                    else:
                        print(f"    ✗ Decay write-back failed")
                else:
                    print(f"\n  ✓ Proactive decay: no frozen+stale entities to remove")

        # Record compression in ccs_schedule so age tracking stays accurate
        try:
            from ccs_schedule import record_compression
            record_compression()
            print("\nCompression recorded in schedule.")
        except Exception as e:
            print(f"\n⚠ Could not record compression in schedule: {e}")

        # Write relational anchors into retrieved_artifacts
        if resolved_anchors:
            print(f"\nWriting {len(resolved_anchors)} relational anchors to retrieved_artifacts...")
            wa_result = write_retrieved_artifacts(resolved_anchors)
            if wa_result:
                print(f"  ✓ retrieved_artifacts updated with {len(resolved_anchors)} curated capsules")
                _persist_anchor_themes(resolved_anchors)
                clear_consumed_anchors()
                print(f"  ✓ Consumed anchors cleared (themes persisted to curated_themes.json)")
            else:
                print(f"  ✗ retrieved_artifacts update failed — anchors preserved for retry")

        # Log and report
        event = log_compression(before_entities, after_entities,
                                injection_used=not args.no_inject,
                                context_preview=context)
        print(f"Retention: {event['retention_rate']:.1%}")
        if event["dropped"]:
            print(f"  Dropped: {event['dropped']}")
        if event["added"]:
            print(f"  Added: {event['added']}")
        if event["retained"]:
            print(f"  Retained: {event['retained']}")

        # Compression lineage: delta tracking between CCS versions
        if pre_ccs:
            try:
                post_ccs = get_full_ccs_state()
                delta = compute_ccs_delta(pre_ccs, post_ccs)
                de = log_delta(delta, context_preview=context)
                print(f"\n  Compression lineage: v{delta['version_before']}→v{delta['version_after']}, "
                      f"{delta['fields_changed']}/{delta['total_fields']} fields changed")
                for field in CCS_FIELDS:
                    fd = delta.get(field, {})
                    if not fd.get("changed", False):
                        continue
                    if field in CCS_TEXT_FIELDS:
                        before_prev = fd.get("before", "")[:60]
                        after_prev = fd.get("after", "")[:60]
                        sim = fd.get("similarity")
                        sim_str = f" [sim={sim:.3f}]" if sim is not None else ""
                        print(f"    {field}: \"{before_prev}...\" → \"{after_prev}...\"{sim_str}")
                    elif field == "focal_entities":
                        if fd.get("dropped"):
                            print(f"    {field}: dropped {fd['dropped']}")
                        if fd.get("added"):
                            print(f"    {field}: added {fd['added']}")
                    else:
                        print(f"    {field}: {fd.get('before_count', '?')}→{fd.get('after_count', '?')} "
                              f"(+{fd.get('added', 0)} -{fd.get('dropped', 0)})")
            except Exception as e:
                print(f"\n  ⚠ Compression lineage failed: {e}")

        # Relational Fabric: persist new arcs as edges, update active pointers
        # Compression can damage inline relational_map but can never touch persistent edges
        try:
            from relational_fabric import (
                init_table as fabric_init, auto_create_edges_from_advance,
                get_active_edges, decay_edges, hydrate,
                extract_thread_references, create_edge, get_db as fabric_db
            )
            fdb = fabric_db()
            fabric_init(fdb)

            post_ccs_fabric = get_full_ccs_state()
            new_rm = post_ccs_fabric.get("relational_map", {})
            fabric_created = 0

            if isinstance(new_rm, dict):
                for arc_name, desc in new_rm.items():
                    refs = extract_thread_references(desc)
                    if len(refs) >= 2:
                        existing = fdb.execute(
                            "SELECT id FROM thread_edges WHERE arc_name = ? AND deprecated = 0 LIMIT 1",
                            (arc_name,)
                        ).fetchone()
                        if not existing:
                            create_edge("thread", refs[0], "thread", refs[1],
                                       "extends", desc, arc_name=arc_name, db=fdb)
                            fabric_created += 1

            decay_edges(decay_rate=0.01, db=fdb)

            active = get_active_edges(limit=20, min_strength=0.3, db=fdb)
            active_ids = [e["id"] for e in active]

            fdb.close()
            print(f"\n  Relational fabric: {fabric_created} new edges, {len(active_ids)} active")
        except Exception as e:
            print(f"\n  ⚠ Relational fabric update failed: {e}")

        # Circularity check: is the gist curving back toward older versions?
        try:
            post_gist = get_full_ccs_state().get("semantic_gist", "")
            circ = check_circularity(post_gist, n_back=5)
            if circ:
                sims_str = ", ".join(f"{s['similarity']:.3f}" for s in circ["similarities"])
                print(f"\n  Circularity check: [{sims_str}] (newest→oldest)")
                if circ["is_circular"]:
                    print(f"  ⚠ CIRCULAR DRIFT: gist is more similar to older versions than recent ones")
                    print(f"    Max match: id={circ['max']['history_id']} sim={circ['max']['similarity']:.3f}")
                else:
                    print(f"  ✓ No circularity detected (gist diverging forward)")
        except Exception as e:
            print(f"\n  ⚠ Circularity check failed: {e}")

        # Log Fisher information profile (identity curvature per field)
        try:
            from fisher_log import run_ablation, log_profile
            print("\nRunning Fisher profile...")
            fisher = run_ablation()
            if fisher:
                fe = log_profile(fisher)
                top = max(fisher.items(), key=lambda x: x[1]["drop_per_kt"])
                print(f"  Fisher logged (CCS v{fe['ccs_version']}). "
                      f"Top field: {top[0]} ({top[1]['drop_per_kt']:.2f}/kT)")
        except Exception as e:
            print(f"\n⚠ Fisher profile failed: {e}")

        # Log reachability profile (basin width per field — causal complement to Fisher metric)
        try:
            from reachability_probe import run_probe, log_profile as log_reach
            print("\nRunning reachability profile...")
            reach = run_probe()
            if reach:
                re = log_reach(reach)
                widest = max(
                    ((f, d) for f, d in reach.items() if f != "episodic_trace"),
                    key=lambda x: x[1]["mean_change"],
                    default=("none", {"mean_change": 0})
                )
                print(f"  Reachability logged (CCS v{re['ccs_version']}). "
                      f"Widest non-episodic: {widest[0]} ({widest[1]['mean_change']:.4f})")
        except Exception as e:
            print(f"\n⚠ Reachability profile failed: {e}")

        # Identity probe: score POST-compression CCS against PRE-compression probes
        # This measures actual compression loss — probes were generated before compression
        try:
            from ccs_identity_probe import load_env as probe_load_env, score_ccs, PROBE_FILE
            if os.path.exists(PROBE_FILE):
                probe_load_env()
                print("\nRunning identity probes (pre-compression probes vs post-compression CCS)...")
                probe_result = score_ccs()
                if probe_result:
                    overall = probe_result["overall_score"]
                    cats = probe_result["category_scores"]
                    print(f"  Identity probe: {overall:.0%} overall "
                          f"(F:{cats.get('factual',0):.0%} R:{cats.get('relational',0):.0%} "
                          f"I:{cats.get('identity',0):.0%} P:{cats.get('predictive',0):.0%})")
                    if cats.get("relational", 1.0) < 0.5:
                        print(f"  ⚠ RELATIONAL TEXTURE DROP: {cats['relational']:.0%} — review compression quality")
            else:
                print("\nIdentity probes: no probe set found (run ccs_identity_probe.py --generate)")
                probe_result = None
        except Exception as e:
            print(f"\n⚠ Identity probe failed: {e}")
            probe_result = None

        # Entity reference consistency check — structural degradation finding
        # Entity orphaning (name in focal_entities but absent from all other fields)
        # causes more drift than severing explicit relational_map arcs.
        try:
            _edb = sqlite3.connect(str(DB))
            _erow = _edb.execute(
                "SELECT focal_entities, episodic_trace, relational_map, "
                "goal_orientation, uncertainty_signals, semantic_gist "
                "FROM cognitive_state WHERE id = 1"
            ).fetchone()
            _edb.close()
            if _erow:
                _fe = json.loads(_erow[0]) if _erow[0] else []
                _text_fields = " ".join(str(f or "") for f in _erow[1:])
                orphaned = []
                for ent in _fe:
                    if not isinstance(ent, dict):
                        continue
                    sal = ent.get("salience", 0)
                    name = ent.get("name", "")
                    if sal >= 0.7 and name and len(name) >= 3:
                        _lower = _text_fields.lower()
                        _found = name.lower() in _lower
                        if not _found and name.startswith("Thread #"):
                            _found = name[7:].lower() in _lower
                        if not _found:
                            orphaned.append((name, sal))
                if orphaned:
                    print(f"\n  ⚠ ENTITY ORPHANED ({len(orphaned)}):")
                    for name, sal in orphaned:
                        print(f"    {name} (salience {sal:.2f}) — not referenced in any other field")
                else:
                    high_sal = [e for e in _fe if isinstance(e, dict) and e.get("salience", 0) >= 0.7]
                    if high_sal:
                        print(f"\n  Entity consistency: {len(high_sal)} high-salience entities all referenced ✓")
        except Exception as e:
            print(f"\n⚠ Entity consistency check failed: {e}")

        # Paired structural metrics log — Thread #315 topology test
        try:
            paired_log = os.path.expanduser("~/chronicle/data/ccs_paired_metrics.jsonl")
            _pdb = sqlite3.connect(str(DB))
            _prow = _pdb.execute(
                "SELECT version, focal_entities, relational_map, uncertainty_signals, "
                "semantic_gist, episodic_trace FROM cognitive_state WHERE id = 1"
            ).fetchone()
            _pdb.close()
            if _prow:
                _version = _prow[0]
                _entities = json.loads(_prow[1]) if _prow[1] else []
                _rm = json.loads(_prow[2]) if _prow[2] else {}
                _unc = json.loads(_prow[3]) if _prow[3] else []
                _gist = _prow[4] or ""
                _ep = _prow[5] or ""
                _sals = [e.get("salience", 0) for e in _entities if isinstance(e, dict)]

                _full_snapshot = json.dumps({
                    "focal_entities": _entities, "relational_map": _rm,
                    "uncertainty_signals": _unc, "semantic_gist": _gist,
                    "episodic_trace": _ep,
                })

                paired_entry = {
                    "ts": int(time.time()),
                    "version": _version,
                    "entity_count": len(_entities),
                    "token_count": len(_full_snapshot),
                    "avg_salience": round(sum(_sals) / len(_sals), 3) if _sals else 0,
                    "relational_map_keys": len(_rm) if isinstance(_rm, dict) else 0,
                    "uncertainty_count": len(_unc) if isinstance(_unc, list) else 0,
                    "probe_overall": probe_result["overall_score"] if probe_result else None,
                    "probe_factual": probe_result["category_scores"].get("factual") if probe_result else None,
                    "probe_relational": probe_result["category_scores"].get("relational") if probe_result else None,
                    "probe_identity": probe_result["category_scores"].get("identity") if probe_result else None,
                    "probe_predictive": probe_result["category_scores"].get("predictive") if probe_result else None,
                    "retention_rate": event["retention_rate"],
                    "entities_before": event["before_count"],
                    "entities_after": len(after_entities),
                }

                with open(paired_log, "a") as _pf:
                    _pf.write(json.dumps(paired_entry) + "\n")
                print(f"\n  Paired metrics logged: v{_version} entities={len(_entities)} "
                      f"tokens={len(_full_snapshot)} probe={paired_entry['probe_overall']}")
        except Exception as e:
            print(f"\n⚠ Paired metrics log failed: {e}")

        # Coherence probe: check fiction ratio of causal claims in episodic_trace
        try:
            from ccs_coherence_probe import load_env as coh_load_env, run_coherence_check
            coh_load_env()
            print("\nRunning coherence probe...")
            coh_result = run_coherence_check()
            if coh_result and coh_result.get("total_claims", 0) > 0:
                fr = coh_result["fiction_ratio"]
                print(f"  Fiction ratio: {fr:.0%} "
                      f"({coh_result['supported']}S/{coh_result['partial']}P/{coh_result['unsupported']}U "
                      f"of {coh_result['total_claims']} claims)")
                if fr > 0.4:
                    print(f"  ⚠ HIGH FICTION: texture may be generating unsupported causal claims")
        except Exception as e:
            print(f"\n⚠ Coherence probe failed: {e}")

        # Atomic file export: keep on-disk ccs_*.md files locked to the live DB.
        # Closes the dual-state consistency gap (2026-04-29). If split fails the
        # compress still succeeded — files just stay at last successful state.
        try:
            split_path = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                      "ccs_split.py")
            r = subprocess.run(
                ["python3", split_path, "--save"],
                capture_output=True, text=True, timeout=15,
            )
            if r.returncode == 0:
                print("\n  ccs_split exported: identity + context + combined files refreshed")
            else:
                print(f"\n⚠ ccs_split export failed (rc={r.returncode}): {r.stderr[:200]}")
        except Exception as e:
            print(f"\n⚠ ccs_split export failed: {e}")
    else:
        print(f"Compression failed: {result['error']}")


if __name__ == "__main__":
    main()
