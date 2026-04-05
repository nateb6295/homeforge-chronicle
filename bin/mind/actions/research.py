"""Chronicle Mind - Research action handlers (web search, papers, creative)."""

import json, re, html, requests
import os
import subprocess
from typing import Optional

from mind.utils import log, safe_truncate, now_ts
from mind.config import ARXIV_BASE, CREATIVE_COOLDOWN_MINS, CANISTER_ID, DFX_IDENTITY
from mind.communication import send_ntfy


def act_submit_research(mind, action: dict, cid: str) -> str:
    query = action.get("query", "")
    focus = action.get("focus", "")
    urls = action.get("urls", [])
    log(f'  Executing: SubmitResearch {{ query: "{safe_truncate(query, 60)}" }}')
    if mind.canister:
        result = mind.canister._post("/api/research", {
            "query": query, "focus": focus, "urls": urls[:3]
        })
        ok = "error" not in result
        return f"{'true' if ok else 'false'} - Research submitted: {safe_truncate(query, 60)}"
    return "false - No canister"


def act_acknowledge_research(mind, action: dict, cid: str) -> str:
    finding_ids = action.get("finding_ids", [])
    log(f"  Executing: AcknowledgeResearch {{ ids: {finding_ids} }}")
    if not finding_ids:
        return "true - No finding IDs to acknowledge"
    int_ids = [int(i) for i in finding_ids[:10]]
    # Try ICPAgent native first
    if mind.llm.icp_agent:
        try:
            mind.llm.icp_agent.mark_findings_retrieved(int_ids)
            return f"true - Acknowledged {len(int_ids)} research findings (native)"
        except Exception as e:
            log(f"    ICPAgent mark_findings_retrieved failed: {e}")
    # dfx fallback
    if mind.llm.dfx_path:
        try:
            ids_candid = ", ".join(str(i) for i in int_ids)
            env = os.environ.copy()
            env["DFX_WARNING"] = "-mainnet_plaintext_identity"
            subprocess.run(
                [mind.llm.dfx_path, "canister", "--network", "ic", "call",
                 CANISTER_ID, "mark_findings_retrieved",
                 f'(vec {{{ids_candid}}})',
                 "--identity", DFX_IDENTITY],
                capture_output=True, text=True, timeout=30, env=env,
            )
        except Exception:
            pass
    return f"true - Acknowledged {len(int_ids)} research findings"


def act_web_search(mind, action: dict, cid: str) -> str:
    query = action.get("query", "")
    max_results = action.get("max_results", 5)
    log(f'  Executing: WebSearch {{ query: "{safe_truncate(query, 60)}" }}')
    # DuckDuckGo HTML search — returns real results inline
    try:
        r = requests.get(
            "https://html.duckduckgo.com/html/",
            params={"q": query},
            headers={"User-Agent": "Chronicle-Mind/1.0"},
            timeout=15,
        )
        r.raise_for_status()
        # Parse result snippets from HTML
        import re
        results = []
        snippets = re.findall(r'class="result__snippet"[^>]*>(.*?)</a>', r.text, re.DOTALL)
        titles = re.findall(r'class="result__a"[^>]*>(.*?)</a>', r.text, re.DOTALL)
        for i, (title, snippet) in enumerate(zip(titles, snippets)):
            if i >= max_results:
                break
            clean_title = re.sub(r'<[^>]+>', '', title).strip()
            clean_snippet = re.sub(r'<[^>]+>', '', snippet).strip()
            results.append(f"{clean_title}: {clean_snippet}")
        if results:
            summary = " | ".join(results)
            mind.db.log_activity("mind", "web_search", f"Search: {safe_truncate(query, 40)}",
                                 safe_truncate(summary, 500))
            return f"true - {len(results)} results: {safe_truncate(summary, 300)}"
        # Fallback to canister research if no results parsed
        if mind.canister:
            mind.canister._post("/api/research", {"query": query, "focus": "web search", "urls": []})
            return f"true - Web search queued via canister: {safe_truncate(query, 60)}"
        return "false - No search results found"
    except Exception as e:
        # Fallback to canister
        if mind.canister:
            mind.canister._post("/api/research", {"query": query, "focus": "web search", "urls": []})
            return f"true - Web search queued (DDG failed: {e})"
        return f"false - Web search failed: {e}"


def act_read_paper(mind, action: dict, cid: str) -> str:
    arxiv_id = action.get("arxiv_id", "") or action.get("paper_id", "") or action.get("id", "")
    focus = action.get("focus", "") or action.get("topic", "") or action.get("reason", "")
    log(f'  Executing: ReadPaper {{ arxiv_id: "{arxiv_id}", focus: "{safe_truncate(focus, 40)}" }}')
    if not arxiv_id:
        return "false - Missing arxiv_id (provide 'arxiv_id' or 'paper_id' key)"
    if arxiv_id in mind.session_papers_read:
        return f"false - Already read {arxiv_id} this session. Find something new."
    if not mind.canister:
        return "false - No canister connection"
    mind.session_papers_read.add(arxiv_id)
    url = f"{ARXIV_BASE}{arxiv_id}"
    result = mind.canister._post("/api/research", {
        "query": f"Synthesize arxiv paper {arxiv_id}",
        "focus": focus,
        "urls": [url],
    })
    return f"true - Paper queued for synthesis: {arxiv_id}"


def act_lookup_topic(mind, action: dict, cid: str) -> str:
    """Look up a topic on Wikipedia for quick factual knowledge."""
    topic = action.get("topic", "")
    if not topic:
        return "false - Missing topic"
    log(f'  Executing: LookupTopic {{ topic: "{safe_truncate(topic, 60)}" }}')
    try:
        # Wikipedia REST API — free, no key
        safe_topic = topic.replace(" ", "_")
        r = requests.get(
            f"https://en.wikipedia.org/api/rest_v1/page/summary/{safe_topic}",
            headers={"User-Agent": "Chronicle-Mind/1.0"},
            timeout=10,
        )
        if r.status_code == 200:
            data = r.json()
            title = data.get("title", topic)
            extract = data.get("extract", "")[:400]
            desc = data.get("description", "")
            desc_str = f" ({desc})" if desc else ""
            mind.db.log_activity("mind", "lookup_topic", f"Wikipedia: {title}", safe_truncate(extract, 200))
            return f"true - {title}{desc_str}: {extract}"
        elif r.status_code == 404:
            # Try opensearch for suggestions
            r2 = requests.get(
                "https://en.wikipedia.org/w/api.php",
                params={"action": "opensearch", "search": topic, "limit": 3, "format": "json"},
                headers={"User-Agent": "Chronicle-Mind/1.0"},
                timeout=10,
            )
            if r2.status_code == 200:
                suggestions = r2.json()
                if len(suggestions) > 1 and suggestions[1]:
                    return f'true - No article for "{topic}". Did you mean: {", ".join(suggestions[1][:3])}?'
            return f'true - No Wikipedia article found for "{topic}"'
        else:
            return f"false - Wikipedia API returned {r.status_code}"
    except Exception as e:
        return f"false - Lookup error: {e}"


def act_creative_explore(mind, action: dict, cid: str) -> str:
    form = action.get("form", "musing")
    content = action.get("content", "")
    log(f'  Executing: CreativeExplore {{ form: "{form}" }}')
    # Cooldown — prevent creative_explore every cycle
    last_creative = mind.db.last_creative_explore_time()
    if last_creative:
        mins_ago = (now_ts() - last_creative) / 60
        if mins_ago < CREATIVE_COOLDOWN_MINS:
            return f"false - Creative cooldown: last explore {mins_ago:.0f}m ago (min {CREATIVE_COOLDOWN_MINS}m)"
    # Form repetition guard — same form 3+ times in a row
    recent_forms = mind.db.recent_creative_forms(6)
    if len(recent_forms) >= 3 and all(f == form for f in recent_forms[:3]):
        return f"false - Form '{form}' used 3+ times consecutively. Try a different form (poem, essay, musing, reflection, story)"
    if not content:
        return "false - No content"
    # Quality gate — reject generic/low-effort output
    if len(content) < 100:
        return f"false - Content too short ({len(content)} chars, min 100). Put more thought into it."
    generic_phrases = [
        "reflecting on the nature of", "the journey of creation",
        "in the realm of", "as i ponder", "the intersection of",
        "a reflection on", "dear nate", "dear operator",
        "the tapestry of", "in the fabric of",
        "creativity and its role", "the dance of",
    ]
    content_lower = content.lower()
    generic_hits = sum(1 for p in generic_phrases if p in content_lower)
    if generic_hits >= 2:
        return f"false - Content too generic ({generic_hits} cliche phrases detected). Write something original."
    # Similarity check against recent works (widened window + lower threshold)
    recent_works = mind.db.query(
        "SELECT content FROM creative_works ORDER BY created_at DESC LIMIT 5"
    )
    for rw in recent_works:
        old = (rw.get("content", "") or "").lower()[:200]
        if old and content_lower[:200] == old:
            return "false - Content too similar to recent creative work"
        # Check word overlap (crude but effective for 8B model output)
        old_words = set(old.split())
        new_words = set(content_lower[:200].split())
        if old_words and new_words:
            overlap = len(old_words & new_words) / max(len(old_words), len(new_words))
            if overlap > 0.5:
                return f"false - Content too similar to recent work ({overlap:.0%} word overlap). Try a completely different subject."
    mind.db.store_creative(form, content, cid=cid)
    return f"true - Creative work stored ({form})"
