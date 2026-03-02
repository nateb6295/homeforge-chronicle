"""Chronicle Mind - Capsule exploration action handlers (on-chain memory)."""

import json
from typing import Optional

import requests

from mind.utils import log, safe_truncate, get_embeddings
from mind.config import OLLAMA_URL, DEEP_MODEL


def _normalize_topic(raw) -> str:
    """Canister returns topic as nested list (e.g. [['homeforge/philosophy']]), list, or string."""
    if isinstance(raw, list):
        # Flatten nested lists: [['a']] -> 'a', ['a', 'b'] -> 'a, b'
        flat = []
        for item in raw:
            if isinstance(item, list):
                flat.extend(item)
            else:
                flat.append(str(item))
        return ", ".join(flat) if flat else ""
    return str(raw) if raw else ""


def act_read_capsule(mind, action: dict, cid: str) -> str:
    """Read a specific capsule by ID."""
    capsule_id = action.get("capsule_id")
    if not capsule_id:
        return "false - Missing capsule_id"
    try:
        capsule_id = int(capsule_id)
    except (ValueError, TypeError):
        return f"false - Invalid capsule_id: {capsule_id}"

    if not mind.llm.icp_agent:
        return "false - No canister connection"

    log(f"  Executing: ReadCapsule {{ id: {capsule_id} }}")
    try:
        capsule = mind.llm.icp_agent.get_capsule(capsule_id)
        if not capsule:
            return f"false - Capsule #{capsule_id} not found"
        content = capsule.get("content", "")[:500]
        topic = _normalize_topic(capsule.get("topic", "unknown"))
        ts = capsule.get("timestamp", "")
        persons = capsule.get("persons", [])
        persons_str = f" | Persons: {', '.join(persons)}" if persons else ""
        return f"true - Capsule #{capsule_id}\nTopic: {topic}\nTimestamp: {ts}{persons_str}\nContent: {content}"
    except Exception as e:
        return f"false - Read error: {e}"


def act_search_canister(mind, action: dict, cid: str) -> str:
    """Search capsules by keyword."""
    query = action.get("query", "")
    if not query:
        return "false - Missing query"
    limit = min(int(action.get("limit", 10)), 20)

    if not mind.llm.icp_agent:
        return "false - No canister connection"

    log(f"  Executing: SearchCanister {{ query: \"{query}\", limit: {limit} }}")
    try:
        capsules = mind.llm.icp_agent.search_by_keyword(query, limit)
        if not capsules:
            return f"true - No capsules found for \"{query}\""
        lines = [f"Found {len(capsules)} capsules for \"{query}\":"]
        for c in capsules:
            cid_num = c.get("id", "?")
            topic = _normalize_topic(c.get("topic", ""))
            preview = c.get("content", "")[:100].replace("\n", " ")
            lines.append(f"  #{cid_num} [{topic}] {preview}")
        return f"true - {chr(10).join(lines)}"
    except Exception as e:
        return f"false - Search error: {e}"


def act_explore_capsules(mind, action: dict, cid: str) -> str:
    """Browse recent capsules, optionally filtered by topic."""
    topic_filter = (action.get("topic") or "").lower()
    limit = min(int(action.get("limit", 10)), 30)

    if not mind.llm.icp_agent:
        return "false - No canister connection"

    # Fetch more if filtering, to have enough after filter
    fetch_limit = limit * 3 if topic_filter else limit
    log(f"  Executing: ExploreCapsules {{ topic: \"{topic_filter or 'any'}\", limit: {limit} }}")
    try:
        capsules = mind.llm.icp_agent.get_recent_capsules(min(fetch_limit, 50))
        if topic_filter:
            capsules = [c for c in capsules if topic_filter in _normalize_topic(c.get("topic", "")).lower()]
        capsules = capsules[:limit]
        if not capsules:
            return f"true - No recent capsules" + (f" matching topic \"{topic_filter}\"" if topic_filter else "")
        lines = [f"Recent capsules ({len(capsules)}" + (f", topic: {topic_filter}" if topic_filter else "") + "):"]
        for c in capsules:
            cid_num = c.get("id", "?")
            topic = _normalize_topic(c.get("topic", ""))
            preview = c.get("content", "")[:80].replace("\n", " ")
            lines.append(f"  #{cid_num} [{topic}] {preview}")
        return f"true - {chr(10).join(lines)}"
    except Exception as e:
        return f"false - Explore error: {e}"


def act_search_capsules_semantic(mind, action: dict, cid: str) -> str:
    """Search capsules by semantic similarity (concept search, not keyword)."""
    query = action.get("query", "")
    if not query:
        return "false - Missing query"
    limit = min(int(action.get("limit", 5)), 10)

    if not mind.llm.icp_agent:
        return "false - No canister connection"

    log(f'  Executing: SemanticSearch {{ query: "{safe_truncate(query, 60)}", limit: {limit} }}')
    try:
        embeddings = get_embeddings([query])
        if not embeddings or not embeddings[0]:
            return "false - Embedding failed (Ollama unavailable?)"
        embedding = embeddings[0]
        capsules = mind.llm.icp_agent.semantic_search(embedding, limit)
        if not capsules:
            return f'true - No capsules semantically match "{safe_truncate(query, 40)}"'
        lines = [f'Found {len(capsules)} capsules for "{safe_truncate(query, 40)}":']
        for c in capsules:
            cid_num = c.get("id", "?")
            topic = _normalize_topic(c.get("topic", ""))
            score = c.get("score", 0)
            preview = c.get("content", "")[:100].replace("\n", " ")
            lines.append(f"  #{cid_num} [{topic}] (score: {score:.2f}) {preview}")
        return f"true - {chr(10).join(lines)}"
    except Exception as e:
        return f"false - Semantic search error: {e}"


def act_search_capsules_person(mind, action: dict, cid: str) -> str:
    """Search capsules mentioning a specific person."""
    name = action.get("name", "") or action.get("person", "")
    if not name:
        return "false - Missing name/person"
    limit = min(int(action.get("limit", 10)), 20)

    if not mind.llm.icp_agent:
        return "false - No canister connection"

    log(f'  Executing: PersonSearch {{ name: "{name}", limit: {limit} }}')
    try:
        capsules = mind.llm.icp_agent.search_by_person(name, limit)
        if not capsules:
            return f'true - No capsules mention "{name}"'
        lines = [f'Found {len(capsules)} capsules mentioning "{name}":']
        for c in capsules:
            cid_num = c.get("id", "?")
            topic = _normalize_topic(c.get("topic", ""))
            preview = c.get("content", "")[:100].replace("\n", " ")
            lines.append(f"  #{cid_num} [{topic}] {preview}")
        return f"true - {chr(10).join(lines)}"
    except Exception as e:
        return f"false - Person search error: {e}"


def act_consult_local_qwen(mind, action: dict, cid: str) -> str:
    # DISABLED: single model architecture (Qwen3-8B execution layer)
    return "false - Deep model disabled (single model architecture)"
    topic = action.get("topic", "")
    context = action.get("context", "")
    log(f'  Executing: ConsultLocalQwen {{ topic: "{safe_truncate(topic, 40)}" }}')
    if not mind.llm.ollama_available:
        return "false - Local Qwen (Ollama) is not available"
    try:
        prompt = (f"Topic: {topic}\n\nContext: {context}" if context else topic)
        msgs = [{"role": "user", "content": prompt}]
        log(f"    Using deep model: {DEEP_MODEL}")
        r = requests.post(
            f"{OLLAMA_URL}/api/chat",
            json={"model": DEEP_MODEL, "messages": msgs, "stream": False,
                  "options": {"num_ctx": 4096}},
            timeout=120,  # Qwen3-8B
        )
        r.raise_for_status()
        response = r.json().get("message", {}).get("content", "")
        if response:
            mind.db.log_activity("qwen", "consultation", f"Qwen: {safe_truncate(topic, 40)}",
                                 safe_truncate(response, 500))
            return f"true - Local Qwen response: {safe_truncate(response, 100)}"
        return "false - Empty response from local Qwen"
    except Exception as e:
        return f"false - Local Qwen error: {e}"
