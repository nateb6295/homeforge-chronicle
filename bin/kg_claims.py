#!/usr/bin/env python3
"""Concept-aware KG extraction: extract CLAIMS from capsules, not just entity pairs.

Instead of: entity A + entity B → relationship verb
This does:  text → what is being CLAIMED? → structured claim with direction

"Curcumin shows therapeutic potential against Alzheimer's" becomes:
  claim: "curcumin has therapeutic potential against Alzheimer's"
  type: empirical_finding
  concepts: [curcumin, Alzheimer's, therapeutic potential]
  direction: curcumin → Alzheimer's (treats)

Run: python3 kg_claims.py [--limit N] [--dry-run] [--cloud]
"""

import sqlite3, os, re, json, time, sys, requests

DB_PATH = os.environ.get(
    "CHRONICLE_DB",
    "/mnt/hdd/chronicle-data/processed.db",
)
GEMMA_URL = os.environ.get("GEMMA_URL", "http://localhost:11435")
MODEL = "gemma-4-26B-A4B-it-Q4_K_M.gguf"
ENGINE_URL = "http://localhost:11436"

CLAIM_EXTRACTION_PROMPT = """Extract 1-3 claims from the text. A claim is an assertion about how things relate.

Reply ONLY with a JSON array of objects. Example:
[{"claim":"X does Y to Z","source_entity":"X","target_entity":"Z","relation":"does_Y_to","confidence":0.8}]

Required keys per object: claim (string), source_entity (string), target_entity (string), relation (string), confidence (number 0-1).

No other text. Just the JSON array."""


def log(msg):
    print(f"[{time.strftime('%H:%M:%S')}] {msg}")


def get_unprocessed_capsules(db, limit=50):
    """Get capsules that haven't been claim-extracted yet."""
    # Use capsules that have KG mentions but look for ones with poor relationships
    return db.execute("""
        SELECT kc.id, kc.restatement, kc.topic
        FROM knowledge_capsules kc
        WHERE kc.restatement IS NOT NULL
          AND length(kc.restatement) > 300
          AND kc.topic NOT LIKE 'threads/%'
          AND kc.topic NOT LIKE 'self/%'
          AND kc.topic NOT LIKE 'hermes/%'
          AND kc.topic NOT LIKE 'chronicle/reflection%'
          AND kc.id NOT IN (
              SELECT DISTINCT source_id FROM kg_mentions
              WHERE source_type = 'claim_extract'
          )
        ORDER BY kc.id DESC
        LIMIT ?
    """, (limit,)).fetchall()


def extract_claims_cloud(text):
    """Use Qwen3-235B via chronicle-engine for higher quality claim extraction."""
    try:
        r = requests.post(
            f"{ENGINE_URL}/api/chat",
            json={
                "model": "chronicle-deep",
                "messages": [
                    {"role": "system", "content": CLAIM_EXTRACTION_PROMPT},
                    {"role": "user", "content": f"Text: {text[:1200]}"},
                ],
                "options": {"num_predict": 1024, "temperature": 0.2},
                "stream": False,
            },
            timeout=60,
        )
        if r.status_code != 200:
            log(f"  Cloud error: {r.status_code}")
            return []

        raw = r.json().get("message", {}).get("content", "").strip()
        # Strip think tags
        if '</think>' in raw:
            raw = raw.split('</think>')[-1].strip()
        raw = re.sub(r'```json\s*', '', raw)
        raw = re.sub(r'```', '', raw)
        raw = raw.strip()
        match = re.search(r'\[.*\]', raw, re.DOTALL)
        if not match:
            return []
        json_str = match.group()
        json_str = re.sub(r',\s*([}\]])', r'\1', json_str)
        try:
            claims = json.loads(json_str)
        except json.JSONDecodeError:
            objs = re.findall(r'\{[^{}]+\}', json_str)
            claims = []
            for obj in objs:
                obj = re.sub(r',\s*}', '}', obj)
                try:
                    claims.append(json.loads(obj))
                except json.JSONDecodeError:
                    continue
        return claims[:3]
    except Exception as e:
        log(f"  Cloud extract error: {e}")
        return []


def extract_claims(text):
    """Use Gemma to extract structured claims from text."""
    try:
        r = requests.post(
            f"{GEMMA_URL}/v1/chat/completions",
            json={
                "model": MODEL,
                "messages": [
                    {"role": "system", "content": CLAIM_EXTRACTION_PROMPT},
                    {"role": "user", "content": f"Text: {text[:800]}"},
                ],
                "max_tokens": 1024,
                "temperature": 0.2,
            },
            timeout=60,
        )
        if r.status_code != 200:
            log(f"  LLM error: {r.status_code}")
            return []

        raw = r.json().get("choices", [{}])[0].get("message", {}).get("content", "").strip()
        # Strip think tags
        if '</think>' in raw:
            raw = raw.split('</think>')[-1].strip()
        raw = re.sub(r'```json\s*', '', raw)
        raw = re.sub(r'```', '', raw)
        raw = raw.strip()
        match = re.search(r'\[.*\]', raw, re.DOTALL)
        if not match:
            return []
        json_str = match.group()
        # Fix common Gemma JSON issues: trailing commas, single quotes
        json_str = re.sub(r',\s*([}\]])', r'\1', json_str)
        json_str = json_str.replace("'", '"')
        try:
            claims = json.loads(json_str)
        except json.JSONDecodeError:
            # Last resort: try to extract individual objects
            objs = re.findall(r'\{[^{}]+\}', json_str)
            claims = []
            for obj in objs:
                obj = re.sub(r',\s*}', '}', obj)
                try:
                    claims.append(json.loads(obj))
                except json.JSONDecodeError:
                    continue
        return claims[:3]
    except Exception as e:
        log(f"  Extract error: {e}")
        return []


def store_claim(db, capsule_id, claim, timestamp):
    """Store a claim as a high-quality KG relationship."""
    src_name = claim.get("source_entity", "").strip()
    tgt_name = claim.get("target_entity", "").strip()
    relation = claim.get("relation", "").strip()
    confidence = min(1.0, max(0.0, float(claim.get("confidence", 0.5))))
    claim_text = claim.get("claim", "")
    claim_type = claim.get("type", "unknown")
    concepts = claim.get("concepts", [])

    if not src_name or not tgt_name or not relation:
        return False

    # Normalize relation: lowercase, underscores
    relation = re.sub(r'\s+', '_', relation.lower().strip())
    # Cap length
    if len(relation) > 60:
        relation = relation[:60]

    # Find or create entities
    src_id = find_or_create_entity(db, src_name, timestamp)
    tgt_id = find_or_create_entity(db, tgt_name, timestamp)

    if src_id == tgt_id:
        return False

    # Store with claim text as evidence
    evidence = json.dumps({
        "claim": claim_text,
        "type": claim_type,
        "concepts": concepts,
        "source": "claim_extract",
    })

    existing = db.execute(
        "SELECT id FROM kg_relationships WHERE source_entity = ? AND target_entity = ? AND relation_type = ?",
        (src_id, tgt_id, relation),
    ).fetchone()

    if existing:
        db.execute(
            "UPDATE kg_relationships SET mention_count = mention_count + 1, "
            "confidence = MIN(1.0, confidence + 0.05), last_seen = ?, evidence = ? WHERE id = ?",
            (timestamp, evidence, existing[0]),
        )
    else:
        db.execute(
            "INSERT INTO kg_relationships (source_entity, target_entity, relation_type, "
            "confidence, evidence, first_seen, last_seen, valid_from) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (src_id, tgt_id, relation, confidence, evidence, timestamp, timestamp, timestamp),
        )

    return True


def find_or_create_entity(db, name, timestamp):
    """Find existing entity or create new one."""
    row = db.execute(
        "SELECT id FROM kg_entities WHERE LOWER(canonical_name) = LOWER(?)", (name,)
    ).fetchone()
    if row:
        db.execute(
            "UPDATE kg_entities SET mention_count = mention_count + 1, last_seen = ? WHERE id = ?",
            (timestamp, row[0]),
        )
        return row[0]

    cur = db.execute(
        "INSERT INTO kg_entities (canonical_name, entity_type, first_seen, last_seen) VALUES (?, 'concept', ?, ?)",
        (name, timestamp, timestamp),
    )
    return cur.lastrowid


def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=10)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--cloud", action="store_true", help="Use Qwen3-235B via DeepInfra instead of local Gemma")
    args = parser.parse_args()

    extractor = extract_claims_cloud if args.cloud else extract_claims
    backend = "cloud (Qwen3-235B)" if args.cloud else "local (Gemma)"

    db = sqlite3.connect(DB_PATH)
    capsules = get_unprocessed_capsules(db, args.limit)
    log(f"Found {len(capsules)} capsules to process via {backend}")

    total_claims = 0
    total_stored = 0

    for cap_id, restatement, topic in capsules:
        log(f"Processing capsule {cap_id} [{topic}]")
        claims = extractor(restatement)

        if not claims:
            log(f"  No claims extracted")
            continue

        for claim in claims:
            total_claims += 1
            claim_text = claim.get("claim", "")[:80]
            log(f"  Claim: {claim_text}...")

            if args.dry_run:
                relation = claim.get("relation", "?")
                src = claim.get("source_entity", "?")
                tgt = claim.get("target_entity", "?")
                log(f"    → {src} --[{relation}]--> {tgt}")
                total_stored += 1
            else:
                ts = int(time.time())
                if store_claim(db, cap_id, claim, ts):
                    total_stored += 1

        if not args.dry_run:
            # Mark capsule as claim-extracted so we don't reprocess
            ts = int(time.time())
            db.execute(
                "INSERT OR IGNORE INTO kg_mentions (entity_id, source_type, source_id, context, timestamp) "
                "VALUES (0, 'claim_extract', ?, ?, ?)",
                (cap_id, f"{len(claims)} claims", ts),
            )
            db.commit()  # commit per capsule, not per batch — avoids DB lock storms

    log(f"\nDone: {total_claims} claims extracted, {total_stored} stored from {len(capsules)} capsules")
    db.close()


if __name__ == "__main__":
    main()
