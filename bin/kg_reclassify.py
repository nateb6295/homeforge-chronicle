#!/usr/bin/env python3
"""KG Predicate Reclassifier — Recover signal from related_to fallback.

Build #82's normalizer collapsed 47% of predicates to related_to because it
only looks at the predicate string. But the evidence text usually reveals the
actual relationship. Gemma flagged the signal loss.

This reclassifier reads the evidence text and uses keyword patterns to recover
the intended predicate. Conservative — only reclassifies when confidence is high.

Usage:
    python3 kg_reclassify.py preview          # show what would change
    python3 kg_reclassify.py apply            # apply reclassification
    python3 kg_reclassify.py stats            # show before/after distribution

Phase 1 (Build #89): Core patterns (creates, uses, threatens, enables, etc.)
Phase 2 (Build #90): Action-narrative patterns (challenges, warns, asserts,
  frames, attacks, mediates, leads). Dropped related_to from 41% to ~32%.

Build #89 + #90. Gemma's signal loss concern, addressed in two phases.
"""

import os
import sys
import re
import sqlite3
import time
import requests
from collections import Counter

DB_PATH = os.environ.get("CHRONICLE_DB", "/mnt/hdd/chronicle-data/processed.db")

# Evidence keyword patterns → canonical predicate
# Each pattern: (compiled_regex, canonical_predicate, weight)
# Higher weight = more specific/reliable signal
EVIDENCE_PATTERNS = [
    # Creation / release
    (re.compile(r'\b(released|launches?d?|unveiled|announced|introduced|rolled out|shipped|deployed)\b', re.I), "creates", 3),
    (re.compile(r'\b(developed|built|designed|engineered|crafted)\s+(a |an |the )?', re.I), "creates", 2),
    (re.compile(r'\bnew (framework|tool|model|system|platform|library|method|approach|technique)\b', re.I), "creates", 2),
    (re.compile(r'\bpaper\s+(titled|called|named|proposes|introduces)\b', re.I), "creates", 2),
    (re.compile(r'\b(open[- ]sourced?|publishes?d?)\b', re.I), "creates", 2),

    # Research / analysis / reporting
    (re.compile(r'\b(study|research|paper|publication|journal|findings|demonstrates?d?)\b', re.I), "reports_on", 2),
    (re.compile(r'\b(analyzes?d?|investigates?d?|examines?d?|explores?d?)\b', re.I), "reports_on", 2),
    (re.compile(r'\b(survey|review|meta-analysis|benchmark)\b', re.I), "reports_on", 2),
    (re.compile(r'\b(shows?|reveals?|found that|evidence that)\b', re.I), "reports_on", 1),

    # Comparison / contrast
    (re.compile(r'\b(compared to|versus|vs\.?|alternative to|unlike|in contrast)\b', re.I), "differs_from", 3),
    (re.compile(r'\b(outperforms?|surpass(es)?|beats?|exceeds?)\b', re.I), "differs_from", 2),
    (re.compile(r'\b(competitor|rival|competing)\b', re.I), "differs_from", 3),
    (re.compile(r'\b(without|rather than|instead of|not .{0,20} but)\b', re.I), "differs_from", 1),

    # Similarity / alignment
    (re.compile(r'\b(mirrors?|echoes?|resembles?|parallel(s|ing)?)\b', re.I), "similar_to", 3),
    (re.compile(r'\b(aligns? with|consistent with|supports? the)\b', re.I), "aligns_with", 2),
    (re.compile(r'\b(endors(es?|ed|ement)|prais(es?|ed)|validated)\b', re.I), "aligns_with", 3),

    # Threat / opposition
    (re.compile(r'\b(threatens?|attacks?|targets?|undermin(es?|ing))\b', re.I), "threatens", 3),
    (re.compile(r'\b(critiqu(es?|ed|ing)|criticiz(es?|ed|ing)|opposes?d?)\b', re.I), "threatens", 3),
    (re.compile(r'\b(walking away|pulling out|severed|terminated|lawsuit|sued)\b', re.I), "threatens", 2),
    (re.compile(r'\b(accused|allegations?|fraud|scandal)\b', re.I), "threatens", 2),

    # Enabling / supporting
    (re.compile(r'\b(funded?|funding|grant|invested?|investment|backed by)\b', re.I), "enables", 3),
    (re.compile(r'\b(enables?d?|facilitates?d?|empowers?)\b', re.I), "enables", 2),
    (re.compile(r'\b(partnership|collaborate|collaboration|joint)\b', re.I), "collaborates_with", 3),

    # Usage / dependency
    (re.compile(r'\b(uses?|using|leverag(es?|ing)|utiliz(es?|ing)|built on|powered by)\b', re.I), "uses", 2),
    (re.compile(r'\b(relies? on|depends? on|based on|implemented (with|in|using))\b', re.I), "uses", 3),

    # Hosting / organizing
    (re.compile(r'\b(conference|summit|event|workshop|symposium)\b', re.I), "hosts", 2),
    (re.compile(r'\b(hosts?|hosting|organized?|organizing)\b', re.I), "hosts", 2),

    # Impact / influence
    (re.compile(r'\b(impacts?|reshap(es?|ing)|transform(s|ing)?|disrupts?)\b', re.I), "impacts", 2),
    (re.compile(r'\b(implications? for|consequences? for|affects?)\b', re.I), "impacts", 2),

    # Ownership / acquisition
    (re.compile(r'\b(acquir(es?|ed|ing)|bought|purchased|owns)\b', re.I), "owns", 3),
    (re.compile(r'\b(subsidiary|parent company|division of)\b', re.I), "part_of", 3),

    # Identification / discovery
    (re.compile(r'\b(identif(ies|ied|ying)|discover(s|ed|ing)?|detect(s|ed|ing)?)\b', re.I), "identifies", 2),
    (re.compile(r'\b(reveal(s|ed|ing)?|uncover(s|ed|ing)?|expos(es?|ed|ing))\b', re.I), "identifies", 2),

    # Location
    (re.compile(r'\b(based in|headquartered in|located in|operating in|set for)\b', re.I), "located_in", 3),

    # Implementation
    (re.compile(r'\b(implement(s|ed|ing)?|execut(es?|ed|ing)|deploy(s|ed|ing)?)\b', re.I), "implements", 2),
    (re.compile(r'\b(automat(es?|ed|ing)|integrat(es?|ed|ing))\b', re.I), "integrates_with", 2),

    # --- Phase 2 (Build #90): Action-narrative patterns ---

    # Challenge / opposition (stronger than "threatens" — institutional/legal)
    (re.compile(r'\b(challeng(es?|ed|ing))\b', re.I), "challenges", 3),
    (re.compile(r'\b(reject(s|ed|ing)?|oppos(es?|ed|ing))\b', re.I), "challenges", 3),
    (re.compile(r'\b(invalidat(es?|ed|ing)|contest(s|ed|ing)?)\b', re.I), "challenges", 3),

    # Warning / caution
    (re.compile(r'\b(warn(s|ed|ing)?|caution(s|ed|ing)?)\b', re.I), "warns", 3),

    # Assertion / claims
    (re.compile(r'\b(assert(s|ed|ing)?)\b', re.I), "asserts", 3),
    (re.compile(r'\b(claim(s|ed|ing)?\s+responsibility)\b', re.I), "asserts", 3),
    (re.compile(r'\b(argu(es?|ed|ing)\s+that)\b', re.I), "asserts", 2),
    (re.compile(r'\b(contend(s|ed|ing)?)\b', re.I), "asserts", 2),

    # Framing / characterization
    (re.compile(r'\b(frame[sd]?\b|framing)\b', re.I), "frames", 3),
    (re.compile(r'\b(characteriz(es?|ed|ing))\b', re.I), "frames", 3),

    # Cyber / breach
    (re.compile(r'\b(breach(es?|ed|ing)?)\b', re.I), "attacks", 3),
    (re.compile(r'\b(hack(s|ed|ing|ers?)?)\b', re.I), "attacks", 3),
    (re.compile(r'\b(intrusion(s)?)\b', re.I), "attacks", 3),

    # Mediation / negotiation
    (re.compile(r'\b(mediat(es?|ed|ing|or|ion))\b', re.I), "mediates", 3),
    (re.compile(r'\b(negotiat(es?|ed|ing|ion|ions))\b', re.I), "mediates", 3),

    # Leadership transitions
    (re.compile(r'\b(nominat(es?|ed|ing|ion))\b', re.I), "leads", 3),
    (re.compile(r'\b(appoint(s|ed|ing)?)\b', re.I), "leads", 3),
    (re.compile(r'\b(sworn in)\b', re.I), "leads", 3),
]

# Minimum total weight to reclassify (prevents weak signals from triggering)
RECLASSIFY_THRESHOLD = 3


def _score_evidence(evidence: str) -> dict:
    """Score evidence text against all patterns. Returns {predicate: total_weight}."""
    scores = Counter()
    for pattern, predicate, weight in EVIDENCE_PATTERNS:
        matches = pattern.findall(evidence)
        if matches:
            scores[predicate] += weight * min(len(matches), 3)  # cap at 3 matches
    return scores


def _best_predicate(evidence: str) -> tuple:
    """Return (best_predicate, score) or ("related_to", 0) if no strong signal."""
    scores = _score_evidence(evidence)
    if not scores:
        return ("related_to", 0)

    best = scores.most_common(1)[0]
    predicate, score = best

    # Must meet threshold
    if score < RECLASSIFY_THRESHOLD:
        return ("related_to", 0)

    # If top two are close (within 1 point), it's ambiguous — keep related_to
    if len(scores) > 1:
        second = scores.most_common(2)[1][1]
        if score - second <= 1:
            return ("related_to", 0)

    return (predicate, score)


def preview():
    """Show what reclassification would do."""
    db = sqlite3.connect(DB_PATH)
    rows = db.execute(
        "SELECT r.id, e1.canonical_name, e2.canonical_name, r.evidence "
        "FROM kg_relationships r "
        "JOIN kg_entities e1 ON r.source_entity = e1.id "
        "JOIN kg_entities e2 ON r.target_entity = e2.id "
        "WHERE r.relation_type = 'related_to' AND length(r.evidence) > 50"
    ).fetchall()
    db.close()

    changes = Counter()
    kept = 0
    examples = {}

    for row_id, src, tgt, evidence in rows:
        pred, score = _best_predicate(evidence)
        if pred != "related_to":
            changes[pred] += 1
            if pred not in examples or score > examples[pred][1]:
                examples[pred] = (f"{src} → {tgt}", score, evidence[:100])
        else:
            kept += 1

    total_changed = sum(changes.values())
    print(f"\nKG Predicate Reclassification Preview")
    print(f"{'='*60}")
    print(f"  related_to relationships: {len(rows)}")
    print(f"  Would reclassify: {total_changed} ({total_changed/len(rows)*100:.0f}%)")
    print(f"  Would keep as related_to: {kept} ({kept/len(rows)*100:.0f}%)")

    print(f"\n  Reclassification breakdown:")
    for pred, count in changes.most_common():
        ex = examples.get(pred, ("", 0, ""))
        print(f"    {pred:25} {count:4}")
        print(f"      e.g. {ex[0][:50]:50} (score={ex[1]})")
        print(f"           {ex[2][:80]}")

    # Show the new distribution
    db = sqlite3.connect(DB_PATH)
    current = dict(db.execute(
        "SELECT relation_type, COUNT(*) FROM kg_relationships GROUP BY relation_type"
    ).fetchall())
    db.close()

    print(f"\n  Projected distribution (top 10):")
    projected = Counter(current)
    projected["related_to"] -= total_changed
    for pred, count in changes.items():
        projected[pred] = projected.get(pred, 0) + count

    for pred, count in projected.most_common(10):
        total = sum(projected.values())
        pct = count / total * 100
        print(f"    {pred:25} {count:4} ({pct:.1f}%)")

    return total_changed


def apply():
    """Apply reclassification to related_to relationships."""
    db = sqlite3.connect(DB_PATH)
    rows = db.execute(
        "SELECT id, evidence FROM kg_relationships "
        "WHERE relation_type = 'related_to' AND length(evidence) > 50"
    ).fetchall()

    changes = 0
    for row_id, evidence in rows:
        pred, score = _best_predicate(evidence)
        if pred != "related_to":
            db.execute(
                "UPDATE kg_relationships SET relation_type = ? WHERE id = ?",
                (pred, row_id)
            )
            changes += 1

    db.commit()
    db.close()

    print(f"\nKG Predicate Reclassification Applied")
    print(f"  Processed: {len(rows)} related_to relationships")
    print(f"  Reclassified: {changes}")
    print(f"  Remaining related_to: {len(rows) - changes}")

    # Show new stats
    stats()
    return changes


def stats():
    """Show current predicate distribution."""
    db = sqlite3.connect(DB_PATH)
    rows = db.execute(
        "SELECT relation_type, COUNT(*) as cnt FROM kg_relationships "
        "GROUP BY relation_type ORDER BY cnt DESC"
    ).fetchall()
    total = db.execute("SELECT COUNT(*) FROM kg_relationships").fetchone()[0]
    db.close()

    related_to_count = next((c for r, c in rows if r == "related_to"), 0)
    related_to_pct = related_to_count / total * 100

    print(f"\nKG Predicate Distribution")
    print(f"{'='*50}")
    print(f"  Total: {total}  |  Unique predicates: {len(rows)}  |  related_to: {related_to_count} ({related_to_pct:.1f}%)")
    for rel_type, cnt in rows[:15]:
        pct = cnt / total * 100
        bar = "█" * int(pct / 2)
        print(f"    {rel_type:25} {cnt:4} ({pct:4.1f}%) {bar}")


def gemma_classify(dry_run=False, batch_size=10, limit=50):
    """Phase 3: Use Gemma to reclassify remaining related_to entries.

    For entries the regex couldn't handle, ask Gemma to read the evidence
    and pick from a controlled vocabulary.
    """
    import requests
    import time

    GEMMA_URL = "http://localhost:11435"
    GEMMA_MODEL = "gemma-4-26B-A4B-it-Q4_K_M.gguf"

    # Controlled vocabulary for Gemma to pick from
    VOCAB = [
        "uses", "creates", "part_of", "hosts", "enables",
        "located_in", "affiliated_with", "implements", "impacts",
        "challenges", "similar_to", "differs_from", "leads",
        "reports_on", "threatens", "developed_by", "authored",
        "collaborates_with", "regulates", "funds", "analyzes",
        "supports", "critiques", "subject_of", "owns",
        "inspired_by", "competes_with", "preceded_by", "succeeded_by",
        "captures", "mediates", "references", "contains", "frames",
        "identifies", "spans",
    ]

    db = sqlite3.connect(DB_PATH, timeout=10)
    rows = db.execute(
        "SELECT r.id, e1.canonical_name, e2.canonical_name, r.evidence, "
        "e1.entity_type, e2.entity_type "
        "FROM kg_relationships r "
        "JOIN kg_entities e1 ON r.source_entity = e1.id "
        "JOIN kg_entities e2 ON r.target_entity = e2.id "
        "WHERE r.relation_type = 'related_to' AND length(r.evidence) > 30 "
        "ORDER BY r.mention_count DESC "
        f"LIMIT {limit}"
    ).fetchall()

    if not rows:
        print("No related_to entries to reclassify.")
        db.close()
        return

    print(f"\nGemma Reclassification — {'DRY RUN' if dry_run else 'LIVE'}")
    print(f"  Entries: {len(rows)}, batch size: {batch_size}")
    print(f"  Vocabulary: {len(VOCAB)} types\n")

    total_updated = 0
    total_kept = 0

    for i in range(0, len(rows), batch_size):
        batch = rows[i:i+batch_size]

        pair_text = "\n".join(
            f"{j+1}. {src} ({stype}) → {tgt} ({ttype})\n"
            f"   Evidence: {evidence[:300]}"
            for j, (rid, src, tgt, evidence, stype, ttype) in enumerate(batch)
        )

        prompt = (
            f"Each line below describes a relationship between two entities. "
            f"Replace 'related_to' with the BEST single term from this vocabulary:\n"
            f"{', '.join(VOCAB)}\n\n"
            f"Be decisive. Most entries WILL have a better fit than 'related_to'. "
            f"Only keep 'related_to' if truly none fit.\n"
            f"Respond with ONLY numbered lines like: 1. relation_type\n\n"
            f"{pair_text}"
        )

        try:
            resp = requests.post(
                f"{GEMMA_URL}/v1/chat/completions",
                json={
                    "model": GEMMA_MODEL,
                    "messages": [{"role": "user", "content": prompt}],
                    "max_tokens": 200,
                    "temperature": 0.1,
                    "repeat_penalty": 1.1,
                },
                timeout=60,
            )
            resp.raise_for_status()
            data = resp.json()
            response = data["choices"][0]["message"]["content"].strip()
        except Exception as e:
            print(f"  Gemma error: {e}")
            continue

        # Parse response
        classifications = []
        for line in response.split("\n"):
            line = line.strip()
            if not line:
                continue
            parts = line.split(".", 1)
            if len(parts) == 2 and parts[0].strip().isdigit():
                rel = parts[1].strip().lower().replace(" ", "_").replace("-", "_").strip("*`\"'")
            else:
                rel = line.strip().lower().replace(" ", "_").replace("-", "_").strip("*`\"'")
            classifications.append(rel)

        # Apply
        for j, (rid, src, tgt, evidence, stype, ttype) in enumerate(batch):
            if j >= len(classifications):
                total_kept += 1
                continue

            new_rel = classifications[j]
            # Filter: must be in the controlled vocabulary
            valid = (new_rel and new_rel != "related_to"
                     and new_rel in VOCAB)
            if valid:
                if dry_run:
                    print(f"  WOULD: {src} → {tgt}: → {new_rel}")
                else:
                    db.execute(
                        "UPDATE kg_relationships SET relation_type = ? WHERE id = ?",
                        (new_rel, rid),
                    )
                    print(f"  ✓ {src} → {tgt}: → {new_rel}")
                total_updated += 1
            else:
                total_kept += 1

        if not dry_run:
            db.commit()

        print(f"  Batch {i//batch_size + 1}: updated {total_updated}, kept {total_kept}")
        if i + batch_size < len(rows):
            time.sleep(2)  # Rate limit

    db.close()
    print(f"\nDone. Reclassified: {total_updated}, kept: {total_kept}")
    stats()


def normalize_predicates(dry_run=False, limit=50, batch_size=10):
    """Normalize sentence-length predicates to controlled vocabulary via Gemma."""
    GEMMA_URL = "http://localhost:11435"
    VOCAB = [
        "uses", "creates", "part_of", "hosts", "enables",
        "located_in", "affiliated_with", "implements", "impacts",
        "challenges", "similar_to", "differs_from", "leads",
        "reports_on", "threatens", "developed_by", "authored",
        "collaborates_with", "regulates", "funds", "analyzes",
        "supports", "critiques", "subject_of", "owns",
        "inspired_by", "competes_with", "preceded_by", "succeeded_by",
        "captures", "mediates", "references", "contains", "frames",
        "identifies", "spans",
    ]

    db = sqlite3.connect(DB_PATH, timeout=10)
    rows = db.execute(
        "SELECT r.id, e1.canonical_name, e2.canonical_name, r.relation_type, "
        "e1.entity_type, e2.entity_type "
        "FROM kg_relationships r "
        "JOIN kg_entities e1 ON r.source_entity = e1.id "
        "JOIN kg_entities e2 ON r.target_entity = e2.id "
        "WHERE r.superseded_by IS NULL AND LENGTH(r.relation_type) > 25 "
        "ORDER BY r.mention_count DESC "
        f"LIMIT {limit}"
    ).fetchall()

    if not rows:
        print("No sentence-length predicates to normalize.")
        db.close()
        return

    print(f"\nPredicate Normalization — {'DRY RUN' if dry_run else 'LIVE'}")
    print(f"  Entries: {len(rows)}, batch size: {batch_size}")
    print(f"  Vocabulary: {len(VOCAB)} types\n")

    total_updated = 0
    total_kept = 0

    for i in range(0, len(rows), batch_size):
        batch = rows[i:i+batch_size]

        pair_text = "\n".join(
            f"{j+1}. {src} ({stype}) --[{rel}]--> {tgt} ({ttype})"
            for j, (rid, src, tgt, rel, stype, ttype) in enumerate(batch)
        )

        prompt = (
            f"Each line below has a verbose relation between two entities. "
            f"Replace it with the BEST single term from this vocabulary:\n"
            f"{', '.join(VOCAB)}\n\n"
            f"If NONE fit well, write 'related_to'.\n"
            f"Respond with ONLY numbered lines like: 1. relation_type\n\n"
            f"{pair_text}"
        )

        try:
            resp = requests.post(
                f"{GEMMA_URL}/v1/chat/completions",
                json={
                    "model": "gemma-4-26B-A4B-it-Q4_K_M.gguf",
                    "messages": [{"role": "user", "content": prompt}],
                    "temperature": 0.1,
                    "max_tokens": 200,
                },
                timeout=30,
            )
            resp.raise_for_status()
            text = resp.json()["choices"][0]["message"]["content"].strip()
        except Exception as e:
            print(f"  Gemma error: {e}")
            continue

        # Parse: "1. relation_type" lines
        classifications = []
        for line in text.split("\n"):
            line = line.strip()
            if not line:
                continue
            parts = line.split(".", 1)
            if len(parts) == 2 and parts[0].strip().isdigit():
                rel = parts[1].strip().lower().replace(" ", "_")
                # Clean any trailing punctuation or explanation
                rel = rel.split()[0] if rel else ""
                classifications.append(rel)

        for j, (rid, src, tgt, old_rel, stype, ttype) in enumerate(batch):
            if j >= len(classifications):
                total_kept += 1
                continue

            new_rel = classifications[j]
            valid = (new_rel and new_rel != old_rel
                     and new_rel in VOCAB)
            if valid:
                if dry_run:
                    print(f"  WOULD: {src} → {tgt}: [{old_rel[:40]}...] → {new_rel}")
                else:
                    db.execute(
                        "UPDATE kg_relationships SET relation_type = ? WHERE id = ?",
                        (new_rel, rid),
                    )
                    print(f"  ✓ {src} → {tgt}: → {new_rel}")
                total_updated += 1
            else:
                if dry_run and new_rel:
                    print(f"  KEEP:  {src} → {tgt}: [{old_rel[:40]}...] (Gemma said: {new_rel})")
                total_kept += 1

        if not dry_run:
            db.commit()

        print(f"  Batch {i//batch_size + 1}: updated {total_updated}, kept {total_kept}")
        if i + batch_size < len(rows):
            time.sleep(2)

    db.close()
    print(f"\nDone. Normalized: {total_updated}, kept: {total_kept}")
    stats()


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage:")
        print("  kg_reclassify.py preview    # show what regex would change")
        print("  kg_reclassify.py apply      # apply regex reclassification")
        print("  kg_reclassify.py stats      # current distribution")
        print("  kg_reclassify.py gemma      # Gemma-based reclassification (Phase 3)")
        print("  kg_reclassify.py gemma --dry-run  # preview Gemma changes")
        print("  kg_reclassify.py normalize  # normalize sentence-length predicates")
        print("  kg_reclassify.py normalize --dry-run  # preview normalization")
        sys.exit(0)

    cmd = sys.argv[1]
    if cmd == "preview":
        preview()
    elif cmd == "apply":
        apply()
    elif cmd == "stats":
        stats()
    elif cmd == "gemma":
        dry = "--dry-run" in sys.argv
        lim = 50
        for a in sys.argv:
            if a.startswith("--limit="):
                lim = int(a.split("=")[1])
        gemma_classify(dry_run=dry, limit=lim)
    elif cmd == "normalize":
        dry = "--dry-run" in sys.argv
        lim = 50
        for a in sys.argv:
            if a.startswith("--limit="):
                lim = int(a.split("=")[1])
        normalize_predicates(dry_run=dry, limit=lim)
    else:
        print(f"Unknown: {cmd}")
