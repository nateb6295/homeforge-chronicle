"""Chronicle Mind - Sleep consolidation (prune, embed, cluster, merge)."""

from typing import Optional, List, Tuple

from mind.utils import log, now_ts, safe_truncate, get_embeddings, cosine_sim
from mind.config import (
    OPERATOR_PROTECTED_CATEGORIES,
    CONSOLIDATE_MIN_NOTES_TO_RUN,
    CONSOLIDATE_SIMILARITY_THRESHOLD,
    CONSOLIDATE_CROSS_CAT_THRESHOLD,
    CONSOLIDATE_MAX_CLUSTER_SIZE,
)


def sleep_consolidation(db) -> dict:
    """Consolidate scratch_pad notes: prune stale, cluster similar, merge duplicates.
    Called every Nth cycle during the sleep gap. Returns metrics dict."""
    metrics = {"pruned": 0, "merged": 0, "clusters": 0, "total_resolved": 0, "skipped": False}

    notes = db.unresolved_notes_full(200)
    if len(notes) < CONSOLIDATE_MIN_NOTES_TO_RUN:
        metrics["skipped"] = True
        return metrics

    # Phase 1: Prune by age/category rules
    surviving, pruned_ids = _consolidate_prune(notes)
    metrics["pruned"] = len(pruned_ids)
    if pruned_ids:
        db.bulk_resolve_notes(pruned_ids)

    # Exclude missions from embedding/clustering/merging — their content is
    # structured JSON that must not be annotated or resolved by consolidation
    surviving = [n for n in surviving if n.get("category") != "mission"]

    if len(surviving) < 2:
        metrics["total_resolved"] = metrics["pruned"]
        return metrics

    # Phase 2: Embed surviving notes
    embedded = _consolidate_embed(surviving)
    if not embedded:
        # Embedding failed — prune-only run is still useful
        metrics["total_resolved"] = metrics["pruned"]
        return metrics

    # Phase 3: Cluster by similarity
    clusters = _consolidate_cluster(embedded)
    metrics["clusters"] = len(clusters)

    # Phase 4: Merge clusters
    merged_count = _consolidate_merge(clusters, db)
    metrics["merged"] = merged_count

    metrics["total_resolved"] = metrics["pruned"] + metrics["merged"]
    return metrics


def _consolidate_prune(notes: list) -> Tuple[list, List[int]]:
    """Prune notes by age+category rules. Returns (surviving, pruned_ids)."""
    now = now_ts()
    surviving = []
    pruned_ids = []

    for note in notes:
        age_hours = (now - note["created_at"]) / 3600
        cat = note.get("category", "thought")
        pri = note.get("priority", 0)

        # Never prune operator-protected categories (directives, tasks)
        if cat in OPERATOR_PROTECTED_CATEGORIES:
            surviving.append(note)
            continue

        # Never prune high-priority goals/reminders
        if cat in ("goal", "reminder") and pri >= 8:
            surviving.append(note)
            continue

        # Topic cooldowns expire after ~30 min (6 cycles)
        if cat == "meta-block" and age_hours > 0.5:
            pruned_ids.append(note["id"])
            continue

        # Short-lived categories
        if cat in ("meta-eval", "reflection") and age_hours > 24:
            pruned_ids.append(note["id"])
            continue

        # Medium-lived categories
        if cat in ("research", "shell_exec", "sibling") and age_hours > 72:
            pruned_ids.append(note["id"])
            continue

        # Low-priority thoughts/ideas after a week
        if cat in ("thought", "idea") and age_hours > 168 and pri < 5:
            pruned_ids.append(note["id"])
            continue

        # Anything very old and not high-priority
        if age_hours > 336 and pri < 8:  # 14 days
            pruned_ids.append(note["id"])
            continue

        surviving.append(note)

    return surviving, pruned_ids


def _consolidate_embed(notes: list) -> Optional[List[dict]]:
    """Embed note contents via Ollama in batches. Returns notes with 'embedding' key, or None on failure."""
    BATCH_SIZE = 50
    texts = [safe_truncate(n["content"], 500) for n in notes]
    all_embeddings = []
    for i in range(0, len(texts), BATCH_SIZE):
        batch = texts[i:i + BATCH_SIZE]
        embeddings = get_embeddings(batch)
        if embeddings is None or len(embeddings) != len(batch):
            return None
        all_embeddings.extend(embeddings)
    result = []
    for note, emb in zip(notes, all_embeddings):
        entry = dict(note)
        entry["embedding"] = emb
        result.append(entry)
    return result


def _consolidate_cluster(embedded: list) -> List[List[dict]]:
    """Greedy single-link clustering. Returns list of clusters (size >= 2)."""
    # Sort by priority DESC so high-priority notes anchor clusters
    embedded.sort(key=lambda n: (-n.get("priority", 0), n["created_at"]))

    assigned = set()
    clusters = []

    for i, anchor in enumerate(embedded):
        if anchor["id"] in assigned:
            continue
        cluster = [anchor]
        assigned.add(anchor["id"])

        for j, candidate in enumerate(embedded):
            if candidate["id"] in assigned:
                continue
            if len(cluster) >= CONSOLIDATE_MAX_CLUSTER_SIZE:
                break

            # Check similarity against anchor
            sim = cosine_sim(anchor["embedding"], candidate["embedding"])
            threshold = CONSOLIDATE_SIMILARITY_THRESHOLD
            if anchor.get("category") != candidate.get("category"):
                threshold = CONSOLIDATE_CROSS_CAT_THRESHOLD

            if sim >= threshold:
                cluster.append(candidate)
                assigned.add(candidate["id"])

        if len(cluster) >= 2:
            clusters.append(cluster)

    return clusters


def _consolidate_merge(clusters: List[List[dict]], db) -> int:
    """Merge each cluster: keep highest-priority note, resolve rest. Returns count merged."""
    total_merged = 0

    for cluster in clusters:
        # Sort: highest priority first, oldest as tiebreak
        cluster.sort(key=lambda n: (-n.get("priority", 0), n["created_at"]))
        keeper = cluster[0]
        rest = cluster[1:]

        # Skip if keeper already has a consolidation annotation
        if "[consolidated:" in keeper["content"]:
            continue

        rest_ids = [n["id"] for n in rest]
        annotation = f" [consolidated: merged {len(rest)} similar notes (ids: {','.join(str(i) for i in rest_ids)})]"
        new_content = safe_truncate(keeper["content"], 800) + annotation
        db.update_note_content(keeper["id"], new_content)
        db.bulk_resolve_notes(rest_ids)
        total_merged += len(rest)

    return total_merged
