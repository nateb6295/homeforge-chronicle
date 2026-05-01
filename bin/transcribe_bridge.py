#!/usr/bin/env python3
"""
transcribe_bridge.py — Bridge transcriber output to activity_feed.

Gap #7: Whisper transcriber runs and writes to scratch_pad (4486 entries)
but nothing reads from it. This bridges heard-speech entries into
activity_feed so they're visible to the pipeline.

Groups nearby transcriptions into conversation segments (within 30s of each other),
filters out noise/short fragments, and writes meaningful segments to activity_feed.

Usage:
    transcribe_bridge.py sync [--hours N]   # Bridge recent transcriptions
    transcribe_bridge.py status             # Show transcription pipeline health
    transcribe_bridge.py segments [--hours N] # Show grouped conversation segments
"""
import json
import os
import sqlite3
import sys
import time

DB = "/mnt/hdd/chronicle-data/processed.db"
SEGMENT_GAP_S = 30  # Merge transcriptions within 30s of each other
MIN_SEGMENT_WORDS = 5  # Skip segments shorter than this
MIN_SEGMENT_DURATION = 3.0  # Skip segments shorter than 3s total


def get_recent_transcriptions(hours=6):
    """Get recent heard-speech entries from scratch_pad."""
    cutoff = int(time.time()) - (hours * 3600)
    db = sqlite3.connect(DB)
    rows = db.execute(
        "SELECT id, content, created_at FROM scratch_pad "
        "WHERE category = 'heard-speech' AND created_at > ? "
        "ORDER BY created_at ASC",
        (cutoff,)
    ).fetchall()
    db.close()

    entries = []
    for rid, content, ts in rows:
        try:
            data = json.loads(content)
            entries.append({
                "id": rid,
                "text": data.get("text", "").strip(),
                "duration": data.get("duration_s", 0),
                "timestamp": data.get("timestamp", ts),
                "source": data.get("source", "unknown"),
            })
        except (json.JSONDecodeError, TypeError):
            continue
    return entries


def group_segments(entries):
    """Group nearby transcriptions into conversation segments."""
    if not entries:
        return []

    segments = []
    current = {
        "texts": [entries[0]["text"]],
        "start": entries[0]["timestamp"],
        "end": entries[0]["timestamp"] + entries[0]["duration"],
        "total_duration": entries[0]["duration"],
        "entry_ids": [entries[0]["id"]],
    }

    for entry in entries[1:]:
        gap = entry["timestamp"] - current["end"]
        if gap <= SEGMENT_GAP_S:
            # Same conversation segment
            current["texts"].append(entry["text"])
            current["end"] = entry["timestamp"] + entry["duration"]
            current["total_duration"] += entry["duration"]
            current["entry_ids"].append(entry["id"])
        else:
            # New segment
            segments.append(current)
            current = {
                "texts": [entry["text"]],
                "start": entry["timestamp"],
                "end": entry["timestamp"] + entry["duration"],
                "total_duration": entry["duration"],
                "entry_ids": [entry["id"]],
            }

    segments.append(current)

    # Filter: meaningful segments only
    meaningful = []
    for seg in segments:
        combined = " ".join(seg["texts"])
        word_count = len(combined.split())
        if word_count >= MIN_SEGMENT_WORDS and seg["total_duration"] >= MIN_SEGMENT_DURATION:
            seg["combined_text"] = combined
            seg["word_count"] = word_count
            meaningful.append(seg)

    return meaningful


def sync_to_activity_feed(hours=6):
    """Bridge transcription segments to activity_feed."""
    entries = get_recent_transcriptions(hours)
    if not entries:
        print(f"No transcriptions in last {hours}h.")
        return

    segments = group_segments(entries)
    if not segments:
        print(f"No meaningful segments (found {len(entries)} raw entries, all too short/fragmented).")
        return

    db = sqlite3.connect(DB)

    synced = 0
    for seg in segments:
        # Check if already synced (avoid duplicates)
        existing = db.execute(
            "SELECT COUNT(*) FROM activity_feed "
            "WHERE source = 'transcriber:speech' AND created_at = ?",
            (seg["start"],)
        ).fetchone()[0]

        if existing:
            continue

        ts_str = time.strftime('%Y-%m-%d %H:%M', time.localtime(seg["start"]))
        title = f"Heard speech segment ({seg['word_count']} words, {seg['total_duration']:.0f}s)"
        metadata = json.dumps({
            "entry_ids": seg["entry_ids"],
            "duration_s": seg["total_duration"],
            "word_count": seg["word_count"],
        })

        db.execute(
            "INSERT INTO activity_feed (source, activity_type, title, content, metadata, created_at) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            ("transcriber:speech", "transcription", title, seg["combined_text"], metadata, seg["start"])
        )
        synced += 1

    db.commit()
    db.close()

    print(f"TRANSCRIBE BRIDGE ({hours}h)")
    print(f"  Raw entries: {len(entries)}")
    print(f"  Segments formed: {len(segments)}")
    print(f"  Synced to activity_feed: {synced}")
    print(f"  Already synced: {len(segments) - synced}")


def show_status():
    """Show transcription pipeline health."""
    db = sqlite3.connect(DB)

    total = db.execute(
        "SELECT COUNT(*) FROM scratch_pad WHERE category='heard-speech'"
    ).fetchone()[0]

    last_24h = db.execute(
        "SELECT COUNT(*) FROM scratch_pad WHERE category='heard-speech' "
        "AND created_at > strftime('%s','now','-24 hours')"
    ).fetchone()[0]

    latest = db.execute(
        "SELECT created_at FROM scratch_pad WHERE category='heard-speech' "
        "ORDER BY created_at DESC LIMIT 1"
    ).fetchone()

    synced = db.execute(
        "SELECT COUNT(*) FROM activity_feed WHERE source='transcriber:speech'"
    ).fetchone()[0]

    db.close()

    age = ""
    if latest:
        age_h = (time.time() - latest[0]) / 3600
        age = f"{age_h:.1f}h ago"

    print(f"TRANSCRIPTION PIPELINE STATUS")
    print(f"  Total heard-speech: {total}")
    print(f"  Last 24h entries:   {last_24h}")
    print(f"  Latest entry:       {age}")
    print(f"  Synced to feed:     {synced}")

    if not latest or (time.time() - latest[0]) > 86400:
        print(f"\n  WARNING: No transcriptions in 24h+ — check ear/whisper pipeline")
    elif last_24h == 0:
        print(f"\n  NOTE: No recent transcriptions")
    else:
        print(f"\n  Pipeline: active")


def show_segments(hours=6):
    """Show grouped conversation segments."""
    entries = get_recent_transcriptions(hours)
    segments = group_segments(entries)

    if not segments:
        print(f"No meaningful segments in last {hours}h.")
        return

    print(f"CONVERSATION SEGMENTS ({hours}h, {len(segments)} segments)\n")
    for i, seg in enumerate(segments):
        ts = time.strftime('%H:%M', time.localtime(seg["start"]))
        print(f"  [{ts}] {seg['word_count']}w / {seg['total_duration']:.0f}s")
        print(f"    {seg['combined_text'][:120]}...")
        print()


def main():
    if len(sys.argv) < 2:
        print(__doc__)
        return

    cmd = sys.argv[1]

    if cmd == "sync":
        hours = 6
        for i, arg in enumerate(sys.argv[2:], 2):
            if arg == "--hours" and i + 1 < len(sys.argv):
                hours = int(sys.argv[i + 1])
        sync_to_activity_feed(hours)

    elif cmd == "status":
        show_status()

    elif cmd == "segments":
        hours = 6
        for i, arg in enumerate(sys.argv[2:], 2):
            if arg == "--hours" and i + 1 < len(sys.argv):
                hours = int(sys.argv[i + 1])
        show_segments(hours)

    else:
        print(f"Unknown command: {cmd}")
        print(__doc__)


if __name__ == "__main__":
    main()
