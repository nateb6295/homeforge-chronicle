#!/usr/bin/env python3
"""Export successful thought_stream cycles from Mind's DB for action LoRA training.

Runs on AGX. Queries thought_stream for cycles where:
- reasoning is valid JSON array of action dicts
- At least one action succeeded (action_results contains =true)
- Model is hermes3-mind or hermes3:8b (skip nemotron/hermes4)

Output: ~/thought_stream_export.json
Target: 400+ clean cycles
"""

import argparse
import json
import os
import re
import sqlite3
import sys
from datetime import datetime


def parse_args():
    parser = argparse.ArgumentParser(description="Export successful Mind cycles for training")
    parser.add_argument("--db", default=os.path.expanduser("~/.homeforge-chronicle/processed.db"),
                        help="Path to Mind's processed.db")
    parser.add_argument("--output", default=os.path.expanduser("~/thought_stream_export.json"),
                        help="Output JSON file")
    parser.add_argument("--min-actions", type=int, default=1,
                        help="Minimum number of actions in a cycle (default: 1)")
    parser.add_argument("--include-no-action", action="store_true",
                        help="Include cycles where the only action was no_action")
    parser.add_argument("--limit", type=int, default=0,
                        help="Limit number of exported cycles (0 = no limit)")
    parser.add_argument("--verbose", "-v", action="store_true",
                        help="Print each exported cycle")
    return parser.parse_args()


def strip_think_tags(text):
    """Remove <think>...</think> blocks that Hermes sometimes includes."""
    return re.sub(r'<think>.*?</think>', '', text, flags=re.DOTALL).strip()


def try_parse_actions(reasoning_text):
    """Try to parse the reasoning field as a JSON array of action dicts.

    Returns (actions_list, raw_json_str) or (None, None) if invalid.
    """
    if not reasoning_text:
        return None, None

    text = strip_think_tags(reasoning_text).strip()

    # Try direct parse
    try:
        parsed = json.loads(text)
        if isinstance(parsed, list) and len(parsed) > 0:
            # Check that items look like action dicts
            if all(isinstance(item, dict) and "action" in item for item in parsed):
                return parsed, text
        return None, None
    except (json.JSONDecodeError, TypeError):
        pass

    # Try extracting JSON array from surrounding text
    # Sometimes the model wraps it in markdown or extra text
    match = re.search(r'\[[\s\S]*\]', text)
    if match:
        try:
            parsed = json.loads(match.group())
            if isinstance(parsed, list) and len(parsed) > 0:
                if all(isinstance(item, dict) and "action" in item for item in parsed):
                    return parsed, match.group()
        except (json.JSONDecodeError, TypeError):
            pass

    return None, None


def has_successful_action(action_results_text):
    """Check if action_results contains at least one success."""
    if not action_results_text:
        return False
    # action_results format: "action_name=true, other_action=false"
    # or JSON: [{"name": "...", "result": "..."}]
    if "=true" in action_results_text.lower():
        return True
    if "success" in action_results_text.lower():
        return True
    # Try JSON format
    try:
        results = json.loads(action_results_text)
        if isinstance(results, list):
            for r in results:
                if isinstance(r, dict):
                    result_val = str(r.get("result", "")).lower()
                    if "true" in result_val or "success" in result_val:
                        return True
    except (json.JSONDecodeError, TypeError):
        pass
    return False


def extract_context_from_row(row):
    """Reconstruct the cycle context that would have been shown to the model.

    Uses actual thought_stream columns: context_summary, trigger_tags, created_at.
    """
    context_parts = []

    # Timestamp context
    if row.get("created_at"):
        try:
            ts = row["created_at"]
            if isinstance(ts, (int, float)):
                dt = datetime.fromtimestamp(ts)
            else:
                dt = datetime.fromisoformat(str(ts))
            day_name = dt.strftime("%A")
            period = "morning" if dt.hour < 12 else "afternoon" if dt.hour < 17 else "evening" if dt.hour < 21 else "night"
            context_parts.append(f"Time: {day_name} {period}")
        except (ValueError, OSError):
            pass

    # Context summary (the actual prompt context Mind saw)
    if row.get("context_summary"):
        summary = row["context_summary"].strip()
        # Truncate very long summaries for training efficiency
        if len(summary) > 1500:
            summary = summary[:1500] + "..."
        context_parts.append(summary)

    # Trigger tags (what caused this cycle)
    if row.get("trigger_tags"):
        try:
            tags = json.loads(row["trigger_tags"])
            if tags:
                context_parts.append(f"Triggers: {', '.join(tags)}")
        except (json.JSONDecodeError, TypeError):
            pass

    return "\n".join(context_parts) if context_parts else "Standard cognitive cycle."


def main():
    args = parse_args()

    if not os.path.exists(args.db):
        print(f"[ERROR] Database not found: {args.db}")
        sys.exit(1)

    conn = sqlite3.connect(args.db)
    conn.row_factory = sqlite3.Row

    # Query thought_stream for cycles with reasoning
    # Actual schema: id, cycle_id, reasoning, context_summary, actions_taken,
    #   created_at, action_results, action_signatures, trigger_tags
    query = """
        SELECT id, reasoning, action_results, context_summary,
               trigger_tags, created_at, actions_taken
        FROM thought_stream
        WHERE reasoning IS NOT NULL
          AND reasoning != ''
        ORDER BY created_at DESC
    """

    cursor = conn.execute(query)
    rows = cursor.fetchall()
    print(f"[INFO] Found {len(rows)} thought_stream rows with reasoning")

    exported = []
    skipped_parse = 0
    skipped_no_success = 0
    skipped_no_action_only = 0

    for row in rows:
        row_dict = dict(row)
        reasoning = row_dict.get("reasoning", "")

        # Parse actions from reasoning
        actions, raw_json = try_parse_actions(reasoning)
        if actions is None:
            skipped_parse += 1
            continue

        # Check minimum action count
        if len(actions) < args.min_actions:
            continue

        # Skip no_action-only cycles unless explicitly included
        if not args.include_no_action:
            action_names = [a.get("action", "") for a in actions]
            if all(name == "no_action" for name in action_names):
                skipped_no_action_only += 1
                continue

        # Check for at least one successful action
        action_results = row_dict.get("action_results", "")
        if not has_successful_action(action_results):
            skipped_no_success += 1
            continue

        # Build export entry
        context = extract_context_from_row(row_dict)
        action_names = [a.get("action", "unknown") for a in actions]

        entry = {
            "id": row_dict["id"],
            "context": context,
            "actions_json": raw_json,
            "actions": actions,
            "action_names": action_names,
            "action_results": action_results,
            "created_at": row_dict.get("created_at"),
        }

        exported.append(entry)

        if args.verbose:
            print(f"  [EXPORT] id={entry['id']} actions={action_names}")

        if args.limit and len(exported) >= args.limit:
            break

    conn.close()

    # Write output
    with open(args.output, "w") as f:
        json.dump(exported, f, indent=2)

    print(f"\n=== Export Summary ===")
    print(f"Total rows scanned:        {len(rows)}")
    print(f"Skipped (parse failed):    {skipped_parse}")
    print(f"Skipped (no success):      {skipped_no_success}")
    print(f"Skipped (no_action only):  {skipped_no_action_only}")
    print(f"Exported:                  {len(exported)}")
    print(f"Output:                    {args.output}")
    print(f"File size:                 {os.path.getsize(args.output) / 1024:.1f} KB")

    if exported:
        # Action distribution
        all_actions = []
        for entry in exported:
            all_actions.extend(entry["action_names"])
        action_counts = {}
        for a in all_actions:
            action_counts[a] = action_counts.get(a, 0) + 1

        print(f"\nTop actions:")
        for action, count in sorted(action_counts.items(), key=lambda x: -x[1])[:15]:
            print(f"  {action}: {count}")

    if len(exported) < 400:
        print(f"\n[WARN] Only {len(exported)} cycles exported (target: 400+)")
        print("Consider: --include-no-action, or check if model filter is too strict")
    else:
        print(f"\n[OK] {len(exported)} cycles exported (target: 400+)")


if __name__ == "__main__":
    main()
