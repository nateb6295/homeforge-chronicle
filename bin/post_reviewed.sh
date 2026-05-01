#!/bin/bash
# post_reviewed.sh — run self_reviewer on a post, log the verdict, post to
# #operator unless RED (without --force). Called as:
#   bash post_reviewed.sh /tmp/opus_post.json
#
# Keeps the convention that the JSON file is a Discord webhook payload
# with a "content" field.

set -u

JSON_FILE="${1:-}"
FORCE="${2:-}"

if [ -z "$JSON_FILE" ] || [ ! -f "$JSON_FILE" ]; then
  echo "usage: post_reviewed.sh <json_file> [--force]" >&2
  exit 1
fi

# Extract the content
CONTENT=$(python3 -c "import json; d=json.load(open('$JSON_FILE')); print(d.get('content',''))")

if [ -z "$CONTENT" ]; then
  echo "ERROR: empty content in $JSON_FILE" >&2
  exit 1
fi

# Discord webhook limit is 2000 chars. Fail fast.
CONTENT_LEN=${#CONTENT}
if [ "$CONTENT_LEN" -gt 2000 ]; then
  echo "ERROR: content is $CONTENT_LEN chars, Discord limit is 2000. Split into parts." >&2
  exit 4
fi

# Run reviewer
REVIEW_OUT=$(mktemp)
REVIEW_JSON=$(mktemp)
python3 /home/nate-agx/chronicle/bin/self_reviewer.py --text "$CONTENT" --json-out > "$REVIEW_JSON" 2>"$REVIEW_OUT"
RC=$?

VERDICT=$(python3 -c "import json; d=json.load(open('$REVIEW_JSON')); print(d.get('should_ship','unknown'))" 2>/dev/null)
REASON=$(python3 -c "import json; d=json.load(open('$REVIEW_JSON')); print(d.get('reason',''))" 2>/dev/null)

# Log review to history
REVIEW_HIST="/home/nate-agx/chronicle/data/post_review_history.jsonl"
mkdir -p "$(dirname "$REVIEW_HIST")"
python3 -c "
import json, time
review = json.load(open('$REVIEW_JSON'))
review['timestamp'] = int(time.time())
review['content_preview'] = '''$(echo "$CONTENT" | head -c 200 | sed "s/'/\\\\'/g")'''
with open('$REVIEW_HIST', 'a') as f:
  f.write(json.dumps(review) + chr(10))
"

echo "review: $VERDICT  reason: $REASON" >&2

# Gate on RED
if [ "$VERDICT" = "red" ] && [ "$FORCE" != "--force" ]; then
  echo "RED verdict, refusing to post without --force flag" >&2
  echo "flags:" >&2
  python3 -c "import json; d=json.load(open('$REVIEW_JSON')); [print('  -', f) for f in d.get('flags',[])]" >&2
  rm -f "$REVIEW_OUT" "$REVIEW_JSON"
  exit 3
fi

# Load env + post
source /home/nate-agx/chronicle/chronicle.env
HTTP=$(curl -s -o /tmp/post_out -w "%{http_code}" -X POST -H 'Content-Type: application/json' --data @"$JSON_FILE" "$OPERATOR_WEBHOOK")
echo "HTTP $HTTP" >&2

rm -f "$REVIEW_OUT" "$REVIEW_JSON"

if [ "$HTTP" = "204" ]; then
  # Track post timestamp for cadence checker
  date +%s > /home/nate-agx/chronicle/data/last_opus_post.txt
  exit 0
else
  cat /tmp/post_out >&2
  exit 1
fi
