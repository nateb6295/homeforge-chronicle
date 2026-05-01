#!/bin/bash
# post_operator.sh — post a message to OPERATOR_WEBHOOK and update the
# Discord cadence timestamp file atomically. Replaces the raw `curl ... ;
# date > last_opus_post.txt` pattern that's easy to forget the second half of.
#
# Usage:
#   bin/post_operator.sh "your message text"
#   echo "your message" | bin/post_operator.sh -
#
# Reads OPERATOR_WEBHOOK from chronicle.env. Updates
# data/last_opus_post.txt with current epoch on successful post.

set -euo pipefail

CHRONICLE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

# Load OPERATOR_WEBHOOK
# shellcheck disable=SC1090
source "$CHRONICLE_DIR/chronicle.env"

if [ -z "${OPERATOR_WEBHOOK:-}" ]; then
    echo "ERROR: OPERATOR_WEBHOOK not set in chronicle.env" >&2
    exit 1
fi

# Read message from arg or stdin
if [ "${1:-}" = "-" ]; then
    MESSAGE="$(cat)"
elif [ -n "${1:-}" ]; then
    MESSAGE="$1"
else
    echo "Usage: $0 <message>  OR  echo msg | $0 -" >&2
    exit 1
fi

# Discord 2000-char hard limit; pre-flight reject so we fail loudly instead
# of silently truncating into a 400 the caller doesn't always check.
LEN=${#MESSAGE}
if [ "$LEN" -ge 2000 ]; then
    echo "ERROR: message is $LEN chars; Discord hard limit is 2000. Split before posting." >&2
    exit 3
fi
if [ "$LEN" -ge 1900 ]; then
    echo "WARN: message is $LEN chars, approaching Discord 2000-char limit" >&2
fi

# Post via curl. Capture both body AND HTTP status — Discord returns empty body
# on 204 success, JSON on 4xx. Body alone misses errors that don't include "code"
# (e.g. {"content": ["Must be 2000 or fewer in length."]} returns 400).
RESPONSE=$(python3 -c "
import json, sys
print(json.dumps({'content': sys.stdin.read()}))
" <<< "$MESSAGE" | curl -s -w '\nHTTP_%{http_code}' -X POST -H 'Content-Type: application/json' \
    -d @- "$OPERATOR_WEBHOOK")

HTTP_CODE=$(echo "$RESPONSE" | tail -1 | sed 's/^HTTP_//')
BODY=$(echo "$RESPONSE" | sed '$d')

if [ "$HTTP_CODE" != "204" ] && [ "$HTTP_CODE" != "200" ]; then
    echo "ERROR: Discord returned HTTP $HTTP_CODE: $BODY" >&2
    exit 2
fi

# Update the cadence timestamp on success
date +%s > "$CHRONICLE_DIR/data/last_opus_post.txt"

echo "posted ($LEN chars) at $(date '+%Y-%m-%d %H:%M:%S')"
