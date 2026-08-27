#!/bin/bash
# PreCompact hook — flush durable state to disk before context is compressed.
#
# The Stop hook already compresses CCS on a turn counter, which is a proxy for
# "compaction is coming." This fires at the actual moment. Everything here must
# be fast and silent; a hook that hangs is worse than one that does nothing.
#
# Deliberately does NOT touch cycle-context.md — that is hand-written state and
# an automated write could clobber a session's real notes.

cd "$HOME/chronicle" || exit 0
export PATH="$HOME/.local/share/dfx/bin:$PATH"

# keep CCS staleness accurate (cheap; full compression stays on the 4h cron)
timeout 45 python3 bin/ccs_touch.py >/dev/null 2>&1

# refresh the digest so the re-entry brief reads something current
timeout 60 python3 bin/session_digest.py >/dev/null 2>&1

# push any capsules that are still local-only, so a compaction never strands memory
timeout 90 python3 bin/capsule_ops.py sync >/dev/null 2>&1

# breadcrumb the re-entry brief can read
date +%s > "$HOME/chronicle/data/last_compaction"

exit 0
