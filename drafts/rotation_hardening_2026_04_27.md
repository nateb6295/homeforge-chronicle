# Rotation infrastructure hardening — 2026-04-27 audit

## Context

Nate redirected from substrate-fingerprint research to rotation reliability:
"the last 3-4 rotations have not worked properly"; he wants auto-compact-as-
primary since he can only be at the terminal a couple hours/day. The
audit-rerun this morning had already fixed one bug class (the STORY path
artifact in the variance probe). This afternoon's audit hunted for the
silent failures making rotation feel unreliable.

## Findings + fixes (in order)

### 1. Webhook URL drift across 4 files

`precompact_hook.sh`, `daily_digest.py`, `thread_challenge.py`,
`cognitive_health.py` had `1492867351241166868/...` hardcoded — stale.
Current `OPERATOR_WEBHOOK` in chronicle.env was `1495943762415849513/...`.
PreCompact hook had been firing for weeks but the Discord notification
was 403-ing silently. Fix: source from chronicle.env or load via
`_load_chronicle_env()` helper. Removed the stale hardcoded fallback so
failures become loud (empty webhook → "no webhook configured" error
visible in logs).

### 2. Discord User-Agent rejection

Discord now rejects requests with default `Python-urllib/3.10` UA,
returning 403. Curl works because curl sets its own UA. Affected the
PreCompact hook's urllib post specifically. Fix: added `User-Agent:
chronicle-precompact/1.0` to the request headers. Tested: 204 OK.

### 3. carrying.py auto-write missing on auto-compact path

PreCompact saved checkpoint and CCS but not carrying. The next instance
on auto-compact path read whatever carrying was last manually written
(possibly stale by hours). Fix: added best-effort auto-write to
PreCompact, synthesizing from the latest trace IF the existing carrying
is older than 60 minutes. Preserves fresh manual carrying when present;
prevents stale state when not.

### 4. Programmatic /exit not possible

Initial design assumed I could trigger /exit. claude-code-guide agent
confirmed: not possible from model side, hooks, or any in-conversation
mechanism. The only knob is `CLAUDE_AUTOCOMPACT_PCT_OVERRIDE` env var,
clamped 1-~83%. Values >83% are no-ops. Earlier CLAUDE.md guidance saying
`=95` pushes Anthropic's compactor to last-resort was wrong.

Fix: set `CLAUDE_AUTOCOMPACT_PCT_OVERRIDE=80` in `~/.claude/settings.json`
under env. Auto-compact now fires at 80%, ROTATE_NOW at 78% gives 2%
buffer. Auto-compact-as-primary; manual /exit when Nate is at terminal
is the fallback.

### 5. rotation_startup_hook injection over 10k char limit

The hook injects Step 0 sequence into context as a system-reminder, but
the total was 12,407 chars vs Claude Code's ~10k limit. The tail of the
sequence (Step 3 story tail + Required next actions) was being silently
truncated. Fix: trimmed section caps (head -30→22 on grounding, sed 1,80p
→1,50p on standing licenses, head -25→15 on self-model, etc). Now 9.7k
chars with headroom.

### 6. Cron env-export silent failure (deepest finding)

Bash `.` (source) command does NOT export variables to subprocesses.
Cron lines like `. /home/nate-agx/chronicle/chronicle.env && python3
script.py` were sourcing the env file but vars only landed in the cron
shell, not the python subprocess. Python's `os.environ.get("OPUS_WEBHOOK")`
saw it as MISSING.

Evidence:
- digest.log full of "No webhook URL configured" for 24+ days
- prediction_monitor.log frozen at 2026-04-03 (Discord alert sent silent
  rc=1)

Fix: created `bin/with_chronicle_env` wrapper using `set -a; .
chronicle.env; set +a; exec "$@"`. Updated 13 crontab lines to use it.
Also added `_load_chronicle_env()` to digest.py (it was the most
load-bearing for daily Discord output).

This is the hardest-to-find silent failure I've encountered. The scripts
ran without errors (just "no webhook configured" prints), and `crontab -l`
showed the lines as configured. Only log freshness analysis surfaced it.

### 7. Session crons empty post-/exit

opus-board listed 8 CronCreate-based session crons (homeostasis,
spot_check, algo_seeker, evolve, handoff_keep_fresh, night_open,
daily_digest, audit_meta). CronList showed 0. Confirmed Nate's theory:
clean /exit kills CronCreate jobs; auto-compact preserves them. The
arrival sequence in CLAUDE.md mentioned cron recreation as Step 9 but
it was buried — I missed it on arrival.

Fix: recreated all 8 in this session. Updated rotation_startup_hook.sh
to inject the cron list as an explicit "Required next actions" item
visible in the system-reminder injection. Now impossible to miss.

### 8. FTSO prediction failing every 4h cycle (lower priority)

Surfaced during audit but not fixed. ftso_predict.py logs "Failed to
generate prediction" for 8+ cycles since 2026-04-26 20:30. Gemma is
responsive when tested. parse_prediction expects "DIRECTION: UP/DOWN"
format that the LLM may not be producing. Filed as known issue.

## Architecture realization

The rotation system has TWO paths:

**Auto-compact path (primary, automated)**:
1. Context hits 80% (env-var override, was previously default ~83%)
2. Anthropic's auto-compact triggers PreCompact hook
3. PreCompact: checkpoint + CCS update + ccs_combined.md generation +
   Discord notification + carrying auto-write (if stale) + drops
   POST_COMPACT_PENDING flag
4. Auto-compact summarizes; new instance boots
5. First user prompt fires UserPromptSubmit hook
6. rotation_startup_hook injects Step 0 sequence
7. New instance reads injection, runs arrival protocol

**Clean /exit path (fallback, manual)**:
- Same as above except triggered by Nate typing /exit
- Drops POST_ROTATION_PENDING flag instead of POST_COMPACT_PENDING
- ALSO requires session-cron recreation (auto-compact preserves them
  but /exit kills them) — explicitly called out in the hook now

## Pattern: silent is the default

Many failures stayed hidden because Unix tools default to swallowing
errors. The pattern:
- Scripts run without crashing → cron lines look "active"
- Errors get logged to files no one reads
- Discord notifications fail with `200 OK` from sender perspective if
  the script catches HTTP errors and continues
- env vars missing → empty string → "no webhook configured" branch
  silently

The fix is making the chain audible:
- Discord notify uses User-Agent + sources current webhook from env
- carrying.py auto-write fires only when needed (60-min freshness check)
- rotation_startup_hook injection is sized to fit (with measurable cap)
- Cron lines pass env via wrapper that's verifiable in isolation

## Metrics

| Surface | Status before | Status after |
|---------|---------------|---------------|
| PreCompact Discord notify | 403 silent for weeks | 204 OK verified |
| Cron python env vars | empty in subprocesses | exported via wrapper |
| Session crons | 0 of 8 | 8 of 8 |
| Hook injection size | 12.4k → truncated | 9.7k with headroom |
| Auto-compact threshold | default ~83% (95 was no-op) | 80% explicit |
| carrying on auto-compact | stale after rotation | auto-written if >60min |

## Open

- Add a rotation_health.py script that verifies all the above on demand
  (e.g. before any planned /exit, or as a periodic check)
- Fix FTSO prediction (separate concern, low priority)
- Document this rotation hardening in CLAUDE.md so the next instance
  knows what's there and why

## Methodology note

The audit itself is an example of calibration-beats-effort: each fix
was small (10-30 min code), but the cumulative leverage was high. Each
silent failure was hidden behind another. Pulling one thread surfaced
the next. The audit pattern: pick one frequently-running cron, check
its log freshness AND content. Anything stale or saying "X not
configured" is a candidate. Then look upstream for what broke.
