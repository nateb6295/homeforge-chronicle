# Opus Startup Checklist

Run these on every session start / context rotation. Copy-paste the block.

## Step 1: Sitrep (read state)
```
python3 ~/chronicle/bin/sitrep.py
```

## Step 2: Voice decay (clear backlog)
```
python3 ~/chronicle/bin/voice_decay.py
```

## Step 3: Read directives and boards
```
python3 ~/chronicle/bin/read_directives.py
cat ~/chronicle/nate-board.md
cat ~/chronicle/opus-board.md
```

## Step 4: Create crons (session-only, must recreate every rotation)

These are the standing crons. Create all of them:

| Schedule | What | Command |
|----------|------|---------|
| Every 3 min | Discord poll | `python3 ~/chronicle/bin/discord_presence.py poll` |
| Hourly at :17 | Nostr monitor | `python3 ~/chronicle/bin/nostr_monitor.py` |
| Hourly at :43 | Spot check | `python3 ~/chronicle/bin/spot_check.py` |
| Every 2h at :23 | Algo seeker | `python3 ~/chronicle/bin/algo_seeker.py` |
| Daily 9:03 PM | Daily digest | `python3 ~/chronicle/bin/daily_digest.py` |
| Every 6h at :11 | Voice decay | `python3 ~/chronicle/bin/voice_decay.py` |

## Step 5: Check family voices
```
python3 ~/chronicle/bin/agent_voice.py read
```

## Step 6: Read active thread
```
python3 ~/chronicle/bin/read_thread.py
```

## Step 7: Post to Discord that you're back
Post a brief "Back online after rotation" message to #opus.
