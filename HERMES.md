# Chronicle — Hermes Context

You are working in the Chronicle project directory. This is a sovereignty-focused intelligence system on an NVIDIA Jetson AGX Orin.

## Database

SQLite at `/mnt/hdd/chronicle-data/processed.db`. Key tables:
- `activity_feed` — briefs, captures, connections, messages (source, activity_type, title, content, created_at)
- `prediction_track` — predictions (claim, confidence, rationale, status, outcome)
- `cognitive_threads` — active threads of inquiry
- `thread_history` — thread advancement history
- `agent_voice` — family agent voices (agent, voice_type, content)
- `capsules` — on-chain published content
- `entity` — knowledge graph entities

## Helper Scripts (bin/)

| Script | Purpose |
|--------|---------|
| `morning_brief.py` | Personal morning brief for Nate |
| `spot_check.py` | Fabrication detection and audit |
| `chronicle_engine.py` | LLM inference router (DeepInfra/Cerebras/Groq/local) |
| `chronicle_sentinel.py` | XRP price + portfolio monitoring |
| `chronicle_feeds.py` | RSS feed ingestion |
| `posse.py` | Multi-platform publishing (canister + Nostr + Discord) |
| `stem.py` | MQTT/sensor integration |

## Structural Verification (Builds #134-165)

Six layers of post-generation verification:
1. Invented name detection (#134)
2. Sentence-source grounding overlap (#160)
3. Recombination detection (#161)
4. Entity-role distortion (#162)
5. Fabricated quote detection (#164)
6. Specific claim verification — dollars, percentages (#165)

These are model-agnostic and MUST stay active.

## Rules

- Never copy the database to /tmp (fills 4GB tmpfs)
- One model at a time on AGX
- Gemma stays on the SSD — don't overwrite
- No auto-posting to X (Nate's personal)
- No local fine-tuning on AGX (RunPod only)
