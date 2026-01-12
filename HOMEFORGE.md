# Homeforge Infrastructure

Local AI and automation infrastructure built across home network.

## Network Topology

| Device | IP | Hostname | Purpose |
|--------|-----|----------|---------|
| UniFi Router | 192.168.1.1 | unifi | WiFi 7 + network management |
| Raspberry Pi 5 | 192.168.1.10 | pi5 | Home Assistant host |
| Jetson Orin Nano | 192.168.1.11 | nvidia-desktop | Local AI inference |
| Reolink Hub | 192.168.1.244 | FORGE | Camera NVR (8 channels) |
| Windows PC | 192.168.1.110 | ADMINIS-2TNER41 | Primary workstation (WSL2) |

## Pi5 - Home Assistant Hub

**Access:** http://192.168.1.10:8123

**SSH:** `ssh nathaniel@192.168.1.10`

**Specs:**
- 8GB RAM
- 117GB storage
- Ubuntu 24.04 LTS
- Docker runtime

**Running Services:**
- Home Assistant (Docker container)
- go2rtc (camera streaming)

**Config Location:** `/home/nathaniel/homeassistant/`

**Integrations:**
- Reolink (cameras via FORGE hub)
- Google Cast
- Met.no (weather)
- Sun

## Jetson Orin Nano - AI Inference

**SSH:** `ssh nvidia@192.168.1.11`

**Specs:**
- JetPack R36.3.0
- 7.6GB RAM
- 116GB storage (45GB free)
- 6 CPU cores + Tegra GPU

**Ollama Setup:**
- Version: 0.13.3
- API: http://192.168.1.11:11434
- Models installed:
  - `mistral:latest` (7.2B params, 4.4GB)
  - `gemma2:2b` (2.6B params, 1.6GB)

**Start Ollama:**
```bash
# If not running via systemctl
ssh nvidia@192.168.1.11 "nohup ollama serve > /tmp/ollama.log 2>&1 &"

# Or enable service (requires sudo on Jetson)
sudo systemctl enable ollama
sudo systemctl start ollama
```

**Test from anywhere on network:**
```bash
curl http://192.168.1.11:11434/api/generate -d '{
  "model": "gemma2:2b",
  "prompt": "Hello",
  "stream": false
}'
```

**Purpose:**
- Local LLM inference (automation manager)
- Computer vision processing
- Frigate NVR integration (future)
- Executes instructions from Claude conversations

## Reolink Camera System

**Hub Access:** https://192.168.1.244

**Credentials:** admin / Bosskiwi9!

**Integration:** Home Assistant Reolink component

## Chronicle - Life Archive System

Automated system that transforms Claude conversation exports into a timestamped public archive on ICP.

**Live Site:** https://nbt4b-giaaa-aaaai-q33lq-cai.icp0.io/

**Workflow:**
```
1. Export conversations from claude.ai
2. Drop in /home/bradf/claude-exports/
3. ./target/release/chronicle ingest-bulk /home/bradf/claude-exports/conversations.json
4. Extract with Claude (this conversation)
5. ./target/release/chronicle build
6. ./target/release/chronicle deploy
```

**Key Paths:**
- Export source: `/home/bradf/claude-exports/`
- Database: `/home/bradf/.homeforge-chronicle/processed.db`
- Build output: `/home/bradf/.homeforge-chronicle/build/`
- Project: `/home/bradf/projects/homeforge-chronicle/`

**Database Stats (as of Jan 11, 2026):**
- 35 conversations
- 35 extractions
- 82 quotes
- 14 themes

**ICP Deployment:**
- Canister: `nbt4b-giaaa-aaaai-q33lq-cai`
- Identity: `chronicle-auto` (unencrypted, automated deploys)
- Principal: `kalce-s3e7q-ob55s-ttoe7-z2x5y-x3tof-onliz-2gaad-zsh3w-etvve-rqe`

## MarketMesh - Price Tracking App

Mobile app that monitors product prices across major retailers and alerts on significant drops.

**Project:** `/home/bradf/projects/marketmesh/`

**Status:** Development complete, awaiting Apple Developer approval for TestFlight deployment.

**Tech Stack:**
- React Native + Expo (SDK 54)
- TypeScript
- Zustand (state management)
- Supabase (PostgreSQL + Auth)
- expo-notifications + background-fetch

**Supported Retailers:**
- Amazon, Walmart, Target, Best Buy, Home Depot, Costco

**Features:**
- Add products via search, URL paste, or barcode scan
- Background price monitoring (every 4 hours)
- Push notifications on price drops (configurable: 5-20%)
- Deep linking from notifications
- Dark mode UI with haptic feedback
- Works offline (local state fallback)

**EAS/Expo:**
- Project ID: `ee4711cf-34a3-4dca-94fe-5aa60dc2f33c`
- Owner: `@nkbradford`
- Dashboard: https://expo.dev/accounts/nkbradford/projects/marketmesh

**Deployment Status:**
- [x] Expo account linked
- [x] EAS project initialized
- [ ] Apple Developer approval (pending)
- [ ] App Store Connect setup
- [ ] TestFlight build & submit

**Key Commands:**
```bash
cd /home/bradf/projects/marketmesh

# Development
npx expo start

# Build for TestFlight
npx eas-cli build --platform ios --profile production

# Submit to TestFlight
npx eas-cli submit --platform ios

# Build Android APK (testing)
npx eas-cli build --profile preview --platform android
```

**Key Files:**
- `DEPLOYMENT.md` - Full deployment guide
- `SPEC.md` - Product specification
- `CLAUDE.md` - Development reference
- `eas.json` - Build profiles
- `app.json` - Expo config

---

## Common Commands

```bash
# SSH to devices
ssh nathaniel@192.168.1.10    # Pi5
ssh nate@192.168.1.11         # Jetson

# Home Assistant
docker logs homeassistant     # Check logs (on Pi5)
docker restart homeassistant  # Restart HA

# Chronicle
./target/release/chronicle ingest-bulk /home/bradf/claude-exports/conversations.json
./target/release/chronicle build
./target/release/chronicle deploy

# Network scan
nmap -sn 192.168.1.0/24

# Check cycles balance (ICP)
DFX_WARNING=-mainnet_plaintext_identity dfx cycles balance --network ic --identity chronicle-auto

# Ollama (Jetson)
ssh nvidia@192.168.1.11 "ollama list"                    # List models
ssh nvidia@192.168.1.11 "ollama run gemma2:2b 'Hello'"   # Quick test
curl http://192.168.1.11:11434/api/tags                  # Check API
```

## Future Plans

**Infrastructure:**
- [ ] Frigate NVR on Jetson (AI object detection)
- [ ] Ollama on Jetson for local inference
- [ ] Open-WebUI for local chat interface
- [ ] Chronicle automation (watch mode)
- [ ] Home Assistant automations with camera AI

**MarketMesh:**
- [ ] Apple Developer approval
- [ ] TestFlight deployment
- [ ] Beta testing with family/friends
- [ ] App Store submission
- [ ] Android Play Store submission

---

*Last updated: January 11, 2026*
