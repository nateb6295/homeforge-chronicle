#!/usr/bin/env bash
# Chronicle Setup — Generate configuration and install services.
# Part of Objective #8: Make Chronicle plantable.
#
# Usage:
#   ./chronicle-setup.sh              — interactive setup
#   ./chronicle-setup.sh --env-only   — generate chronicle.env template only
#   ./chronicle-setup.sh --services   — install systemd services only
#   ./chronicle-setup.sh --all        — full setup (env + db + services + verify)
#
# Prerequisites:
#   - Python 3.10+
#   - Ollama running with embedding model
#   - pip packages from requirements-core.txt
#   - dfx (for IC canister, optional)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
CHRONICLE_DIR="$(dirname "$SCRIPT_DIR")"
BIN_DIR="$CHRONICLE_DIR/bin"
ENV_FILE="$CHRONICLE_DIR/chronicle.env"
DATA_DIR="$HOME/.homeforge-chronicle"
SERVICE_DIR="$HOME/.config/systemd/user"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

log() { echo -e "${GREEN}[setup]${NC} $1"; }
warn() { echo -e "${YELLOW}[warn]${NC} $1"; }
err() { echo -e "${RED}[error]${NC} $1"; }

# ═══════════════════════════════════════════════════════════════
#  Service definitions — tier/name/description/script/deps
# ═══════════════════════════════════════════════════════════════

declare -A SERVICE_TIER SERVICE_DESC SERVICE_SCRIPT SERVICE_DEPS SERVICE_EXTRA_ENV

# Tier 1: Core
SERVICE_TIER[engine]=1;   SERVICE_DESC[engine]="LLM Proxy Engine";              SERVICE_SCRIPT[engine]="chronicle_engine.py"
SERVICE_TIER[feeds]=1;    SERVICE_DESC[feeds]="RSS/Feed Ingestion";             SERVICE_SCRIPT[feeds]="chronicle_feeds.py"
SERVICE_TIER[intern]=1;   SERVICE_DESC[intern]="Research and Briefing Agent";    SERVICE_SCRIPT[intern]="intern.py"
SERVICE_TIER[crossref]=1; SERVICE_DESC[crossref]="Connection Discovery";        SERVICE_SCRIPT[crossref]="crossref.py"
SERVICE_TIER[sentinel]=1; SERVICE_DESC[sentinel]="System Monitor";              SERVICE_SCRIPT[sentinel]="chronicle_sentinel.py"

# Tier 2: Intelligence
SERVICE_TIER[gemma]=2;       SERVICE_DESC[gemma]="Gate Classifier";                SERVICE_SCRIPT[gemma]="gemma.py"
SERVICE_TIER[provocateur]=2; SERVICE_DESC[provocateur]="Synthesis and Contrarian";  SERVICE_SCRIPT[provocateur]="provocateur.py"
SERVICE_TIER[analyst]=2;     SERVICE_DESC[analyst]="Knowledge Graph Extraction";    SERVICE_SCRIPT[analyst]="chronicle_analyst.py"
SERVICE_TIER[scribe]=2;      SERVICE_DESC[scribe]="Metrics and Scribe Context";     SERVICE_SCRIPT[scribe]="chronicle_scribe.py"

# Tier 3: Perception (optional — requires MQTT/HAL)
SERVICE_TIER[hal]=3;         SERVICE_DESC[hal]="Home Awareness Layer";          SERVICE_SCRIPT[hal]="chronicle_hal.py"
SERVICE_TIER[presence]=3;    SERVICE_DESC[presence]="Contextual Awareness";     SERVICE_SCRIPT[presence]="hal_presence.py"
SERVICE_DEPS[presence]="chronicle-hal.service"
SERVICE_EXTRA_ENV[presence]="Environment=MQTT_BROKER=\${MQTT_BROKER:-192.168.1.10}\nEnvironment=MQTT_PORT=\${MQTT_PORT:-1883}"
SERVICE_TIER[transcriber]=3; SERVICE_DESC[transcriber]="Audio Transcription";   SERVICE_SCRIPT[transcriber]="chronicle_transcriber.py"

# Tier 4: Communication (optional)
SERVICE_TIER[mind]=4;      SERVICE_DESC[mind]="Discord Integration";      SERVICE_SCRIPT[mind]="mind_discord_bot.py"

TIER_NAMES=("" "Core" "Intelligence" "Perception" "Communication")

# ═══════════════════════════════════════════════════════════════
#  Environment template
# ═══════════════════════════════════════════════════════════════

generate_env() {
    if [[ -f "$ENV_FILE" ]]; then
        warn "chronicle.env already exists at $ENV_FILE"
        read -p "Overwrite? [y/N] " confirm
        [[ "$confirm" != "y" && "$confirm" != "Y" ]] && { log "Keeping existing env."; return; }
    fi

    log "Generating chronicle.env..."
    echo ""
    echo "Required configuration:"
    echo "  (press Enter for defaults shown in brackets)"
    echo ""

    # Required
    read -p "  Ollama URL [http://localhost:11434]: " OLLAMA_URL
    OLLAMA_URL="${OLLAMA_URL:-http://localhost:11434}"

    read -p "  Embedding model [mxbai-embed-large]: " EMBED_MODEL
    EMBED_MODEL="${EMBED_MODEL:-mxbai-embed-large}"

    read -p "  Local chat model [falcon3:7b]: " LOCAL_MODEL
    LOCAL_MODEL="${LOCAL_MODEL:-falcon3:7b}"

    read -p "  Deep analysis model [llama-3.3-70b-versatile]: " DEEP_MODEL
    DEEP_MODEL="${DEEP_MODEL:-llama-3.3-70b-versatile}"

    read -p "  IC canister ID (or 'none'): " CANISTER_ID
    CANISTER_ID="${CANISTER_ID:-none}"

    read -p "  DFX identity [chronicle-auto]: " DFX_IDENTITY
    DFX_IDENTITY="${DFX_IDENTITY:-chronicle-auto}"

    echo ""
    echo "Optional API keys (press Enter to skip):"

    read -p "  Anthropic API key: " ANTHROPIC_KEY
    read -p "  Groq API key: " GROQ_KEY
    read -p "  Discord token: " DISCORD_TOKEN
    read -p "  Discord channel ID: " DISCORD_CHANNEL
    read -p "  Nostr nsec (hex): " NOSTR_NSEC
    read -p "  Home Assistant URL: " HASS_URL
    read -p "  Home Assistant token: " HASS_TOKEN
    read -p "  MQTT broker IP [192.168.1.10]: " MQTT_BROKER
    MQTT_BROKER="${MQTT_BROKER:-192.168.1.10}"

    cat > "$ENV_FILE" << ENVEOF
# Chronicle Environment — generated $(date +%Y-%m-%d)
# Core
CHRONICLE_OLLAMA_URL=$OLLAMA_URL
CHRONICLE_EMBEDDING_MODEL=$EMBED_MODEL
CHRONICLE_LOCAL_MODEL=$LOCAL_MODEL
CHRONICLE_DEEP_MODEL=$DEEP_MODEL
CHRONICLE_CANISTER_ID=$CANISTER_ID
CHRONICLE_IDENTITY=$DFX_IDENTITY

# Cycle timing (seconds)
CYCLE_INTERVAL=300
CROSSREF_INTERVAL=600
PROVOCATEUR_INTERVAL=900

# Models for specific agents
INTERN_MODEL=$LOCAL_MODEL
CROSSREF_MODEL=$EMBED_MODEL
PROVOCATEUR_MODEL=$LOCAL_MODEL

# Provocateur tuning
PROVOCATEUR_THREAD_WEIGHTS=35,35,15,15
PROVOCATEUR_SYNTHESIS_DEDUP=1

# Feed tuning
SEED_SKIP_SOURCES=
SEED_INTERN_BRIEF_VISIBLE=1
FEEDBACK_GRADIENT=1
AVOID_THEMES_DECAY=7

# API Keys
ANTHROPIC_API_KEY=${ANTHROPIC_KEY:-}
GROQ_API_KEY=${GROQ_KEY:-}

# Discord
DISCORD_TOKEN=${DISCORD_TOKEN:-}
DISCORD_CHANNEL_ID=${DISCORD_CHANNEL:-}

# Nostr
NOSTR_NSEC=${NOSTR_NSEC:-}

# Home Assistant
HASS_URL=${HASS_URL:-}
HASS_TOKEN=${HASS_TOKEN:-}
HA_URL=${HASS_URL:-}
HA_TOKEN=${HASS_TOKEN:-}
MQTT_BROKER=$MQTT_BROKER

# Feature flags
CROSSREF_DEDUP_MOD=1
SEED_DECAY_AWARE=1
CONNECTED_ROUTING_FIX=1
ENVEOF

    chmod 600 "$ENV_FILE"
    log "chronicle.env written to $ENV_FILE"
}

# ═══════════════════════════════════════════════════════════════
#  Service installation
# ═══════════════════════════════════════════════════════════════

install_services() {
    mkdir -p "$SERVICE_DIR"

    echo ""
    echo "Service tiers:"
    echo "  1. Core     — engine, feeds, intern, crossref, sentinel"
    echo "  2. Intel    — gemma gate, provocateur, analyst, scribe"
    echo "  3. Percept  — HAL, presence, transcriber (needs MQTT)"
    echo "  4. Comms    — Discord, dashboard"
    echo ""
    read -p "Install up to tier [1-4, default=2]: " MAX_TIER
    MAX_TIER="${MAX_TIER:-2}"

    local installed=0
    for name in "${!SERVICE_TIER[@]}"; do
        local tier=${SERVICE_TIER[$name]}
        (( tier > MAX_TIER )) && continue

        local script="${SERVICE_SCRIPT[$name]}"
        local desc="${SERVICE_DESC[$name]}"
        local deps="${SERVICE_DEPS[$name]:-}"
        local extra="${SERVICE_EXTRA_ENV[$name]:-}"
        local svc_file="$SERVICE_DIR/chronicle-${name}.service"

        # Check script exists
        if [[ ! -f "$BIN_DIR/$script" ]]; then
            warn "Skipping $name — $BIN_DIR/$script not found"
            continue
        fi

        local after="network-online.target"
        local requires=""
        if [[ -n "$deps" ]]; then
            after="$after $deps"
            requires="Requires=$deps"
        fi

        cat > "$svc_file" << SVCEOF
[Unit]
Description=Chronicle ${desc}
After=${after}
${requires}

[Service]
Type=simple
EnvironmentFile=%h/chronicle/chronicle.env
ExecStart=/usr/bin/python3 %h/chronicle/bin/${script}
Restart=always
RestartSec=30
StandardOutput=journal
StandardError=journal
$(echo -e "${extra}")

[Install]
WantedBy=default.target
SVCEOF

        log "Installed chronicle-${name}.service (tier ${tier})"
        installed=$((installed + 1))
    done

    systemctl --user daemon-reload
    log "Installed $installed services. daemon-reload done."
    echo ""
    echo "To enable and start all:"
    echo "  systemctl --user enable --now chronicle-engine chronicle-feeds chronicle-intern ..."
    echo ""
    echo "Or start individually:"
    for name in "${!SERVICE_TIER[@]}"; do
        local tier=${SERVICE_TIER[$name]}
        (( tier > MAX_TIER )) && continue
        echo "  systemctl --user enable --now chronicle-${name}"
    done | sort
}

# ═══════════════════════════════════════════════════════════════
#  Cron setup
# ═══════════════════════════════════════════════════════════════

setup_cron() {
    log "Setting up cron jobs..."

    local tmp_cron
    tmp_cron=$(mktemp)
    crontab -l 2>/dev/null > "$tmp_cron" || true

    # Digest — twice daily
    if ! grep -q "digest.py" "$tmp_cron" 2>/dev/null; then
        echo "# Chronicle Digest (7am + 5pm)" >> "$tmp_cron"
        echo "0 7,17 * * * . $ENV_FILE && python3 $BIN_DIR/digest.py --hours 10 >> $CHRONICLE_DIR/digest.log 2>&1" >> "$tmp_cron"
        log "Added digest cron (7am, 5pm)"
    else
        log "Digest cron already exists"
    fi

    crontab "$tmp_cron"
    rm -f "$tmp_cron"
}

# ═══════════════════════════════════════════════════════════════
#  Prerequisites check
# ═══════════════════════════════════════════════════════════════

check_prereqs() {
    log "Checking prerequisites..."
    local ok=1

    # Python 3.10+
    if command -v python3 &>/dev/null; then
        local pyver
        pyver=$(python3 -c "import sys; print(f'{sys.version_info.major}.{sys.version_info.minor}')")
        if python3 -c "import sys; exit(0 if sys.version_info >= (3,10) else 1)"; then
            log "  Python $pyver"
        else
            err "  Python $pyver (need 3.10+)"
            ok=0
        fi
    else
        err "  Python 3 not found"
        ok=0
    fi

    # SQLite
    if command -v sqlite3 &>/dev/null; then
        log "  sqlite3 found"
    else
        err "  sqlite3 not found"
        ok=0
    fi

    # Ollama
    if command -v ollama &>/dev/null; then
        log "  ollama found"
    else
        warn "  ollama not found (needed for local models)"
    fi

    # pip packages (package_name:import_name)
    local missing=0
    for entry in "paho-mqtt:paho.mqtt.client" "requests:requests" "feedparser:feedparser"; do
        local pkg="${entry%%:*}"
        local mod="${entry##*:}"
        if ! python3 -c "import $mod" 2>/dev/null; then
            warn "  Missing python package: $pkg"
            missing=$((missing + 1))
        fi
    done
    if (( missing > 0 )); then
        warn "  Install missing packages: pip install -r $SCRIPT_DIR/requirements-core.txt"
    fi

    # Data directory
    mkdir -p "$DATA_DIR"
    log "  Data dir: $DATA_DIR"

    return $ok
}

# ═══════════════════════════════════════════════════════════════
#  Full setup
# ═══════════════════════════════════════════════════════════════

full_setup() {
    echo ""
    echo "═══════════════════════════════════════════════════"
    echo "  Chronicle Setup"
    echo "  Sovereign intelligence infrastructure"
    echo "═══════════════════════════════════════════════════"
    echo ""

    check_prereqs || { err "Prerequisites not met. Fix issues above and re-run."; exit 1; }

    echo ""
    generate_env

    echo ""
    log "Seeding database..."
    bash "$SCRIPT_DIR/chronicle-seed.sh"

    echo ""
    install_services

    echo ""
    setup_cron

    echo ""
    log "Running verification..."
    python3 "$SCRIPT_DIR/seed-verify.py" 2>&1 || true

    echo ""
    echo "═══════════════════════════════════════════════════"
    log "Setup complete."
    echo ""
    echo "Next steps:"
    echo "  1. Review chronicle.env: $ENV_FILE"
    echo "  2. Pull Ollama models: ollama pull <model>"
    echo "  3. Start services: systemctl --user start chronicle-engine"
    echo "  4. Check status: systemctl --user status 'chronicle-*'"
    echo ""
}

# ═══════════════════════════════════════════════════════════════
#  Main
# ═══════════════════════════════════════════════════════════════

case "${1:-}" in
    --env-only)
        generate_env
        ;;
    --services)
        install_services
        ;;
    --cron)
        setup_cron
        ;;
    --check)
        check_prereqs
        ;;
    --all|"")
        full_setup
        ;;
    -h|--help)
        echo "Usage: $0 [--env-only|--services|--cron|--check|--all]"
        echo ""
        echo "  --env-only   Generate chronicle.env template"
        echo "  --services   Install systemd service files"
        echo "  --cron       Set up cron jobs"
        echo "  --check      Check prerequisites only"
        echo "  --all        Full setup (default)"
        ;;
    *)
        err "Unknown option: $1"
        echo "Run $0 --help for usage"
        exit 1
        ;;
esac
