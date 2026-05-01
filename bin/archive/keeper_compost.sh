#!/bin/bash
# Keeper compost cron — restarts timer and triggers compost cycle
# The IC canister timer silently dies on instruction limit overruns.
# This cron ensures compost runs reliably every 4h regardless.

export DFX_WARNING=-mainnet_plaintext_identity

dfx canister --network ic call mjoko-laaaa-aaaai-q7tlq-cai restart_timer \
  --identity chronicle-auto --network ic 2>/dev/null

dfx canister --network ic call mjoko-laaaa-aaaai-q7tlq-cai trigger_compost \
  --identity chronicle-auto --network ic 2>/dev/null

echo "[$(date '+%Y-%m-%d %H:%M:%S')] Keeper compost triggered"
