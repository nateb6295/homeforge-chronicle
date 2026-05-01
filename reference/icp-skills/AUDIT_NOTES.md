# ICP Skills Audit Notes for Chronicle Canisters

Source: https://skills.internetcomputer.org/.well-known/skills/index.json
Fetched: 2026-04-16

## Chronicle Canisters
- Backend: fqqku-bqaaa-aaaai-q4wha-cai
- Frontend: nbt4b-giaaa-aaaai-q33lq-cai
- Keeper: (see canister_ids)
- Lab: (see canister_ids)

## Priority Skills (fetched)
1. **canister-security** — 12 pitfalls, CallerGuard pattern, inspect_message
2. **stable-memory** — StableBTreeMap, MemoryManager, persistent actor, 7 pitfalls
3. **multi-canister** — inter-canister calls, 2MB payload, factory pattern
4. **cycles-management** — freezing thresholds, balance checks, CMC

## Security Audit Checklist (from canister-security skill)
- [ ] Anonymous principal rejection on all authenticated endpoints
- [ ] CallerGuard (reentrancy prevention) on state-mutating async methods
- [ ] No secrets stored in canister state (use vetKD)
- [ ] Bounded storage per user (prevent memory exhaustion)
- [ ] Backup controllers configured
- [ ] Freezing threshold set to 90+ days for production
- [ ] No fetchRootKey in production code
- [ ] Bounded wait calls for all inter-canister operations
- [ ] Callback trap handling (mutations before await persist on trap)
- [ ] inspect_message NOT relied on for access control

## Stable Memory Checklist (from stable-memory skill)
- [ ] Using StableBTreeMap (Rust) or persistent actor (Motoko)
- [ ] No thread_local RefCell<HashMap> for persistent data
- [ ] pre_upgrade/post_upgrade avoided for large data (instruction limit traps)
- [ ] Storable implemented with CBOR serialization
- [ ] MemoryId uniqueness verified across all structures
- [ ] Transient vars used for caches/request counters

## Multi-Canister Checklist
- [ ] Shared types module prevents type divergence
- [ ] 2MB payload limits accounted for
- [ ] Saga pattern for non-atomic multi-step operations
- [ ] Idempotency keys for non-idempotent operations
- [ ] Deploy order respects dependencies

## Audit Results (2026-04-16)

### Settings fixes shipped:
- [x] All 4 canisters: freezing threshold 30d → 90d (7,776,000 seconds)
- [x] Lab: backup controller added (was single, now 2)

### Code fixes:
- [x] Lab: access control added (owner + allowed_callers, 10 update methods gated)
  - DEPLOYED to mainnet, owner set, data verified (135 experiments intact)
- [x] Keeper: require_authorized() on 6 admin methods (reinforce_capsule, prune_weak, trigger_compost, keeper_ask, update_metabolism_config, import_metabolism_state)
- [x] Keeper: check_sync_guard() on 8 import_* methods (auth + rate limiting)
- [x] Keeper: CallerGuard (reentrancy prevention) on keeper_ask (async LLM method)
  - DEPLOYED to mainnet, data verified (15K embeddings, 25K meta, 50K connections)
- [~] Backend: API token stored in state — assessed as LOW risk
  - It's an HTTP bearer token for the HTTP gateway, not a signing key
  - Risk: subnet validators could read it from canister memory
  - Proper fix: migrate HTTP auth to IC identity-based (architectural change)
  - Mitigation: token is optional (None = open), and HTTP API traffic is minimal
  - NOT blocking — defer to IC identity migration when HTTP API is next touched

## Next Steps
1. Wire relevant skills into Hermes context for ICP work
2. Backend HTTP auth → IC identity migration (when HTTP API next touched)
