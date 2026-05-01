# Keeper Stable-Memory Migration — Design Sketch

**Status:** draft 2026-04-16 08:53 PDT. Greenlit for PLANNING by Nate.
Not greenlit for implementation — this doc is the gate.

## The problem

All three Chronicle canisters store working state in heap-resident
`thread_local! { RefCell<State> }` and serialize/deserialize the entire
State through Candid on every upgrade via `stable_save`/`stable_restore`.

DFINITY's stable-memory skill flags this as pitfall #1: at scale, the
serialize path runs out of instructions and bricks the canister. Pay-per-
call cycles scale with whole-structure size, not hot-set size.

Three canisters affected:
- **keeper** (1890 lines, 16 structs) — biggest burner, biggest collections
- **backend** (9406 lines, 63 structs, 16 schema versions) — deepest; XRP wallet canister, highest-risk to touch
- **lab** (686 lines, 13 structs) — shallowest, smallest burn

**Keeper first.** Migration order: keeper → lab (low risk shakedown) → backend (scariest, do last with full experience in hand).

## Scope of keeper state

Keeper's `State` today:
```rust
struct State {
    embeddings: HashMap<u64, CapsuleEmbedding>,      // SCALES - O(n) capsules
    capsule_meta: HashMap<u64, CapsuleMeta>,         // SCALES - O(n) capsules
    keeper: KeeperState,
        connections: Vec<KeeperConnection>,          // SCALES - cap 50K
        clusters: Vec<KeeperCluster>,                // bounded 100
        orphans: Vec<u64>,                           // bounded
        composted_ids: Vec<u64>,                     // SCALES - monotone
        + digest, counters, next_id
    metabolism: PatternMetabolismState,
        patterns: Vec<MetabolismPattern>,            // bounded
        somatic_markers: Vec<SomaticMarker>,         // bounded (action-indexed)
        emotional_memories: Vec<EmotionalMemory>,    // SCALES with cycles
        causal_edges: Vec<CausalEdge>,               // SCALES with edges
        + config, counters
    last_synced_id, owner, budget_window_*           // scalars
}
```

Five collections that scale unboundedly with usage:
`embeddings`, `capsule_meta`, `connections`, `composted_ids`, `emotional_memories`, `causal_edges`.

## Migration pattern (per DFINITY skill)

```rust
use ic_stable_structures::{StableBTreeMap, DefaultMemoryImpl, memory_manager::{MemoryManager, MemoryId, VirtualMemory}};

type Memory = VirtualMemory<DefaultMemoryImpl>;

thread_local! {
    static MEMORY_MANAGER: RefCell<MemoryManager<DefaultMemoryImpl>> =
        RefCell::new(MemoryManager::init(DefaultMemoryImpl::default()));

    static EMBEDDINGS: RefCell<StableBTreeMap<u64, CapsuleEmbeddingBlob, Memory>> =
        RefCell::new(StableBTreeMap::init(MEMORY_MANAGER.with(|m| m.borrow().get(MemoryId::new(0)))));
    // ... one MemoryId per stable collection
}
```

Each type needs a `Storable` impl. For keeper's types, easiest path
is Candid-encoded bytes:
```rust
impl Storable for KeeperConnection {
    fn to_bytes(&self) -> Cow<[u8]> {
        Cow::Owned(candid::encode_one(self).unwrap())
    }
    fn from_bytes(bytes: Cow<[u8]>) -> Self {
        candid::decode_one(&bytes).unwrap()
    }
    const BOUND: Bound = Bound::Bounded { max_size: 512, is_fixed_size: false };
}
```
Seven collections → seven `Storable` impls.

## Phased rollout

**Phase 0: Baseline measurement (before touching code)**
- Record current keeper upgrade cost: wasm_size + stable_save cycles
- Record current keeper burn/day (already have: ~4T/day per directive #525)
- Record avg call cycles for a representative op (e.g., `discover_connections`)
- This is the before-picture for the success metric.

**Phase 1: Extract Storable impls and MemoryManager scaffolding**
- Add `ic-stable-structures = "0.6"` to Cargo.toml
- Write Storable impls for the 7 scaling types in a new `stable_types` module
- No State changes yet — purely additive
- Build + unit-test encode/decode roundtrip
- Deployable, no behavior change

**Phase 2: Migrate embeddings + capsule_meta (smallest hot collections)**
- Replace `HashMap<u64, CapsuleEmbedding>` with `StableBTreeMap<u64, CapsuleEmbedding, Memory>`
- Replace `HashMap<u64, CapsuleMeta>` similarly
- Write a ONE-TIME migration in `post_upgrade` that reads the old Candid-blob state and populates the StableBTreeMap, then drops the heap copy
- Test on local replica with test dfx keeper
- Deploy to lab first (mini-version), then keeper after verify

**Phase 3: Migrate connections + composted_ids + emotional_memories + causal_edges**
- Four more collections in separate deploys (one per upgrade, reduces blast radius)
- Each deploy does one-time drain from old State.keeper.X into new StableBTreeMap

**Phase 4: Simplify pre_upgrade**
- Once all scaling collections are in stable structures, the remaining State
  is ~10KB of scalars + bounded collections (clusters ≤100, patterns bounded)
- Keep `stable_save` for the scalar State at a dedicated MemoryId (or migrate
  to a `Cell<State>` in ic-stable-structures)
- Upgrade cost becomes O(scalar state) — effectively free.

## Success metrics

Before → after comparison:
- **Upgrade cycle cost** — target: 100x reduction (today dominated by
  embeddings + connections serialization)
- **Per-call cycle cost** for `discover_connections` — target: connection-count
  proportional to hot-scan size, not to total connection table size
- **Keeper daily burn** — target: 4T/day → <1T/day (directive #525 conditions)
- **Upgrade latency** — deploys today hang several minutes during stable_save;
  target: sub-second

If Phase 2 deploys and any of these don't move, the hypothesis was wrong
about where the cost lives. Stop and re-measure.

## Risks

- **One-time migration bugs.** If `post_upgrade` migration panics, canister
  state is toast. Mitigation: local replica rehearsal with real state size
  (pull current keeper state via query, replay into local, upgrade locally, verify).
- **Candid-encoded Storable bounds.** Max size 512 is a guess; need to
  sample real CapsuleEmbedding sizes and size Bound appropriately, otherwise
  BTreeMap inserts panic.
- **Composed lookups.** Some current ops iterate `connections` looking for
  pairs. A BTreeMap keyed by `u64` gives O(log n) lookup per id, but pairwise
  lookups may need a secondary index (e.g., `BTreeMap<(u64, u64), KeeperConnection>`).
  Add the pair-key map if benchmark shows regression.
- **Backend is scarier.** 63 structs, 16 schema versions with `Option<Vec<X>>`
  incremental fields. If keeper migration gets stuck on any structural issue,
  that issue is 10x worse on backend. Keeper-first is de-risking backend.

## Prior art in codebase

- `src/chronicle_keeper/src/lib.rs:236-237` — current anti-pattern
- `src/chronicle_keeper/src/lib.rs:297-310` — current pre/post_upgrade
- Backend canister has 16 schema version handling in State; study before
  attempting backend Phase 1. Keeper has just 1 version — cleaner start.

## Timeline estimate

- Phase 0 (baseline): 1 session, mostly dfx queries
- Phase 1 (scaffolding): 1-2 sessions
- Phase 2 (embeddings+meta): 1 session + local rehearsal
- Phase 3 (remaining collections): 2-3 sessions (spread over days, one deploy each)
- Phase 4 (scalar cleanup): 1 session

Total: ~1 week of focused work, spread across rotations. Phase 0 can start
immediately and is reversible.

## Gate conditions for implementation

This is the gate I'm asking you to set:

1. **Phase 0 (baseline measurement)** — I can start this now if you greenlight.
   It's read-only and produces the before-picture regardless of whether we
   migrate.

2. **Phase 1+** — requires Nate greenlight after seeing Phase 0 numbers.
   If baseline shows keeper's burn is dominated by compost/graph logic
   rather than upgrade/scan costs, the migration hypothesis is wrong and
   we should look elsewhere.

**Holding position:** doc exists, ready to execute. Starting Phase 0 on
greenlight; Phase 1+ blocked behind baseline review.

## Open design questions

1. Should embeddings live in backend (source of truth) and NOT be replicated
   in keeper? Today keeper holds a copy for local similarity compute. If
   keeper could stream from backend on-demand, we'd cut the largest
   collection from keeper's state entirely. Bigger architectural change
   than a stable-memory migration — separate question.
2. Should pattern_metabolism collections stay in keeper or move to a
   dedicated fourth canister? They're conceptually separate from the
   graph. Adjacent to the migration question, not blocking.
3. Is the Candid-Storable shim sufficient or do we want hand-rolled
   `Storable` for the hot types (CapsuleEmbedding has a `Vec<f32>` — raw
   `bytemuck` encoding would be ~4x smaller than Candid)? Can answer
   from Phase 0 size measurements.
