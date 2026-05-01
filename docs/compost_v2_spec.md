# Compost v2 — Utility-Under-Recall (draft spec)

**Status:** design sketch, 2026-04-14
**Supersedes:** keeper in-canister compost (disabled 2026-04-12)
**Related:** `bin/capsule_survival.py` v0 (shipped tonight), essay "Grounding Without Accumulation" (post #165)

---

## Problem

v1 compost was **age-triggered**: old = compostable. That conflates two different properties. A claim from six months ago that still reliably informs retrievals is doing more work than a claim from this morning that nothing cites. Pruning by age punishes the first and protects the second.

The essay frame (post #165): grounding is selection pressure, not storage. A memory should earn its persistence by being useful under recall. Compost v2 has to measure utility, not age.

## Trigger

A capsule becomes **compost-eligible** when:

1. `capsule_survival.score < -0.2` for **2+ consecutive weekly computes**, AND
2. `capsule_survival.survived_at IS NULL` (no human confirm-survive), AND
3. `memory_type IN ('claim', 'prediction', 'observation')` (not directives, not facts-from-Nate), AND
4. Not referenced by any active thread or objective (lineage check).

Second-pass requirement prevents transient dips (e.g., a contradiction that gets rejected on review) from triggering removal.

## Action

When triggered:
- Add `compostable_at` timestamp to `capsule_survival`
- Set `knowledge_capsules.visible_in_retrieval=0` (new col; defaults 1)
- Row remains in graph; contradiction/thread lineage edges intact
- Excluded from default similarity search and agent-context loads

Never deleted. **Dormancy, not deletion.** A future survival compute can un-dormant the capsule if recall signal returns.

## Reversal

- `capsule_survival.py survive ID` clears `compostable_at`, sets `visible_in_retrieval=1`
- A contradiction rejection (e.g., the scaffold/life-cycle-stages case from tonight) triggers recompute of the losing capsule's score; if score recovers above -0.2, dormancy lifts automatically on next compute

## Open questions

1. **Recall signal calibration.** v0 shows 98.6% of capsules in neutral bucket, mostly because the multi-token LIKE scan returns 0 for most tokens. Before enabling compost v2, either:
   - Broaden the token match (any-token OR instead of all-token AND)
   - Add fts5 virtual table for fulltext — **confirmed available** (sqlite 3.37.2 compiled with ENABLE_FTS5, probe passed 2026-04-14)
   - Restore `memory_access_log` from Hermes cron jobs (post-pivot agents retired)
   Recommend all three; they compose.

2. **Threshold tuning.** -0.2 is a guess. Real calibration requires labeled data: capsules Nate or I can look at and agree "this should survive" / "this has rotted." 20 labels each way would let me validate the threshold empirically.

3. **Compost frequency.** Weekly compute, but maybe compost-eligibility trigger should require 2 consecutive weeks at sub-threshold AND a full month without retrieval. 4-week dormancy window feels right — Chronicle's natural rhythm.

4. **Canister side.** v1 lived in the keeper canister. v2 runs in sqlite first; canister parity ships later. Don't rebuild canister compost until the sqlite-side calibration is validated.

## Next steps (ordered)

1. **First** — recall signal calibration pass (broaden tokens, add fts5). Without this, compost would fire on too many neutral capsules.
2. **Second** — hand-label 40 capsules (20 survive / 20 compost) to validate threshold.
3. **Third** — add `visible_in_retrieval` column, wire default-retrieval filters in `memory.py`, `vector_index.py`.
4. **Fourth** — weekly cron, two-pass gate, post summary to #operator.
5. **Fifth** — canister-side port (Motoko), once sqlite side has run 4+ weeks stable.

## Why this matters

Compost v1 fired on a proxy for utility (age). v2 fires on utility directly. That's not a minor refactor — it changes what the system is *optimizing for*. v1 kept young things and pruned old things regardless of whether they were doing work. v2 keeps working things and prunes sleeping things regardless of age.

The essay's line: "a system optimized for storage is a library. A system optimized for selection is a body." Compost v2 is the part of the body that lets tissue die when it stops being used.
