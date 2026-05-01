# P25 Deployment Plan: Selective Preservation in stabilized_compress.py

## Finding (2-model, pending 3rd)

Selective preservation of identity fields (gist, constraints, goal) produces
13-23% tighter identity expression than full lossy compression. The LLM's
rewriting of identity fields pushes expression further from the centroid on
every compression.

## Current pipeline

```
stabilized_compress.py
  → generate_injection() — stability context prepended
  → compress_cognitive_state() — MCP call, LLM rewrites ALL fields
  → enforce_quota() — entity guard, prevents excessive entity replacement
```

All three layers operate on the LOSSY output. The stabilizer guides the LLM;
the guard repairs entity damage; but the gist/constraints/goal are still fully
LLM-rewritten every compression.

## Proposed change

Add a **post-compression identity restoration** step:

```
stabilized_compress.py
  → save pre-compression identity fields (gist, constraints, goal)
  → generate_injection() + compress_cognitive_state() — unchanged
  → enforce_quota() — unchanged
  → NEW: identity_restore()
      IF staleness override NOT active for field:
        → write pre-compression value back via update_cognitive_state
      ELSE:
        → keep the LLM's rewritten value (gist genuinely needs update)
```

## Integration with existing stability infrastructure

- **Staleness detector** (compression_stabilizer.py): already detects when gist
  has been frozen 5+ snapshots. When stale, routing = REBUILD. This is the
  signal that allows Type 6 (generative) evolution of the gist.
  
- **Entity guard** (entity_guard.py): operates on focal_entities, not identity
  fields. No conflict — they're separate layers.

- **Voice directive**: currently instructs LLM to write gist in first person.
  Under selective preservation, the voice directive only matters when staleness
  allows a rewrite. Should still include it for those cases.

## Implementation (< 20 lines of code)

In `stabilized_compress.py`, after `call_compress()` succeeds:

```python
# Identity restoration (P25 finding: selective preservation)
if not args.no_guard and not staleness_active:
    # Read pre-compression identity fields (already saved above)
    identity_restore = {
        "semantic_gist": pre_gist,
        "goal_orientation": pre_goal,
        "constraints": json.dumps(pre_constraints),
    }
    write_identity_back(identity_restore)
```

## Risks

1. **Gist freeze**: If staleness detector has false negatives, the gist could
   stay frozen even when work direction changes. Mitigation: staleness
   threshold is already tuned (5 snapshots). Monitor.

2. **Constraint drift prevention**: Constraints are near-invariant (0.97 Jaccard).
   Preserving them verbatim is consistent with their actual behavior. But if a
   genuine new constraint needs to be added, the LLM can't do it. Mitigation:
   manual `update_cognitive_state` for new constraints (already the pattern —
   gate #465 was added manually).

3. **Goal stagnation**: Goal might need to evolve faster than gist. Consider
   treating goal as a "soft preserve" — allow LLM rewrite but only if the
   rewrite is semantically close (embedding distance < threshold). Future
   refinement.

## Status: DEPLOYED (Build 29, 2026-04-20)

V3.2 confirmed the direction (3/3 models favor Type 2). `--selective` flag
deployed in stabilized_compress.py. Implementation:
- `get_identity_fields()`: reads gist, goal, constraints from CCS pre-compression
- `write_identity_back()`: restores fields via MCP update_cognitive_state
- Staleness-gated: if `detect_staleness()` flags a field, the LLM's rewrite is kept
- Currently both gist and goal are stale (20 snapshots), so only constraints would
  be preserved. This is correct — the gist needs to update.

## Next: A/B test

Next rotation: run `stabilized_compress.py --selective "session summary"` and compare:
1. Entity retention rate vs non-selective
2. Identity expression (run identity probe before and after)
3. Gist drift measurement

## Connection to thread

Build 29, Thread 318 advance 33. P25 final results across 3 architectures.
Nate opened P26 (compressor model identity) as the next variable.
