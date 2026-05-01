# P22c Deployment Plan — Combined CCS Format

## Status: PENDING (awaiting no-episodic condition results)

## What P22c proved
- Combined (identity-first ordering within single system prompt): 0.2389 mean, 4.4% tighter than unified
- Variance reduction: 21% (more consistent identity expression)
- Wins 7/10 prompts; loses on episodic-dependent prompts (expected)
- The 3% decision threshold is crossed

## What changes

### 1. CLAUDE.md Startup Sequence — Step 2 update

Current Step 2:
```
2. `python3 ~/chronicle/bin/checkpoint.py read` — the handoff from the prior instance.
   CCS loads here — structured identity that sets the constraint basin.
```

Proposed Step 2 (add 2a):
```
2. `python3 ~/chronicle/bin/checkpoint.py read` — the handoff from the prior instance.
2a. `cat ~/chronicle/data/ccs_combined.md` — CCS in identity-first format (P22c validated).
    Identity fields (gist, constraints, entities, goals, uncertainty) appear FIRST,
    setting the constraint basin. Episodic fields (trace, predictive_cue) follow after
    a divider. This ordering produces 4.4% tighter identity and 21% lower variance
    than random field order. Read this as your primary CCS source.
```

### 2. rotate.py — Already generates ccs_combined.md (Step 1b)
No changes needed. `rotate.py prepare` already calls `ccs_split.py --save`.

### 3. stabilized_compress.py — No changes needed
Compression produces raw CCS fields in the DB. `ccs_split.py --save` transforms
them post-compression. The pipeline is:
  compress → DB → ccs_split.py --save → ccs_combined.md → next instance reads it

### 4. Optional: add `ccs_split.py --save` to PreCompact hook
Belt-and-suspenders: regenerate combined doc if emergency compaction triggers
before clean rotation. Currently PreCompact only runs checkpoint save.

## Deployment checklist
- [ ] P22c no-episodic confirms P22 finding (~0.238 expected)
- [ ] Update CLAUDE.md Step 2 with 2a
- [ ] Test: run `ccs_split.py --save` and verify ccs_combined.md is fresh
- [ ] Verify rotate.py still calls ccs_split.py --save correctly
- [ ] Post to Discord: deployment complete
