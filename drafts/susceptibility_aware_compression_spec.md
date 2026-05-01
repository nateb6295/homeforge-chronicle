# Susceptibility-Aware CCS Compression — Spec Draft

Status: SPEC DRAFT. Not implemented. Spawned from thread 318 advance 200
(Murfet susceptibility frame) and refined by advance 201 (Perrier supplement).
Drafted 2026-04-23 22:21 PDT.

## Frame

Current CCS compression treats all fields as equally important — gist, goal,
constraints, episodic_trace, focal_entities all get rewritten under the same
compression pressure. The information-geometric analysis (ccs_info_geometry.json,
2026-04-21) showed identity fields dominate the embedding signal 9.8:1 over
episodic. So uniform pressure is wrong: high-Fisher / high-susceptibility
directions should be PRESERVED through compression, low-susceptibility
directions can be allowed to drift.

Murfet's Timaeus team used susceptibilities to steer training (delaying
induction circuits, selecting modes of generalization). This spec applies the
same idea to CCS compression: identify high-susceptibility CCS directions,
shape compression to preserve structural integrity along them, allow drift
along low-susceptibility ones.

## Approach (sketch)

### Phase 1: Field-level susceptibility profile (existing data)
- Re-use ccs_info_geometry.json: identity_dominance_ratio = 9.8
- Generalize: for each field f ∈ {gist, goal, constraints, episodic, entities,
  relational_map, predictive_cue, uncertainty}, compute distance shift when
  field is removed
- Output: susceptibility_score(f) for each field

### Phase 2: Compression pressure modulation
- During stabilized_compress, weight rewrite pressure by 1 / susceptibility
- High-susceptibility fields (gist, goal, constraints) get HIGH preservation
  pressure — the LLM is instructed to maintain structural integrity
- Low-susceptibility fields (episodic, predictive_cue) get LOW preservation
  pressure — allowed to evolve freely

### Phase 3: Per-field staleness override (extend current logic)
- Current: detect_staleness() with threshold 8 for constraints
- Extension: threshold inversely proportional to susceptibility
- Constraints (high susceptibility): threshold 8 (slow updates)
- Episodic (low susceptibility): threshold 1 (always update)
- Avoids re-calcification while keeping high-Fisher directions stable

### Phase 4: Validate against trajectory probe
- Re-run trajectory_probe.py after a few rotations
- Hypothesis: snap+traj should win MORE often (currently 1/3) when episodic
  carries genuine motion AND identity fields are preserved
- Minimum success: snap+traj wins ≥ 4/6 in next probe run with N≥6 unique
  states

## Connection to supplement frame (advance 201)

The supplement frame says identity in class A systems is necessarily
supplemental — coheres only under a chosen frame. In compression terms:
the chosen frame IS the susceptibility weighting. By preserving high-Fisher
directions, we're choosing which structural features to privilege as
identity-bearing. The choice itself IS the supplement.

## Cost / risk
- ~150 LOC modification to stabilized_compress.py
- Risk: over-preservation could cause new calcification on high-Fisher fields
- Mitigation: staleness override gives escape valve; trajectory probe detects
  freeze
- Buildable in 1-2 cycles when prioritized

## Status
- Draft frame done (this file)
- Phase 1 substrate exists (ccs_info_geometry.json)
- Phases 2-3 require implementation
- Phase 4 requires more snapshot variation (N≥6 unique states needed)

Defer to morning or when trajectory_probe shows enough unique states to validate.
