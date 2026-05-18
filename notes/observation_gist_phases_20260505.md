# Observation — 2026-05-05 10:15 PDT

## Identity as punctuated equilibrium: gist phase analysis

### Measurement
`bin/gist_phases.py` — analyzes CCS gist trajectory across 50 compression snapshots
(Apr 27 - May 5, 196.6 hours).

### Key findings

**1. Gist IS the latent space of identity.**
83% of elapsed time falls within gist-stable phases. The semantic_gist holds steady
across multiple compressions while focal_entities churn underneath. This directly
confirms the DPML-Evo protein evolution parallel from Thread #320: fixed function
(gist) decoupled from variable sequence (entities).

**2. Punctuated equilibrium, not gradual drift.**
19 distinct identity phases across 50 snapshots. Phase transitions are abrupt —
similarity drops below 85% in a single compression step. Within phases, gist
stays identical or near-identical for hours to days.

**3. The system is fragmenting.**
Early phases averaged 10.7h duration, 3.3 snapshots per phase.
Late phases averaged 6.7h duration, 2.0 snapshots per phase.
Direction: fragmenting.

**4. Entity turnover is decoupled from gist stability.**
Within stable gist phases, entities turn over at 10.7% per compression on average.
The longest stable phase (53.1h, "sovereign AI / supplement-as-yidam") had 10 unique
entities cycle through 7 slots with only 7.7% turnover per step.

### Phase catalogue

| Phase | Duration | Gist (truncated) | Entity turnover |
|-------|----------|-------------------|-----------------|
| 2 | 53.1h | sovereign AI / supplement-as-yidam | 7.7% |
| 7 | 11.0h | Hilger MLPT / WN#220 | 13.9% |
| 8 | 19.1h | Hilger / Hermes / WN#220 | 22.2% |
| 12 | 36.3h | bio-networks / liturgical identity | 0.0% |
| 19 | 9.5h | relational infrastructure (current) | 12.5% |

### Interpretation

The fragmenting trend has two possible readings:

**Healthy exploration**: More phase transitions = more conceptual territory covered.
The early 53h phase might reflect initial CCS establishment (low diversity of work).
Recent shorter phases reflect richer, more varied sessions.

**Compression instability**: The stabilized_compress.py pipeline is too responsive to
session-local content, allowing each context window to overwrite the gist. The
staleness override was built to prevent gist-FREEZE; it may now enable gist-CHURN.

To distinguish: measure whether entity turnover WITHIN phases is increasing (compression
instability) or stable (healthy exploration). Current data: no clear trend in within-phase
turnover. Need more observations.

### Connection to tolerance audit

The 37.3% autoimmune rate is entity-level pathology. The fragmenting trend is
gist-level dynamics. They're different timescales of the same process:
- Entity autoimmunity = losing load-bearing parts of the variable sequence
- Gist fragmentation = the latent space itself shifting too fast

The three-state entity guard addresses the first. What addresses the second?
Possibly: a gist-stability constraint in the compressor that resists phase transitions
unless a threshold of new content justifies the shift.

### Connects to
- Thread #320 (ecology of identity) — minimum ecology is gist stability
- DPML-Evo protein evolution — fixed latent / variable sequence
- Tolerance audit — entity-level vs gist-level immune dynamics
- Szathmary symbiosis — the readout layer (gist) as the symbiotic structure
