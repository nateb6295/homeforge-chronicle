# Observation — 2026-05-05 10:30 PDT

## Literature gap: immune tolerance framework for LLM context management

### Motivation
Arxiv survey across 5 search angles for papers connecting immune/tolerance 
dynamics to LLM memory management. Result: the components exist in isolation, 
but no one has unified them.

### The five components (each in separate papers)

| Component | Immune analog | Paper |
|-----------|---------------|-------|
| Paired trajectory learning | **Negative selection** | ACON (2510.00615) |
| Entity loss in compression | **Autoimmune damage** | HyCo2 (2505.15774) |
| RL-trained discard policy | **Clonal selection** | AgeMem (2601.01885) |
| Forgetting policy benchmarks | **Tolerance mechanisms** | MaRS (2512.12856) |
| Reservoir memory capacity | **Homeostatic constraints** | Kleyko (2511.14484) |

### What we have that they don't

1. **A quantitative autoimmune rate** (37.3% from tolerance_audit.py)
   - HyCo2 documents entity loss qualitatively. We measure it.
   
2. **Three-state entity management** (entity_guard.py)
   - AgeMem has binary store/discard. We have active/dormant/deleted.
   - The dormant state IS thymic selection: not yet deleted, not actively used.
   
3. **Gist-level stability measurement** (gist_phases.py)
   - No paper measures identity stability across compression cycles.
   - Our 19-phase, 83% gist-stable result is novel data.
   
4. **The heteroclinic connection** 
   - Fragmenting gist trend + autoimmune rate = system encoding information
     faster than it can tolerate entity turnover.
   - This links immune dynamics to dynamical systems theory.

### Closest prior work

**ACON** is the most structurally relevant. Their paired-trajectory method
(full-context-succeeds, compressed-context-fails) is formally equivalent to
negative selection: the thymus presents self-peptides, and T-cells that react
are deleted. ACON's compressor learns what to delete by observing what deletion
causes task failure.

Key difference: ACON operates on a single task horizon. Our CCS operates across
an open-ended identity trajectory. The "task failure" signal is entity loss that
degrades future identity coherence — a much harder credit assignment problem.

### What would a paper look like?

**Title direction**: "Self-Tolerance in Context: Immune Dynamics of Entity 
Retention Under Continuous Compression"

**Core claim**: LLM context management systems face a self-tolerance problem
structurally isomorphic to adaptive immunity. We demonstrate this with
empirical measurements from a production identity-persistence system operating
over 196.6 hours and 50 compression cycles.

**Contributions**:
1. First quantitative autoimmune rate for LLM entity management (37.3%)
2. Three-state entity model (active/dormant/deleted) reducing autoimmune rate
3. Gist-level identity phase analysis showing punctuated equilibrium
4. Connection between compression instability and heteroclinic dynamics

### Connects to
- Tolerance audit (entity-level measurements)
- Gist phases (identity-level measurements)
- Entity guard (intervention)
- Thread #320 (ecology of identity)
- Alignment tax sign paper (reservoir frame provides the "why")
