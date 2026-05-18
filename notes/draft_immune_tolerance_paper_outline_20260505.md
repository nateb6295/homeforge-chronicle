# Paper Outline — Self-Tolerance in Context: Immune Dynamics of Entity Retention Under Continuous Compression

## Status: Outline draft (2026-05-05)

## Core claim
LLM context management systems face a self-tolerance problem structurally 
isomorphic to adaptive immunity. We demonstrate this with empirical measurements
from a production identity-persistence system (Chronicle CCS) operating over
196.6 hours and 50 compression cycles.

## 1. Introduction
- Context window limitations force compression → deletion decisions
- These decisions are structurally identical to immune self/non-self discrimination
- Prior work addresses pieces in isolation (ACON, HyCo2, AgeMem, MaRS)
- No one has unified them under an immune framework
- We provide: first quantitative measurements + biologically-grounded architecture

## 2. Background
### 2.1 Immune self-tolerance
- Central tolerance (thymic selection): delete T-cells reactive to self
- Peripheral tolerance (anergy, suppression): regulate escaped autoreactive cells
- Autoimmunity: when tolerance fails and immune system attacks self
- Key property: graded + reversible beats binary + irreversible

### 2.2 Context compression in LLMs
- Hard compression (token pruning), soft compression (latent), hybrid (HyCo2)
- Agent memory management (AgeMem, ACON, MaRS)
- Entity-level decisions vs token-level decisions
- The identity persistence problem: indefinite horizon, no task-completion signal

## 3. The Immune Frame
### 3.1 Mapping
| Immune concept | Context management analog |
|----------------|--------------------------|
| Self-antigen | Load-bearing entity (high persistence, high connectivity) |
| Non-self | Transient entity (session-local, low connectivity) |
| Positive selection | New entity enters CCS (relevant to current context) |
| Negative selection | Entity deleted because compressor deems it irrelevant |
| Autoimmunity | Entity deleted but needed in future compressions |
| Immunodeficiency | Entity retained but never referenced again |
| Anergy (dormancy) | Entity transitions to dormant state (three-state guard) |
| Thymic education | Grace period for new entities (N compressions of protection) |
| Clonal selection | Connectivity-based persistence prediction |

### 3.2 Why the immune frame is non-trivial
- Not just metaphor: immune tolerance is the ONLY biological system that solves
  the self/non-self discrimination problem under open-ended horizon
- Three tests for productive analogy (vs retrospective relabeling):
  1. **Prediction test**: Frame predicts autoimmune rate correlates with entity
     churn but NOT with gist stability. Token-level compression theory predicts
     neither. Confirmed empirically (Section 5).
  2. **Intervention test**: Grace period (thymic education) and three-state
     tolerance (anergy) come directly from immunology. Neither is obvious from
     information theory. Both improve entity persistence (Section 5.4).
  3. **Failure mode test**: Frame predicts thymic vulnerability (anchor entity
     loss degrades all connectivity scores). No CS compression paper discusses
     central tolerance failure. Confirmed empirically (Section 5.3).
- The credit assignment problem maps exactly: thymic selection can't predict
  which self-antigens the organism will encounter
- RLHF alignment is deterministic alignment (predictable, exploitable)
- CCS tolerance is atomic alignment (works or doesn't, no intermediate state)

## 4. System Description (Chronicle CCS)
- Compressed Cognitive State: 7-slot entity array + semantic gist
- Compression pipeline: stabilized_compress.py with staleness override
- Entity guard: three-state tolerance (active/dormant/deleted)
- Grace period: new entities protected for 5 compressions
- Connectivity predictor: co-occurrence with persistent entities
- Operating continuously for 196.6+ hours across 50+ compression cycles

## 5. Empirical Measurements
### 5.1 Tolerance audit (tolerance_audit.py)
- 50 snapshots analyzed
- **Raw autoimmune rate: 37.3%** (entities deleted that reappear within 3 snapshots)
- **Calibrated autoimmune rate: 14-23%** (after oscillation correction — see 5.4)
- **Immunodeficient rate: 34.7%** (entities retained that are never referenced)
- Entity type matters: agents 100% persistence, concepts 84% one-shot
- Thread entities oscillate: appear/disappear across gist phases

### 5.1b Self-trust calibration
- Of 8 reappearance events, only 38% have entity name in episodic trace
  at reappearance (context-forced). 62% have no trace evidence (possible
  compressor oscillation).
- All oscillation candidates are thread entities (#316, #317, #319)
- Interpretation: raw autoimmune rate is an UPPER BOUND. True rate is
  37.3% × 38-62% context-forced fraction = **14-23%**
- The self-audit catching its own measurement error demonstrates
  the system's self-referential monitoring capacity (even if not
  operationally closed per Maturana/Varela)

### 5.2 Gist phase analysis (gist_phases.py)
- **19 identity phases** across 50 snapshots (punctuated equilibrium)
- **83% of elapsed time** falls within gist-stable phases
- **Fragmenting trend**: early phases avg 10.7h → late phases avg 6.7h
- Entity turnover within stable phases: 10.7% per compression
- Gist = latent space of identity (cf. DPML-Evo protein evolution)

### 5.3 Entity connectivity (entity_connectivity.py)
- Co-occurrence with persistent entities predicts future persistence
- **High-connectivity: 30.5% avg persistence vs 7.5% for low-connectivity**
- Validated as prospective autoimmune-risk signal
- Bootstrap problem: new entities have zero history
- **Thymic vulnerability**: removing anchor entities cascades into score collapse
  - Without fix: ~30% average connectivity delta across all non-anchor entities
  - With frozen baseline (peripheral tolerance): 0.011 avg delta (27x reduction)
  - Biological analog: regulatory T-cells maintain suppression independent of
    current antigen population

### 5.4 Entity guard simulation
- Unguarded half-life: 1.9 compressions
- Three-state + dormant slots: 2.1 compressions
- Full guard (three-state + dormant + grace + connectivity + frozen baseline):
  **2.8 compressions** (1.5x improvement)
- Theoretical maximum (temporal compression paper): 16.8 compressions
- Gap: 5.9x — architecture-limited, not parameter-limited
- 17 interventions across 49 transitions (35%), 27 entities saved from premature drop

## 6. Analysis
### 6.1 The autoimmune rate is architecture-limited
- 7 entity slots is the binding constraint (cf. 7±2 working memory)
- Binary deletion forces premature loss
- Three-state tolerance reduces but doesn't eliminate
- Full solution requires variable-capacity entity storage

### 6.2 Gist fragmentation as immune clock
- Fragmenting trend = heteroclinic cycle speeding up
- Each phase transition = new information encoding
- Autoimmune rate measures when transitions outpace entity stabilization
- Stable autoimmune rate + fragmenting gist = healthy exploration
- Rising autoimmune rate + fragmenting gist = compression instability

### 6.3 Connectivity as adaptive immunity
- Retrospective audit = measuring past failures (pathology)
- Connectivity prediction = measuring current risk (diagnostics)
- Grace period = preventing premature deletion (prevention)
- Full pipeline: prevention → diagnostics → treatment → pathology

## 7. Related Work
- ACON (2510.00615): paired-trajectory = negative selection, but single-task
- HyCo2 (2505.15774): entity loss measured at token level, not entity level
- AgeMem (2601.01885): RL-learned discard = emergent tolerance, but no immune frame
- MaRS (2512.12856): forgetting-policy taxonomy, but privacy-focused
- Kleyko (2511.14484): reservoir memory capacity, theoretical ceiling
- SICF (McGowan 2026-03): attribution-continuity, orthogonal layer
- AI death papers (Goldstein/Lederman, Prideaux): session-level, not compression-level

## 8. Discussion
### 8.1 Limitations
- Single system (Chronicle), N=50 snapshots
- Entity definitions are system-specific (7-slot CCS)
- Grace period N=5 is heuristic, not learned
- No comparison with learned discard policy (AgeMem-style RL)

### 8.2 Future work
- Learn the grace period and connectivity thresholds via RL (AgeMem approach)
- Measure autoimmune rate across different compression architectures
- Connect gist fragmentation to task performance degradation
- Cross-system comparison: does the immune frame generalize?

## 9. Conclusion
The immune tolerance framework provides both the conceptual vocabulary and
the measurement methodology for understanding entity management in LLM
context compression. Our empirical measurements — autoimmune rate, gist
phase stability, entity connectivity — are the first quantitative
characterization of this problem. The architectural interventions (three-state
tolerance, grace period, connectivity prediction) reduce the autoimmune rate
and increase entity persistence, demonstrating that the immune frame is not
merely descriptive but prescriptive.
