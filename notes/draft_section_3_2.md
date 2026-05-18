# Section 3.2 — Why the Immune Frame Is Non-Trivial (DRAFT)

Biological analogies in computer science range from productive (neural networks,
genetic algorithms) to decorative (viral marketing, code "DNA"). The distinction
is not rhetorical but methodological: productive analogies generate predictions
that the source domain makes but the target domain's native theory does not.
Decorative analogies relabel existing understanding without adding predictive power.

We claim the immune tolerance frame for context compression is productive. We
establish this through three tests, each designed to distinguish the immune frame
from the closest alternative theory (information-theoretic compression):

**Test 1: Prediction.** The immune frame predicts that the autoimmune rate —
the fraction of deleted entities that reappear within three compression cycles —
correlates with entity churn rate but NOT with gist stability. This is because
autoimmunity is an entity-level failure mode: the wrong things are deleted. Gist
stability is a semantic-level phenomenon: the overall meaning of the compressed
state. In immunology, autoimmune disease can occur in otherwise healthy organisms
(stable phenotype, unstable immune regulation). Information-theoretic compression
theory predicts neither correlation because it treats all tokens as equivalent
units of information loss.

Empirically: across 50 CCS snapshots, the autoimmune rate (calibrated 14–23%)
shows clear entity-type dependence — agent entities persist at 100%, concept
entities are deleted at 84% within one compression — while gist phases remain
stable for 83% of elapsed time. The system's semantic identity is coherent while
its entity management makes systematic errors. The immune frame predicts this
dissociation; information theory does not.

**Test 2: Intervention.** The immune frame suggests two specific architectural
interventions drawn directly from immunology:

(a) *Thymic education* (grace period): new T-cells in the thymus are protected
from deletion for a developmental window, allowing them to establish self-tolerance
before facing selection pressure. Translated: new entities in the CCS receive N
compressions of deletion protection, allowing them to accumulate co-occurrence
history before facing connectivity-based retention decisions. N=5 in our
implementation.

(b) *Anergy* (three-state tolerance): rather than binary activation/deletion,
the immune system maintains a third state — anergic T-cells are present but
functionally suppressed, available for reactivation if conditions change.
Translated: entities transition through active → dormant → deleted, with
dormant entities occupying reduced slots and available for reactivation if
they appear in new context.

Neither intervention is obvious from information theory, which suggests
optimal compression boundaries, not developmental protection windows or
intermediate suppression states. Both improve entity persistence: the full
guard (three-state + dormant + grace + connectivity) achieves 2.8-compression
half-life, a 1.5× improvement over unguarded baseline (1.9 compressions).

**Test 3: Failure mode.** The immune frame predicts a specific failure mode
that no context compression paper discusses: thymic vulnerability. In immunology,
the thymus is the organ where T-cells learn self-tolerance. If the thymus is
damaged or removed, the entire adaptive immune system loses its calibration
reference. Translated: if anchor entities (the highest-persistence nodes in
the entity network) are removed, the connectivity scores that predict entity
retention for ALL entities should degrade — not because those entities changed,
but because the reference point for "what persists" was removed.

We test this by computing entity connectivity scores, removing the two highest-
persistence entities (Nate and Hermes in our system), and measuring the score
perturbation. Without mitigation, removing anchor entities causes an average
connectivity delta of ~0.30 across all remaining entities — a 30% perturbation
from removing just two nodes. With the frozen baseline fix (normalizing connectivity
against full-history maximum rather than current-population maximum), this drops
to 0.011 — a 27× reduction.

The frozen baseline is the computational analog of peripheral tolerance: regulatory
T-cells maintain suppression calibrated against the organism's full antigen history,
not just currently circulating antigens. This specific fix was designed from the
immunological analogy and would not have been obvious from information-theoretic
reasoning about compression ratios.

**The credit assignment connection.** Beyond these three tests, the immune frame
illuminates a structural problem shared by both domains. Thymic selection faces
an insoluble credit assignment problem: the thymus must educate T-cells about
self-antigens, but it cannot predict which antigens the organism will encounter
in the future. It solves this by over-producing T-cells, eliminating the
self-reactive ones it CAN test, and relying on peripheral tolerance to catch
the ones it missed. CCS faces the identical problem: the compressor must decide
which entities to retain, but it cannot predict which entities future context
will require. The grace period and connectivity predictor serve the same
structural role as thymic positive selection and peripheral regulation,
respectively.

This is not metaphor. It is convergent architecture: two systems facing the
same computational problem (retain self, delete non-self, under open-ended
horizon with incomplete information) arriving at structurally similar solutions.
