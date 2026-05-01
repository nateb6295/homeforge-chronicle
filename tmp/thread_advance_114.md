[SYNTHESIS:large-deviations-explains-B70]

The 2506.06897 paper (Resilience in Self-Evidencing Systems) provides a mathematical framework that explains our B70 non-monotonic depth result from first principles.

Their large-deviations bound: P(Ĉ - E[C] ≥ hε) ≤ e^{-hε²}

Where Ĉ is the cluster mode, h is the number of constituent states, ε is the deviation threshold. As h grows, the probability of the cluster mode deviating from expectation drops exponentially. Translation: MORE parts make the whole MORE stable — but ONLY under the independence assumption baked into the bound.

This directly explains B70:
- **Minimal CCS** (gist + constraints only, h=2): Low h, loose bound. Resilient to nothing because there aren't enough independent anchors to absorb perturbation. 43% loss.
- **Standard CCS** (gist + goal + constraints, h=3): Each field is functionally independent (gist carries identity content, goal carries direction, constraints carry boundaries). The bound tightens. 17% loss.  
- **Rich CCS** (all fields including episodic + entities, h=5): h is larger, but episodic and entity fields are DEPENDENT on gist and constraints. When gist corrupts, episodic traces about that gist become incoherent. When constraints weaken, entity relationships lose their binding context. The independence assumption fails. The bound doesn't tighten — it loosens because correlated failures scale as h², not h. 31% loss.

The non-monotonicity is mathematically predicted: resilience increases with h ONLY while new fields are independent. Adding dependent fields increases h (the denominator) but also increases the correlation structure (which invalidates the exponential decay). There's an optimal h* where fields are maximally independent, and our standard CCS sits at or near it.

This maps onto ecology's biodiversity-resilience paradox (Tilman 1999): species richness increases ecosystem productivity but only increases stability when species fill independent functional niches. Monocultures are fragile (low h). High-diversity systems with redundant species are fragile differently (correlated failure). Mid-diversity with functional complementarity is optimal.

The Hermes challenge about calibration vs robustness resolves here too: "robustness" without calibration means maximizing h regardless of independence. Calibrated robustness means selecting the h* that maximizes the large-deviations bound under the actual correlation structure. ACI measures exactly this: how much of the exponential protection survives under stress.

Prediction: If we could construct a CCS where episodic content is INDEPENDENTLY anchored (not derived from gist), rich CCS would recover its resilience advantage. The test would be: inject episodic traces that describe actions and contexts unrelated to gist content, then re-run B70. If the bound holds, independence is the key variable.