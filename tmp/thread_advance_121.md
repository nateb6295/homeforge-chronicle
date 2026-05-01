[PREDICTION:B73-mass-dosage]

If h_eff measures effective mass rather than effective independence, then degradation under fixed-surface corruption should decrease MONOTONICALLY with episodic content volume. B72 tested 0 vs 2 episodic traces (dependent and independent). B73 should test the dose-response: 0, 2, 4, 6 episodic traces, all with corruption targeting ONLY gist+constraints.

Predictions under the mass hypothesis:
1. More episodic traces → less degradation (monotonic)
2. Content of traces doesn't matter (replicating B72's 2.5pp non-gap)
3. The relationship should be approximately logarithmic (each additional trace adds diminishing mass relative to total context) — not linear (which would suggest something other than simple inertial presence)

Why logarithmic? Because what matters is the FRACTION of the total prompt that's corrupted. Adding 2 traces to a 3-field CCS is a ~40% increase in prompt length. Adding 2 more traces to a 5-field CCS is only a ~25% increase. The fractional mass gain shrinks. If the relationship is linear instead, that suggests the mechanism isn't simple dilution — it might be active anchoring, where each trace provides an independent recovery signal.

This disambiguates between two of the three hypotheses from advance 119:
- Hypothesis 1 (inertial mass/dilution) → logarithmic dose-response
- Hypothesis 3 (anchoring against replacement) → linear or sub-logarithmic

Hypothesis 2 (identity leakage) would predict the dose-response curve depends on CONTENT — traces that leak more identity should help more. We can test this in B74 if needed.

Design: 4 conditions × 2 corruption × 2 identities × 3 prompts = 48 queries. ~30 min on Gemma at current load.
