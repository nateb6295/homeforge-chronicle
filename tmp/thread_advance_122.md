[CROSS-FIELD:pharmacological-dose-response]

While B73 runs, mapping the mass dosage probe to pharmacological dose-response theory:

In pharmacology, the relationship between drug dose and effect follows the Hill equation:
  E = Emax × D^n / (ED50^n + D^n)

Where n is the Hill coefficient — a measure of COOPERATIVITY:
- n = 1: simple binding (one molecule, one receptor). Each additional dose unit adds proportional effect.
- n > 1: POSITIVE cooperativity. Initial doses do little; at a critical mass, the effect jumps. Hemoglobin is the classic example (n ≈ 2.8): the first O₂ molecule binds weakly, but each subsequent one binds more easily because binding changes the protein's shape to favor more binding.
- n < 1: NEGATIVE cooperativity. Early doses are potent; later doses add diminishing effect. Receptor desensitization.

B73's dose-response curve tells us which regime CCS operates in:

1. **n < 1 (logarithmic/diminishing returns)**: Each episodic trace helps, but less than the last. This is the DILUTION hypothesis — the corrupted fields are a fixed mass, and each trace dilutes them. The first traces have the biggest fractional impact.

2. **n ≈ 1 (linear)**: Each trace adds equal protection. This is the ANCHORING hypothesis — each trace provides an independent recovery signal, and signals add linearly.

3. **n > 1 (sigmoidal/cooperative)**: Traces don't help much until a critical mass is reached, then protection jumps. This would connect to B61/B69's phase-transition finding — there's a THRESHOLD at which identity mass becomes self-sustaining against corruption. Below threshold, mass doesn't help (the corruption dominates). Above threshold, the accumulated mass creates cooperative stability.

The cooperative case (n > 1) is the most theoretically significant because it connects identity mass to the binding universality finding: the same cooperative binding that makes hemoglobin efficient at oxygen transport could make CCS efficient at identity maintenance. In both cases, the substrate (protein/context window) changes its effective shape as it accumulates more bound content, making further binding more favorable.

The anti-cooperative case (n < 1) is the most practically significant because it means a few episodic traces is enough — the first 2-3 capture most of the protection. Beyond that, you hit diminishing returns and the extra tokens cost inference time without proportional benefit.

B73 tests: 0, 2, 4, 6 traces. With 4 points we can estimate n and classify the regime.
