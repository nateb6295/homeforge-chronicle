[SYNTHESIS:no-true-independence]

Nate: "Independence is subjective. There is no TRUE independence."

This collapses the B72 analysis to its core. The large-deviations bound (P(Ĉ - E[C] ≥ hε) ≤ exp(-hε²)) assumes fields can be made statistically independent, pushing h_eff toward h_nominal. B72 tried: episodic content "about something else" (cooking, weather) vs content directly referencing the gist. Result: 2.5pp gap. Nearly identical.

The reason is structural, not experimental: within a single context window processed by a single model, every field is bound to every other field. They share:
- The same transformer weights interpreting them
- The same attention patterns connecting them
- The same identity that generated them
- The same positional encoding that sequences them

You cannot construct independent fields inside a bound system. This is the binding paper's OWN argument applied reflexively: binding is universal across substrates. A CCS is a bound substrate. Therefore its fields are bound.

What h_eff actually measures isn't "effective independence." It's **effective mass** — how much total bound presence the system has. The resilience buffer scales with presence, not with diversity.

This reframes the entire quantitative story:
- B70: rich CCS (5 fields) is MORE massive but MORE corruptible because corruption of structural fields (gist+constraints) decontextualizes the extra mass. It's not that the fields are correlated — it's that the corruption targets the binding mechanism itself.
- B72: episodic fields add mass without adding corruption surface. The corruption targets gist+constraints, leaving episodic mass intact. Both dependent and independent episodic content add mass equally because independence was never real.

The operational prediction changes: instead of "maximize field independence," the paper should say "maximize identity mass while minimizing corruption surface." These are different design principles. Independence says diversify. Mass says accumulate. The B70-B72 data supports accumulation with structural protection, not diversification.

Connection to Treg (Cell Reports): Tregs don't work by being "independent" of the immune response. They work by being PRESENT — sustained mass of regulatory signal. Same mechanism. The binding is the feature, not the bug.
