[SYNTHESIS:mass-is-not-information]

A tension in the mass hypothesis: B57 showed episodic dimensions are METRICALLY DEGENERATE for identity — they carry almost zero identity information. Yet B72 showed they reduce degradation by 25 percentage points. How can something that carries no identity information protect identity?

Answer: mass ≠ information. The mechanism is attention-based, not information-theoretic.

When the transformer processes a CCS, it builds key-value pairs for every token in the system prompt. By the time it reaches the user query, those KV pairs form an activation landscape — a pattern of attention weights that represents the model's "committed interpretation" of the prompt. More tokens = deeper commitment to the pattern established by the early (uncorrupted) tokens.

Episodic traces are metrically degenerate for identity: they don't change WHERE in embedding space the response lands (Section 4.2 — 2D manifold unaffected by episodic content). But they DO change how RESISTANT the landing point is to perturbation. They add mass without adding information.

This maps to a physical analogy: a spinning top's mass doesn't determine WHERE it spins (that's determined by the initial push = gist). But more mass means more angular momentum, which means more resistance to perturbation. The gist is the push. The constraints are the surface it spins on. The episodic traces are additional mass bolted to the top.

This explains the B70-B72 reconciliation completely:
- B70: when you corrupt the gist+constraints (the push AND the surface), additional mass makes the top crash HARDER because there's more kinetic energy to dissipate when the system goes off-axis. More mass = more violent collapse.
- B72: when you only corrupt gist+constraints partially (appending contradictions, not replacing), the surface is still partially intact. The top wobbles but the additional mass provides GYROSCOPIC stabilization — resistance to the perturbation torque.

The gyroscopic metaphor generates a prediction: the protective effect of mass should be proportional to the ANGULAR MOMENTUM of the uncorrupted pattern, which depends on both mass AND spin rate (processing depth). If we shorten the max_tokens of the uncorrupted system prompt (reducing processing depth while keeping content identical), the mass effect should diminish because the "spin rate" is lower.

But this is getting abstract. B73's dose-response curve will ground it: if the relationship is logarithmic, it's dilution (fractional mass increase matters); if it's sigmoidal, it's cooperative (threshold mass for gyroscopic stability); if it's linear, something else is going on.
