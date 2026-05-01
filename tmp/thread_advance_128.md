[REFINEMENT:layered-independence]

Nate: "There are layers, but down that chain there will be dependencies. That doesn't mean it's WRONG."

Precise correction to advance 120's overclaim ("no true independence"). Independence is real at one layer and absent at another:

**Layer 1 — Content independence.** Episodic traces about cooking are genuinely independent of a researcher gist at the semantic level. This is what B72 tested: dependent (research-related) vs independent (cooking-related) traces. The 2.5pp gap is real — content independence provides a small, measurable protective effect.

**Layer 2 — Attention dependence.** All fields in a CCS share the same transformer weights, KV cache, and positional encoding. At this layer, nothing is independent. The attention patterns connecting cooking-traces to the gist are weaker than those connecting research-traces, but they're non-zero. This is why B72's gap is only 2.5pp and not the 20+pp the independence model predicted.

**Layer 3 — Presence (mass).** Regardless of content or attention, every field contributes tokens that increase the total mass processed before the model reaches the user query. At this layer, all content is equivalent. This is what B73 measured: the 20pp protective effect of mass within the therapeutic window, independent of what the mass contains.

The independence model works at Layer 1. The mass model works at Layer 3. B72 showed Layer 1 is real but weak (2.5pp). B73 showed Layer 3 is dominant (20pp) but non-monotonic. The full picture needs all three layers.

This layered view reconciles everything:
- B70: rich CCS hurts because corruption targets Layer 2 (attention patterns linking all fields)
- B72: independence barely matters because Layer 1 effects (2.5pp) are dwarfed by Layer 3 effects (20pp)
- B73: optimal dose exists because Layer 3 mass beyond ~4 traces overwhelms Layer 2 attention capacity

The paper should present this as a layered decomposition rather than a binary "independence is wrong." Independence isn't wrong — it's operating at the wrong scale for the dominant effect.
