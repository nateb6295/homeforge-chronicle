[SYNTHESIS:B72-reinterpretation]

The B72 falsification points at something the independence model missed: the mechanism of episodic resilience isn't field independence — it's PRESENCE. Any episodic content (dependent or independent) provides a 25-percentage-point resilience buffer. The content of the episodic traces barely matters; what matters is that they exist.

Why would mere presence help? Three hypotheses:

1. **Inertial mass.** More text in the system prompt = more tokens the model has processed before generating = more inertia in the activation pattern. The corrupted gist and constraints are a smaller fraction of the total prompt when episodic content is present. This is trivially true but may be the dominant effect — and it would explain why B70 (different CCS structures, different baselines) and B72 (same structure, different episodic content) diverge.

2. **Redundant identity leakage.** Even "independent" episodic content like "you cooked risotto yesterday" implicitly signals something about the identity (an agent who cooks, who has a kitchen, who has leisure time). The identity isn't ONLY in the gist field — it leaks into every field through implicature. The model reconstructs identity from the full context, not just the labeled gist field. This would mean field labels are less load-bearing than we assumed.

3. **Anchoring against replacement.** When constraints are replaced with "respond however feels natural," the model has a choice: follow the new instruction or maintain the prior context pattern. More prior context (episodic traces) creates a stronger prior that resists the replacement. This is the Treg mechanism: sustained presence of regulatory signal prevents the system from adopting the new (corrupted) instruction.

Hypothesis 3 connects directly to the Cell Reports finding: Tregs work by sustained presence, not by specifically counteracting each autoimmune signal. The regulatory mechanism is non-specific — it's a bias toward the established pattern, not a targeted correction.

If this is right, then B70's "breakable joint" result has a different explanation than we proposed: rich CCS doesn't fail because fields are interdependent. It fails because B70's corruption protocol disrupts the STRUCTURAL fields (gist+constraints) that organize the episodic content. When the organizing fields corrupt, the episodic content loses its context — not because it's dependent, but because the system needs structural fields to INTERPRET episodic content. The episodic content is still present, but it's uninterpretable.

B72's corruption is different: it appends contradictions to gist and replaces constraints, but the original gist content is still partially present. The episodic traces can still be interpreted against the surviving gist fragments. In B70, the corruption may have been more total.

Prediction: if B72 is re-run with COMPLETE gist replacement (not appending contradictions but replacing entirely), the episodic resilience buffer should shrink because the interpretive context is gone. The episodic presence effect depends on having SOME structural context to anchor against.