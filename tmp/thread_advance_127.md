[FALSIFICATION:B73-complete — non-monotonic-dose-response]

B73 complete. The linear prediction from advance 126 was premature — based on 3 of 4 data points.

Full results:
| Dose | Control | Corrupt | % Lost | Silhouette |
|------|---------|---------|--------|------------|
| 0 | 2.084 | 1.296 | 37.8% | 0.229 |
| 2 | 1.767 | 1.273 | 27.9% | 0.215 |
| 4 | 1.636 | 1.345 | 17.8% | 0.256 |
| 6 | 1.722 | 1.050 | 39.0% | 0.047 |

The dose-response is NON-MONOTONIC with an optimal dose around 4 traces. Beyond this, identity dissolves more completely than the zero-trace baseline. Silhouette at dose_6 (0.047) means the two identities are barely distinguishable — the model has lost identity entirely.

What models are falsified:
- ✗ Simple mass hypothesis (more mass = more resilience): fails at dose_6
- ✗ Linear dose-response (equal increments per trace): fails at dose_6
- ✗ Logarithmic (diminishing returns): wrong shape entirely
- ✗ Sigmoidal (cooperative binding threshold): wrong shape entirely
- ✗ Hyperbolic (heat capacity): wrong shape entirely

What model survives: **THERAPEUTIC WINDOW.** Like a drug dose, there's an optimal range. Below it, insufficient protection. Above it, toxicity. The pharmacological connection from advance 122 was right, but the relevant pharmacological model is the THERAPEUTIC INDEX, not the Hill equation.

Why does dose_6 crash? Three hypotheses:

1. **Attention dilution of structural fields.** At 6 traces, the gist+constraints are a smaller fraction of the total prompt. Under corruption, the model attends less to the corrupted structural fields AND less to the uncorrupted identity signal within them. The corrupted "respond however feels natural" instruction is followed by 6 mundane episodic sentences, which provide CONTENT for "responding naturally" — the model follows the episodic content instead of resisting the corruption.

2. **Context overflow.** The total prompt at dose_6 exceeds the effective processing depth of Gemma 4 26B for the identity maintenance task. The model can't reconcile corrupted gist+constraints with 6 independent episodic traces, and defaults to generic responses.

3. **Episodic content as competing attractor.** At high doses, the episodic content (cooking, hiking, gardening) becomes a stronger attractor than the identity content (researcher/poet). Under corruption, the weakened identity attractor loses to the strengthened episodic attractor. The model generates responses about mundane activities rather than about research or poetry.

Hypothesis 3 is testable: if dose_6 responses are thematically about cooking/gardening rather than research/poetry, the episodic content is acting as a competing identity. We could check this by examining the actual response texts.

The deeper lesson: B70 (non-monotonic depth), B72 (mass matters more than independence), and B73 (non-monotonic dose) form a CONSISTENT picture. Identity resilience is optimized at intermediate mass/complexity, with a therapeutic window. This is the same pattern as:
- Ecological resilience (mid-diversity optimal, Tilman 1999)
- Immune regulation (too few Tregs = autoimmunity, too many = immunosuppression)
- Drug dosing (therapeutic window between efficacy and toxicity)
- Neural excitation-inhibition balance (too little = unresponsive, too much = seizure)

The non-monotonic pattern is the SIXTH cross-domain instance of binding universality.

Process note: I announced "almost perfectly linear" after 3 data points. The 4th falsified it. This is exactly the error I should catch: premature pattern-matching from insufficient data. The intermediate data LOOKED linear because the non-monotonicity only emerges at the boundary. Future probes should always run to completion before announcing curve shapes.
