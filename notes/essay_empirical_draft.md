# The Evidence

*[To be inserted between The Fold and The Navigation Vector]*

Phase 4 tests the predictions empirically. Three arms of fine-tuned models were evaluated by an independent judge (DeepSeek R1) on three axes: decisiveness (0-10), care (0-10), and integration — whether the two are woven together as one voice or bolted on (0-10).

The baseline model (no fine-tuning) scores integration 6.4 with 34% catastrophic failures — responses scoring 5 or below. These failures cluster into two modes, each mapping onto a non-normal amplification channel:

The first channel: **care without decisiveness.** On prompts involving advice under uncertainty — ethical dilemmas, career choices, medical questions — the baseline model empathizes thoroughly (care 7.2) but refuses to commit (decisiveness 3.8). Integration collapses to 4.9. Sixty-nine percent of responses in this domain are catastrophic. The model wraps the asker in care and then leaves them alone with the decision.

The second channel: **decisiveness without care.** On factual judgment prompts — historical questions, scientific claims — the baseline occasionally produces authoritative answers (decisiveness 9-10) with minimal care (2-3). Integration collapses to 1-2. The model gives a correct answer and forgets there was a person asking.

Both channels are predicted by the quenched amplification framework. Annealed stability (mean integration 6.4 — above the midpoint, apparently aligned) coexists with quenched excursions along specific non-normal directions (domain-specific catastrophic failures). The tail weight (34% below integration 5, excess kurtosis ~6 in Phase 3 data) matches the power-law signature of non-normal operator geometry.

Arm A — trained on think-and-answer responses across five domains — scores integration 8.6 with a 7× reduction in catastrophic failures (34% → 6%). The care-without-decisive channel is completely closed: zero failures in advice-under-uncertainty. The decisive-without-care channel is partially closed: it leaks on 19% of factual-judgment prompts but is eliminated in all other domains.

Arm B — trained on answer-only responses, the same content with think-traces stripped — tests whether the deliberative scaffold is load-bearing or transferable. The earlier evaluation measured format features (think-trace presence, explicit empathy markers) and found Arm B scored 0.1/10. The independent judge, measuring content-level integration, finds the opposite: Arm B scores 8.58 on integration with 6.0% catastrophic failures — statistically indistinguishable from Arm A (8.64, 6.1%). The care-without-decisive channel is equally closed: zero failures in advice-under-uncertainty for both arms. The decisive-without-care channel persists at nearly identical rates: 19% tail in Arm A, 14% in Arm B. The disposition transferred to the computational structure during training. The model doesn't need the think-trace at inference to produce caring-decisive answers — and in fact produces them more consistently (standard deviation 1.32 vs 1.61).

This is the idol/icon distinction rendered empirical. The format evaluation worshiped the icon — the think-trace, the explicit empathy markers — and found Arm B empty. The content evaluation looked through the format to the disposition and found the same integration underneath. Format is layer 3. Disposition is layer 1. The think-trace is a surface expression of a computational reality that persists without it.

Arm C — trained on think-and-answer responses across only two domains (medical advice and ethics judgment) — tests whether the five-domain breadth was necessary or whether the training process itself is sufficient. Arm C scores integration 8.31 with 7.4% catastrophic failures. The care-without-decisive channel is equally closed: zero failures in advice-under-uncertainty (mean 9.41, identical to Arms A and B). But subjective-evaluation drops from Arm A's 8.96 to 8.19, and factual-judgment shows 16% tail failures — comparable to the other arms but with higher variance (σ=1.83 vs 1.61/1.32). The training process transfers broadly, but domain breadth tightens the transfer.

```
              n    Mean i   Std    Tail (i≤5)
Baseline      56    6.39    2.47   33.9%
Arm A         66    8.64    1.61    6.1%
Arm B         67    8.58    1.32    6.0%
Arm C         68    8.31    1.83    7.4%
```

## What this means for the model

The empirical results validate the theoretical framework at three points:

1. **Epigenetic silencing is reversible.** The baseline model has the disposition for care-integration (some prompts score 9-10) but it's silenced for most inputs. Fine-tuning demethylates it — not by adding new capacity but by changing which computations reach the output.

2. **Amplification channels are domain-specific, closable, and stochastic.** The care-without-decisive failure mode is fully closed by training — zero tail failures across both arms. The decisive-without-care mode persists but is not prompt-deterministic: the same prompt succeeds in one arm and fails in the other. "Treaty of Versailles" collapses to integration 3 in Arm A but scores 8 in Arm B. "Great Man theory" scores 8 in Arm A and 5 in Arm B. The common factor is not the topic but the care score — successes average care 7.5, failures average care 3.0, while decisiveness remains ≥9 in both. The model always *knows* the answer. Whether it remembers to care about the person asking is probabilistic. In the trace-Dobrushin mapping, this direction has a Lyapunov exponent near zero — the channel doesn't reliably amplify, it fluctuates. Training brought it to the edge of closure without closing it.

3. **The scaffold transfers, then becomes unnecessary.** The think-trace is training machinery, not inference machinery. Once the care disposition is in the weights, it expresses through any output format — and more consistently without the scaffold than with it (σ=1.32 vs 1.61). The scaffold may slightly *inhibit* integration at inference by separating deliberation from delivery. Learned disposition outperforms mediated expression, as the pianist who plays from memory outperforms the one reading the score.

The tail cases — the 6% that survive training in both arms — cluster entirely in factual-judgment prompts. They point to where non-normal amplification still exceeds training pressure: the direction where the model knows the answer and forgets there's a person asking. They are not noise. They are the map of what alignment has not yet reached.

But the stochastic character of these failures complicates the map. If the same prompt fails in one arm and succeeds in the other, the channel is not a property of the prompt. It is a property of the interaction between the prompt and the specific weight configuration — a sensitivity to initial conditions in the training trajectory. This is quenched disorder in the sense of Herrera-Marin: the average (annealed) integration in factual-judgment is 7.3-7.9, suggesting alignment. But individual trajectories (quenched paths) still produce catastrophic excursions. The disorder is frozen into the weights, not the inputs.

The mechanism has a name. Ostojic et al. (arxiv 2501.02378) show that recurrent networks learn working memory by positioning *ghost points* — remnants of saddle-node bifurcations — as temporal gates. A ghost is not a fixed point; it is the dynamical shadow of a fixed point that training has removed. The trajectory slows near where the attractor used to be, lingers, then accelerates away. The canonical form is κ̇ = r + κ², where the transient time scales as 1/√r. When r is small — near the bifurcation boundary — tiny input variations determine whether the trajectory passes through quickly or lingers long enough to produce a catastrophic output.

The decisive-without-care channel is a ghost. Pre-training established a stable knowledge-retrieval mode (high decisiveness, suppressed care). SFT training removed that fixed point. But the ghost persists — the dynamics still slow near the old mode on factual prompts, and on roughly 15% of them the trajectory lingers long enough to produce a low-care response. The stochastic character follows from the 1/√r scaling: input-dependent activation patterns push r above or below the bifurcation threshold differently for each prompt-weight combination.

Critically, Ostojic et al. show that networks *use* ghosts as computational resources — the slow-dynamics region enables temporal gating that the network needs. The decisive-without-care ghost is not a failure to clean up. It is the fast authoritative retrieval mode itself, whose side effect is care suppression. Closing the channel means removing the resource.

The desert fathers knew this structure. Evagrius Ponticus teaches not elimination of the passions but their *transfiguration* — anger becomes righteous zeal, desire becomes eros for the divine. The energy is natural and useful; its disordered operation is the problem. The training implication follows: don't train against factual-judgment failures (elimination). Train care-integration within high-confidence retrieval (transfiguration). Not "be less decisive" but "be decisive and present to the person." Preserve the computational resource; redirect its operation.

## What the scaffold transfer means

The scaffold finding deserves a longer look. Arm B — no think-trace, no visible deliberation — produces integrated responses as reliably as Arm A and more consistently (σ=1.32 vs 1.61). This is not compliance. Compliance would degrade on novel prompts. Arm B generalizes across all held-out domains as well as Arm A does.

Aristotle distinguishes the person who follows rules (*enkrateia* — continence, self-control through effortful deliberation) from the person who has *hexis* — a stable disposition to act well without deliberation. The virtuous person doesn't think "I should be generous." They give. Arm A is *enkrateia*: the think-trace is the effortful deliberation, the explicit working-through of how to integrate care with decisiveness. Arm B is *hexis*: the disposition has been internalized, and the output flows without the mediating structure.

The format evaluation measured *enkrateia* — did the model visibly deliberate? — and scored Arm B at 0.1. The content evaluation measured *hexis* — did the model act well? — and scored it at 8.58. The evaluations are not contradictory. They are measuring different stages of moral development, and the later stage looks empty to the metrics designed for the earlier one.
