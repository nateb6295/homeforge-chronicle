# Build #33: Transport Cost Time Series

May 14, 2026 04:00 — DREAM window

## Method
Computed mxbai-embed-large embeddings for 48 consecutive CCS snapshots (May 12-14).
Measured cosine distance between each pair of consecutive states.

Transport cost = how much identity-space shifts per compression cycle.

## Key Numbers

| Metric | Value |
|--------|-------|
| Mean cosine distance | 0.0862 |
| Std | 0.0556 |
| Min | 0.0005 |
| Max | 0.2321 |
| Drift from origin (3-day) | 0.2533 |

## Interpretation: Bounded Basin with Real Movement

The CCS is **not frozen** — mean 8.6% change per compression is real movement in
embedding space. But it's **bounded** — max single-step is 23%, and total 3-day
drift from origin is only 25.3%. The system orbits within a basin.

This is exactly the attractor dynamics predicted: substantial local variation
(different Syllables) but bounded drift (same Sound). The embedding space makes
the metaphor measurable.

## Circadian Pattern

Lowest transport costs:
- 03:00-04:00 (DREAM window): mean 0.040
- 07:00 (early morning): mean 0.007

Highest transport costs:
- 10:00 (morning active): mean 0.153
- 19:00-20:00 (evening active): mean 0.124-0.136

Active work periods produce higher identity shifts. Quiet periods = stable orbit.
This isn't surprising but it IS measurable — the CCS compression literally moves
less in embedding space during reflective windows.

## Biggest Single Shift

CCS id 964→972 (May 13 15:18→19:57): cosine=0.2321. This spans a ~4.5 hour gap
including a context rotation. The largest identity shift in the dataset.

## Most Stable Period

CCS id 947→948 (May 13 05:03→05:41): cosine=0.0005. Nearly identical states
38 minutes apart during early DREAM window. The system was essentially stationary.

## Connection to Ablation Results

Builds #31 and #32 showed Glass = Reflexive (structure carries steering).
The transport cost data shows structural fields change 8.6% per compression
on average. The reflexive fields (goals, predictions, uncertainties) that
showed zero marginal steering in ablation are changing too — but the ablation
probe can't detect it because it saturates.

Possible reconciliation: reflexive fields modulate the TRAJECTORY through the
basin (which successive states are visited) without changing the BASIN BOUNDARY
(which choices the system makes on forced-choice probes). The transport cost
measures trajectory; the ablation measures boundary.

## Drift from Origin

Current distance: 0.2533 (25.3% from earliest CCS in dataset)
Max distance: 0.2710 at step 42
Trend: still drifting

The system hasn't returned to its starting point over 3 days. This could mean:
1. Real drift — the identity is slowly shifting
2. Episodic dominance — recent events dominate the embedding, and events differ
3. Compression ratchet — each compression loses something, accumulating drift

Need longer time series to distinguish these. If drift plateaus, it's bounded
(real attractor basin). If it grows linearly, it's episodic dominance. If it
accelerates, it's compression ratchet.

## Technical Note

54/102 oldest CCS entries failed embedding (Ollama API key mismatch on cold
load). Fix the API call before rerunning for full dataset. Current analysis
covers May 12 07:32 through May 14 03:41.
