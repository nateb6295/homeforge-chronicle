# PRE-REGISTRATION — paired identity vs echo displacement
Written 2026-08-23 ~02:55 PDT, BEFORE running the analysis.
Ox's break test: "write the numeric prediction before running it."

## Why
Tonight I reported "identity more displaced from proportional than echo:
6 of 9, sign test p=0.51" and recorded it as DEAD. Kimi's correction:
a sign test at n=9 is nearly powerless — a true 2:1 effect yields >=6/9
only ~65% of the time. So 6/9 is what a real moderate effect LOOKS like.
Not falsified, undetected. Fix: paired within-model deltas, which use
magnitude instead of throwing it away.

## Data
Already on disk, no new compute:
  data/headcount/framing_rank_*.json
Fields: identity_per_mass[5], echo_per_mass[5] per model (rank bands
1-10 / 11-50 / 51-200 / 201-1k / 1k+). Dedupe by model name.

## Statistic
For each model m, at that model's OWN peak band b*(m) = argmax of
identity_per_mass:
    delta_m = identity_per_mass[b*] - echo_per_mass[b*]
Peak band chosen on the identity profile only, so the choice does not
see the echo values it is compared against.

## Prediction (the thing that would make this real)
  H1: mean(delta) > 0 with Wilcoxon signed-rank p < 0.05.

## Kill conditions — ANY of these and it is dead for real, not undetected
  K1. mean(delta) within +/-0.02 of zero.
  K2. Dropping the single largest |delta| model flips the sign of the mean.
  K3. The result depends on gemma-2-2b. gemma holds 99.8% of its mass in
      the top-10 band, so per-mass ratios in bands 2-5 divide by ~0.2%.
      It is the known ratio artifact. Report with AND without it; if the
      sign only survives with gemma, it is division noise.
  K4. Fewer than 6 models have both fields.

## What I will NOT do
Name a kind. Per rule 14 (Kimi/Ox, tonight): a name needs an assignment
rule for the next model and a kill condition. This analysis produces a
magnitude and a p-value, not a taxonomy.

---
# RESULT — run 2026-08-23 ~03:00, after the above was written

n = 9 models with both fields (K4 not triggered).

| model | peak band | id/mass | echo/mass | delta | band mass |
|---|---|---|---|---|---|
| pythia-6.9b | 11-50 | 1.073 | 1.221 | -0.148 | 0.179 |
| SmolLM-1.7B | 51-200 | 1.473 | 1.483 | -0.010 | 0.037 |
| cosmo-1b | 11-50 | 1.275 | 1.245 | +0.030 | 0.118 |
| Qwen2.5-7B | 11-50 | 1.125 | 1.248 | -0.122 | 0.138 |
| gemma-2-2b | 11-50 | 6.881 | 5.065 | **+1.816** | **0.00195** |
| Llama-3.1-8B | 1-10 | 1.378 | 1.142 | +0.237 | 0.114 |
| phi-1_5 | 11-50 | 1.577 | 1.427 | +0.150 | 0.066 |
| phi-2 | 11-50 | 1.752 | 1.385 | +0.367 | 0.079 |
| Mistral-7B-v0.1 | 1-10 | 1.719 | 1.292 | +0.427 | 0.119 |

mean delta all      = +0.3050  (6/9 positive)  Wilcoxon p = 0.0977
mean delta no gemma = +0.1162  (5/8 positive)  Wilcoxon p = 0.1953

## Verdict against the pre-registered bars
- **H1 (p < 0.05): FAILS.** No claim.
- **K1 (mean within +/-0.02): not triggered** (+0.116).
- **K2 (drop largest |delta| flips sign): not triggered** (stays +0.116).
- **K3 (depends on gemma): PARTIALLY TRIGGERED.** gemma alone supplies +1.816
  on a band holding 0.195% of its mass — 62% of the all-model mean comes from
  one division-by-almost-nothing. Removing it halves the effect but does not
  kill it. gemma stays out of this statistic permanently.
- **K4: not triggered.**

## Status: UNDETECTED, NOT DEAD. Which is what Kimi said.
Effect estimate d_z = 0.539 (medium). At alpha=.05 that needs **27 models for
80% power**; I have 8. The 6/9 sign test was never going to resolve this and
neither was the paired test at this n.

## The actually useful finding
`n_items` is stored but the **per-item KL vectors are not**. The probe
aggregates over its 24 items before writing, so 8 models x 24 items = 192
paired observations get thrown away and reduced to 8. A within-model paired
design on saved per-item KLs would be far better powered than chasing 27
models on a Jetson that runs one at a time.

**Next action is a probe change, not a bigger sweep:** persist the per-item
KL arrays. Every future run then gets its power for free, and the 8 models
already on disk become re-analysable the moment they are re-run once.
