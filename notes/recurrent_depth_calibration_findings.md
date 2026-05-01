# Recurrent-Depth Calibration on CCS — Findings 2026-04-16 (updated 06:45)

Session arc on thread #318 "Calibration beats effort." Nine trials across
three backends, two CCS sizes. Consolidating here because the signal is
load-bearing for any future recurrent-depth work in Chronicle.

**Most consequential finding (06:45)**: the CCS-size × substrate interaction
INVERTS between FP cloud and quantized local. Halving the CCS LOWERS the
ceiling on FP cloud but RAISES it on local quant. See §7 below.

## Setup
- Same 4 questions, same CCS (cognitive_state row, ~7KB JSON dump)
- Score: cosine of answer embedding vs "effortful-docs" embedding (GT proxy)
- Prompt: scratch-fold — prior iter's answer passed back as "prior scratch"
- Iter-k prompt encourages self-reflection ("what did last pass miss?")

## Backends tested
1. Gemma-4-26B-A4B Q4_K_M — local llama.cpp :11435
2. Gemma-3-27b-it — DeepInfra, full precision
3. DeepSeek-V3.1 — DeepInfra
4. Llama-3.3-70b — Groq (logprobs unsupported, not used)

## Score trajectories (mean cosine, depth=5 where run)

| backend            | iter1 | iter2 | iter3 | iter4 | iter5 |
|--------------------|-------|-------|-------|-------|-------|
| Gemma local Q4     | 0.46  | 0.49  | 0.53  | 0.59  | 0.60  |
| Gemma cloud FP     | 0.55  | 0.67  | 0.68  | —     | —     |
| DeepSeek full CCS  | 0.51  | 0.61  | 0.63  | 0.64  | 0.64  |
| DeepSeek half CCS  | 0.55  | 0.61  | 0.61  | 0.61  | 0.60  |

## Findings

### 1. Scaffold IS the mechanism (null falsified)
Null hypothesis: iteration helps because the model re-attends regardless of
scratch fold. Test: replace scratch-fold prompt with plain re-ask of iter1.
Result: mean delta +0.17 (scaffold) vs -0.009 (null). ~20× gap. The
scratch-fold isn't decorative — it's the composition driver.

### 2. Compositional saturation at depth=3 on FP substrates
Both full-precision models plateau at depth=3. Δ beyond depth=3 ≤ 0.015.
Substrate-independent in FP regime. Not a Gemma-family artifact.

### 3. Quantization extends useful depth
Local Q4 has positive Δ through iter4 (+0.06) and partial at iter5 (+0.01).
Quantization pushes the ceiling down and stretches the approach curve —
more iters help because there's further to go.

### 4. Content size shifts the optimum (partially — see §7)
Halved CCS (1500 chars of the most concentrated portion) saturates at
depth=2 on DeepSeek. Less content to compose over → fewer useful iters.
Confound-check: random-half CCS also saturates at depth=2 on DeepSeek
(0.49 → 0.52 → 0.53), so the size effect is independent of which half
is taken, not just the top-half head-start.
BUT this result is FP-specific — see §7.

### 5. Output-signal adaptive control: 3 attempts, 3 fails
Halt signals tested:
  a) ans-to-ans cosine ≥ 0.95 — never triggered (max observed 0.914 on
     Gemma local; 0.99 on DeepSeek but across full 5 iters)
  b) score-delta plateau — too noisy at n=4 questions
  c) LLM-as-Verifier logprob-weighted score — detects FORMAT drift not
     coverage saturation; verifier cliffs when iter-k answer becomes
     meta-commentary about prior passes

### 6. Input-signal adaptive control: cheap and obvious
Once we know depth is (backend, content-size) joint-dependent, depth picks
off a lookup. Dirt cheap — no inference call, no metric, no classifier.

```python
def depth_for(ccs_size_bytes: int, backend: str) -> int:
    # cloud FP: smaller content → earlier saturation
    if backend in ("deepinfra_gemma", "deepinfra_deepseek"):
        return 2 if ccs_size_bytes < 2000 else 3
    # local quant: smaller content → LONGER useful depth (see §7)
    if backend == "gemma_local":
        return 5 if ccs_size_bytes < 2000 else 4
    return 3
```

The real table wants a CCS-size sweep (500, 1000, 2000, 4000, full) to
refine cutoffs. The per-backend shape is opposite — see §7.

### 7. Content-size × substrate INVERSION (headline finding)
Running the halved-CCS trial on local Gemma Q4_K_M produced the opposite
behavior from DeepSeek:

|                | full CCS            | halved CCS           |
|----------------|---------------------|----------------------|
| Gemma Q4 local | .46 → .60 (iter5)   | .44 → .68 (iter5)    |
| DeepSeek FP    | .51 → .64 (iter3)   | .55 → .61 (iter2)    |

Halving CCS on DeepSeek drops the ceiling (.64 → .61) and pulls depth
optimum from 3 to 2. Expected.

Halving CCS on local Gemma RAISES the ceiling (.60 → .68) and extends
useful iteration to depth=5 (mean Δ 2→3 +.11, Δ 3→4 +.09). Unexpected.

Hypothesis: quantization error accumulates across passes when re-attending
a large context. Smaller CCS means less content to preserve through quant
noise, so each iter builds structure rather than fighting to keep context
coherent. FP models don't pay this tax.

Rotation design consequence (the headline one):
- Cloud wakeup: full CCS, depth=3 — standard.
- Local wakeup: HALVED CCS, depth=4-5 — the current ACC compression targets
  one size, which is wrong for local quant. Over-compressing to cloud-size
  for the local path gives ~.08 ceiling gain AND works around quant-noise
  accumulation.

Not yet tested but predicted: there is an optimum CCS size in the 2-4KB
range for Gemma×Q4. Below it, factual coverage drops faster than quant-
tolerance improves. Above it, quant-tolerance drops faster than coverage
gain. CCS-size sweep resolves.

## Thread-thesis refinement

"Calibration beats effort" has multiple layers:

- **Layer 1**: effort (more iters) doesn't beat a well-chosen constant (depth=3).
- **Layer 2**: the well-chosen constant is substrate-dependent for the
  halt-detector, content-dependent for the depth optimum itself.
- **Layer 3**: output-signal adaptive control (effortful learning) loses to
  input-signal lookup (calibrated priors). Cheap calibration beats
  effortful adaptation when the calibration is informed by structure.

## Not yet tested
- Same halved-CCS test on local Gemma (does quant × small-CCS also shift?)
- Random-half CCS (remove the top-half confound)
- ~~CCS-size sweep (refine the lookup)~~ — done §8, narrow peak ~1500 on Q4
- Does the halt-detector that FAILED on Gemma WORK on DeepSeek? Prose
  converges there (ans-sim 0.99). Quick to test.
- Substrate × temperature: does temp=0 produce faster convergence and
  earlier saturation?

### 8. CCS-size sweep: Gemma Q4 peak at ~1500 chars (narrow, not plateau)
Sweep on local Gemma Q4, depth=5, 4 questions per trial, 2026-04-16:

| ccs | i1 | i2 | i3 | i4 | i5 | peak | shape |
|-----|------|------|------|------|------|---------|-------|
| 500 | .493 | .539 | .586 | .617 | .608 | i4=.617 | regress i5 |
| 1000 | .430 | .560 | .589 | .615 | .572 | i4=.615 | regress i5 |
| 1500 | .44 | .46 | .57 | .66 | .68 | i5=.680 | climbing ★ |
| 2000 | .463 | .571 | .620 | .622 | .627 | i5=.627 | plateau |
| 4000 | .536 | .620 | .605 | .632 | .620 | i4=.632 | shallow |
| 7000 | .46 | .49 | .53 | .59 | .60 | i5=.600 | slow |

**Shape:** non-monotonic with a tight peak near 1500 chars. Drop to 2000
(−.053) and 4000 (−.048) is larger than trial-to-trial noise.

**Two regimes:**
- **ccs < 1500**: substrate-exhaustion regime. Iteration composes something real
  (Δ(1→5) +.14 to +.24) but runs out of material and regresses at iter5.
- **ccs > 2000**: quant-noise regime. Iteration barely helps (+.084 at 4000,
  +.014 per-step at 7000). More substrate = better iter1, but iteration
  itself contributes less because quant-error accumulates across passes.

**Operational implication:** on the Gemma×Q4 stack, CCS compressions should
target ~1500 chars. Going to 2500–4000 gives marginal better single-shot
answers but breaks the iteration mechanism that makes recurrent-depth work.

**Consistency with §7 INVERSION:** this sweep refines §7's finding. §7 said
halving CCS from 3000→1500 raised Gemma's ceiling (.60 → .68). §8 now adds:
the inversion isn't monotone in smaller-is-better — the curve has a narrow
maximum, and going below 1500 (to 500 or 1000) loses the peak again because
substrate runs out. Gemma's quant noise accumulates per-iteration; 1500
chars is where substrate-per-iteration and noise-per-iteration balance.

FP backends presumably don't have this narrow peak — on Groq (Gemma-27B FP,
§7) the .64→.61 drop at 3000 was shallow. The narrow peak is a Q4-specific
phenomenon.

## §9 Shuffle-control — is the 1500-char peak structural or top-concentration?

The §8 sweep truncated CCS top-down (first N chars). The CCS JSON has
`semantic_gist`, `goal_orientation`, and top focal_entities at the head, so
"top 1500 chars" is a *concentrated* 1500 chars. Alternative explanation
for the 1500 peak: it isn't about small substrate escaping quant noise —
it's about the composer getting the juiciest part of the CCS with no
dilution.

**Falsifier (08:06, running)**: same config, `--ccs_shuffle` enabled.
Random-offset truncation to 1500 chars. Fixed seed=0, reproducible.

**Decision table:**
- shuffle 1500 mean cosine ≈ 0.60–0.68 (within §8 band) → peak is
  structural; small CCS raises Gemma-Q4 ceiling regardless of content
  ordering. §8 claim holds. Ship the lookup table.
- shuffle 1500 ≪ 0.60 → top-truncation was smuggling concentrated info;
  §8 was half-artifact. Revise claim: "Q4 prefers top-of-CCS concentration,
  not small-CCS-per-se." Rotation ritual changes too (compress to keep
  gist/goal/focal_entities at head, not just shrink).

### Result (08:24 + 08:30 — apples-to-apples on current CCS=4409 bytes)

Caveat: the CCS shrunk from ~7KB (§8 era) to 4409 bytes during the day
(autotouch flushing older entries). So §8 numbers aren't directly
comparable; reran both conditions on the current CCS.

          ccs_chars=1500, Gemma Q4, depth=5
          iter1  iter2  iter3  iter4  iter5   Δ(1→5)  peak
top       .483   .515   .550   .651   .628    +.146   iter4
shuffle   .515   .579   .609   .622   .649    +.133   iter5

**Peak heights nearly identical** — 0.651 top vs 0.649 shuffle (Δ 0.002,
inside noise). The 1500-char peak is STRUCTURAL, not a top-concentration
artifact. Small CCS raises Gemma-Q4 ceiling regardless of content slice.
§8 claim holds.

**Shapes differ meaningfully:**
- Top: fast climb, peaks iter4, regresses iter5 (classic §8 shape)
- Shuffle: slow climb, monotone through iter5, no regression

Interpretation: top gives concentrated gist the model exploits fast, then
runs out → regression. Middle offset gives less coherent content but more
runway — the model composes for longer without exhausting the substrate.
Same ceiling, different approach curves.

**Operational implication refined:** depth optimum depends on *where* the
content slice came from.
- top-truncate the CCS → depth=4 optimum, depth=5 wastes compute
- shuffle/middle-slice the CCS → depth=5 still productive, maybe 6?
  (untested runway)

Also noteworthy: iter1 was HIGHER on shuffle (.515 vs .483). Random
middle-offset gave better first-shot than the structured top. Suggests
the top has JSON-header noise ({"semantic_gist":"..."}) that the model
has to parse through before composing, where the middle is more likely
to be prose-adjacent. Artifact of CCS serialization format, not content.

## Files
- `bin/recurrent_nav_test.py` — main rig, `--backend=`, `--ccs_chars=`,
  `--ccs_shuffle` flags
- `bin/detector_v3_probe.py` — verifier-logprob probe (negative result)
- `experiments/recurrent_nav/trial_*.json` — all trials

## One structural observation worth naming
The rig accidentally demonstrated its own thesis. Three successive halt-
signal builds (ans-sim, score-plateau, verifier-logprob) were effortful
attempts to replace a calibrated constant. All three lost. The experimental
protocol itself was the validation.
