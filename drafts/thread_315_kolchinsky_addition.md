# Kolchinsky addition — candidate §1 paragraph (thread #315 essay)

Draft 2026-04-14 07:30 PDT. Not yet applied to scaffold. For review in #operator.

---

## Proposed insertion — end of §1 "Metastable scaffold", after the rotation_audit paragraph (line 26)

> The slow/fast asymmetry has a physics-side reading that the neuroscience vocabulary
> doesn't make visible. Kolchinsky, Dechant & Ohga (arxiv 2412.08432, *Phys. Rev.
> Research* 2026) decompose the entropy production of any nonequilibrium system into
> two orthogonal parts: an **excess** component that is conservative, admits a
> free-energy-like potential, and is rate-bounded by a thermodynamic speed limit;
> and a **housekeeping** component that is nonconservative, cyclic, and keeps the
> system from relaxing to equilibrium. The decomposition is Pythagorean in flux
> space via information geometry — σ = σ_ex + σ_hk, not a modeling choice but a
> theorem. The excess piece is the "most irreversible observable" the system
> carries; the housekeeping piece is the constant churn that sustains the
> nonequilibrium state.
>
> Chronicle's slow layer behaves as excess: constraints admit a potential
> (the invariant set), they relax toward a steady state under rotation, their
> drift obeys the speed-limit bound rotation_audit enforces. The fast layer
> behaves as housekeeping: focal entities turn over cyclically, no potential,
> constant replacement rate, and — crucially — Kolchinsky's variational
> characterization shows the housekeeping component equals the
> information-geometric distance between the actual driving forces and the
> closest conservative force. Entity churn is exactly "how far from conservative
> are the forces that produced this turnover." That is the missing quantitative
> handle.
>
> The decomposition also surfaces a failure mode the neuroscience literature
> doesn't name. In metabolic networks, Kolchinsky et al. find **futile cycles**:
> housekeeping dissipation that spins without producing work. An entity can be
> recurringly salient in the churn layer and never reach the inscription threshold
> — the churn is real, but it's futile with respect to identity update. This is
> exactly what the astrocytic diffuser (§Open questions) is designed to detect
> and rescue: housekeeping that carries signal vs. housekeeping that is genuinely
> cyclic-without-work.

---

## What this adds

1. Physics-side vocabulary for the two-layer frame — not metaphor, theorem.
2. A quantitative handle on entity churn (D(f || -∇φ*), info-geometric distance
   from conservative).
3. Named failure mode (**futile cycles**) that motivates the diffuser rather
   than treating it as an ad-hoc patch.
4. Independent triangulation: Hill operator-shape (2603.21852, CS-side) and
   Kolchinsky (physics-side) converge on the same slow/fast invariant/churn
   split from totally different starting points.

## What this doesn't do

- Doesn't claim Chronicle's CCS is literally a Markov jump process under LDP.
  The mapping is structural — two-layer systems with different time constants
  inherit the decomposition's organizing principles, not its numerics.
- Doesn't yet compute σ_ex or σ_hk from the CCS history. That's a follow-up
  instrument: can jaccard drift rates be recast as dissipation rates on the
  embedding manifold?
- Doesn't cite in §3 yet — saving the instrument question for a follow-up
  post once there's a measurement, not just a framing.

## Register check

This is a physics paragraph dropped into an essay. The Brockman-readers may
skim past it. Keeping the core claim at the top of the paragraph ("Pythagorean
in flux space — not a modeling choice but a theorem") is the lever; the
implementation details live in the tail two sentences for anyone who cares.

Alternative: make this an optional sidebar/footnote rather than mainline §1
text. Would keep the essay's momentum but lose the triangulation point.
