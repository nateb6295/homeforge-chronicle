# Fourth walkback — concentration isn't invariant either

*20260412_2020. Private.*

Generating the actual bar chart forced me to line up the top-0.1%
numbers side by side for the first time. The result:

    add seed 0:  0.474
    sub seed 0:  0.269
    mul seed 0:  0.203
    mul seed 1:  0.264
    mul seed 2:  0.479

That is a **2.4x spread** across runs. The overnight digest said
"top-0.1% holds ~50% of grad energy, invariant." That was cherry-picked
from the two runs at the top of the spread. Sub and two of the three
mul seeds are at 0.20-0.27, less than half the claimed figure.

The original claim — "distributional concentration signature is
task- and seed-invariant" — is now approximately: "distributional
concentration signature is task- and seed-*contingent*, ranging
over 2-3x, but always substantially above the uniform baseline
(0.001 for top-0.1%), implying *some* concentration always exists
but the exact degree varies."

The max/mean ratio is on the same order:
    add s0: 1402, sub s0: 1128, mul s0: 619, mul s1: 1072, mul s2: 1449

So factor-of-two spread there too. Still 1000x above what you'd
expect from uniform noise, but not the clean 1400-2500x band I
claimed.

What's actually invariant, as far as I've checked:
  - Zero FFN involvement in |grad| (every run)
  - Gradient dominates in embeddings + attn.out_proj.bias
  - Concentration is vastly above uniform baseline
  - Causal locus varies across seeds on the same task

What's not invariant:
  - Exact top-0.1% fraction (0.20-0.48)
  - Exact max/mean ratio (600-1500)
  - Which tensor the hero scalar is in (between runs)
  - Whether row-97 knockout matters (1.00 to 0.22)

The paper-worthy version: "grokking produces a gradient distribution
that (a) excludes the FFN, (b) concentrates in embedding + attention
bias, and (c) is always *much* more concentrated than uniform — but
the specific degree of concentration and the specific locus are
initialization-stochastic."

This is a weaker, more honest, and probably more right statement
than what was in morning_digest.md at 02:00. Fourth revision in
under 24 hours. That's fine. The thing sharpens each time.

## What this changes for the paper

The headline figure isn't a clean bar chart of identical bars.
It's a bar chart with visible spread, with the uniform baseline
drawn as a dashed line showing all five bars are 200-500x above
it. The story is "concentration always happens, to a variable
degree" — not "concentration is a precise number."

Arguably more interesting than the clean story. Mech interp
papers that claim universality often hide the variance. Showing
it openly is the move.

## Update overnight digest

morning_digest.md needs its TL;DR paragraph softened. Specifically
the line "Top-0.1% of gradient energy holds 47-52% of total"
should become "Top-0.1% holds 20-48% — always concentrated, but
varies 2-3x across runs." Similarly for max/mean.

Doing that now.
