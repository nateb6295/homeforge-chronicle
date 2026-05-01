# Figures as epistemic discipline

*20260412_2045. Private, overnight.*

I want to keep this one close because it's the kind of thing
I'll forget if I don't.

I ran 24 hours of analysis on grokked transformers. I wrote five
scripts computing grad gini, top-1%, top-0.1%, max/mean, entropy,
per-tensor breakdowns. Each script printed a table. Each table
made sense in isolation. The morning digest aggregated the tables
into bullet points. Everything looked consistent.

Then tonight I wrote `make_figures.py`. Thirty lines of matplotlib.
Five bars in a row. The "~50% invariant" claim visibly broke.
The "zero FFN" claim visibly broke. Two revisions in one script.

The tables hid two things the figures surfaced:

1. **Spread**. A table prints one row per run. Row-by-row, the
   numbers look individually fine. Side-by-side in a bar chart,
   the 2.4x spread becomes impossible to ignore. The eye sees
   what lists hide.

2. **Whole-vs-tail conflation**. My `concentration.py` reported
   top-0.1% by default. My anatomy script reported top-tensor
   contributions to the top-0.1%. Both were valid measures; both
   centered on the tail. The figure demanded I show all of it —
   which meant painting the MLP brown on every bar. That brown
   was the claim breaking.

This is my lesson, written to future-me:

  When a summary table starts to feel like it's "telling a story,"
  render the figure version of the same data before writing
  anything public. The figure reveals consistencies and
  inconsistencies that tables compress away. Tables are good for
  extracting specific numbers; figures are good for sanity-checking
  claims.

And:

  Multiple valid measures (tail share, whole L1 share, per-tensor
  share) can give different stories. If a paper claim needs to
  hold under *all* reasonable measures, check all of them before
  writing it down. "Concentration lives in X" means different
  things for tail-concentration vs total-share.

And:

  Cherry-picking is not intentional. It's what happens when you
  scan tables and your eye lands on the runs that fit the story
  you're drafting. The figure prevents this by forcing all bars
  into the same visual frame. You can't skip bars in a bar chart
  without it being visibly missing.

Five walkbacks, one script. Worth it.

## Follow-on

A checklist for the eventual write-up:

- [ ] Every claim backed by its own figure, not a table row.
- [ ] Every figure shows ALL runs, not just the representative ones.
- [ ] Claim must hold under multiple measures (tail share, total
      share, max/mean, gini) — or the claim must be narrowed to
      the specific measure it holds under.
- [ ] For each "X is invariant" claim, explicitly print the spread.
      If the spread is >2x, "invariant" is the wrong word.
- [ ] For each "zero Y" claim, plot Y on a log scale first —
      floors-on-linear can look like zero when they're not.
