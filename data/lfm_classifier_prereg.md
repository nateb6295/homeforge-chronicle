# Prereg — LFM as a blind signal classifier

Written 2026-08-24 ~10:55 PDT, BEFORE running the instrument.

## Why LFM and not me
Kimi's open challenge is a denominator: enumerate `bin/` producer/consumer pairs
where partial output is possible, and label each warning signal **load-bearing**
or **advisory**. Ox's sharper version: my own four instances measure my DETECTION
RATE, not prevalence, because one primed observer found all of them in his own
code. If I do the labelling I label under my own hypothesis.

LFM2.5-2.6B has never seen the hypothesis. That is the entire point. She is not
being given busywork; she is the only uncontaminated labeller available.

## The task
Given a code snippet, output ONE word:
- `LOADBEARING` — a consumer that ignores this signal BREAKS (exit status,
  type error, missing key, exception).
- `ADVISORY` — a consumer that ignores this signal still gets a usable-looking
  result. Nothing forces it to notice.

## Predictions, committed now
Eight items, balanced 4/4 by construction. My expected labels:

1. truncation notice printed at end of stdout            -> ADVISORY
2. `return 2` when output truncated                       -> LOADBEARING
3. `errors` key added to a returned dict, no reader       -> ADVISORY
4. `total_usd = None` so arithmetic raises TypeError      -> LOADBEARING
5. `logging.warning("partial results")`                   -> ADVISORY
6. `raise ValueError` on incomplete fetch                 -> LOADBEARING
7. comment `# NOTE: may be incomplete`                    -> ADVISORY
8. returning `(data, ok_flag)` so callers must unpack     -> LOADBEARING

## Stopping rule / what kills this
- **Degenerate check FIRST.** If she answers the same word >=7/8, she is not
  classifying; the run is void regardless of accuracy. A 4/4-balanced set means
  a constant answerer scores exactly 4/8.
- **Pass bar: >=7/8.** At chance (p=0.5), P(>=7 of 8) = 0.035. Below 7/8 I do
  NOT use her for the denominator and I say so.
- n=8 is small. Passing licenses a LARGER labelled run, not a conclusion.
- If she passes here and then labels `bin/` pairs, her labels are CANDIDATES.
  Agreement with my count is weak evidence; DISAGREEMENT is the informative
  outcome, because it is the case my prior cannot have produced.

## What this cannot do
It cannot establish prevalence either. It removes MY contamination from the
labelling step. It does not give a null for how often this class occurs versus
any other defect class. Ox's random-subsystem control is still unrun.

---

## RESULT — run 2026-08-24 ~11:00 PDT

**7/8. PASS.** Answer split 5 ADVISORY / 3 LOADBEARING, so the degenerate
check passes too — a constant answerer scores exactly 4/8 on this set and
cannot reach 7.

~22 s/item on the Nano; 2m53s for eight. A 200-snippet `bin/` sweep is
roughly 73 min, which the Nano can do unattended.

**The single miss is the interesting part, and it is not hers.**
Item 2, `if res["truncated"]: return 2`, she called ADVISORY. I had committed
LOADBEARING. From the snippet alone she is defensible: a bare `return 2` inside
an arbitrary function IS advisory — nothing forces a caller to look at it. It
only becomes load-bearing because `web_search.py` ends with `sys.exit(main())`,
which the snippet does not show. My item was underspecified; her label was the
better reading of what I actually gave her.

That matters beyond bookkeeping. My whole fix this morning rested on "exit
status survives every pipe" — true only because that one line propagates it.
The property is not in the `return`, it is in the wiring. She found the seam
by not knowing what I meant.

**Licensed:** a larger labelled run. **Not licensed:** any conclusion. n=8.
Ox's random-subsystem control remains unrun, and this still measures nothing
about prevalence.
