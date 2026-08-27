#!/usr/bin/env python3
"""A number you cannot print without saying how you made it.

WHY THIS EXISTS. On 2026-08-22 I caught seven of my own errors in one day:

  1. published a claim with the falsifying number already on disk
  2. double-normed the final layer in seven files
  3. reported argmax agreement with the unrelated-pair floor at 1.000
  4. called a control "matched" at 1.3 tokens against 4.4
  5. read "pronoun share 0.3%" as evidence while it counted the wrong pronouns
  6. selected a layer on |entity - person| and reported entity from it
  7. compared two probes normalised by different floors

Every one is the same failure: a number I trusted without asking how it was
made. The argument built on top was usually fine -- GIVEN the number. And every
fix I shipped was local (print the raw thing, check the denominator, report a
floor, don't peak-pick), which is seven patches for one missing step.

So this is not another reminder. A Measured refuses to render unless it carries
its denominator, its selection rule, and a sample of the raw items behind it.
The check cannot be forgotten because omitting it is a TypeError, not an
oversight.

    band = Measured(
        0.154,
        denom=("floor: mean pairwise KL over 2nd-person prompts", 1.83),
        selection=Selection.prespecified("40-60% depth, from F499c L12-19"),
        raw=[0.12, 0.19, 0.14, ...],
    )
    print(band.line("phi-2 entity"))

Selection.none() is a legitimate answer. "I don't know" is not available.
"""

from dataclasses import dataclass, field
from typing import Sequence


class Selection:
    """How the reported subset was chosen. Every constructor is explicit."""

    def __init__(self, kind: str, detail: str, inflates: bool):
        self.kind, self.detail, self.inflates = kind, detail, inflates

    @classmethod
    def none(cls):
        return cls("none", "all items, no subset chosen", False)

    @classmethod
    def prespecified(cls, detail: str):
        """A band or subset fixed BEFORE seeing these numbers, from an external
        prior. The only selection that does not inflate."""
        return cls("pre-specified", detail, False)

    @classmethod
    def peak(cls, over: str, on: str):
        """Chose the extreme over `over`, selecting on quantity `on`. Inflates.
        If `on` differs from what is being reported, that is error 6 above."""
        return cls("PEAK", f"max over {over}, selected on {on}", True)

    @classmethod
    def post_hoc(cls, detail: str):
        return cls("POST-HOC", detail, True)

    def __str__(self):
        flag = "  [INFLATES]" if self.inflates else ""
        return f"{self.kind}: {self.detail}{flag}"


# Two guards, each for a failure that actually happened on 2026-08-22.
#
# UNSTABLE_DENOM: gemma-2-2b is peaked enough that its mass in the upper rank
# bands is near zero, so a KL/mass ratio blows up -- 6.88 at 11pm, and 1093 in a
# different probe at 4pm. I wrote "the ratio estimator needs a floor" after the
# first one and then built a second unfloored ratio seven hours later. Looking
# was not enough. Writing it down was not enough.
#
# OUTLIER_DRIVEN: phi-2's reported delta of -0.215 had a single item at -1.644,
# seven times the mean, which moved the median to -0.123. The aggregate was
# real in direction and inflated in magnitude, and nothing in the output said so.
UNSTABLE_DENOM_RATIO = 20.0    # |value| / denom above this -> the ratio is fragile
OUTLIER_RATIO = 4.0            # max|raw| / |value| above this -> tail-driven


@dataclass
class Measured:
    value: float
    denom: tuple            # (what it was divided by, its value)
    selection: Selection
    raw: Sequence           # the items behind the aggregate
    n_show: int = 6

    def __post_init__(self):
        if not (isinstance(self.denom, (tuple, list)) and len(self.denom) == 2):
            raise TypeError("denom must be (description, value) -- what was this "
                            "divided by? If nothing, pass ('none', 1.0).")
        if not isinstance(self.selection, Selection):
            raise TypeError("selection must be a Selection. Selection.none() is "
                            "a valid answer; omitting it is not.")
        if self.raw is None or len(self.raw) == 0:
            raise TypeError("raw must contain the items behind the aggregate. "
                            "Six times today the aggregate disagreed with them.")

    def warnings(self) -> list:
        """Conditions that made a number wrong today, checked automatically."""
        out = []
        _, d_val = self.denom
        if d_val and abs(self.value) / abs(d_val) > UNSTABLE_DENOM_RATIO:
            out.append(f"UNSTABLE DENOMINATOR: |value|/denom = "
                       f"{abs(self.value)/abs(d_val):.0f} — near-zero denominator, "
                       f"this ratio is fragile (cf. gemma-2-2b)")
        vals = [float(x) for x in self.raw]
        if vals and abs(self.value) > 1e-9:
            worst = max(vals, key=abs)
            if abs(worst) / abs(self.value) > OUTLIER_RATIO:
                srt = sorted(vals)
                med = srt[len(srt) // 2]
                out.append(f"OUTLIER-DRIVEN: one item at {worst:+.3f} is "
                           f"{abs(worst)/abs(self.value):.1f}x the reported value; "
                           f"median is {med:+.3f}")
        return out

    def line(self, label: str) -> str:
        d_txt, d_val = self.denom
        head = f"{label}: {self.value:+.3f}"
        prov = (f"    ÷ {d_txt} = {d_val:.4g}\n"
                f"    selection: {self.selection}\n"
                f"    n={len(self.raw)} raw[:{self.n_show}]: "
                + ", ".join(f"{float(x):+.3f}" for x in list(self.raw)[:self.n_show]))
        rng = (f"\n    raw range: {min(map(float, self.raw)):+.3f} .. "
               f"{max(map(float, self.raw)):+.3f}")
        warn = "".join("\n    !! " + w for w in self.warnings())
        return head + "\n" + prov + rng + warn


if __name__ == "__main__":
    ok = Measured(0.154, denom=("floor: pairwise KL over 2nd-person prompts", 1.83),
                  selection=Selection.prespecified("40-60% depth, from F499c L12-19"),
                  raw=[0.12, 0.19, 0.14, 0.17, 0.15, 0.16, 0.13])
    print(ok.line("phi-2 entity")); print()
    bad = Measured(0.116, denom=("floor", 1.83),
                   selection=Selection.peak(over="32 layers", on="|entity-person|"),
                   raw=[0.12, 0.19, 0.14])
    print(bad.line("phi-2 entity, as I reported it at 21:35")); print()
    print(Measured(6.88, denom=("band mass share", 0.079),
                   selection=Selection.none(),
                   raw=[6.9, 7.1, 6.5]).line("gemma-2-2b ranks 11-50, as reported at 23:40"))
    print()
    print(Measured(-0.215, denom=("floor", 2.04), selection=Selection.none(),
                   raw=[0.078, 0.007, -1.644, -0.251, -0.041, -0.082, 0.871]
                   ).line("phi-2 delta, as reported at 21:00"))
    print()
    for kw, why in ((dict(denom=1.83), "denom not a pair"),
                    (dict(selection=None), "selection omitted"),
                    (dict(raw=[]), "raw empty")):
        args = dict(value=0.1, denom=("floor", 1.0),
                    selection=Selection.none(), raw=[0.1])
        args.update(kw)
        try:
            Measured(**args); print(f"  NOT CAUGHT: {why}")
        except TypeError as e:
            print(f"  refused ({why}): {str(e)[:60]}...")
