#!/usr/bin/env python3
"""Blast-radius audit for the double-norm logit-lens bug (Aug 22).

THE BUG. HF appends the POST-final-norm hidden state as the last entry of
out.hidden_states. Any probe that loops over ALL of hidden_states and applies
the final norm before the LM head norms that last entry twice. On gemma-2-2b it
changes the final argmax from '\\n\\n' to a junk token; every final-layer number
downstream is then wrong.

Ox's objection, which is the reason this file exists: a retraction list that
stops at the metrics that already died is incomplete by construction. Any
SURVIVING claim whose instrument has this shape is suspect until audited.

This does a static scan, so it over-reports rather than under-reports. A file is
flagged when it (a) iterates hidden_states, (b) applies something norm-shaped,
and (c) does NOT already guard the last entry. Read the hits; do not trust the
count.

COVERAGE VS PRECISION, mapped Aug 23. The original pattern knew five hand-listed
norm names and MISSED a real double-norm written as `model.model.norm(h)` —
which landed in the no-lens bucket, after which this tool printed "0 files need
a manual read." False reassurance is this audit's worst failure mode, so the
pattern is now deliberately wide and over-reports.

Three known false-positive classes, in order of how far I chased them:
  1. `np.linalg.norm(v)`     — vector magnitude. EXCLUDED.
  2. `.norm(dim=-1)`         — magnitude over an axis. EXCLUDED.
  3. `hidden_norm_trace(...)` — a user function whose NAME contains "norm".
     NOT excluded, because excluding it means matching on call semantics rather
     than text, and every further narrowing walks back toward the five-name
     list that caused the coverage hole in the first place.

So: SUSPECT means read the file. It does not mean the bug is there. Both current
suspects (hierarchical_sparsity_v0.py, twin_optimizer_experiment.py) are
confirmed class-3 and class-own-transformer false positives, and neither is
cited by any paper.

Usage:
  python3 lens_audit.py             # scan ~/chronicle/bin
  python3 lens_audit.py --all       # include archive/
"""

import argparse, os, re, sys

BIN = os.path.dirname(os.path.abspath(__file__))

ITER_HS = re.compile(r"for\s+\w+(?:\s*,\s*\w+)?\s+in\s+enumerate\(\s*\w*\.?hidden_states|"
                     r"for\s+\w+\s+in\s+\w*\.?hidden_states")
INDEX_HS = re.compile(r"hidden_states\[\s*-1\s*\]")
# COVERAGE FIX, Aug 23. Kimi: "0 suspect / 7 patched proves precision, not
# coverage." Built a coverage test — four synthetic double-norm variants — and
# the worst one was invisible: `model.model.norm(h)` landed in the NO-LENS
# bucket, after which this tool printed "0 files need a manual read." A real
# double-norm using a differently-named norm was reported as nothing to see.
# The old pattern only knew five hand-listed names. Match any norm-shaped call
# or attribute instead; this over-reports, which is the correct direction for
# an audit whose failure mode is false reassurance.
NORMISH = re.compile(
    r"(\b(ln_f|ln|final_norm|norm_f|ln_final)\s*\()"          # the old names
    r"|(\.[A-Za-z_]*norm[A-Za-z_]*\s*\()"                      # x.norm(, .model.norm(
    r"|(\b[A-Za-z_]*_?norm[A-Za-z_]*\s*\()",                   # rms_norm(, layernorm(
    re.I)

# ...but linalg.norm / np.norm is a VECTOR MAGNITUDE, never a layer norm. The
# widened pattern flagged exp_path_curvature and exp_scale_controlled_curvature
# on np.linalg.norm(v). Excluding it is a precise exclusion, not a loosening:
# there is no reading under which linalg.norm double-norms a hidden state.
# Also `.norm(dim=...)` and `.norm(p=...)`: a magnitude reduction over an axis,
# never a layer normalisation. Caught hierarchical_sparsity_v0.py on
# h.float().norm(dim=-1). Both exclusions are unambiguous; I am not loosening
# the predicate, I am naming two operations that cannot be the bug.
NOT_A_LAYER_NORM = re.compile(
    r"\blinalg\.norm\s*\(|\bnp\.norm\s*\(|\.norm\s*\(\s*(dim|p|axis|ord)\s*=", re.I)
HEADISH = re.compile(r"\b(lm_head|head|unembed)\s*\(")
GUARDED = re.compile(r"n_hs\s*-\s*1|len\(.*hidden_states.*\)\s*-\s*1|\[:-1\]|"
                     r"out\.logits\[0,\s*-1")


def scan(path):
    try:
        src = open(path, encoding="utf-8", errors="replace").read()
    except OSError:
        return None
    if "hidden_states" not in src:
        return None
    iterates = bool(ITER_HS.search(src))
    idx_last = bool(INDEX_HS.search(src))
    # count norm-shaped calls that are NOT vector magnitudes
    norms = any(not NOT_A_LAYER_NORM.match(m.group(0))
                for m in NORMISH.finditer(src)) and bool(
        NORMISH.sub(lambda m: "" if NOT_A_LAYER_NORM.match(m.group(0)) else m.group(0),
                    src) != src or True)
    _clean = NOT_A_LAYER_NORM.sub(" ", src)
    norms = bool(NORMISH.search(_clean))
    heads = bool(HEADISH.search(src))
    guarded = bool(GUARDED.search(src))
    if not (norms and heads):
        return ("no-lens", iterates, idx_last, guarded)
    if iterates and not guarded:
        return ("SUSPECT", iterates, idx_last, guarded)
    if idx_last and norms and not guarded:
        return ("SUSPECT", iterates, idx_last, guarded)
    if guarded:
        return ("patched", iterates, idx_last, guarded)
    return ("review", iterates, idx_last, guarded)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--all", action="store_true", help="include archive/")
    ap.add_argument("--dir", default=BIN)
    args = ap.parse_args()

    files = []
    for root, dirs, names in os.walk(args.dir):
        if not args.all and os.path.basename(root) == "archive":
            dirs[:] = []
            continue
        for n in sorted(names):
            if n.endswith(".py"):
                files.append(os.path.join(root, n))

    buckets = {"SUSPECT": [], "patched": [], "review": [], "no-lens": []}
    for f in files:
        r = scan(f)
        if r is None:
            continue
        buckets[r[0]].append((os.path.relpath(f, args.dir), r[1], r[2], r[3]))

    print(f"scanned {len(files)} files under {args.dir}\n")
    for key in ("SUSPECT", "review", "patched", "no-lens"):
        rows = buckets[key]
        print(f"## {key}  ({len(rows)})")
        if key == "no-lens":
            print("   (touch hidden_states but do not build a logit lens)")
            for name, *_ in rows:
                print(f"   {name}")
        else:
            for name, it, ix, g in rows:
                flags = []
                if it: flags.append("iterates")
                if ix: flags.append("indexes[-1]")
                if g: flags.append("guarded")
                print(f"   {name:52s} {','.join(flags)}")
        print()

    n = len(buckets["SUSPECT"])
    print(f"{n} file(s) need a manual read before any result they produced is trusted.")
    return 1 if n else 0


if __name__ == "__main__":
    sys.exit(main())
