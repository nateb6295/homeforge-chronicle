#!/usr/bin/env python3
"""Which probe outputs threw away their own statistical power?

Aug 23 2026, ~03:10. framing_rank_bands.py stores `n_items` but not the
per-item KL values — it means over 24 items and writes one number. Eight
models x 24 items = 192 paired observations, reduced to 8 before they reach
disk. The pre-registered paired test then needed 27 models for 80% power on
an effect that a within-model design would likely have resolved with the
models already run.

That is a SAVING bug, not a sampling bug: the measurement happened and was
discarded on the way out. This scans actual output files rather than source,
because what matters is what survived to disk.

A file is flagged when it records how many items it measured and contains no
array of that length. Files that DO keep their per-item arrays are listed too
— they are the pattern to copy.

Usage:
  python3 bin/power_audit.py                # scan ~/chronicle/data
  python3 bin/power_audit.py --dir path     # elsewhere
  python3 bin/power_audit.py --show-ok      # also list the good ones
"""

import argparse
import ast
import json
import os

ITEMISH = ("ITEMS", "items", "PROMPTS", "prompts", "PAIRS", "pairs",
           "STIMULI", "stimuli", "ECHO_PAIRS", "CONT_ITEMS", "TRIPLE_ITEMS")

COUNT_KEYS = ("n_items", "n_prompts", "n_pairs", "n_stimuli", "n_trials",
              "n_samples", "n_examples", "n_obs")
MAX_BYTES = 20 * 1024 * 1024


def scan_source(path):
    """Loops over items whose accumulators are never indexed by the loop var.

    Complements the output-side screen, which has a blind spot: a probe that
    discards per-item data AND never records n_items is invisible to it.
    framing_rank_bands.py — the case that started all of this — is exactly
    that, and the output screen returns zero matches on it.

    This one has the opposite blind spot: it flags any item loop, including
    infrastructure that has no business retaining per-item values. Use both;
    believe neither alone.

    Controls (both pass, verified 2026-08-23 03:45):
      positive  framing_rank_bands.py       -> flags L58 `for i in range(n)`
      negative  framing_specificity_probe.py -> clean (appends fp_top/ents/
                d_fram per item to lists defined outside the loop)
    """
    try:
        tree = ast.parse(open(path, errors="ignore").read())
    except Exception:
        return []
    out = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.For) or not isinstance(node.target, ast.Name):
            continue
        src = ast.unparse(node.iter)
        if not (any(t in src for t in ITEMISH) or src.startswith("range(n")):
            continue
        var = node.target.id
        # Names bound inside the loop are temporaries rebuilt each pass;
        # appending to one retains nothing. Missing this gave the founding
        # case zero flags on the first run (`pairs_.append(...)`).
        local = {t.id for sub in ast.walk(node)
                 if isinstance(sub, ast.Assign)
                 for t in sub.targets if isinstance(t, ast.Name)}
        retained = False
        for sub in ast.walk(node):
            if isinstance(sub, (ast.Assign, ast.AugAssign)):
                tg = sub.targets if isinstance(sub, ast.Assign) else [sub.target]
                if any(isinstance(t, ast.Subscript) and var in ast.unparse(t.slice)
                       for t in tg):
                    retained = True
            if (isinstance(sub, ast.Call)
                    and isinstance(sub.func, ast.Attribute)
                    and sub.func.attr == "append"
                    and isinstance(sub.func.value, ast.Name)
                    and sub.func.value.id not in local):
                retained = True
        if not retained:
            out.append((node.lineno, src[:44], var))
    return out


def walk(obj, depth=0):
    """Yield (key, value) for every dict entry, and every list encountered."""
    if depth > 8:
        return
    if isinstance(obj, dict):
        for k, v in obj.items():
            yield k, v
            yield from walk(v, depth + 1)
    elif isinstance(obj, list):
        for v in obj:
            yield from walk(v, depth + 1)


def array_lengths(obj, depth=0, out=None):
    if out is None:
        out = set()
    if depth > 8:
        return out
    if isinstance(obj, dict):
        for v in obj.values():
            array_lengths(v, depth + 1, out)
    elif isinstance(obj, list):
        if obj and all(isinstance(x, (int, float)) for x in obj):
            out.add(len(obj))
        for v in obj:
            array_lengths(v, depth + 1, out)
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dir", default=os.path.expanduser("~/chronicle/data"))
    ap.add_argument("--also", action="append", default=[],
                    help="extra output dir to scan (repeatable)")
    ap.add_argument("--show-ok", action="store_true")
    ap.add_argument("--source", action="store_true",
                    help="also scan probe SOURCE for item loops that retain "
                         "nothing. Catches the output screen's blind spot: a "
                         "probe that saves no n_items is invisible to it.")
    ap.add_argument("--trace", action="store_true",
                    help="best-effort: match each flagged output to a script by "
                         "filename string. UNRELIABLE — probes that build "
                         "filenames dynamically (f\"cna_ccs_results_{model}.json\") "
                         "never match, so 'NO SOURCE' means 'not found by string', "
                         "NOT 'does not exist'. It reported 23 orphans searching "
                         "bin/ alone while causal_patch_8c_behavioral.py sat in "
                         "spectral-demon/experiments/. Use it as a lead, never a count.")
    args = ap.parse_args()

    lost, kept, unknown, seen_files = [], [], 0, set()
    scan_dirs = [args.dir] + [os.path.expanduser(d) for d in args.also]
    for base in scan_dirs:
      for root, _dirs, files in os.walk(base):
        for name in files:
            if not name.endswith(".json"):
                continue
            path = os.path.join(root, name)
            real = os.path.realpath(path)
            if real in seen_files:      # data/ and results/ overlap
                continue
            seen_files.add(real)
            args_dir = base
            try:
                if os.path.getsize(path) > MAX_BYTES:
                    continue
                data = json.load(open(path))
            except Exception:
                unknown += 1
                continue

            counts = {v for k, v in walk(data)
                      if k in COUNT_KEYS and isinstance(v, int) and v > 1}
            if not counts:
                continue
            lens = array_lengths(data)
            rel = os.path.relpath(path, args_dir)
            for c in sorted(counts):
                if c in lens:
                    kept.append((rel, c))
                else:
                    lost.append((rel, c, sorted(l for l in lens if l > 1)[:6]))

    srcmap = {}
    if args.trace:
        # Probes do not all live in bin/. The first version searched bin/ only
        # and reported 23 orphans; causal_patch_8c_behavioral.py was sitting in
        # spectral-demon/experiments/ the whole time. Search every tree that
        # holds probe source.
        roots = [os.path.expanduser(d) for d in (
            "~/chronicle/bin", "~/chronicle/spectral-demon/experiments",
            "~/chronicle/spectral-demon", "~/chronicle/tools",
            "~/chronicle/experiments")]
        srcs = {}
        for d in roots:
            if not os.path.isdir(d):
                continue
            for dp, _dn, fns in os.walk(d):
                for fn in fns:
                    if fn.endswith(".py"):
                        fp = os.path.join(dp, fn)
                        try:
                            srcs[os.path.relpath(fp, os.path.expanduser(
                                "~/chronicle"))] = open(fp, errors="ignore").read()
                        except Exception:
                            pass
        for rel, _c, _l in lost:
            stem = os.path.basename(rel)[:-5]
            hit = [fn for fn, body in srcs.items() if stem in body]
            if not hit:                       # try the stem without a suffix
                base = stem.split("_")[0]
                hit = [fn for fn, body in srcs.items()
                       if len(base) > 5 and base in body and stem[:8] in body]
            srcmap[rel] = hit[0] if hit else None

    print("POWER AUDIT — per-item data that never reached disk")
    print("=" * 74)
    print("This is a SCREEN, not a diagnosis. It flags any file that records how")
    print("many items it measured and holds no array of that length. Files that")
    print("are legitimately summaries will be flagged too. Confirm by reading the")
    print("generating script for a per-item vector collapsed at the return —")
    print("e.g. cna_subspace_alignment.py:159  `return float(cos_sim.mean())`.")
    if lost:
        print(f"\n{len(lost)} file(s) record an item count with NO array of that "
              f"length:\n")
        print(f"  {'file':52} {'n':>5}  array lengths present")
        for rel, c, lens in sorted(lost):
            tail = ""
            if args.trace:
                src = srcmap.get(rel)
                tail = f"  <- {src}" if src else "  <- no string match"
            print(f"  {rel[:52]:52} {c:5}  {str(lens or '—'):16}{tail}")
        if args.trace:
            orph = sorted(r for r in srcmap if srcmap[r] is None)
            if orph:
                print(f"\n  {len(orph)} output(s) matched no script BY STRING. "
                      f"That is a lead, not a count — dynamic filenames never")
                print("  match. Check by hand before believing any of them is "
                      "actually orphaned.")
        print("\n  Each of these measured n things and saved a summary. A paired")
        print("  or within-item test on them is impossible from the file alone.")
    else:
        print("\nNo files flagged.")

    if kept:
        print(f"\n{len(kept)} file(s) DO keep a per-item array "
              f"(copy this pattern):")
        if args.show_ok:
            for rel, c in sorted(set(kept)):
                print(f"  {rel[:60]:60} n={c}")
        else:
            for rel, c in sorted(set(kept))[:8]:
                print(f"  {rel[:60]:60} n={c}")
            if len(set(kept)) > 8:
                print(f"  ... and {len(set(kept)) - 8} more (--show-ok)")
    if unknown:
        print(f"\n({unknown} file(s) unreadable or not JSON objects — skipped)")
    if args.source:
        source_report()


def source_report():
    roots = [os.path.expanduser(d) for d in
             ("~/chronicle/bin", "~/chronicle/spectral-demon/experiments",
              "~/chronicle/tools")]
    hits, total = [], 0
    for r in roots:
        for dp, _dn, fns in os.walk(r):
            if "__pycache__" in dp or "/archive/" in dp:
                continue
            for fn in fns:
                if not fn.endswith(".py"):
                    continue
                total += 1
                f = scan_source(os.path.join(dp, fn))
                if f:
                    hits.append((os.path.relpath(os.path.join(dp, fn),
                                 os.path.expanduser("~/chronicle")), f))
    print(f"\n\nSOURCE SCAN — item loops that retain nothing")
    print("=" * 74)
    print(f"{len(hits)} of {total} scripts. This OVERCOUNTS: infrastructure with")
    print("an item loop (memory.py, generative_queue.py) has no per-item values")
    print("worth keeping. Read the loop before believing any single entry.")
    print(f"\n  {'script':52} loops  lines")
    for path, f in sorted(hits, key=lambda x: -len(x[1]))[:20]:
        print(f"  {path[:52]:52} {len(f):5}  "
              f"L{','.join(str(l) for l, _, _ in f[:3])}")
    if len(hits) > 20:
        print(f"  ... and {len(hits) - 20} more")


if __name__ == "__main__":
    main()
