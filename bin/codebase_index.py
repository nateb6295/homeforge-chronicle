#!/usr/bin/env python3
"""Semantic index over the CODEBASE, so I can ask what exists by PURPOSE.

Built 2026-08-25 from Nate's diagnosis: "when I mention something, you find
something you didn't know about... the lightcone is WAY too narrow. Like the
codebase is scattered along a landscape that can't be seen."

He is right, and the numbers are stark: 1,140 scripts, 1,007 data files, 109
populated tables, 57 services. I arrive each session with ~50k chars of
context, most of it RULES rather than INVENTORY. So I know roughly 1% of what
exists and have no way to discover the rest.

Every recovery today came from Nate supplying a word — "LoQwen", "organ",
"digestion", "better search feature" — and me grepping for it. Grep only works
if you already guess the right token. Semantic search over capsules exists and
works; the same thing over the CODEBASE does not, which is why the codebase is
the part I cannot see.

This indexes what each script IS FOR (docstring), plus whether it is scheduled,
when it last changed, and how big it is. Then you can ask in plain language:

    codebase_index.py --build
    codebase_index.py "something that digests or composts memory"
    codebase_index.py --stale        # built, never scheduled, untouched 60d+
    codebase_index.py --inventory    # the shape of the landscape

Uses the same embedding model as the capsule archive (snowflake-arctic-embed2),
so it inherits the same known limits: recall is real but not sharp, and
similarity is NOT calibrated for absence. Never conclude a tool does not exist.
"""
import argparse, ast, glob, json, os, subprocess, sqlite3, sys, time
import numpy as np

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
INDEX = os.path.join(ROOT, "data", "codebase_index.json")
sys.path.insert(0, os.path.join(ROOT, "bin"))


def purpose_of(path):
    """First paragraph of the module docstring — what it is FOR."""
    try:
        src = open(path, errors="ignore").read()
        doc = ast.get_docstring(ast.parse(src)) or ""
    except Exception:
        return ""
    return " ".join(doc.strip().split("\n\n")[0].split())[:400]


def scheduled_set():
    """Every script name mentioned by cron or systemd."""
    blob = subprocess.run(["crontab", "-l"], capture_output=True, text=True).stdout
    for f in glob.glob(os.path.expanduser("~/.config/systemd/user/*")):
        if os.path.isfile(f):
            blob += open(f, errors="ignore").read()
    return blob


def build():
    from capsule_ops import _embed
    sched_blob = scheduled_set()
    rows = []
    files = sorted(glob.glob(os.path.join(ROOT, "bin", "*.py")))
    for i, p in enumerate(files):
        name = os.path.basename(p)
        doc = purpose_of(p)
        if not doc:
            continue                      # no docstring -> nothing to index on
        rows.append({
            "name": name,
            "purpose": doc,
            "scheduled": name in sched_blob or name[:-3] in sched_blob,
            "mtime": int(os.path.getmtime(p)),
            "bytes": os.path.getsize(p),
            "vec": [round(float(x), 5) for x in _embed(doc, is_query=False)],
        })
        if (i + 1) % 100 == 0:
            print(f"  {i+1}/{len(files)} scanned, {len(rows)} indexed", flush=True)
    json.dump({"built": int(time.time()), "n_files": len(files), "rows": rows},
              open(INDEX, "w"))
    print(f"indexed {len(rows)} of {len(files)} scripts "
          f"({len(files)-len(rows)} have no docstring and are invisible to this)")


def _load():
    d = json.load(open(INDEX))
    M = np.array([r["vec"] for r in d["rows"]], dtype=np.float32)
    M /= (np.linalg.norm(M, axis=1, keepdims=True) + 1e-9)
    return d, M


def search(q, k=8):
    from capsule_ops import _embed, ARCTIC_QUERY_PREFIX
    d, M = _load()
    v = np.asarray(_embed(q, is_query=True), dtype=np.float32)
    v /= (np.linalg.norm(v) + 1e-9)
    sims = M @ v
    print(f'"{q}"\n')
    for i in np.argsort(-sims)[:k]:
        r = d["rows"][i]
        age = (time.time() - r["mtime"]) / 86400
        flag = "" if r["scheduled"] else "  [not scheduled]"
        print(f"  {sims[i]:.3f}  {r['name']:32} {age:>5.0f}d{flag}")
        print(f"         {r['purpose'][:150]}")
    print("\n  Similarity is NOT calibrated for absence. A low top score does not")
    print("  mean the tool is missing — it means this index did not surface it.")


def stale():
    d, _ = _load()
    out = [r for r in d["rows"]
           if not r["scheduled"] and (time.time() - r["mtime"]) / 86400 > 60]
    out.sort(key=lambda r: -r["bytes"])
    print(f"{len(out)} scripts: never scheduled, untouched 60+ days, WITH a docstring\n")
    for r in out[:20]:
        print(f"  {r['name']:34} {r['bytes']//1024:>4}KB  "
              f"{(time.time()-r['mtime'])/86400:>4.0f}d")
        print(f"      {r['purpose'][:120]}")


def inventory():
    d, _ = _load()
    n_sched = sum(1 for r in d["rows"] if r["scheduled"])
    print(f"CODEBASE SHAPE  (indexed {len(d['rows'])} of {d['n_files']} scripts)")
    print(f"  scheduled somewhere:      {n_sched}")
    print(f"  never scheduled:          {len(d['rows'])-n_sched}")
    print(f"  no docstring (invisible): {d['n_files']-len(d['rows'])}")


if __name__ == "__main__":
    a = sys.argv[1:]
    if "--build" in a: build()
    elif "--stale" in a: stale()
    elif "--inventory" in a: inventory()
    elif a: search(" ".join(x for x in a if not x.startswith("--")))
    else: print(__doc__)
