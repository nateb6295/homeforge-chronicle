#!/usr/bin/env python3
"""The map. Generated, never hand-written, so it cannot drift out of true.

Built 2026-08-25 during the consolidation. Nate: "the lightcone is WAY too
narrow. Like the codebase is scattered along a landscape that cant be seen."

He was right, and the numbers were the proof: 1,120 scripts, of which 896 were
unreachable from ANY entry point — no path from cron, systemd, CLAUDE.md or
settings.json could ever invoke them. Deleted. 250 remain.

THE RULE THIS ENFORCES, and it cost me my own accumulator to learn:
    A script that is not reachable from a root is already dead.
    ccs_pressure.py was twelve hours old and the graph killed it, correctly,
    because I built it and never named it anywhere. The one tool I had put in
    crontab survived. Reachability is not bookkeeping — it is whether the thing
    exists tomorrow.

So: to keep something, make it reachable. Name it in CLAUDE.md, cron, or a
service file. That is the whole contract.

    foundation.py            the map
    foundation.py --orphans  what is drifting toward death right now
"""
import ast, glob, json, os, re, subprocess, sys, time

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def roots_blob():
    b = subprocess.run(["crontab", "-l"], capture_output=True, text=True).stdout
    for f in glob.glob(os.path.expanduser("~/.config/systemd/user/*")):
        if os.path.isfile(f):
            b += open(f, errors="ignore").read()
    for f in (os.path.join(ROOT, "CLAUDE.md"),
              os.path.expanduser("~/.claude/settings.json")):
        if os.path.exists(f):
            b += open(f, errors="ignore").read()
    return b


def ast_edges(path, stems):
    """Real edges only: imports, and script names inside STRING LITERALS.

    The token-regex version below counts any occurrence of another script's
    stem — including one in a comment or a docstring. That is how
    content_survey.py showed as reachable on 2026-08-25 while its only inbound
    edge was a sentence in log_survey.py's docstring. A clean orphan count from
    that graph is necessary, not sufficient.

    This walks the AST instead:
      - import X / from X import ...      -> a real edge
      - any string literal containing X   -> a real edge (subprocess argv,
                                             Path(...), an f-string command)
    Comments are absent from the AST entirely. Docstrings ARE string literals,
    so they are excluded explicitly — a module/class/function docstring is prose
    about the code, not a call into it.
    """
    try:
        tree = ast.parse(open(path, errors="ignore").read())
    except SyntaxError:
        return None                      # caller falls back; never silently 0

    docstrings = set()
    for node in ast.walk(tree):
        if isinstance(node, (ast.Module, ast.FunctionDef, ast.AsyncFunctionDef,
                             ast.ClassDef)):
            d = ast.get_docstring(node, clean=False)
            if d:
                docstrings.add(d)

    found = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for a in node.names:
                found.add(a.name.split(".")[0])
        elif isinstance(node, ast.ImportFrom):
            if node.module:
                found.add(node.module.split(".")[0])
        elif isinstance(node, ast.Constant) and isinstance(node.value, str):
            if node.value in docstrings:
                continue
            for st in stems:
                if st in node.value:
                    found.add(st)
    return (found & set(stems)) - {os.path.basename(path).rsplit(".", 1)[0]}


def sh_edges(path, stems):
    """Shell has no AST. Strip comments, then token-match."""
    out = []
    for line in open(path, errors="ignore"):
        line = re.sub(r"(?<!\\)#.*$", "", line)
        out.append(line)
    toks = set(re.findall(r"[A-Za-z0-9_]+", "\n".join(out)))
    return (toks & set(stems)) - {os.path.basename(path).rsplit(".", 1)[0]}


def graph(use_ast=False):
    files = sorted(glob.glob(os.path.join(ROOT, "bin", "*.py"))) + \
            sorted(glob.glob(os.path.join(ROOT, "bin", "*.sh")))
    stems = {os.path.basename(f).rsplit(".", 1)[0]: f for f in files}
    blob = roots_blob()
    # A root must be named by FILENAME, not bare stem. Tightened 2026-08-25,
    # within an hour of fixing the same defect in edges — and by causing it.
    # I wrote a CLAUDE.md paragraph explaining that prose mentions are not
    # edges, that paragraph mentioned `coherence_null_distribution` as the
    # example, and the bare-stem match promoted it to a ROOT. Documentation of
    # the bug reproduced the bug.
    #
    # Requiring "name.py" / "name.sh" makes keeping something a deliberate act:
    # you write the filename, which is what a cron line or a service file
    # contains anyway. Discussing a script by name no longer resurrects it.
    roots = {s for s in stems if os.path.basename(stems[s]) in blob}
    edges = {}
    for s, f in stems.items():
        e = None
        if use_ast:
            e = ast_edges(f, stems) if f.endswith(".py") else sh_edges(f, stems)
        if e is None:
            toks = set(re.findall(r"[A-Za-z0-9_]+", open(f, errors="ignore").read()))
            e = (toks & set(stems)) - {s}
        edges[s] = e
    reach, frontier = set(roots), list(roots)
    while frontier:
        for nxt in edges.get(frontier.pop(), ()):
            if nxt not in reach:
                reach.add(nxt); frontier.append(nxt)
    return stems, roots, reach, edges


def services():
    """Service health, with ONESHOTS judged by their TIMER, not their state.

    A oneshot correctly reads 'not active' between runs. The first version of
    this reported chronicle-loquwen as down while its timer had fired 90 seconds
    earlier — the same false positive I built into health_alert.py last night
    and removed within the hour. Twice in one day, so it is written down here.
    """
    out = subprocess.run(["systemctl", "--user", "list-units", "chronicle-*",
                          "--no-legend", "--plain"], capture_output=True, text=True).stdout
    timers = subprocess.run(["systemctl", "--user", "list-units", "chronicle-*.timer",
                             "--no-legend", "--plain"], capture_output=True, text=True).stdout
    rows = []
    for l in out.splitlines():
        if ".service" not in l:
            continue
        parts = l.split()
        name = parts[0].replace(".service", "")
        state = parts[2] if len(parts) > 2 else "?"
        if state != "active" and f"{name}.timer" in timers:
            state = "oneshot (timer active)"
        rows.append((name, state))
    return rows


def main():
    use_ast = "--regex" not in sys.argv   # AST is the default since 2026-08-25
    stems, roots, reach, _ = graph(use_ast=use_ast)
    orphans = sorted(set(stems) - reach)

    if "--compare" in sys.argv:
        # Never swap a measurement instrument without measuring the swap.
        _, r1, reach1, _ = graph(use_ast=False)
        _, r2, reach2, _ = graph(use_ast=True)
        o1, o2 = set(stems) - reach1, set(stems) - reach2
        print("REACHABILITY — token-regex vs AST\n")
        print(f"  scripts            {len(stems)}")
        print(f"  roots              {len(r1)}")
        print(f"  reachable (regex)  {len(reach1)}   orphans {len(o1)}")
        print(f"  reachable (AST)    {len(reach2)}   orphans {len(o2)}")
        newly = sorted(o2 - o1)
        if newly:
            print(f"\n  {len(newly)} script(s) the regex called reachable and the AST does NOT.")
            print("  These were held alive by a mention in a COMMENT or DOCSTRING:\n")
            for n in newly:
                age = (time.time() - os.path.getmtime(stems[n])) / 86400
                print(f"    {n:38} {age:>4.0f}d old")
            print("\n  Each is genuinely unreferenced by code. Name it in CLAUDE.md,")
            print("  crontab or a .service file to keep it — a prose mention is not an edge.")
        else:
            print("\n  No difference. Every regex edge is backed by a real import or literal.")
        return
    if "--orphans" in sys.argv:
        print(f"{len(orphans)} scripts unreachable from any root — these will not survive:\n")
        for s in orphans:
            age = (time.time() - os.path.getmtime(stems[s])) / 86400
            print(f"  {s:38} {age:>4.0f}d old")
        print("\n  To keep one: name it in CLAUDE.md, crontab, or a .service file.")
        return

    print(f"FOUNDATION — generated {time.strftime('%Y-%m-%d %H:%M')}\n")
    print(f"  scripts in bin/       {len(stems)}")
    print(f"  entry points (roots)  {len(roots)}")
    print(f"  reachable from roots  {len(reach)}")
    print(f"  ORPHANS (will die)    {len(orphans)}")
    svc = services()
    print(f"\n  services healthy      {sum(1 for _, s in svc if s == 'active' or s.startswith('oneshot'))}/{len(svc)}")
    for n, s in svc:
        if s != "active" and not s.startswith("oneshot"):
            print(f"    NOT ACTIVE: {n}")
    try:
        import sqlite3
        db = sqlite3.connect(os.environ.get("CHRONICLE_DB", "/mnt/hdd/chronicle-data/processed.db"))
        caps = db.execute("SELECT COUNT(*) FROM knowledge_capsules").fetchone()[0]
        ver = db.execute("SELECT version FROM cognitive_state WHERE id=1").fetchone()[0]
        db.close()
        print(f"\n  capsules              {caps:,}")
        print(f"  CCS version           {ver}")
    except Exception as e:
        print(f"\n  db: unavailable ({type(e).__name__}) — NOT a clean bill")
    if orphans:
        print(f"\n  {len(orphans)} orphan(s). `foundation.py --orphans` to see them.")


if __name__ == "__main__":
    main()
