#!/usr/bin/env python3
"""Audit LOG files the way content_survey.py audits tables.

Why this exists (Aug 25 2026): prediction_monitor.py ran from crontab every 6
hours for five months. 429 runs died on 'no such table', 97 on a missing dfx
binary — 695 tracebacks total, into a log whose only reader was the script that
wrote it. content_survey.py could never have found it: that audits TABLES, and
this failure lived in a FILE.

A table nothing SELECTs from and a log nothing greps are the same defect. This
covers the second surface.

Three things it reports that nothing else does:
  1. ERRORS NOBODY READ    — tracebacks in a log with no consumer
  2. NEW SINCE LAST CHECK  — byte-watermarked, so health_alert can stay silent
                             until something actually breaks
  3. UNDATED               — a log stamped [HH:MM:SS] with no date is
                             unauditable. chronicle-sentinel.log is 446,270
                             lines of exactly this. You cannot ask "when".

Failure is LOUD. A log that cannot be read prints UNREADABLE, never 0 errors.
An absent watermark file prints FIRST RUN, never "no new errors".

Usage:
  log_survey.py                 full survey, worst first
  log_survey.py --new           only errors since last run (health_alert mode)
  log_survey.py --undated       only logs with no parseable date
  log_survey.py --orphans       only logs nothing reads
  log_survey.py --file PATH     taxonomy for one log
"""
import os, re, sys, json, glob, time

ROOT   = os.path.expanduser("~/chronicle")
WMFILE = os.path.join(ROOT, "data", "log_watermarks.json")

# Lines that mean something went wrong. Deliberately narrow: 'error' as a bare
# substring matches 'error_rate' and every prose mention, which floods the
# signal until nobody reads THIS either.
ERR = re.compile(
    r"(Traceback \(most recent call last\)"
    r"|^[A-Za-z_0-9.]*(?:Error|Exception): "
    r"|\bCRITICAL\b|\bFATAL\b"
    r"|\bFAILED\b|\bfailed to\b)", re.M)

DATE = re.compile(r"\b20[0-9]{2}-[01][0-9]-[0-3][0-9]\b")
FINAL = re.compile(r"([A-Za-z_0-9.]*(?:Error|Exception): .{0,60})")


def logs():
    seen, out = set(), []
    for pat in ("*.log", "logs/*.log", "data/*.log", "bin/*.log"):
        for p in glob.glob(os.path.join(ROOT, pat)):
            rp = os.path.realpath(p)
            if rp in seen:
                continue
            seen.add(rp)
            out.append(p)
    return sorted(out)


def readers(path):
    """Who greps/tails/opens this log, other than whoever writes it."""
    name = os.path.basename(path)
    hits = []
    for f in glob.glob(os.path.join(ROOT, "bin", "*.py")) + \
             glob.glob(os.path.join(ROOT, "bin", "*.sh")):
        try:
            with open(f, "r", errors="replace") as fh:
                if name in fh.read():
                    hits.append(os.path.basename(f))
        except OSError:
            continue
    return hits


def scan(path, start=0):
    """Returns (nerr, taxonomy, size, dated, err) — err is a string or None."""
    try:
        size = os.path.getsize(path)
    except OSError as e:
        return None, {}, 0, None, f"cannot stat: {e}"
    if start > size:            # truncated or rotated under us
        start = 0
    try:
        with open(path, "r", errors="replace") as fh:
            fh.seek(start)
            body = fh.read()
    except OSError as e:
        return None, {}, size, None, f"cannot read: {e}"
    tax = {}
    for m in FINAL.findall(body):
        k = m.strip()
        tax[k] = tax.get(k, 0) + 1
    return len(ERR.findall(body)), tax, size, bool(DATE.search(body)), None


def load_wm():
    if not os.path.exists(WMFILE):
        return None                      # None means FIRST RUN, not empty
    try:
        with open(WMFILE) as f:
            return json.load(f)
    except (OSError, ValueError) as e:
        print(f"WATERMARKS UNREADABLE ({e}) — treating as first run, "
              f"NOT as 'no new errors'", file=sys.stderr)
        return None


def save_wm(wm):
    os.makedirs(os.path.dirname(WMFILE), exist_ok=True)
    tmp = WMFILE + ".tmp"
    with open(tmp, "w") as f:
        json.dump(wm, f, indent=1, sort_keys=True)
    os.replace(tmp, WMFILE)


def mode_new():
    """Only what appeared since last run. Silent when genuinely clean."""
    wm = load_wm()
    first = wm is None
    wm = wm or {}
    new, out = {}, []
    for p in logs():
        k = os.path.basename(p)
        n, tax, size, _, err = scan(p, wm.get(k, 0))
        if err:
            out.append(f"  LOG UNREADABLE — {k}: {err}. This is NOT an all-clear.")
            new[k] = wm.get(k, 0)
            continue
        new[k] = size
        if n and not first:
            top = sorted(tax.items(), key=lambda x: -x[1])[:2]
            detail = "; ".join(f"{c}x {m}" for m, c in top) or "no typed exception"
            out.append(f"  {n} new error line(s) in {k} — {detail}")
    save_wm(new)
    if first:
        print(f"log_survey: first run, watermarked {len(new)} logs. "
              f"Next run reports only what is new.")
        return 0
    for line in out:
        print(line)
    return 1 if out else 0


def mode_survey(only=None):
    rows = []
    for p in logs():
        n, tax, size, dated, err = scan(p)
        rows.append((os.path.basename(p), p, n, tax, size, dated, err,
                     readers(p)))
    rows.sort(key=lambda r: -(r[2] or 0))

    print(f"=== LOG SURVEY — {len(rows)} logs under {ROOT} ===\n")
    for name, p, n, tax, size, dated, err, rd in rows:
        if err:
            print(f"{name}\n    UNREADABLE — {err}\n")
            continue
        # A log is an orphan if nothing but its own writer mentions it.
        stem = name.replace(".log", "")
        others = [r for r in rd if not r.startswith(stem)]
        if only == "orphans" and (others or not n):
            continue
        if only == "undated" and dated:
            continue
        age = time.time() - os.path.getmtime(p)
        flags = []
        if not dated:
            flags.append("UNDATED — cannot ask 'when'")
        if n and not others:
            flags.append("ERRORS NOBODY READS")
        if age < 3600:
            flags.append("LIVE")
        print(f"{name}  {size/1e6:.1f}MB  {n} err  "
              f"last {age/3600:.0f}h ago")
        if flags:
            print(f"    ** {' | '.join(flags)}")
        print(f"    readers: {', '.join(others) if others else 'NONE (writer only)'}")
        for m, c in sorted(tax.items(), key=lambda x: -x[1])[:3]:
            print(f"      {c:>6}x  {m}")
        print()
    return 0


if __name__ == "__main__":
    a = sys.argv[1:]
    if "--new" in a:
        sys.exit(mode_new())
    if "--file" in a:
        p = a[a.index("--file") + 1]
        n, tax, size, dated, err = scan(p)
        if err:
            print(f"UNREADABLE — {err}")
            sys.exit(2)
        print(f"{p}: {n} errors, {size/1e6:.1f}MB, "
              f"{'dated' if dated else 'UNDATED'}")
        for m, c in sorted(tax.items(), key=lambda x: -x[1]):
            print(f"  {c:>6}x  {m}")
        sys.exit(0)
    sys.exit(mode_survey("orphans" if "--orphans" in a else
                         "undated" if "--undated" in a else None))
