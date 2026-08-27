#!/usr/bin/env python3
"""Scan files or a git range for credentials BEFORE publishing them.

Built 2026-08-25, an hour after my ad-hoc version passed a set containing a live
Hugging Face token and I told Nate it was clean. GitHub push protection rejected
the push and named the five files. I had checked ghp_, sk-, AKIA, xoxb- and
PRIVATE KEY — the five formats I happened to think of — and treated "no hits" as
"no secrets".

READ THIS BEFORE TRUSTING A CLEAN RESULT:

    A pattern list can only find what someone already thought of. This file has
    ~20 patterns and there are hundreds of credential formats. A clean run here
    means "none of these twenty appear", NOT "there are no secrets". It is a
    tripwire, not a proof.

    GitHub's push protection is strictly better than this and runs server-side.
    If you are pushing to GitHub, that is the real check and this is a courtesy
    pass so you find out before the rejection rather than after.

Usage:
  secret_sweep.py FILE...                 scan specific files
  secret_sweep.py --range origin/master   scan files changed vs a git ref
  secret_sweep.py --staged                scan what is staged
  secret_sweep.py --dir PATH              scan a tree (skips .git)
"""
import argparse, os, re, subprocess, sys

# name, regex. Kept deliberately narrow — a loose pattern produces the OTHER
# failure from the same day, where grepping "ghp_" matched a conversation ABOUT
# tokens and I nearly reported it as a credential.
PATTERNS = [
    ("HuggingFace token",      r"hf_[A-Za-z0-9]{30,}"),
    ("GitHub PAT (classic)",   r"gh[pousr]_[A-Za-z0-9]{36}"),
    ("GitHub PAT (fine)",      r"github_pat_[A-Za-z0-9_]{60,}"),
    ("OpenAI-style key",       r"sk-[A-Za-z0-9_-]{20,}"),
    ("Anthropic key",          r"sk-ant-[A-Za-z0-9_-]{20,}"),
    ("AWS access key id",      r"AKIA[0-9A-Z]{16}"),
    ("Slack token",            r"xox[baprs]-[A-Za-z0-9-]{10,}"),
    ("Google API key",         r"AIza[0-9A-Za-z_-]{35}"),
    ("GitLab PAT",             r"glpat-[A-Za-z0-9_-]{20,}"),
    ("Replicate token",        r"r8_[A-Za-z0-9]{37}"),
    ("NVIDIA API key",         r"nvapi-[A-Za-z0-9_-]{30,}"),
    ("Private key block",      r"-----BEGIN [A-Z ]*PRIVATE KEY"),
    ("Bearer literal",         r"Bearer\s+[A-Za-z0-9_\-\.]{30,}"),
    ("Discord bot token",      r"[MNO][A-Za-z0-9_-]{23}\.[A-Za-z0-9_-]{6}\.[A-Za-z0-9_-]{27}"),
    ("Discord webhook",        r"discord(?:app)?\.com/api/webhooks/\d{17,}/[A-Za-z0-9_-]{60,}"),
    ("Telegram bot token",     r"\d{8,10}:[A-Za-z0-9_-]{35}"),
    ("Stripe secret",          r"sk_live_[A-Za-z0-9]{24,}"),
    ("Generic assignment",     r"(?i)(api[_-]?key|secret|passwd|password|token)\s*[:=]\s*[\"'][A-Za-z0-9_\-]{24,}[\"']"),
]
COMPILED = [(n, re.compile(p)) for n, p in PATTERNS]
MAX_BYTES = 2 * 1024 * 1024   # files over this are reported as SKIPPED, never as clean
SKIP_EXT = {".png", ".jpg", ".jpeg", ".gif", ".pdf", ".zip", ".gz", ".safetensors",
            ".bin", ".pt", ".pth", ".npy", ".npz", ".woff", ".woff2", ".ico"}


def files_from_range(ref):
    r = subprocess.run(["git", "diff", "--diff-filter=ACM", "--name-only", f"{ref}..HEAD"],
                       capture_output=True, text=True)
    return [f for f in r.stdout.split("\n") if f.strip()]


def files_staged():
    r = subprocess.run(["git", "diff", "--cached", "--diff-filter=ACM", "--name-only"],
                       capture_output=True, text=True)
    return [f for f in r.stdout.split("\n") if f.strip()]


def files_in_dir(d):
    out = []
    for dp, dns, fns in os.walk(d):
        dns[:] = [x for x in dns if x not in (".git", "node_modules", "__pycache__")]
        for f in fns:
            if os.path.splitext(f)[1].lower() not in SKIP_EXT:
                out.append(os.path.join(dp, f))
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("files", nargs="*")
    ap.add_argument("--range", metavar="REF")
    ap.add_argument("--staged", action="store_true")
    ap.add_argument("--dir", metavar="PATH")
    a = ap.parse_args()

    files = list(a.files)
    if a.range:  files += files_from_range(a.range)
    if a.staged: files += files_staged()
    if a.dir:    files += files_in_dir(a.dir)
    if not files:
        print("nothing to scan — pass files, --range, --staged or --dir", file=sys.stderr)
        return 2

    hits, scanned, unreadable, skipped_big = [], 0, 0, []
    for f in files:
        if os.path.splitext(f)[1].lower() in SKIP_EXT or not os.path.isfile(f):
            continue
        try:
            # SYMLINKS: skip, and never size them by following. os.path.getsize
            # follows the link, so on 2026-08-25 this reported an 81-byte
            # published symlink as a 1,579 MB file "NOT scanned, NOT cleared"
            # — because it pointed at a local archive. A warning that cries
            # wolf is the same defect as a check that stays silent.
            if os.path.islink(f):
                continue
            sz = os.path.getsize(f)
            if sz > MAX_BYTES:
                skipped_big.append((f, sz))
                continue
            # Skip BINARIES by content, not extension. bin/kinic/kinic-cli has no
            # extension and is a 100k-line compiled blob; scanning its string
            # table produced a false "OpenAI-style key" on 2026-08-25. A loose
            # match inside binary noise is the same defect as matching prose.
            with open(f, "rb") as bh:
                head = bh.read(8192)
            if b"\x00" in head:
                continue
            # Whole-file search, not per-line. Line-by-line with 18 regexes timed
            # out on 3,590 tracked files — and a scanner too slow to run is the
            # exact defect this whole day has been about.
            with open(f, errors="replace") as fh:
                body = fh.read()
            for name, rx in COMPILED:
                for m in rx.finditer(body):
                    i = body.count("\n", 0, m.start()) + 1
                    ls = body.rfind("\n", 0, m.start()) + 1
                    le = body.find("\n", m.start())
                    line = body[ls:le if le != -1 else len(body)]
                    red = rx.sub(lambda mm: mm.group(0)[:6] + "<REDACTED>", line.strip())
                    hits.append((f, i, name, red[:110]))
                    break          # one report per pattern per file is enough
            scanned += 1
        except OSError:
            unreadable += 1

    if skipped_big:
        # A skipped file must never be silently counted as clean.
        print(f"SKIPPED {len(skipped_big)} file(s) over "
              f"{MAX_BYTES//1024//1024}MB — NOT scanned, NOT cleared:", file=sys.stderr)
        for f, sz in sorted(skipped_big, key=lambda x: -x[1])[:5]:
            print(f"    {sz/1e6:>7.1f} MB  {f}", file=sys.stderr)

    if unreadable:
        # Never let an unreadable file silently become a pass.
        print(f"WARNING: {unreadable} file(s) could not be read. NOT an all-clear.",
              file=sys.stderr)

    if hits:
        print(f"SECRETS FOUND — {len(hits)} in {scanned} files scanned\n")
        for f, i, name, red in hits:
            print(f"  {f}:{i}")
            print(f"    {name}: {red}")
        print("\nRemove these from the WORKING TREE and from every commit being")
        print("published — a clean tip with the secret still in history is not clean.")
        return 1

    print(f"No match across {len(PATTERNS)} patterns in {scanned} files.")
    print("This is a TRIPWIRE, not a proof: a pattern list only finds formats")
    print("someone already thought of. On 2026-08-25 a five-pattern version of")
    print("this passed a set containing a live HuggingFace token.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
