#!/usr/bin/env python3
"""drive_fetch.py — download a Google Drive file by share link or file ID.

Works for files set to "Anyone with the link can view." For private files,
need the Google Drive MCP OAuth path instead.

Usage:
    python3 drive_fetch.py "https://drive.google.com/file/d/FILE_ID/view?usp=sharing"
    python3 drive_fetch.py FILE_ID
    python3 drive_fetch.py FILE_ID --output /tmp/somefile.pdf

Pattern verified 2026-04-29 after Nate shared a Nature Neuroscience PDF.
The /uc?export=download endpoint returns the binary directly when sharing
is set to anyone-with-link.
"""
from __future__ import annotations

import argparse
import re
import subprocess
import sys
import tempfile
import urllib.parse


def extract_file_id(arg: str) -> str:
    """Extract Drive file ID from share URL or pass through if already ID."""
    if "/" not in arg and "?" not in arg:
        return arg
    m = re.search(r"/file/d/([a-zA-Z0-9_-]+)", arg)
    if m:
        return m.group(1)
    m = re.search(r"[?&]id=([a-zA-Z0-9_-]+)", arg)
    if m:
        return m.group(1)
    raise ValueError(f"Could not extract file ID from {arg!r}")


def main() -> int:
    p = argparse.ArgumentParser(description="Download a Google Drive file by ID or share URL")
    p.add_argument("source", help="share URL or file ID")
    p.add_argument("--output", "-o", help="output path (default: /tmp/drive_<id>.bin)")
    p.add_argument("--keep-ext", action="store_true",
                   help="try to detect content-type and use a sensible extension")
    args = p.parse_args()

    try:
        file_id = extract_file_id(args.source)
    except ValueError as e:
        print(f"ERROR: {e}", file=sys.stderr)
        return 1

    output = args.output or f"/tmp/drive_{file_id}.bin"
    url = f"https://drive.google.com/uc?export=download&id={file_id}"

    print(f"Downloading {url} → {output}")
    result = subprocess.run(
        ["curl", "-sL", url, "-o", output],
        capture_output=True, text=True, timeout=120,
    )
    if result.returncode != 0:
        print(f"ERROR: curl exited {result.returncode}: {result.stderr}", file=sys.stderr)
        return 2

    # Detect file type
    ftype = subprocess.run(["file", "-b", output], capture_output=True, text=True).stdout.strip()
    size = subprocess.run(["stat", "-c", "%s", output], capture_output=True, text=True).stdout.strip()
    print(f"Downloaded: {size} bytes")
    print(f"Type: {ftype}")

    # If --keep-ext and we got a PDF, rename
    if args.keep_ext and "PDF" in ftype.upper() and not output.endswith(".pdf"):
        new_output = output.rsplit(".", 1)[0] + ".pdf"
        subprocess.run(["mv", output, new_output])
        print(f"Renamed: {new_output}")
        output = new_output

    # If the file is small + HTML, the share probably isn't public — surface this
    if int(size) < 4096 and "HTML" in ftype.upper():
        print()
        print("WARN: small HTML response suggests Drive returned a virus-scan or auth gate,")
        print("not the actual file. Confirm sharing is set to 'Anyone with the link can view.'")
        return 3

    print(f"\nReady to read: {output}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
