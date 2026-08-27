#!/usr/bin/env python3
"""Single source of truth for locating the dfx binary.

dfx installs to ~/.local/share/dfx/bin/dfx, which is added to PATH by the
shell profile. Anything launched by systemd, a timer, or a cron does NOT get
that profile, so a bare "dfx" in a subprocess call raises FileNotFoundError.

The failure is silent wherever the canister write is a best-effort side path
next to a local SQLite write -- the local write succeeds, the canister write
is skipped, and nothing looks broken until you compare counts.

Import this instead of hardcoding "dfx":

    from dfx_path import DFX_BIN
    subprocess.run([DFX_BIN, "canister", "call", ...])
"""

import os
from shutil import which

_FALLBACKS = (
    "~/.local/share/dfx/bin/dfx",
    "~/bin/dfx",
    "/usr/local/bin/dfx",
)


def dfx_bin():
    found = which("dfx")
    if found:
        return found
    for candidate in _FALLBACKS:
        path = os.path.expanduser(candidate)
        if os.path.exists(path):
            return path
    return "dfx"  # let the caller surface a clear FileNotFoundError


DFX_BIN = dfx_bin()


if __name__ == "__main__":
    print(DFX_BIN)
    print("on PATH" if which("dfx") else "NOT on PATH -- resolved via fallback")
