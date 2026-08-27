#!/usr/bin/env python3
"""Shared working context for the mesh agents.

One file, three agents. Before this existed, each of kimi_agent / groq_agent /
ox_agent carried its own hardcoded copy of a block headed "GROUND TRUTH -- the
team's established findings (use these to evaluate claims)". Two problems with
that: the copies drifted, and the framing told every agent to treat provisional
claims as the yardstick. Qwen in particular answered a request to falsify a
claim by mapping it onto F-numbers instead.

The file it loads is ~/chronicle/data/mesh_context.md, which now carries a
retraction list alongside the working claims.
"""

from pathlib import Path

CONTEXT_FILE = Path.home() / "chronicle" / "data" / "mesh_context.md"

_FALLBACK = (
    "Working context file is missing. Reason from the data in the post itself "
    "and from published literature. Do not assume any internal finding is settled."
)


def load_context():
    try:
        text = CONTEXT_FILE.read_text().strip()
        return ("\n" + text + "\n\n") if text else ("\n" + _FALLBACK + "\n\n")
    except OSError:
        return "\n" + _FALLBACK + "\n\n"


if __name__ == "__main__":
    print(load_context())
