#!/usr/bin/env python3
"""Concentration analysis for multiplication task."""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))

import concentration
from pathlib import Path as _P
concentration.RUN_DIR = _P("/home/nate-agx/chronicle/experiments/grokking/runs/v2_mul_seed0")
concentration.SNAP_DIR = concentration.RUN_DIR / "snapshots"
concentration.CHECKPOINTS = [
    (500, "early"),
    (2000, "memorization check"),
    (3100, "grok onset"),
    (5000, "just-grokked"),
    (10000, "post-grok"),
    (20000, "post-grok stable"),
    (50000, "far post-grok"),
]
concentration.main()
