#!/usr/bin/env python3
"""Concentration analysis for subtraction task."""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))

# Monkeypatch RUN_DIR before importing
import concentration
from pathlib import Path as _P
concentration.RUN_DIR = _P("/home/nate-agx/chronicle/experiments/grokking/runs/v2_sub_seed0")
concentration.SNAP_DIR = concentration.RUN_DIR / "snapshots"
concentration.CHECKPOINTS = [
    (500, "early mem onset (train=0.965, val=0.000)"),
    (2000, "pure memorization (train=0.998, val=0.001)"),
    (5000, "still memorization (train=1.000, val=0.014)"),
    (7500, "grokking! (train=1.000, val=0.975)"),
    (10000, "just-grokked (train=1.000, val=1.000)"),
    (20000, "post-grok (train=1.000, val=1.000)"),
    (50000, "far post-grok (train=1.000, val=1.000)"),
]
concentration.main()
