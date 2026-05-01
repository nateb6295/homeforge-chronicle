#!/usr/bin/env python3
"""Anatomy analysis for multiplication task."""
import sys
from pathlib import Path as _P
sys.path.insert(0, str(_P(__file__).parent))
import anatomy
anatomy.SNAP_DIR = _P("/home/nate-agx/chronicle/experiments/grokking/runs/v2_mul_seed0/snapshots")
anatomy.RUN_DIR = _P("/home/nate-agx/chronicle/experiments/grokking/runs/v2_mul_seed0")
anatomy.CHECKPOINTS = [2000, 10000, 20000, 50000]
anatomy.main()
