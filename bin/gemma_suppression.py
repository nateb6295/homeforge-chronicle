"""
Gemma suppression — rate-selective damping for crossref gate.

Layer 3 of the three-layer plasticity architecture (see
notes/gemma-3-architecture.md). Biological analog: PV+ fast-spiking
interneurons recruited by high-frequency input.

Pure function interface. No internal state. Caller provides recent
event timestamps and gets back a multiplier to apply to the gate
threshold. Higher multiplier = more suppressed = harder to widen.
"""

from __future__ import annotations

import math
import time
from dataclasses import dataclass


@dataclass(frozen=True)
class SuppressionParams:
    """Tunable parameters for the suppression head.

    threshold_rate: events per second above which damping kicks in.
    window_seconds: sliding window used to compute current rate.
    max_multiplier: ceiling on threshold inflation when fully engaged.
                    1.0 = no suppression, 3.0 = gate threshold tripled.
    half_life_seconds: exponential decay half-life for event contribution
                       to the rate estimate. Events older than ~3 * this
                       contribute negligibly.
    """

    threshold_rate: float = 1.0
    window_seconds: float = 60.0
    max_multiplier: float = 3.0
    half_life_seconds: float = 30.0


def _exp_weighted_rate(
    event_timestamps: list[float],
    now: float,
    window: float,
    half_life: float,
) -> float:
    """Events per second with exponential decay on older events."""
    if not event_timestamps:
        return 0.0
    decay_lambda = math.log(2.0) / max(half_life, 1e-6)
    weighted = 0.0
    for ts in event_timestamps:
        age = now - ts
        if age < 0 or age > window:
            continue
        weighted += math.exp(-decay_lambda * age)
    return weighted / max(window, 1e-6)


def compute_suppression(
    event_timestamps: list[float],
    params: SuppressionParams = SuppressionParams(),
    now: float | None = None,
) -> float:
    """Return a gate-threshold multiplier in [1.0, max_multiplier].

    Multiplier of 1.0 means pass through unchanged (no suppression).
    Above threshold_rate, multiplier grows toward max_multiplier as
    the rate climbs. Grows sub-linearly (saturating) so a single burst
    doesn't slam the ceiling — preserves the 'inhibition-on-demand
    without permanent dampening' property.
    """
    if now is None:
        now = time.time()

    rate = _exp_weighted_rate(
        event_timestamps, now, params.window_seconds, params.half_life_seconds
    )
    if rate <= params.threshold_rate:
        return 1.0

    overdrive = rate / params.threshold_rate  # >1.0 by construction
    # Saturating curve: multiplier = 1 + (max - 1) * (1 - exp(-(overdrive - 1)))
    # Reaches ~63% of (max - 1) at overdrive = 2.0x threshold.
    decay = 1.0 - math.exp(-(overdrive - 1.0))
    multiplier = 1.0 + (params.max_multiplier - 1.0) * decay
    return min(multiplier, params.max_multiplier)


def _selftest() -> None:
    """Smoke tests — run with `python3 gemma_suppression.py`."""
    now = 1000.0

    # Empty history: no suppression.
    assert compute_suppression([], now=now) == 1.0

    # A single recent event: below threshold (1 event / 60s window).
    assert compute_suppression([now - 1.0], now=now) == 1.0

    # Dense burst within window: should trip the multiplier above 1.
    burst = [now - i * 0.5 for i in range(120)]  # 120 events in 60s
    m_burst = compute_suppression(burst, now=now)
    assert m_burst > 1.0, f"expected suppression, got {m_burst}"
    assert m_burst <= SuppressionParams().max_multiplier

    # Old events outside the window: ignored.
    stale = [now - 3600.0 - i for i in range(100)]
    assert compute_suppression(stale, now=now) == 1.0

    # Monotonicity: higher rate -> higher multiplier.
    low = [now - i * 2.0 for i in range(20)]  # 20 events / 60s
    high = [now - i * 0.2 for i in range(200)]  # 200 events / 60s
    m_low = compute_suppression(low, now=now)
    m_high = compute_suppression(high, now=now)
    assert m_high > m_low, f"{m_high} should exceed {m_low}"

    # Exponential weighting: recent burst dominates old quiet.
    spread = [now - i * 3.0 for i in range(20)]  # 20 spread over 60s
    cluster = [now - i * 0.5 for i in range(20)]  # 20 clustered in last 10s
    m_spread = compute_suppression(spread, now=now)
    m_cluster = compute_suppression(cluster, now=now)
    assert m_cluster >= m_spread, f"cluster {m_cluster} < spread {m_spread}"

    print("gemma_suppression: all self-tests passed")


if __name__ == "__main__":
    _selftest()
