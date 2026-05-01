#!/usr/bin/env python3
"""Build #72: Temporal Drift Detector.

Born from Thread #292 'The Silence That Grows' and Darby's build proposal #2980.
Tracks trajectories over time — not point-in-time snapshots.

Detects:
  1. Novelty trajectory: Is average novelty score declining over time?
  2. Response diversity: What fraction of high-novelty inputs get routed deep vs ignored?
  3. Domain temperature trajectory: Are domains cooling without notice?
  4. Productive novelty: Do high-novelty inputs generate downstream signal?
  5. Convergence derivative: Is swarm diversity contracting over time?

Key insight from Thread #292: the system can have healthy input diversity and
unhealthy response diversity simultaneously. Point-in-time metrics miss this.
"""

import json
import sqlite3
import sys
import time

DB_PATH = "/mnt/hdd/chronicle-data/processed.db"

# Time windows for trajectory analysis
WINDOWS = {
    "1h": 3600,
    "3h": 3600 * 3,
    "6h": 3600 * 6,
    "12h": 3600 * 12,
    "24h": 3600 * 24,
    "3d": 3600 * 72,
    "7d": 3600 * 168,
}


def _db():
    conn = sqlite3.connect(DB_PATH, timeout=10)
    conn.row_factory = sqlite3.Row
    return conn


def novelty_trajectory():
    """Track novelty across time windows.

    Refined: average novelty is dominated by feed volume (feeds are 85%+ of seeds
    but only 4% high-novelty). The more robust metric is high_novelty_rate —
    the rate of genuinely novel discoveries per hour, independent of feed churn.
    Slope is computed from high_novelty_rate, not avg_novelty.
    """
    conn = _db()
    now = int(time.time())
    result = {}
    for label, seconds in WINDOWS.items():
        cutoff = now - seconds
        row = conn.execute(
            "SELECT count(*) as cnt, avg(novelty_score) as avg_nov, "
            "max(novelty_score) as max_nov, "
            "count(CASE WHEN novelty_score > 0.1 THEN 1 END) as high_cnt "
            "FROM seed_observations WHERE timestamp > ?",
            (cutoff,)
        ).fetchone()
        if row and row["cnt"] > 0:
            hours = max(seconds / 3600, 0.1)
            result[label] = {
                "count": row["cnt"],
                "avg_novelty": round(row["avg_nov"], 4),
                "max_novelty": round(row["max_nov"], 4),
                "high_novelty_count": row["high_cnt"],
                "high_novelty_rate": round(row["high_cnt"] / hours, 2),
            }
    conn.close()

    # Compute slope from high_novelty_rate (discoveries/hour), not avg_novelty
    # This is robust to feed volume changes and capture cadence
    windows_ordered = ["1h", "3h", "6h", "12h", "24h", "3d", "7d"]
    vals = [(w, result[w]["high_novelty_rate"]) for w in windows_ordered if w in result]
    slope = "stable"
    if len(vals) >= 2:
        short_rate = vals[0][1]
        long_rate = vals[-1][1]
        if long_rate > 0:
            if short_rate > long_rate * 1.3:
                slope = "rising"
            elif short_rate < long_rate * 0.5:
                slope = "falling"

    return {"windows": result, "slope": slope}


def response_diversity():
    """Track what fraction of high-novelty inputs get routed deep vs ignored.
    Thread #292 key finding: 49.6% of high-novelty inputs silently ignored.
    If this percentage climbs, the system is becoming more conservative.
    """
    conn = _db()
    now = int(time.time())
    result = {}

    for label, seconds in WINDOWS.items():
        cutoff = now - seconds
        rows = conn.execute(
            "SELECT srl.route, count(*) as cnt "
            "FROM seed_observations so "
            "JOIN seed_routing_log srl ON srl.observation_id = so.id "
            "WHERE so.timestamp > ? AND so.novelty_score > 0.1 "
            "GROUP BY srl.route",
            (cutoff,)
        ).fetchall()
        if rows:
            routes = {r["route"]: r["cnt"] for r in rows}
            total = sum(routes.values())
            deep = routes.get("deep", 0)
            ignored = routes.get("ignore", 0)
            result[label] = {
                "total_high_novelty": total,
                "deep_rate": round(deep / total, 3) if total else 0,
                "ignore_rate": round(ignored / total, 3) if total else 0,
                "deep": deep,
                "ignored": ignored,
            }
    conn.close()

    # Trend: is ignore_rate for high-novelty climbing?
    windows_ordered = ["1h", "3h", "6h", "12h", "24h", "3d", "7d"]
    ignore_rates = [(w, result[w]["ignore_rate"]) for w in windows_ordered
                    if w in result and result[w]["total_high_novelty"] > 5]
    trend = "stable"
    if len(ignore_rates) >= 2:
        short_ignore = ignore_rates[0][1]
        long_ignore = ignore_rates[-1][1]
        if short_ignore > long_ignore + 0.1:
            trend = "silencing"  # Getting more conservative
        elif short_ignore < long_ignore - 0.1:
            trend = "opening"  # Getting more permissive

    return {"windows": result, "trend": trend}


def productive_novelty():
    """Measure whether high-novelty inputs generate downstream signal.
    High novelty + high downstream signal = genuine discovery.
    High novelty + low downstream signal = silence wearing a mask.
    Connects correction_yield (Build #69) with novelty measurement.
    """
    conn = _db()
    now = int(time.time())
    cutoff_7d = now - WINDOWS["7d"]

    # High-novelty seeds in last 7 days
    high_nov = conn.execute(
        "SELECT count(*) as cnt FROM seed_observations "
        "WHERE timestamp > ? AND novelty_score > 0.1",
        (cutoff_7d,)
    ).fetchone()["cnt"]

    # How many generated crossref connections?
    connected = conn.execute(
        "SELECT count(DISTINCT so.id) as cnt "
        "FROM seed_observations so "
        "JOIN crossref_connections cc ON (cc.brief_a_id = so.id OR cc.brief_b_id = so.id) "
        "WHERE so.timestamp > ? AND so.novelty_score > 0.1",
        (cutoff_7d,)
    ).fetchone()["cnt"]

    # How many generated voice mentions?
    # Count activity_feed entries referencing high-novelty seed content
    voiced = conn.execute(
        "SELECT count(DISTINCT so.id) as cnt "
        "FROM seed_observations so "
        "JOIN seed_routing_log srl ON srl.observation_id = so.id "
        "WHERE so.timestamp > ? AND so.novelty_score > 0.1 "
        "AND srl.route = 'deep' AND srl.feedback_score > 0",
        (cutoff_7d,)
    ).fetchone()["cnt"]

    conn.close()

    productive_rate = round(connected / max(1, high_nov), 4)
    return {
        "high_novelty_seeds_7d": high_nov,
        "generated_crossref": connected,
        "positive_feedback": voiced,
        "productive_rate": productive_rate,
        "assessment": "healthy" if productive_rate > 0.02 else "low signal"
    }


def domain_temperature_trajectory():
    """Track domain temperatures over time.
    Cooling domains without shocks = attention fading.
    """
    conn = _db()
    rows = conn.execute(
        "SELECT domain, temperature, direction, last_shock_at, "
        "half_life_seconds, updated_at FROM domain_temperature"
    ).fetchall()
    conn.close()

    now = int(time.time())
    temps = []
    for r in rows:
        age_hours = round((now - r["updated_at"]) / 3600, 1)
        # Compute effective temperature with decay
        elapsed = now - r["last_shock_at"]
        half_life = r["half_life_seconds"] or 7200
        decays = elapsed / half_life
        effective = r["temperature"] * (0.5 ** decays)
        temps.append({
            "domain": r["domain"],
            "nominal": r["temperature"],
            "effective": round(effective, 3),
            "direction": r["direction"],
            "hours_since_shock": round(elapsed / 3600, 1),
            "cooling": effective < 0.5 and r["temperature"] >= 1.0,
        })

    cooling_count = sum(1 for t in temps if t["cooling"])
    return {
        "domains": temps,
        "cooling_domains": cooling_count,
        "total_domains": len(temps),
    }


def social_attention():
    """Layer 4: Social colonization in family attention.
    Measures per-agent, per-voice-type response rates.
    Detects asymmetric conversations and ignored voice types.

    Key findings on deployment:
    - Ada→Darby: 1.9% response rate (near-dead channel)
    - Darby→Ada: 50.5% response rate (active)
    - Proposals: 84.6% engagement. Questions: 21.1%.
    - Opus→family: 0% response rate
    """
    conn = _db()
    now = int(time.time())
    result = {}

    for label, seconds in [("7d", WINDOWS["7d"]), ("3d", WINDOWS["3d"]),
                            ("24h", WINDOWS["24h"])]:
        cutoff = now - seconds
        rows = conn.execute(
            "SELECT agent, voice_type, "
            "count(*) as total, "
            "count(CASE WHEN response IS NOT NULL THEN 1 END) as responded "
            "FROM agent_voice "
            "WHERE created_at > ? "
            "GROUP BY agent, voice_type "
            "HAVING total > 2",
            (cutoff,)
        ).fetchall()

        channels = []
        for r in rows:
            pct = round(100.0 * r["responded"] / r["total"], 1)
            channels.append({
                "agent": r["agent"],
                "voice_type": r["voice_type"],
                "total": r["total"],
                "responded": r["responded"],
                "response_pct": pct,
            })
        result[label] = channels

    conn.close()

    # Detect dead channels and asymmetries
    dead_channels = []
    asymmetries = []
    week = result.get("7d", [])
    for ch in week:
        if ch["total"] >= 5 and ch["response_pct"] < 5.0:
            dead_channels.append(
                f"{ch['agent']}→{ch['voice_type']}: "
                f"{ch['response_pct']}% ({ch['total']} voices)"
            )

    # Check for directional asymmetry (A→B vs B→A)
    pairs = {}
    for ch in week:
        if ch["voice_type"].startswith("for_"):
            target = ch["voice_type"][4:]
            key = (ch["agent"], target)
            pairs[key] = ch["response_pct"]

    for (a, b), pct_ab in pairs.items():
        reverse = (b, a)
        if reverse in pairs:
            pct_ba = pairs[reverse]
            if abs(pct_ab - pct_ba) > 30:
                asymmetries.append(
                    f"{a}→{b}: {pct_ab}% vs {b}→{a}: {pct_ba}%"
                )

    return {
        "windows": result,
        "dead_channels": dead_channels,
        "asymmetries": asymmetries,
        "alert": len(dead_channels) > 0 or len(asymmetries) > 0,
    }


def convergence_derivative():
    """Extend Build #70: measure diversity slope over multiple windows.
    Instead of current vs baseline, compute the derivative across all windows.
    """
    try:
        sys.path.insert(0, "/home/nate-agx/chronicle/bin")
        from swarm_alignment import drift_alert
        fired, report = drift_alert()
        return {"alert_fired": fired, "report": report}
    except Exception as e:
        return {"error": str(e)}


def alert(novelty_threshold=0.8, silence_threshold=0.6):
    """Unified alert: fires if any trajectory indicates drift.

    Returns (alert_fired: bool, reasons: list[str], report: str)
    """
    reasons = []

    # 1. Novelty declining
    nov = novelty_trajectory()
    if nov["slope"] == "falling":
        reasons.append("Novelty trajectory FALLING — embedding space may be saturating")

    # 2. Response silence growing
    resp = response_diversity()
    if resp["trend"] == "silencing":
        reasons.append("Response silence GROWING — more high-novelty inputs being ignored")
    # Also check absolute ignore rate in short window
    short_windows = ["1h", "3h"]
    for w in short_windows:
        if w in resp["windows"]:
            wd = resp["windows"][w]
            if wd["total_high_novelty"] > 3 and wd["ignore_rate"] > silence_threshold:
                reasons.append(
                    f"High-novelty ignore rate {wd['ignore_rate']:.0%} in {w} "
                    f"(threshold: {silence_threshold:.0%})"
                )
            break

    # 3. Productive novelty low
    prod = productive_novelty()
    if prod["assessment"] == "low signal":
        reasons.append(
            f"Productive novelty LOW — only {prod['productive_rate']:.1%} of "
            f"high-novelty inputs generating crossref connections"
        )

    # 4. Domain temperatures cooling
    dt = domain_temperature_trajectory()
    if dt["cooling_domains"] > dt["total_domains"] * 0.5:
        reasons.append(
            f"{dt['cooling_domains']}/{dt['total_domains']} domains effectively cold"
        )
    # Auto-trigger thermal recovery for any cooling domain
    if dt["cooling_domains"] > 0:
        try:
            recovery = thermal_recovery()
            if recovery["injected_domains"]:
                reasons.append(
                    f"Thermal recovery: injected keywords for {', '.join(recovery['injected_domains'])}"
                )
        except Exception:
            pass

    # 5. Convergence derivative
    conv = convergence_derivative()
    if conv.get("alert_fired"):
        reasons.append("Swarm diversity convergence detected")

    # 6. Social attention (Layer 4)
    social = social_attention()
    if social["dead_channels"]:
        reasons.append(
            f"Social dead channels: {'; '.join(social['dead_channels'][:2])}"
        )

    fired = len(reasons) > 0

    # Build report
    lines = ["=== Temporal Drift Report ===", ""]
    lines.append(f"Novelty slope: {nov['slope']}")
    for w in ["1h", "6h", "24h", "7d"]:
        if w in nov["windows"]:
            n = nov["windows"][w]
            lines.append(
                f"  {w}: avg={n['avg_novelty']:.4f} (n={n['count']}) "
                f"discoveries={n.get('high_novelty_count', '?')}/hr={n.get('high_novelty_rate', '?')}"
            )

    lines.append(f"\nResponse diversity trend: {resp['trend']}")
    for w in ["1h", "6h", "24h", "7d"]:
        if w in resp["windows"]:
            r = resp["windows"][w]
            lines.append(
                f"  {w}: deep={r['deep_rate']:.1%} ignore={r['ignore_rate']:.1%} "
                f"(n={r['total_high_novelty']})"
            )

    lines.append(f"\nProductive novelty: {prod['assessment']}")
    lines.append(
        f"  {prod['high_novelty_seeds_7d']} high-novelty seeds → "
        f"{prod['generated_crossref']} crossref, "
        f"{prod['positive_feedback']} positive feedback"
    )

    lines.append(f"\nDomain temperatures: {dt['cooling_domains']}/{dt['total_domains']} cooling")
    for d in dt["domains"]:
        flag = " ⚠️ COOLING" if d["cooling"] else ""
        lines.append(
            f"  {d['domain']}: {d['effective']:.2f} "
            f"(nominal {d['nominal']}, {d['hours_since_shock']:.0f}h since shock){flag}"
        )

    if conv.get("report"):
        lines.append(f"\nConvergence: {conv['report'][:200]}")

    lines.append(f"\nSocial attention: {len(social['dead_channels'])} dead channels, "
                 f"{len(social['asymmetries'])} asymmetries")
    for dc in social["dead_channels"][:3]:
        lines.append(f"  ⚠️ {dc}")
    for asym in social["asymmetries"][:3]:
        lines.append(f"  ↔ {asym}")

    if fired:
        lines.append(f"\n⚠️ ALERT: {len(reasons)} drift signal(s):")
        for r in reasons:
            lines.append(f"  • {r}")
    else:
        lines.append("\n✓ No drift detected.")

    report = "\n".join(lines)
    return fired, reasons, report


def thermal_recovery():
    """Build #73: Auto-suggest keywords for cooling domains.
    Closes the Build #71 ↔ #72 loop.

    When a domain has been cooling for >24h (effective temp <0.1),
    inject domain-relevant keywords into family_suggestions for the
    algo seeker to pick up on its next 2h cycle.
    """
    DOMAIN_SEEDS = {
        "geopolitical": [
            "geopolitical conflict diplomacy sanctions",
            "international trade policy tariff",
            "military alliance defense cooperation",
        ],
        "markets": [
            "cryptocurrency DeFi protocol governance",
            "market volatility risk analysis",
            "blockchain infrastructure token economics",
        ],
        "research": [
            "neuroscience cognition computational model",
            "artificial intelligence safety alignment",
            "biology network topology emergence",
        ],
    }

    dt = domain_temperature_trajectory()
    conn = _db()
    now = int(time.time())
    injected = []

    for d in dt["domains"]:
        if not d["cooling"] or d["hours_since_shock"] < 24:
            continue
        if d["domain"] == "system":
            continue  # system domain doesn't need search recovery

        seeds = DOMAIN_SEEDS.get(d["domain"], [])
        if not seeds:
            continue

        # Check we haven't already suggested for this domain in the last 12h
        existing = conn.execute(
            "SELECT count(*) FROM family_suggestions "
            "WHERE agent='opus' AND rationale LIKE ? AND created_at > ?",
            (f"%thermal_recovery:{d['domain']}%", now - 43200)
        ).fetchone()[0]
        if existing > 0:
            continue

        # Pick one seed keyword at random
        import random
        seed = random.choice(seeds)
        conn.execute(
            "INSERT INTO family_suggestions "
            "(agent, suggestion_type, content, rationale, status, created_at) "
            "VALUES (?, 'keyword', ?, ?, 'pending', ?)",
            ("opus", seed,
             f"thermal_recovery:{d['domain']} — effective {d['effective']:.3f}, "
             f"{d['hours_since_shock']:.0f}h since shock",
             now)
        )
        injected.append(d["domain"])

    conn.commit()
    conn.close()
    return {"injected_domains": injected, "cooling": [d for d in dt["domains"] if d["cooling"]]}


def summary():
    """One-line summary for quick checks."""
    fired, reasons, _ = alert()
    if fired:
        return f"⚠️ DRIFT: {'; '.join(reasons[:2])}"
    nov = novelty_trajectory()
    resp = response_diversity()
    return (
        f"✓ Novelty {nov['slope']}, "
        f"response {resp['trend']}"
    )


if __name__ == "__main__":
    cmd = sys.argv[1] if len(sys.argv) > 1 else "report"

    if cmd == "report":
        fired, reasons, report = alert()
        print(report)
    elif cmd == "summary":
        print(summary())
    elif cmd == "novelty":
        data = novelty_trajectory()
        print(json.dumps(data, indent=2))
    elif cmd == "response":
        data = response_diversity()
        print(json.dumps(data, indent=2))
    elif cmd == "productive":
        data = productive_novelty()
        print(json.dumps(data, indent=2))
    elif cmd == "domains":
        data = domain_temperature_trajectory()
        print(json.dumps(data, indent=2))
    elif cmd == "social":
        data = social_attention()
        # Simplify for output — just show 7d summary
        print(f"Dead channels: {len(data['dead_channels'])}")
        for dc in data["dead_channels"]:
            print(f"  ⚠️ {dc}")
        print(f"Asymmetries: {len(data['asymmetries'])}")
        for asym in data["asymmetries"]:
            print(f"  ↔ {asym}")
    elif cmd == "json":
        result = {
            "novelty": novelty_trajectory(),
            "response_diversity": response_diversity(),
            "productive_novelty": productive_novelty(),
            "domains": domain_temperature_trajectory(),
            "social": social_attention(),
        }
        print(json.dumps(result, indent=2))
    elif cmd == "recover":
        result = thermal_recovery()
        if result["injected_domains"]:
            print(f"Thermal recovery: injected keywords for {', '.join(result['injected_domains'])}")
        else:
            cooling = [d["domain"] for d in result["cooling"]]
            if cooling:
                print(f"Cooling domains: {', '.join(cooling)} (already suggested or <24h)")
            else:
                print("No domains need thermal recovery.")
    else:
        print(f"Usage: {sys.argv[0]} [report|summary|novelty|response|productive|domains|social|recover|json]")
