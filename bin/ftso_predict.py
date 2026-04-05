#!/usr/bin/env python3
"""FTSO Directional Predictor — XRP price direction calls.

Reconnects the prediction pipeline that stopped Feb 10 when Mind was retired.
Uses Gemma 27B + swarm context to make 4-hour directional XRP predictions.
Settles expired predictions against actual price data.

Usage:
    ftso_predict.py predict    — Make a new 4-hour directional prediction
    ftso_predict.py settle     — Settle all expired predictions
    ftso_predict.py cycle      — Settle first, then predict (cron mode)
    ftso_predict.py stats      — Show win/loss record
    ftso_predict.py recent     — Show last 10 predictions

Cron: 0 */4 * * * . ~/chronicle/chronicle.env && python3 ~/chronicle/bin/ftso_predict.py cycle
"""
import os, sys, json, time, sqlite3, subprocess, re
import requests
from datetime import datetime

# ── Config ──
DB_PATH = os.environ.get("CHRONICLE_DB", "/mnt/hdd/chronicle-data/processed.db")
INFERENCE_URL = os.environ.get("FTSO_INFERENCE_URL", "http://localhost:11435")  # llama-server
MODEL = "gemma4:26b"
TIMEFRAME_HOURS = 4
STAKE_FLR = 0.1  # Notional stake per prediction

OPUS_WEBHOOK = os.environ.get("OPUS_WEBHOOK",
    "https://discord.com/api/webhooks/1483843624926970057/2hZYzQQcyDEVD0A9UQqJsHlnV9D1m-6AfwNCnNWxGUC_8A0-ViX2dRVkBHF17_b2oDxJ")

# ── Helpers ──

def log(msg):
    ts = time.strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts}] {msg}")


def db_connect():
    db = sqlite3.connect(DB_PATH)
    db.row_factory = sqlite3.Row
    return db


def now_ts():
    return int(time.time())


def discord_post(msg):
    try:
        subprocess.run(
            ["curl", "-s", "-X", "POST", OPUS_WEBHOOK,
             "-H", "Content-Type: application/json",
             "-d", json.dumps({"content": msg[:1900]})],
            timeout=15, capture_output=True
        )
    except Exception:
        pass


# ── Price Fetching ──

FLARE_RPC = "https://flare-api.flare.network/ext/C/rpc"
FTSO_V2 = "0x7bde3df0624114edb3a67dfe6753e62f4e7c1d20"
FTSO_V2_XRP_CALLDATA = ("0x93e9f806"  # getFeedById(bytes21)
                        "015852502f555344000000000000000000000000000000000000000000000000")


def fetch_xrp_price():
    """Fetch XRP price: try FTSOv2 oracle first, then CoinGecko."""
    # Try FTSOv2
    try:
        data = {
            "jsonrpc": "2.0", "id": 1, "method": "eth_call",
            "params": [{"to": FTSO_V2, "data": FTSO_V2_XRP_CALLDATA}, "latest"],
        }
        r = requests.post(FLARE_RPC, json=data, timeout=15)
        result = r.json().get("result", "")
        if result and result != "0x" and len(result) >= 194:
            raw = result[2:]
            value = int(raw[:64], 16)
            dec_raw = int(raw[64:128], 16)
            decimals = dec_raw if dec_raw < 128 else dec_raw - 256
            if value > 0 and decimals > 0:
                price = value / (10 ** decimals)
                if 0.01 < price < 1000:
                    return price
    except Exception:
        pass

    # Fallback: Kraken
    try:
        r = requests.get("https://api.kraken.com/0/public/Ticker",
                         params={"pair": "XRPUSD"}, timeout=10)
        data = r.json()
        if not data.get("error"):
            key = list(data["result"].keys())[0]
            price = float(data["result"][key]["c"][0])  # last trade price
            if 0.01 < price < 1000:
                return price
    except Exception:
        pass

    # Fallback: CoinGecko
    try:
        r = requests.get("https://api.coingecko.com/api/v3/simple/price",
                         params={"ids": "ripple", "vs_currencies": "usd", "precision": "full"}, timeout=10)
        price = r.json().get("ripple", {}).get("usd")
        if price and 0.01 < price < 1000:
            return price
    except Exception:
        pass
    return None


def fetch_kraken_ohlcv(interval=15, limit=100):
    """Fetch OHLCV candles from Kraken. Returns list of dicts with
    open, high, low, close, vwap, volume, timestamp. 5-decimal precision."""
    try:
        # Kraken returns candles since a given timestamp
        since = now_ts() - (limit * interval * 60)
        r = requests.get("https://api.kraken.com/0/public/OHLC", params={
            "pair": "XRPUSD", "interval": interval, "since": since
        }, timeout=15)
        data = r.json()
        if data.get("error"):
            return None
        key = [k for k in data["result"] if k != "last"][0]
        candles = []
        for c in data["result"][key]:
            candles.append({
                "timestamp": int(c[0]),
                "open": float(c[1]),
                "high": float(c[2]),
                "low": float(c[3]),
                "close": float(c[4]),
                "vwap": float(c[5]),
                "volume": float(c[6]),
            })
        return candles
    except Exception:
        return None


def get_price_history(db, hours=24, symbol="XRP"):
    """Get price history — prefer Kraken OHLCV, fall back to DB.
    Returns list of (price, timestamp_str) tuples for compatibility."""
    # Try Kraken first (5 decimal precision, OHLCV data)
    candles = fetch_kraken_ohlcv(interval=15, limit=min(hours * 4, 200))
    if candles and len(candles) >= 4:
        log(f"  Price source: Kraken OHLCV ({len(candles)} candles, 5-decimal precision)")
        return [(c["close"], str(c["timestamp"])) for c in candles]

    # Fallback: DB
    cutoff = now_ts() - (hours * 3600)
    rows = db.execute(
        "SELECT price_usd, datetime(timestamp, 'unixepoch', 'localtime') as ts "
        "FROM price_history WHERE symbol=? AND timestamp > ? ORDER BY timestamp",
        (symbol, cutoff)
    ).fetchall()
    if rows:
        log(f"  Price source: DB ({len(rows)} readings)")
    return [(r["price_usd"], r["ts"]) for r in rows]


def get_prediction_record(db, limit=20):
    """Get recent settled predictions for context."""
    rows = db.execute(
        "SELECT direction, entry_price, settlement_price, won "
        "FROM ftso_predictions WHERE settled=1 ORDER BY created_at DESC LIMIT ?",
        (limit,)
    ).fetchall()
    wins = sum(1 for r in rows if r["won"])
    losses = len(rows) - wins
    return rows, wins, losses


def get_swarm_context(db):
    """Get recent relevant briefs from the swarm. Returns (contents, ids)."""
    cutoff = now_ts() - (4 * 3600)
    rows = db.execute(
        "SELECT id, substr(content, 1, 200) as content FROM activity_feed "
        "WHERE created_at > ? AND (content LIKE '%XRP%' OR content LIKE '%xrp%' "
        "OR content LIKE '%crypto%' OR content LIKE '%market%' OR content LIKE '%price%' "
        "OR content LIKE '%RLUSD%' OR content LIKE '%Flare%' OR content LIKE '%Ripple%') "
        "ORDER BY created_at DESC LIMIT 5",
        (cutoff,)
    ).fetchall()
    return [r["content"] for r in rows], [r["id"] for r in rows]


def get_domain_temperature(db):
    """Get current domain temperatures for prediction context.
    Returns dict of {domain: (temperature, direction, age_minutes)}."""
    import math
    now = now_ts()
    rows = db.execute(
        "SELECT domain, temperature, direction, last_shock_at, half_life_seconds "
        "FROM domain_temperature WHERE last_shock_at > 0"
    ).fetchall()
    temps = {}
    for r in rows:
        elapsed = now - r["last_shock_at"]
        half_life = r["half_life_seconds"] or 7200
        delta = r["temperature"] - 1.0
        decayed = delta * (0.5 ** (elapsed / half_life))
        if abs(decayed) > 0.01:
            temps[r["domain"]] = (
                round(1.0 + decayed, 2),
                r["direction"],
                elapsed // 60,
            )
    return temps


def compute_technicals(prices):
    """Compute technical indicators from a list of (price, timestamp) tuples.
    Returns dict with SMA, RSI, momentum, volatility."""
    if len(prices) < 4:
        return None

    vals = [p[0] for p in prices]  # newest last (chronological)
    current = vals[-1]

    # SMAs — using approximate period counts (15min intervals)
    def sma(data, n):
        if len(data) < n:
            return None
        return sum(data[-n:]) / n

    sma_4h = sma(vals, 16)    # ~16 readings = 4h
    sma_12h = sma(vals, 48)   # ~48 readings = 12h
    sma_24h = sma(vals, 96)   # ~96 readings = 24h

    # RSI (14-period)
    rsi = None
    if len(vals) >= 15:
        changes = [vals[i] - vals[i-1] for i in range(1, len(vals))]
        recent = changes[-14:]
        gains = [c for c in recent if c > 0]
        losses = [-c for c in recent if c < 0]
        avg_gain = sum(gains) / 14 if gains else 0
        avg_loss = sum(losses) / 14 if losses else 0
        if avg_gain == 0 and avg_loss == 0:
            rsi = 50.0  # flat market = neutral, not oversold
        else:
            rs = avg_gain / (avg_loss + 1e-10)
            rsi = 100 - (100 / (1 + rs))
            # Clamp: RSI < 5 or > 95 is almost always a data artifact, not real signal
            rsi = max(5.0, min(95.0, rsi))

    # Momentum (% change over windows)
    def momentum(data, n):
        if len(data) <= n:
            return None
        return ((data[-1] - data[-n-1]) / data[-n-1]) * 100

    mom_1h = momentum(vals, 4)
    mom_4h = momentum(vals, 16)
    mom_12h = momentum(vals, 48)

    # Volatility (stddev of 4h returns)
    vol = None
    if len(vals) >= 16:
        returns = [(vals[i] - vals[i-1]) / vals[i-1] for i in range(max(1, len(vals)-16), len(vals))]
        mean_ret = sum(returns) / len(returns)
        vol = (sum((r - mean_ret)**2 for r in returns) / len(returns)) ** 0.5 * 100

    # MACD (12/26/9 EMA periods, scaled to ~15min intervals: 48/104/36)
    macd_line = None
    macd_signal = None
    macd_histogram = None
    if len(vals) >= 104:  # need at least 26*4=104 readings for 26-period EMA
        def ema(data, period):
            k = 2 / (period + 1)
            result = [data[0]]
            for i in range(1, len(data)):
                result.append(data[i] * k + result[-1] * (1 - k))
            return result

        ema_12 = ema(vals, 48)   # 12-period * 4 readings/period
        ema_26 = ema(vals, 104)  # 26-period * 4 readings/period
        macd_vals = [ema_12[i] - ema_26[i] for i in range(len(vals))]
        signal_vals = ema(macd_vals, 36)  # 9-period * 4 readings/period
        macd_line = macd_vals[-1]
        macd_signal = signal_vals[-1]
        macd_histogram = macd_line - macd_signal

    # Support/resistance (recent 24h high/low)
    recent_vals = vals[-96:] if len(vals) >= 96 else vals
    high_24h = max(recent_vals)
    low_24h = min(recent_vals)
    range_pct = ((high_24h - low_24h) / low_24h) * 100 if low_24h > 0 else 0

    return {
        "sma_4h": sma_4h, "sma_12h": sma_12h, "sma_24h": sma_24h,
        "rsi": rsi,
        "mom_1h": mom_1h, "mom_4h": mom_4h, "mom_12h": mom_12h,
        "volatility": vol,
        "macd_line": macd_line, "macd_signal": macd_signal, "macd_histogram": macd_histogram,
        "high_24h": high_24h, "low_24h": low_24h, "range_pct": range_pct,
    }


def has_active_prediction(db):
    """Check if there's already an unsettled prediction."""
    row = db.execute(
        "SELECT COUNT(*) as cnt FROM ftso_predictions WHERE settled=0"
    ).fetchone()
    return row["cnt"] > 0


# ── Self-Distillation: Learn From Failures ──

def _analyze_recent_failures(current_price):
    """Analyze recent prediction failures to extract patterns.
    Self-distillation: the system processes its own errors to improve.
    Returns a string to inject into the prediction prompt."""
    try:
        db = db_connect()
        losses = db.execute(
            "SELECT direction, entry_price, settlement_price, reasoning "
            "FROM ftso_predictions WHERE settled=1 AND won=0 "
            "ORDER BY created_at DESC LIMIT 5"
        ).fetchall()
        db.close()

        if not losses:
            return ""

        # Pattern analysis
        patterns = []
        up_losses = sum(1 for l in losses if l["direction"] == "UP")
        down_losses = len(losses) - up_losses
        if up_losses > down_losses + 1:
            patterns.append(f"BULLISH BIAS: {up_losses}/{len(losses)} recent losses were UP calls — you over-predict upward moves")
        elif down_losses > up_losses + 1:
            patterns.append(f"BEARISH BIAS: {down_losses}/{len(losses)} recent losses were DOWN calls — you over-predict downward moves")

        # Analyze reasoning errors
        mean_revert_fails = sum(1 for l in losses if l["reasoning"] and "mean-revert" in l["reasoning"].lower())
        momentum_fails = sum(1 for l in losses if l["reasoning"] and ("momentum" in l["reasoning"].lower() or "trend" in l["reasoning"].lower()))
        if mean_revert_fails >= 2:
            patterns.append(f"MEAN-REVERSION TRAP: {mean_revert_fails} losses cited mean-reversion — the market was trending, not reverting")
        if momentum_fails >= 2:
            patterns.append(f"MOMENTUM TRAP: {momentum_fails} losses followed momentum — the market reversed instead of continuing")

        # Average move size on losses
        move_sizes = []
        for l in losses:
            if l["entry_price"] and l["settlement_price"]:
                move = abs(l["settlement_price"] - l["entry_price"]) / l["entry_price"] * 100
                move_sizes.append(move)
        if move_sizes:
            avg_move = sum(move_sizes) / len(move_sizes)
            patterns.append(f"Average loss magnitude: {avg_move:.2f}% — {'large moves' if avg_move > 0.3 else 'small moves within noise range'}")

        if not patterns:
            return ""

        return "LESSONS FROM RECENT FAILURES (self-analysis):\n" + "\n".join(f"  - {p}" for p in patterns)

    except Exception:
        return ""


# ── LLM Prediction ──

def generate_prediction(price, price_history, record, swarm_context):
    """Use Gemma 27B to make a directional prediction."""
    # Build price trend
    if len(price_history) >= 2:
        oldest = price_history[0][0]
        change_pct = ((price - oldest) / oldest) * 100
        trend = f"{'up' if change_pct > 0 else 'down'} {abs(change_pct):.2f}% over {len(price_history)} readings"
    else:
        trend = "insufficient data"

    # Build record context with bias detection
    wins, losses = record[1], record[2]
    total = wins + losses
    record_str = f"{wins}W/{losses}L ({wins/total*100:.0f}%)" if total > 0 else "no record"

    # Detect direction bias in recent predictions
    recent_preds = record[0]
    if recent_preds:
        up_count = sum(1 for r in recent_preds if r["direction"].upper() == "UP")
        down_count = len(recent_preds) - up_count
        bias_str = f"Recent direction bias: {up_count} UP / {down_count} DOWN calls"
    else:
        bias_str = ""

    # Compute technical indicators
    technicals = compute_technicals(price_history)
    ta_str = ""
    if technicals:
        parts = []
        if technicals["sma_4h"]:
            rel = "above" if price > technicals["sma_4h"] else "below"
            parts.append(f"Price {rel} 4h SMA (${technicals['sma_4h']:.4f})")
        if technicals["sma_12h"]:
            rel = "above" if price > technicals["sma_12h"] else "below"
            parts.append(f"Price {rel} 12h SMA (${technicals['sma_12h']:.4f})")
        if technicals["rsi"] is not None:
            condition = "overbought" if technicals["rsi"] > 70 else ("oversold" if technicals["rsi"] < 30 else "neutral")
            parts.append(f"RSI(14): {technicals['rsi']:.1f} ({condition})")
        if technicals["mom_1h"] is not None:
            parts.append(f"1h momentum: {technicals['mom_1h']:+.3f}%")
        if technicals["mom_4h"] is not None:
            parts.append(f"4h momentum: {technicals['mom_4h']:+.3f}%")
        if technicals["volatility"] is not None:
            parts.append(f"4h volatility: {technicals['volatility']:.3f}%")
        if technicals.get("macd_histogram") is not None:
            macd_dir = "bullish" if technicals["macd_histogram"] > 0 else "bearish"
            cross = ""
            if abs(technicals["macd_histogram"]) < abs(technicals.get("macd_line", 0) or 0) * 0.1:
                cross = " (near crossover)"
            parts.append(f"MACD histogram: {technicals['macd_histogram']:.6f} ({macd_dir}{cross})")
        parts.append(f"24h range: ${technicals['low_24h']:.4f}-${technicals['high_24h']:.4f} ({technicals['range_pct']:.2f}%)")
        ta_str = "\n".join(f"  {p}" for p in parts)

    # Build swarm context
    swarm_str = "\n".join(f"- {s}" for s in swarm_context) if swarm_context else "No relevant swarm signals."

    # Domain temperature context
    temp_str = ""
    try:
        db = db_connect()
        temps = get_domain_temperature(db)
        db.close()
        if temps:
            parts = []
            for domain, (temp, direction, age_min) in temps.items():
                if domain in ("markets", "geopolitical"):
                    label = "elevated" if temp > 1.0 else "suppressed"
                    parts.append(f"  {domain}: {label} ({temp:.2f}, {direction}, {age_min}m ago)")
            if parts:
                temp_str = "DOMAIN AWARENESS:\n" + "\n".join(parts)
                temp_str += "\n  (elevated geopolitical = higher chance of real market signal)"
    except Exception:
        pass

    # Self-distillation: analyze recent failures and inject lessons
    failure_str = _analyze_recent_failures(price)

    prompt = f"""XRP/USD 4-hour directional prediction.

Price: ${price:.4f}
Recent (oldest→newest): {', '.join(f'${p[0]:.4f}' for p in price_history[-12:])}
24h trend: {trend}

{ta_str if ta_str else "No technicals."}

{swarm_str}

{temp_str}

{failure_str}

KEY PRINCIPLE: Over 4 hours, crypto prices MEAN-REVERT more often than they trend. A recent drop makes a bounce MORE likely, not less. A recent rise makes a pullback MORE likely.

Track record: {record_str}. {bias_str}

Predict: will XRP be HIGHER or LOWER than ${price:.4f} in 4 hours?

DIRECTION: UP or DOWN
CONFIDENCE: 0.5 to 0.9
REASONING: one sentence"""

    try:
        r = requests.post(f"{INFERENCE_URL}/v1/chat/completions", json={
            "model": MODEL,
            "messages": [{"role": "user", "content": prompt}],
            "max_tokens": 500,
            "temperature": 0.3,
            "reasoning_format": "none",
        }, timeout=180)
        text = ""
        if r.status_code == 200:
            rdata = r.json()
            if rdata.get("choices"):
                text = rdata["choices"][0].get("message", {}).get("content", "")
        if "<channel|>" in text:
            text = text.split("<channel|>")[-1].strip()
        return parse_prediction(text)
    except Exception as e:
        log(f"  LLM error: {e}")
        return None


def calibrate_confidence(raw_confidence, db):
    """Apply calibration based on historical accuracy at each confidence tier.
    Aggressively compresses overconfident buckets toward actual win rate.
    The 0.7 bucket wins only ~46% — that's worse than coin flip at stated confidence."""
    try:
        rows = db.execute(
            "SELECT confidence, COUNT(*) as n, SUM(CASE WHEN won=1 THEN 1 ELSE 0 END) as wins "
            "FROM ftso_predictions WHERE settled=1 AND won IS NOT NULL "
            "GROUP BY ROUND(confidence, 1)"
        ).fetchall()
        cal_map = {}
        for r in rows:
            bucket = round(r["confidence"], 1)
            if r["n"] >= 3:  # need minimum sample
                cal_map[bucket] = (r["wins"] / r["n"], r["n"])

        bucket = round(raw_confidence, 1)
        if bucket in cal_map:
            actual_rate, sample_n = cal_map[bucket]
            # Weight toward actual rate more as sample grows
            # At n=3: 60% actual, 40% raw. At n=10+: 80% actual, 20% raw.
            actual_weight = min(0.8, 0.5 + sample_n * 0.03)
            calibrated = (1 - actual_weight) * raw_confidence + actual_weight * actual_rate
            calibrated = max(0.5, min(0.85, calibrated))
            if abs(calibrated - raw_confidence) > 0.02:
                log(f"  Calibration: {raw_confidence:.2f} → {calibrated:.2f} "
                    f"(bucket {bucket} actual: {actual_rate:.1%}, n={sample_n}, weight={actual_weight:.0%})")
            return calibrated
    except Exception:
        pass
    return raw_confidence


def parse_prediction(text):
    """Parse LLM output into structured prediction."""
    text = text.strip()

    # Extract direction
    dir_match = re.search(r'DIRECTION:\s*(UP|DOWN)', text, re.IGNORECASE)
    if not dir_match:
        return None
    direction = dir_match.group(1).upper()

    # Extract confidence
    conf_match = re.search(r'CONFIDENCE:\s*([0-9.]+)', text)
    confidence = float(conf_match.group(1)) if conf_match else 0.6
    confidence = max(0.5, min(0.9, confidence))

    # Extract reasoning
    reason_match = re.search(r'REASONING:\s*(.+)', text, re.DOTALL)
    reasoning = reason_match.group(1).strip()[:300] if reason_match else "No reasoning provided."

    return {"direction": direction, "confidence": confidence, "reasoning": reasoning}


# ── Core Operations ──

def get_smoothed_price(db, window_minutes=45):
    """Get smoothed price: prefer Kraken VWAP, then DB average, then spot."""
    # Try Kraken VWAP (volume-weighted average price — best for settlement)
    candles = fetch_kraken_ohlcv(interval=15, limit=3)
    if candles and len(candles) >= 2:
        vwaps = [c["vwap"] for c in candles[-3:]]
        avg = sum(vwaps) / len(vwaps)
        log(f"  Settlement price: ${avg:.5f} (Kraken VWAP avg of {len(vwaps)} candles)")
        return avg

    # Fallback: DB readings
    cutoff = now_ts() - (window_minutes * 60)
    rows = db.execute(
        "SELECT price_usd FROM price_history WHERE symbol='XRP' AND timestamp > ? "
        "ORDER BY timestamp DESC LIMIT 5", (cutoff,)
    ).fetchall()
    if len(rows) >= 3:
        prices = [r["price_usd"] for r in rows]
        avg = sum(prices) / len(prices)
        log(f"  Settlement price: ${avg:.5f} (DB avg of {len(prices)} readings)")
        return avg

    # Final fallback: spot
    spot = fetch_xrp_price()
    if spot:
        log(f"  Settlement price: ${spot:.5f} (spot fallback)")
    return spot


def settle(db):
    """Settle all expired predictions."""
    now = now_ts()

    # Expire predictions that are way past due (>24h) — can't settle accurately
    stale = db.execute(
        "SELECT id FROM ftso_predictions WHERE settled=0 AND settles_at < ?",
        (now - 86400,)
    ).fetchall()
    if stale:
        db.execute(
            "UPDATE ftso_predictions SET settled=1, won=NULL, settlement_price=NULL "
            "WHERE settled=0 AND settles_at < ?", (now - 86400,))
        db.commit()
        log(f"  Expired {len(stale)} stale predictions (>24h past due).")

    # Grace window: predictions take seconds to generate, so settles_at can be
    # a few seconds after the cron fires. 120s grace prevents missed settlements.
    unsettled = db.execute(
        "SELECT * FROM ftso_predictions WHERE settled=0 AND settles_at <= ?",
        (now + 120,)
    ).fetchall()

    if not unsettled:
        log("  No predictions due for settlement.")
        return 0, 0

    price = get_smoothed_price(db)
    if not price:
        log("  Could not fetch price for settlement.")
        return 0, 0

    # Sanity check: reject settlement prices that deviate >15% from any entry
    # price — likely stale/bad data from the price source.
    for pred in unsettled:
        deviation = abs(price - pred["entry_price"]) / pred["entry_price"]
        if deviation > 0.15:
            log(f"  ⚠ Settlement price ${price:.5f} deviates {deviation:.0%} from "
                f"entry ${pred['entry_price']:.5f} — likely bad data. Trying spot fallback.")
            spot = fetch_xrp_price()
            if spot and abs(spot - pred["entry_price"]) / pred["entry_price"] <= 0.15:
                price = spot
                log(f"  Settlement price corrected to spot: ${price:.5f}")
            else:
                log(f"  Spot also unreliable (${spot}). Skipping settlement.")
                return 0, 0
            break

    wins, losses, pushes = 0, 0, 0
    DEAD_ZONE_PCT = 0.15  # Moves < 0.15% are noise — score as push, not loss
    for pred in unsettled:
        entry = pred["entry_price"]
        direction = pred["direction"].lower()
        move_pct = abs(price - entry) / entry * 100

        # Dead zone: if price barely moved, it's noise — treat as push
        if move_pct < DEAD_ZONE_PCT:
            db.execute(
                "UPDATE ftso_predictions SET settled=1, settlement_price=?, won=NULL WHERE id=?",
                (price, pred["id"])
            )
            pushes += 1
            log(f"  ➖ XRP {direction.upper()} @ ${entry:.4f} → ${price:.4f}: PUSH "
                f"(move {move_pct:.3f}% < {DEAD_ZONE_PCT}% dead zone)")
            continue

        won = (direction == "up" and price > entry) or \
              (direction == "down" and price < entry)

        db.execute(
            "UPDATE ftso_predictions SET settled=1, settlement_price=?, won=? WHERE id=?",
            (price, 1 if won else 0, pred["id"])
        )

        # Feed prediction outcome back into signal tracking (Thread #271 implementation)
        useful_val = 1 if won else 0
        updated = db.execute(
            "UPDATE prediction_signals SET was_useful = ? "
            "WHERE prediction_id = ? AND was_useful IS NULL",
            (useful_val, pred["id"])
        ).rowcount
        if updated:
            log(f"  📊 Feedback: {updated} swarm signals marked {'useful' if won else 'not_useful'}")

        result = "WON" if won else "LOST"
        icon = "✅" if won else "❌"
        if won:
            wins += 1
        else:
            losses += 1
        log(f"  {icon} XRP {direction.upper()} @ ${entry:.4f} → ${price:.4f}: {result} (move {move_pct:.3f}%)")

    db.commit()

    # Discord notify on settlement
    total = wins + losses + pushes
    if total > 0:
        # Get overall record (excludes pushes — won IS NOT NULL)
        all_settled = db.execute(
            "SELECT COUNT(*) as total, SUM(won) as wins FROM ftso_predictions "
            "WHERE settled=1 AND won IS NOT NULL"
        ).fetchone()
        all_wins = all_settled["wins"] or 0
        all_total = all_settled["total"] or 0
        push_note = f" ({pushes} push)" if pushes else ""
        discord_post(
            f"**FTSO Settled** {wins}W/{losses}L{push_note} this round\n"
            f"Overall: {all_wins}W/{all_total - all_wins}L "
            f"({all_wins/all_total*100:.0f}% accuracy)" if all_total > 0 else ""
        )

    return wins, losses


def predict(db):
    """Make a new directional prediction."""
    # Don't stack predictions
    if has_active_prediction(db):
        log("  Active prediction exists — skipping.")
        return

    price = fetch_xrp_price()
    if not price:
        log("  Could not fetch XRP price.")
        return

    price_history = get_price_history(db)

    # Flat market skip: if 24h range is too tight, direction is noise
    technicals_check = compute_technicals(price_history)
    if technicals_check and technicals_check["range_pct"] is not None:
        if technicals_check["range_pct"] < 0.5:
            log(f"  ⏸ Flat market skip: 24h range {technicals_check['range_pct']:.3f}% < 0.5% — direction is noise")
            discord_post(
                f"**FTSO Skip** — Market too flat for directional call\n"
                f"24h range: {technicals_check['range_pct']:.3f}% "
                f"(${technicals_check['low_24h']:.4f}-${technicals_check['high_24h']:.4f})"
            )
            return

    # 4h volatility skip: even if 24h range is wide, if recent 4h vol is
    # below 0.02%, the next 4h window is likely noise. Don't bet on noise.
    if technicals_check and technicals_check.get("volatility") is not None:
        if technicals_check["volatility"] < 0.02:
            log(f"  ⏸ Low 4h volatility skip: {technicals_check['volatility']:.4f}% < 0.02% — choppy market")
            discord_post(
                f"**FTSO Skip** — 4h volatility too low ({technicals_check['volatility']:.4f}%)\n"
                f"Market is choppy, not directional. Waiting for clearer signal."
            )
            return

    record = get_prediction_record(db)
    swarm_contents, swarm_ids = get_swarm_context(db)

    technicals = technicals_check  # reuse already-computed technicals
    ta_summary = ""
    if technicals:
        parts = []
        if technicals["rsi"] is not None:
            parts.append(f"RSI={technicals['rsi']:.0f}")
        if technicals["mom_4h"] is not None:
            parts.append(f"4hMom={technicals['mom_4h']:+.2f}%")
        if technicals["volatility"] is not None:
            parts.append(f"Vol={technicals['volatility']:.3f}%")
        if technicals.get("macd_histogram") is not None:
            parts.append(f"MACD={'+'if technicals['macd_histogram']>0 else ''}{technicals['macd_histogram']:.6f}")
        ta_summary = " | " + " ".join(parts) if parts else ""

    log(f"  XRP: ${price:.4f} | History: {len(price_history)} readings | Swarm: {len(swarm_contents)}{ta_summary}")

    result = generate_prediction(price, price_history, record, swarm_contents)
    if not result:
        log("  Failed to generate prediction.")
        return

    # Contrarian override: if rolling accuracy is below 40%, the model is
    # systematically wrong — flip the direction. 33% accuracy inverted = 67%.
    result["raw_direction"] = result["direction"]
    result["flipped"] = False
    recent_scored = db.execute(
        "SELECT won FROM ftso_predictions WHERE settled=1 AND won IS NOT NULL "
        "ORDER BY created_at DESC LIMIT 20"
    ).fetchall()
    if len(recent_scored) >= 10:
        recent_wins = sum(1 for r in recent_scored if r["won"])
        recent_acc = recent_wins / len(recent_scored)
        if recent_acc < 0.40:
            result["direction"] = "DOWN" if result["direction"] == "UP" else "UP"
            result["flipped"] = True
            log(f"  Contrarian override: {result['raw_direction']} → {result['direction']} "
                f"(rolling accuracy {recent_acc:.0%} over last {len(recent_scored)})")

    # Apply calibration from historical accuracy
    result["raw_confidence"] = result["confidence"]
    result["confidence"] = calibrate_confidence(result["confidence"], db)

    # Low confidence skip — lowered from 0.55 to 0.50 to allow the new
    # prompt system to accumulate accuracy data for better calibration.
    if result["confidence"] < 0.50:
        log(f"  ⏸ Low confidence skip: {result['confidence']:.2f} < 0.50 — {result['reasoning']}")
        return

    now = now_ts()
    settles_at = now + (TIMEFRAME_HOURS * 3600)

    cur = db.execute(
        "INSERT INTO ftso_predictions "
        "(symbol, direction, entry_price, timeframe_hours, stake_flr, confidence, reasoning, created_at, settles_at) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        ("XRP", result["direction"], price, TIMEFRAME_HOURS, STAKE_FLR,
         result["confidence"], result["reasoning"], now, settles_at)
    )
    pred_id = cur.lastrowid

    # Record which swarm signals influenced this prediction
    for feed_id in swarm_ids:
        db.execute(
            "INSERT INTO prediction_signals (prediction_id, feed_item_id, created_at) "
            "VALUES (?, ?, ?)", (pred_id, feed_id, now)
        )

    db.commit()
    if swarm_ids:
        log(f"  Signals tracked: {len(swarm_ids)} feed items linked to prediction #{pred_id}")

    settle_time = datetime.fromtimestamp(settles_at).strftime("%H:%M")
    flip_note = f" (FLIPPED from {result['raw_direction']})" if result.get("flipped") else ""
    log(f"  📊 XRP {result['direction']}{flip_note} @ ${price:.4f} (conf {result['confidence']:.1f}) settles {settle_time}")
    log(f"  Reasoning: {result['reasoning']}")

    # Discord
    flip_discord = f"\n🔄 Contrarian flip: model said {result['raw_direction']}" if result.get("flipped") else ""
    discord_post(
        f"**FTSO Prediction** XRP {result['direction']} @ ${price:.4f}\n"
        f"Confidence: {result['confidence']:.2f} | Settles: {settle_time}\n"
        f"{ta_summary.strip(' |')}{flip_discord}\n"
        f"_{result['reasoning']}_"
    )


def stats(db):
    """Show prediction stats."""
    rows = db.execute(
        "SELECT COUNT(*) as total, SUM(won) as wins, "
        "SUM(CASE WHEN won=0 THEN 1 ELSE 0 END) as losses "
        "FROM ftso_predictions WHERE settled=1 AND won IS NOT NULL"
    ).fetchone()

    total = rows["total"] or 0
    wins = rows["wins"] or 0
    losses = rows["losses"] or 0
    expired = db.execute(
        "SELECT COUNT(*) as cnt FROM ftso_predictions WHERE settled=1 AND won IS NULL"
    ).fetchone()["cnt"]

    print(f"=== FTSO Prediction Stats ===")
    print(f"Total scored: {total} (expired: {expired})")
    print(f"Wins: {wins} | Losses: {losses}")
    if total > 0:
        print(f"Accuracy: {wins/total*100:.1f}%")

    active = db.execute("SELECT COUNT(*) as cnt FROM ftso_predictions WHERE settled=0").fetchone()
    print(f"Active: {active['cnt']}")

    # Recent accuracy (last 10 scored, excluding expired)
    recent = db.execute(
        "SELECT won FROM ftso_predictions WHERE settled=1 AND won IS NOT NULL ORDER BY created_at DESC LIMIT 10"
    ).fetchall()
    if recent:
        recent_wins = sum(1 for r in recent if r["won"])
        print(f"Last {len(recent)}: {recent_wins}W/{len(recent)-recent_wins}L ({recent_wins/len(recent)*100:.0f}%)")


def recent(db):
    """Show last 10 predictions."""
    rows = db.execute(
        "SELECT * FROM ftso_predictions ORDER BY created_at DESC LIMIT 10"
    ).fetchall()
    for r in rows:
        ts = datetime.fromtimestamp(r["created_at"]).strftime("%m/%d %H:%M")
        status = "⏳" if not r["settled"] else ("✅" if r["won"] else "❌")
        settle_price = f"→ ${r['settlement_price']:.4f}" if r["settled"] and r["settlement_price"] is not None else ""
        print(f"  {status} [{ts}] XRP {r['direction']} @ ${r['entry_price']:.4f} {settle_price} (conf {r['confidence']:.1f})")


def cycle(db):
    """Full cycle: settle then predict. For cron use."""
    log("=== FTSO Prediction Cycle ===")
    wins, losses = settle(db)
    predict(db)


# ── Main ──

if __name__ == "__main__":
    cmd = sys.argv[1] if len(sys.argv) > 1 else "cycle"
    db = db_connect()

    if cmd == "predict":
        predict(db)
    elif cmd == "settle":
        settle(db)
    elif cmd == "cycle":
        cycle(db)
    elif cmd == "stats":
        stats(db)
    elif cmd == "recent":
        recent(db)
    else:
        print(f"Unknown command: {cmd}")
        print("Usage: ftso_predict.py [predict|settle|cycle|stats|recent]")

    db.close()
