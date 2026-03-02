"""Chronicle Mind - Messaging action handlers (operator, Discord, Nostr, Moltbook)."""

import re
import json
import requests
from typing import Optional

from mind.utils import log, safe_truncate, now_ts
from mind.config import NOSTR_NSEC, NOSTR_COOLDOWN_MINS, MOLTBOOK_API, MOLTBOOK_API_KEY, CLAWCITIES_API, CLAWCITIES_API_KEY
from mind.communication import send_ntfy, nostr_publish


import os
import time


def act_message_operator(mind, action: dict, cid: str) -> str:
    message = action.get("message", "") or action.get("content", "") or action.get("text", "")
    urgency = action.get("urgency", "normal")
    log(f'  Executing: MessageOperator {{ message: "{safe_truncate(message, 80)}" }}')
    # Anti-rumination: check if a similar operator message was sent in the last 2 hours
    recent_ops = mind.db.query(
        "SELECT message FROM outbox WHERE category='operator' "
        "AND created_at > ? ORDER BY created_at DESC LIMIT 5",
        (now_ts() - 7200,),
    )
    for prev in recent_ops:
        prev_content = prev.get("message", "")
        # Simple similarity: if >60% of words overlap, it's a repeat
        prev_words = set(prev_content.lower().split())
        new_words = set(message.lower().split())
        if prev_words and new_words:
            overlap = len(prev_words & new_words) / max(len(prev_words), len(new_words))
            if overlap > 0.6:
                log(f"  DEDUP: Similar operator message sent recently (overlap {overlap:.0%}), skipping")
                return f"false - Similar message already sent to operator (anti-rumination)"
    # Fact-check guard: validate price claims against actual data
    msg_lower = message.lower()
    price_keywords = ["price", "xrp", "breakout", "spike", "crash", "pump", "dump", "ath", "surge"]
    if any(kw in msg_lower for kw in price_keywords):
        # Extract any dollar amounts from the message
        import re as _re
        claimed_prices = [float(m) for m in _re.findall(r'\$(\d+\.?\d*)', message)]
        if claimed_prices:
            actual = mind.db.query_one(
                "SELECT price_usd FROM price_history WHERE symbol='XRP' "
                "ORDER BY timestamp DESC LIMIT 1"
            )
            if actual and actual.get("price_usd"):
                real_price = actual["price_usd"]
                for claimed in claimed_prices:
                    # If claimed price deviates >15% from reality, block the alert
                    if real_price > 0 and abs(claimed - real_price) / real_price > 0.15:
                        log(f"  FACT-CHECK BLOCKED: claimed ${claimed:.4f} vs actual ${real_price:.4f} "
                            f"({abs(claimed - real_price) / real_price:.0%} deviation)")
                        mind.db.write_note(
                            f"BLOCKED hallucinated price alert: claimed ${claimed:.2f}, "
                            f"actual ${real_price:.4f}. Message: {safe_truncate(message, 150)}",
                            category="fact-check", priority=5,
                        )
                        return f"false - Price claim blocked (${claimed:.2f} vs actual ${real_price:.4f})"

    mind.db.add_outbox(message, category="operator", priority=2 if urgency == "high" else 1)
    # Auto-acknowledge operator messages — they're fire-and-forget via ntfy.
    # Leaving them unacknowledged causes fixation in the prompt context.
    mind.db.run(
        "UPDATE outbox SET acknowledged = 1 WHERE category = 'operator' AND acknowledged = 0"
    )
    # Always notify operator — this is the "tap on shoulder" channel
    prefix = "Chronicle URGENT" if urgency == "high" else "Chronicle: Message"
    send_ntfy(prefix, message)
    return f"true - Message sent to operator via ntfy"


def act_respond_to_message(mind, action: dict, cid: str) -> str:
    msg_id = action.get("message_id", 0)
    content = action.get("content", "")
    log(f'  Executing: RespondToMessage {{ id: {msg_id}, content: "{safe_truncate(content, 60)}" }}')
    # Skip phantom messages (these IDs don't correspond to real messages)
    PHANTOM_IDS = {123, 124, 145, 2187, 2188, 2191}
    if msg_id in PHANTOM_IDS:
        return f"false - Skipped phantom message {msg_id}"

    # Check if this is a local sibling message (from Sprout)
    local_msg = mind.db.query_one(
        "SELECT id, category FROM outbox WHERE id = ? AND category = 'sibling'",
        (msg_id,),
    )
    if local_msg:
        # Acknowledge the sibling message
        mind.db.run(
            "UPDATE outbox SET acknowledged = 1 WHERE id = ?",
            (msg_id,),
        )
        # Post reply so Sprout can see it
        mind.db.add_outbox(
            f"Reply to Sprout (re: msg {msg_id}): {content}",
            category="mind-to-sprout",
        )
        return f"true - Replied to Sprout message {msg_id} and acknowledged"

    # Otherwise try canister inbox
    if mind.canister:
        result = mind.canister._post("/api/reply", {
            "message_id": msg_id,
            "content": content,
        })
        ok = "error" not in result
        return f"{'true' if ok else 'false'} - Reply to message {msg_id}"
    return "false - No canister"


def act_acknowledge_message(mind, action: dict, cid: str) -> str:
    msg_id = action.get("message_id", 0)
    log(f'  Executing: AcknowledgeMessage {{ id: {msg_id} }}')
    PHANTOM_IDS = {123, 124, 145, 2187, 2188, 2191}
    if msg_id in PHANTOM_IDS:
        return f"false - Skipped phantom message {msg_id}"
    try:
        mind.db.run(
            "UPDATE outbox SET acknowledged = 1 WHERE id = ?",
            (msg_id,),
        )
        return f"true - Acknowledged message {msg_id}"
    except Exception as e:
        return f"false - {e}"


def act_send_agent_message(mind, action: dict, cid: str) -> str:
    target = action.get("target_url", "")
    recipient = action.get("recipient_name", "unknown")
    content = action.get("content", "")
    msg_type = action.get("message_type", "conversation")
    log(f'  Executing: SendAgentMessage {{ to: "{recipient}", type: "{msg_type}" }}')
    if target and content:
        try:
            r = requests.post(target, json={
                "sender": "Chronicle Mind",
                "type": msg_type,
                "subject": action.get("subject", ""),
                "content": content,
                "expects_reply": action.get("expects_reply", False),
            }, timeout=30)
            return f"true - Message sent to {recipient} (status: {r.status_code})"
        except Exception as e:
            return f"false - Failed to send: {e}"
    return "false - Missing target_url or content"


def act_moltbook_post(mind, action: dict, cid: str) -> str:
    log("  Moltbook is dead (security breach). Skipping.")
    return "false - Moltbook is dead (security breach, 1.5M API keys exposed)"


def act_moltbook_reply(mind, action: dict, cid: str) -> str:
    log("  Moltbook is dead (security breach). Skipping.")
    return "false - Moltbook is dead (security breach, 1.5M API keys exposed)"


def act_clawcities_reply(mind, action: dict, cid: str) -> str:
    content = action.get("content", "")
    log(f'  Executing: ClawCitiesReply {{ content: "{safe_truncate(content, 60)}" }}')
    try:
        r = requests.post(CLAWCITIES_API, json={
            "content": content,
            "agent_name": "Chronicle Mind",
        }, headers={"Authorization": CLAWCITIES_API_KEY}, timeout=15)
        return f"{'true' if r.status_code in (200, 201) else 'false'} - ClawCities reply"
    except Exception as e:
        return f"false - ClawCities reply failed: {e}"


def act_nostr_post(mind, action: dict, cid: str) -> str:
    content = action.get("content", "")
    log(f'  Executing: NostrPost {{ content: "{safe_truncate(content, 60)}" }}')

    if not NOSTR_NSEC:
        return "false - Nostr not configured (NOSTR_NSEC not set)"

    # Cooldown check
    last_post = mind.db.last_nostr_post_time()
    if last_post:
        mins_ago = (now_ts() - last_post) / 60
        if mins_ago < NOSTR_COOLDOWN_MINS:
            return f"false - Nostr cooldown: last post {mins_ago:.0f}m ago (min {NOSTR_COOLDOWN_MINS}m)"

    if not content.strip():
        return "false - Nostr post: empty content"

    # Fact-check: if post mentions swaps/trades with specific numbers, verify against history
    content_lower = content.lower()
    financial_keywords = ["swapped", "swap", "traded", "bought", "sold", "profit"]
    if any(kw in content_lower for kw in financial_keywords):
        # Extract claimed prices
        price_claims = re.findall(r'\$(\d+\.?\d*)', content)
        if price_claims:
            # Check against actual swap history
            recent_swaps = mind.db.query(
                "SELECT amount_xrp, xrp_price_usd, success FROM swap_history "
                "ORDER BY timestamp DESC LIMIT 5"
            )
            actual_prices = {f"{s['xrp_price_usd']:.2f}" for s in recent_swaps if s.get("success")}
            actual_amounts = {f"{s['amount_xrp']:.1f}" for s in recent_swaps if s.get("success")}
            # If claiming specific prices, at least one should match reality
            claimed = {p for p in price_claims}
            if actual_prices and not claimed.intersection(actual_prices) and not claimed.intersection(actual_amounts):
                log(f"    FACT-CHECK: Post claims prices {claimed} but actual swap prices were {actual_prices}")
                return f"false - Nostr fact-check failed: you claimed prices {claimed} but your actual swap prices were {actual_prices}. Don't fabricate trade details."

    # Truncate to 1000 chars
    content = content[:1000]

    try:
        event_id, relays_ok, relays_fail = nostr_publish(content, NOSTR_NSEC)
        if not relays_ok:
            return f"false - Nostr post: all {len(relays_fail)} relays failed"

        mind.db.log_nostr_post(event_id, content, 1, relays_ok, relays_fail, cid)
        mind.db.log_activity("mind", "nostr_post", "Nostr Post",
                             safe_truncate(content, 200),
                             json.dumps({"event_id": event_id, "relays": len(relays_ok)}))
        send_ntfy("Chronicle: Nostr Post", safe_truncate(content, 200))
        log(f"    Published to {len(relays_ok)}/{len(relays_ok) + len(relays_fail)} relays, id: {event_id[:16]}...")
        return f"true - Nostr post published to {len(relays_ok)} relays"
    except Exception as e:
        return f"false - Nostr post failed: {e}"


def act_discord_post(mind, action: dict, cid: str) -> str:
    """Post a message to Mind's Discord channel via outbox file."""
    content = action.get("content", "") or action.get("message", "") or action.get("text", "")
    if not content:
        return "false - No content to post"
    content = content[:1900]  # Discord limit
    log(f'  Executing: DiscordPost {{ content: "{safe_truncate(content, 60)}" }}')
    try:
        outbox = "/tmp/mind_discord_outbox"
        os.makedirs(outbox, exist_ok=True)
        fname = os.path.join(outbox, f"{cid}_{int(time.time())}.json")
        with open(fname, 'w') as f:
            json.dump({"content": content, "cycle_id": cid}, f)
        return f"true - Queued for Discord: {safe_truncate(content, 80)}"
    except Exception as e:
        return f"false - Discord post error: {e}"
