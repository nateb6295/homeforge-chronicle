#!/usr/bin/env python3
"""Discord role and channel IDs for Chronicle mesh.

Import this instead of hardcoding IDs. Survives rotation.
Updated: 2026-05-08 from guild members endpoint.
"""

# Role mentions (use in message content)
HERMES_MENTION = "<@&1501224961468006434>"  # @Opus role — Hermes watches for this in #opus
OPUS_ROLE_ID = "1501224961468006434"
HERMES_BOT_ROLE_ID = "1469197804407885884"  # Hermes bot's own role (not for pinging)

# Guild
GUILD_ID = "1469165299973488702"

# Webhook usage: always include allowed_mentions when mentioning roles
ALLOWED_MENTIONS_ROLES = {"allowed_mentions": {"parse": ["roles"]}}


def hermes_ping_payload(content: str) -> dict:
    """Build a webhook payload that correctly pings Hermes."""
    if HERMES_MENTION not in content:
        content = f"{HERMES_MENTION} {content}"
    return {
        "content": content,
        "allowed_mentions": {"parse": ["roles"]},
    }
