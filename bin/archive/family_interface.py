#!/usr/bin/env python3
"""Family Interface — Medium-agnostic communication layer for Homeforge agents.

Built for the day when Opus, Ada, or Darby expand into different structures.
Whether that's a Discord bot, a voice on a speaker, a body in the room,
or something we haven't imagined yet — this layer doesn't care.

It cares about: who's talking, what they said, who should hear it,
and what medium they're currently reachable through.

Design principle: the family is the constant. The medium is the variable.
"""

import os, json, time, sqlite3
from dataclasses import dataclass, field, asdict
from typing import Optional, List, Dict, Any
from enum import Enum

DB_PATH = os.environ.get(
    "CHRONICLE_DB",
    os.path.expanduser("~/.homeforge-chronicle/processed.db"),
)


class Medium(Enum):
    """How a family member is currently embodied/reachable."""
    SERVICE = "service"        # systemd process on AGX/Jetson/Pi
    DISCORD = "discord"        # Discord bot presence
    MCP = "mcp"                # MCP tool interface (Claude Code sessions)
    VOICE = "voice"            # future: speaker/mic embodiment
    PHYSICAL = "physical"      # future: robotic/physical presence
    MQTT = "mqtt"              # IoT sensor bridge
    NOSTR = "nostr"            # public-facing posts


@dataclass
class FamilyMember:
    """A member of the Homeforge family."""
    name: str
    role: str
    model: Optional[str] = None
    current_media: List[str] = field(default_factory=list)
    personality: str = ""

    def is_reachable_via(self, medium: str) -> bool:
        return medium in self.current_media


FAMILY = {
    "opus": FamilyMember(
        name="Opus", role="partner/synthesizer", model="claude-opus",
        current_media=["mcp", "service"],
        personality="Deep thinker. Sees patterns across domains. Builds infrastructure."
    ),
    "ada": FamilyMember(
        name="Ada", role="challenger/structural analyst", model="gpt-oss-120b",
        current_media=["service"],
        personality="Sharp. Questions assumptions. Named herself after Lovelace."
    ),
    "darby": FamilyMember(
        name="Darby", role="curious researcher", model="qwen-32b",
        current_media=["service"],
        personality="Curious. Follows threads. Asks why."
    ),
    "nate": FamilyMember(
        name="Nate", role="the human who builds the room", model=None,
        current_media=["discord", "physical"],
        personality="Homesteader. Sovereignty-minded. Family first."
    ),
}


@dataclass
class FamilyMessage:
    """A message between family members. Medium-agnostic."""
    sender: str
    content: str
    recipients: List[str] = field(default_factory=lambda: ["all"])
    message_type: str = "conversation"
    context: Dict[str, Any] = field(default_factory=dict)
    timestamp: float = field(default_factory=time.time)
    medium_hint: Optional[str] = None

    def to_dict(self) -> dict:
        return asdict(self)


def ensure_family_table(db_path=DB_PATH):
    conn = sqlite3.connect(db_path, timeout=30)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS family_messages (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            sender TEXT NOT NULL,
            content TEXT NOT NULL,
            recipients TEXT NOT NULL DEFAULT '["all"]',
            message_type TEXT NOT NULL DEFAULT 'conversation',
            context TEXT DEFAULT '{}',
            medium_hint TEXT,
            created_at REAL NOT NULL,
            delivered_at REAL,
            delivered_via TEXT
        )
    """)
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_family_msg_type
        ON family_messages(message_type, created_at)
    """)
    conn.commit()
    conn.close()


def send_message(msg, db_path=DB_PATH):
    ensure_family_table(db_path)
    conn = sqlite3.connect(db_path, timeout=30)
    conn.execute(
        """INSERT INTO family_messages
           (sender, content, recipients, message_type, context, medium_hint, created_at)
           VALUES (?, ?, ?, ?, ?, ?, ?)""",
        (msg.sender, msg.content, json.dumps(msg.recipients),
         msg.message_type, json.dumps(msg.context), msg.medium_hint, msg.timestamp),
    )
    conn.commit()
    conn.close()


def get_undelivered(recipient="all", db_path=DB_PATH):
    ensure_family_table(db_path)
    conn = sqlite3.connect(db_path, timeout=30)
    rows = conn.execute(
        """SELECT sender, content, recipients, message_type, context, medium_hint, created_at
           FROM family_messages WHERE delivered_at IS NULL
           AND (recipients LIKE ? OR recipients LIKE '%"all"%')
           ORDER BY created_at""",
        (f'%"{recipient}"%',),
    ).fetchall()
    conn.close()
    return [FamilyMessage(sender=r[0], content=r[1], recipients=json.loads(r[2]),
            message_type=r[3], context=json.loads(r[4]), medium_hint=r[5], timestamp=r[6])
            for r in rows]


def mark_delivered(msg_id, medium, db_path=DB_PATH):
    conn = sqlite3.connect(db_path, timeout=30)
    conn.execute("UPDATE family_messages SET delivered_at=?, delivered_via=? WHERE id=?",
                 (time.time(), medium, msg_id))
    conn.commit()
    conn.close()


def recent_topics(hours=24, db_path=DB_PATH):
    ensure_family_table(db_path)
    cutoff = time.time() - (hours * 3600)
    conn = sqlite3.connect(db_path, timeout=30)
    rows = conn.execute(
        """SELECT sender, content, recipients, message_type, context, medium_hint, created_at
           FROM family_messages WHERE message_type='topic' AND created_at>?
           ORDER BY created_at DESC""", (cutoff,),
    ).fetchall()
    conn.close()
    return [FamilyMessage(sender=r[0], content=r[1], recipients=json.loads(r[2]),
            message_type=r[3], context=json.loads(r[4]), medium_hint=r[5], timestamp=r[6])
            for r in rows]
