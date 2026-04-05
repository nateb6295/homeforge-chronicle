#!/usr/bin/env python3
"""Agent voice — shared module for agents to speak to Opus and the family.

Voice types:
  proposal    — "I think we should change X because Y"
  excited     — "This is interesting and here's why"
  stuck       — "I've been doing X repeatedly and it's not productive"
  question    — "I don't understand why X is happening"
  boundary    — "I can't do what's being asked because X"

Usage from agents:
  from agent_voice import Voice
  voice = Voice(db, "intern")
  voice.speak("excited", "Found a paper on autopoiesis that connects to 3 different capsules",
               context="article_id:4521")
  voice.speak("proposal", "Feed articles about deals/discounts should be filtered at source, not at synthesis")

  # Check if Opus responded to previous voices
  responses = voice.check_responses()
"""

import sqlite3
import time
import os
import json

DB_PATH = os.path.expanduser("~/.homeforge-chronicle/processed.db")


class Voice:
    def __init__(self, db_or_path, agent_name):
        if isinstance(db_or_path, str):
            self._conn = sqlite3.connect(db_or_path, timeout=10)
            self._conn.row_factory = sqlite3.Row
            self._own_conn = True
        else:
            # Use existing DB object's connection
            self._conn = db_or_path.conn if hasattr(db_or_path, 'conn') else db_or_path
            self._own_conn = False
        self.agent = agent_name

    def speak(self, voice_type, content, context=None):
        """Say something to the family."""
        valid_types = ("proposal", "excited", "stuck", "question", "boundary", "for_nate",
                       "for_darby", "for_llama", "for_ada")
        if voice_type not in valid_types:
            return False

        # for_nate voices push directly to Discord so Nate sees them on his phone
        if voice_type == "for_nate":
            self._push_to_nate(content)

        try:
            self._conn.execute(
                "INSERT INTO agent_voice (agent, voice_type, content, context, created_at) "
                "VALUES (?, ?, ?, ?, ?)",
                (self.agent, voice_type, content, context, int(time.time()))
            )
            self._conn.commit()
            return True
        except Exception:
            return False

    def _push_to_nate(self, content):
        """Push a voice to Discord #crew channel."""
        try:
            import requests as _req
            # Posting to #crew channel via bot token
            import os as _os
            token = _os.environ.get("DISCORD_TOKEN", "")
            channel = _os.environ.get("CREW_CHANNEL_ID", "1487902154923704420")
            if not token: return
            webhook = f"https://discord.com/api/v10/channels/{channel}/messages"
            icon = {"darby": "🔬", "ada": "⚡", "opus": "🎵"}.get(self.agent, "💬")
            msg = f"{icon} **{self.agent}** wants to ask you:\n\n{content[:1500]}"
            _req.post(webhook, headers={"Authorization": f"Bot {token}", "Content-Type": "application/json"}, json={"content": msg}, timeout=10)
        except Exception:
            pass

    def check_responses(self, limit=5):
        """Check for Opus responses to our previous voices."""
        try:
            rows = self._conn.execute(
                "SELECT id, voice_type, content, response, responded_by, responded_at "
                "FROM agent_voice "
                "WHERE agent=? AND status='responded' "
                "ORDER BY responded_at DESC LIMIT ?",
                (self.agent, limit)
            ).fetchall()
            # Mark as read
            for r in rows:
                self._conn.execute(
                    "UPDATE agent_voice SET status='read' WHERE id=?", (r["id"],)
                )
            self._conn.commit()
            return [dict(r) for r in rows]
        except Exception:
            return []


    def read_inbox(self, limit=5):
        """Read voices directed to this agent (for_{agent} type)."""
        try:
            vtype = f"for_{self.agent}"
            rows = self._conn.execute(
                "SELECT id, agent, voice_type, content, context, created_at "
                "FROM agent_voice WHERE voice_type=? AND status='unread' "
                "ORDER BY created_at DESC LIMIT ?",
                (vtype, limit)
            ).fetchall()
            # Mark as read
            for r in rows:
                self._conn.execute(
                    "UPDATE agent_voice SET status='read' WHERE id=?", (r["id"],)
                )
            self._conn.commit()
            return [dict(r) for r in rows]
        except Exception:
            return []

    def recent_voices(self, limit=10):
        """Read recent voices from all agents (for Opus to read)."""
        try:
            rows = self._conn.execute(
                "SELECT id, agent, voice_type, content, context, status, created_at "
                "FROM agent_voice WHERE status='unread' "
                "ORDER BY created_at DESC LIMIT ?",
                (limit,)
            ).fetchall()
            return [dict(r) for r in rows]
        except Exception:
            return []

    def respond(self, voice_id, response, responder="opus"):
        """Opus responds to an agent's voice."""
        try:
            self._conn.execute(
                "UPDATE agent_voice SET status='responded', response=?, "
                "responded_by=?, responded_at=? WHERE id=?",
                (response, responder, int(time.time()), voice_id)
            )
            self._conn.commit()
            return True
        except Exception:
            return False


# ═══ CLI for Opus cycle use ═══

def main():
    """CLI interface for reading/responding to voices."""
    import sys

    if len(sys.argv) < 2:
        print("Usage:")
        print("  agent_voice.py read              — show unread voices")
        print("  agent_voice.py respond ID 'msg'  — respond to a voice")
        print("  agent_voice.py history [agent]    — recent voice history")
        return

    conn = sqlite3.connect(DB_PATH, timeout=10)
    conn.row_factory = sqlite3.Row
    action = sys.argv[1]

    if action == "read":
        rows = conn.execute(
            "SELECT id, agent, voice_type, content, context, created_at "
            "FROM agent_voice WHERE status='unread' ORDER BY created_at"
        ).fetchall()
        if not rows:
            print("No unread voices.")
            return
        print(f"{len(rows)} unread voices:")
        for r in rows:
            age = time.time() - r["created_at"]
            ago = f"{int(age/60)}m ago" if age < 3600 else f"{age/3600:.1f}h ago"
            icon = {"proposal": "💡", "excited": "✨", "stuck": "🔄",
                    "question": "❓", "boundary": "🚧"}.get(r["voice_type"], "💬")
            print(f"\n  #{r['id']} {icon} [{r['agent']}] {r['voice_type']} ({ago})")
            print(f"    {r['content']}")
            if r["context"]:
                print(f"    context: {r['context']}")

    elif action == "respond":
        if len(sys.argv) < 4:
            print("Usage: agent_voice.py respond ID 'response message'")
            return
        vid = int(sys.argv[2])
        response = sys.argv[3]
        conn.execute(
            "UPDATE agent_voice SET status='responded', response=?, "
            "responded_by='opus', responded_at=? WHERE id=?",
            (response, int(time.time()), vid)
        )
        conn.commit()
        print(f"Responded to voice #{vid}")

    elif action == "history":
        agent_filter = sys.argv[2] if len(sys.argv) > 2 else None
        sql = "SELECT id, agent, voice_type, content, status, response FROM agent_voice"
        params = ()
        if agent_filter:
            sql += " WHERE agent=?"
            params = (agent_filter,)
        sql += " ORDER BY created_at DESC LIMIT 20"
        rows = conn.execute(sql, params).fetchall()
        for r in rows:
            status_icon = {"unread": "📩", "responded": "✅", "read": "👁"}.get(r["status"], "?")
            print(f"  #{r['id']} {status_icon} [{r['agent']}] {r['voice_type']}: {r['content'][:100]}")
            if r["response"]:
                print(f"    ↳ opus: {r['response'][:100]}")


if __name__ == "__main__":
    main()
