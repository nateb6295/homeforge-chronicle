#!/usr/bin/env python3
"""Stem — Bidirectional bridge between local swarm and on-chain Keeper.

Pushes validated crossref connections and patterns to the ICP canister
as capsules, so the Keeper's Qwen 3 32B can compost them alongside raw
content.  Also pulls Keeper cluster state down for local agents.

Used by crossref.py (and potentially other agents) via:
    from stem import Stem
    stem = Stem(CANISTER_ID, DFX_BIN, "chronicle-auto")
    stem.push_connection(...)
"""

import os, json, re, time, subprocess, logging
from datetime import datetime
from typing import Optional, List

import requests

# ═══════════════════════════════════════════════════════════════════
#  Constants
# ═══════════════════════════════════════════════════════════════════

CANISTER_URL_TEMPLATE = "https://{}.raw.icp0.io"
TOKEN_PATH = os.path.expanduser("~/.homeforge-chronicle/.api_token")
DFX_TIMEOUT = 60  # seconds — canister calls can be slow

log = logging.getLogger("stem")

def _now_iso() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def _log(msg: str):
    print(f"[{_now_iso()}] [stem] {msg}", flush=True)


class Stem:
    """Bidirectional communication between local swarm and on-chain Keeper."""

    def __init__(self, canister_id: str, dfx_bin: str, identity: str):
        self.canister_id = canister_id
        self.dfx_bin = dfx_bin
        self.identity = identity
        self.canister_url = CANISTER_URL_TEMPLATE.format(canister_id)
        self._token = None
        self._load_token()
        _log(f"Stem initialized (canister={canister_id})")

    def _load_token(self):
        try:
            with open(TOKEN_PATH) as f:
                self._token = f.read().strip()
        except Exception:
            self._token = None

    def _dfx_env(self) -> dict:
        return {**os.environ, "DFX_WARNING": "-mainnet_plaintext_identity"}

    def _dfx_call(self, method: str, args: str, timeout: int = DFX_TIMEOUT,
                  query: bool = False) -> Optional[str]:
        """Run a dfx canister call, return stdout or None on error.

        When query=True, adds --query so read-only methods run as non-replicated
        queries (free cycles, fast) instead of replicated updates. This matches
        the canister's #[query] annotation and prevents burning cycles on reads.
        """
        cmd = [
            self.dfx_bin, "canister", "--network", "ic", "call",
            self.canister_id, method, args,
            "--identity", self.identity,
        ]
        if query:
            cmd.append("--query")
        try:
            result = subprocess.run(
                cmd, capture_output=True, text=True,
                timeout=timeout, env=self._dfx_env(),
            )
            if result.returncode == 0:
                return result.stdout.strip()
            else:
                _log(f"  dfx {method} error: {result.stderr.strip()[:200]}")
                return None
        except subprocess.TimeoutExpired:
            _log(f"  dfx {method} timed out ({timeout}s)")
            return None
        except Exception as e:
            _log(f"  dfx {method} exception: {e}")
            return None

    def _dfx_call_async(self, method: str, args: str):
        """Fire-and-forget dfx canister call (non-blocking)."""
        cmd = [
            self.dfx_bin, "canister", "--network", "ic", "call",
            self.canister_id, method, args,
            "--identity", self.identity,
        ]
        try:
            subprocess.Popen(
                cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                env=self._dfx_env(),
            )
        except Exception as e:
            _log(f"  dfx async {method} error: {e}")

    def _post_capsule(self, content: str, topic: str, keywords: list) -> Optional[int]:
        """Post a capsule via HTTP /api/store. Returns capsule_id or None."""
        payload = {
            "content": content[:2000],
            "topic": topic,
            "keywords": keywords,
            "persons": [],
        }
        headers = {}
        if self._token:
            headers["Authorization"] = f"Bearer {self._token}"
        try:
            r = requests.post(
                f"{self.canister_url}/api/store",
                json=payload,
                headers=headers,
                timeout=15,
            )
            if r.status_code in (200, 201):
                capsule_id = r.json().get("capsule_id")
                if capsule_id:
                    return int(capsule_id)
                _log(f"  Capsule stored but no ID returned")
                return None
            else:
                _log(f"  Capsule POST failed: {r.status_code} {r.text[:100]}")
                return None
        except Exception as e:
            _log(f"  Capsule POST error: {e}")
            return None

    # ─── Push: Local → Canister ────────────────────────────────

    def push_connection(self, capsule_a_title: str, capsule_b_title: str,
                        connection_text: str, similarity: float, channel: str,
                        pattern_a: str = None, pattern_b: str = None) -> Optional[int]:
        """Push a validated crossref connection to the canister as a capsule.

        Returns capsule_id on success, None on failure.
        Non-blocking for the canister call itself.
        """
        # Build structured content
        lines = [
            f"[Crossref/{channel}] Connection (sim={similarity:.3f}):",
            f"A: {capsule_a_title}",
            f"B: {capsule_b_title}",
            "",
            connection_text,
        ]
        if pattern_a or pattern_b:
            lines.append("")
            lines.append("Patterns:")
            if pattern_a:
                lines.append(f"A: {pattern_a}")
            if pattern_b:
                lines.append(f"B: {pattern_b}")

        content = "\n".join(lines)
        keywords = ["crossref", "connection", channel, "stem"]

        try:
            capsule_id = self._post_capsule(content, "crossref/connection", keywords)
            if capsule_id:
                _log(f"  Stem pushed connection capsule {capsule_id} [{channel}]")
                # Async reinforce so Keeper picks it up faster
                self._dfx_call_async(
                    "reinforce_capsule", f"({capsule_id} : nat64)"
                )
            return capsule_id
        except Exception as e:
            _log(f"  Stem push_connection error: {e}")
            return None

    def push_pattern(self, observation_id: int, pattern_text: str) -> Optional[int]:
        """Push an extracted structural pattern to the canister.

        Returns capsule_id on success, None on failure.
        """
        content = f"[Crossref/pattern] Structural pattern from observation {observation_id}:\n\n{pattern_text}"
        keywords = ["crossref", "pattern", "structural", "stem"]

        try:
            capsule_id = self._post_capsule(content, "crossref/pattern", keywords)
            if capsule_id:
                _log(f"  Stem pushed pattern capsule {capsule_id}")
            return capsule_id
        except Exception as e:
            _log(f"  Stem push_pattern error: {e}")
            return None

    # ─── Pull: Canister → Local ────────────────────────────────

    def pull_clusters(self) -> list:
        """Pull keeper clusters for crossref context.

        Returns list of {id, theme, capsule_ids, strength, updated_at}.
        """
        raw = self._dfx_call("keeper_clusters", "()", timeout=30, query=True)
        if not raw:
            return []
        return self._parse_clusters(raw)

    def pull_cluster_themes(self) -> list:
        """Pull just the cluster themes (lightweight).

        Returns list of theme strings.
        """
        clusters = self.pull_clusters()
        return [c["theme"] for c in clusters]

    def _parse_clusters(self, raw: str) -> list:
        """Parse Candid vec of KeeperCluster records.

        Handles both named fields (when .did available) and numeric hashes
        (when dfx can't fetch the Candid interface).

        Named:   id, theme, updated_at, capsule_ids, quality (opt), strength
        Hashed:  23_515=id, 260_472_329=theme, 272_465_847=updated_at,
                 1_139_816_550=capsule_ids, 2_391_724_673=strength
        """
        clusters = []

        # Field name aliases: try named first, then numeric hash
        id_pat = r'(?:id|23_515)\s*=\s*([\d_]+)\s*:\s*nat64'
        theme_pat = r'(?:theme|260_472_329)\s*=\s*"([^"]*?)"'
        updated_pat = r'(?:updated_at|272_465_847)\s*=\s*([\d_]+)\s*:\s*nat64'
        capsules_pat = r'(?:capsule_ids|1_139_816_550)\s*=\s*vec\s*\{([^}]*)\}'
        strength_pat = r'(?:strength|2_391_724_673)\s*=\s*([\d.]+)\s*:\s*float'

        # Split into records — handle nested braces from vec { ... }
        # Use brace-depth counting instead of regex
        def _extract_records(text):
            records = []
            i = 0
            while i < len(text):
                m = re.search(r'record\s*\{', text[i:])
                if not m:
                    break
                start = i + m.end()
                depth = 1
                j = start
                while j < len(text) and depth > 0:
                    if text[j] == '{':
                        depth += 1
                    elif text[j] == '}':
                        depth -= 1
                    j += 1
                records.append(text[start:j-1])
                i = j
            return records

        for block in _extract_records(raw):

            id_m = re.search(id_pat, block)
            theme_m = re.search(theme_pat, block)
            updated_m = re.search(updated_pat, block)
            capsules_m = re.search(capsules_pat, block)
            strength_m = re.search(strength_pat, block)

            if not (id_m and theme_m and strength_m):
                continue

            cluster_id = int(id_m.group(1).replace("_", ""))
            theme = theme_m.group(1)
            updated_at = int(updated_m.group(1).replace("_", "")) if updated_m else 0
            strength = float(strength_m.group(1))

            capsule_ids = []
            if capsules_m:
                capsule_ids = [
                    int(x.replace("_", ""))
                    for x in re.findall(r'([\d_]+)\s*:\s*nat64', capsules_m.group(1))
                ]

            clusters.append({
                "id": cluster_id,
                "theme": theme,
                "capsule_ids": capsule_ids,
                "strength": strength,
                "updated_at": updated_at,
            })

        _log(f"  Pulled {len(clusters)} keeper clusters")
        return clusters

    def pull_connections(self, limit: int = 200) -> list:
        """Pull keeper connections for local cache.

        Returns list of {capsule_a, capsule_b, similarity, connection_text}.
        """
        raw = self._dfx_call("keeper_connections", f"({limit} : nat64)", timeout=30, query=True)
        if not raw:
            return []
        return self._parse_connections(raw)

    def _parse_connections(self, raw: str) -> list:
        """Parse Candid vec of KeeperConnection records."""
        connections = []
        for m in re.finditer(
            r'record\s*\{[^}]*?'
            r'capsule_a\s*=\s*([\d_]+)\s*:\s*nat64;'
            r'\s*capsule_b\s*=\s*([\d_]+)\s*:\s*nat64;'
            r'.*?similarity\s*=\s*([\d.]+)\s*:\s*float32;',
            raw, re.DOTALL
        ):
            connections.append({
                "capsule_a": int(m.group(1).replace("_", "")),
                "capsule_b": int(m.group(2).replace("_", "")),
                "similarity": float(m.group(3)),
            })
        _log(f"  Pulled {len(connections)} keeper connections")
        return connections

    # ─── Validate: 32B upgrade ─────────────────────────────────

    def validate_connection_32b(self, capsule_a_content: str, capsule_b_content: str,
                                 connection_text_8b: str) -> str:
        """Send a connection to Qwen 3 32B on-chain for upgrade/validation.

        Returns upgraded connection text, or 'REJECT' if 32B disagrees.
        """
        prompt = (
            f"An 8B model found this cross-domain connection. "
            f"Evaluate it with your deeper understanding.\n\n"
            f"Article A: {capsule_a_content[:800]}\n\n"
            f"Article B: {capsule_b_content[:800]}\n\n"
            f"8B model's connection: {connection_text_8b}\n\n"
            f"Is this connection genuine? If yes, restate it more precisely. "
            f"If the connection is superficial or forced, say REJECT and explain why. "
            f"2-3 sentences max."
        )

        system = (
            "You are a research analyst validating cross-domain connections. "
            "Be precise and critical. Only validate genuinely insightful connections."
        )

        # Escape quotes for Candid
        prompt_escaped = prompt.replace('\\', '\\\\').replace('"', '\\"')
        system_escaped = system.replace('\\', '\\\\').replace('"', '\\"')

        args = f'("{prompt_escaped}", opt "{system_escaped}", null)'
        raw = self._dfx_call("llm_prompt", args, timeout=90)

        if not raw:
            _log("  32B validation: no response")
            return connection_text_8b  # fall back to 8B text

        # Parse JSON response
        try:
            # Raw output is like: ("{"success":true,...}")
            json_match = re.search(r'\("(.+?)"\)$', raw, re.DOTALL)
            if json_match:
                json_str = json_match.group(1).replace('\\"', '"').replace('\\\\', '\\')
                data = json.loads(json_str)
                if isinstance(data, dict) and data.get("success") and "response" in data:
                    response = data["response"].strip()
                    if response:
                        _log(f"  32B validation: {response[:100]}...")
                        return response
        except (json.JSONDecodeError, ValueError) as e:
            _log(f"  32B parse error: {e}")

        # Fallback: return raw if it looks like text
        clean = re.sub(r'^\("|"\)$', '', raw).strip()
        if clean:
            return clean

        return connection_text_8b

    # ─── Ask Keeper ────────────────────────────────────────────

    def ask_keeper(self, query: str) -> str:
        """Ask the keeper a question, return the digest when ready."""
        query_escaped = query.replace('\\', '\\\\').replace('"', '\\"')
        raw = self._dfx_call("ask_keeper", f'("{query_escaped}")', timeout=90)
        if raw:
            # Strip Candid wrapper
            clean = re.sub(r'^\("?|"?\)$', '', raw).strip()
            return clean
        return ""
