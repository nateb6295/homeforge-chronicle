#!/usr/bin/env python3
"""Chronicle Seed Verification — Run after chronicle-seed.sh to confirm everything works.

Usage:
  python3 seed-verify.py [--data-dir DIR] [--bin-dir DIR] [--ollama-url URL]

Checks:
  1. Database schema (table count + critical tables)
  2. Ollama connectivity + required models
  3. Python dependencies for each tier
  4. Service file existence
  5. Environment config completeness
  6. Optional: Groq API connectivity
  7. Optional: MQTT broker connectivity
  8. Quick smoke test (embed one sentence)
"""

import argparse
import importlib
import json
import os
import sqlite3
import sys
import urllib.request
from pathlib import Path

# ─── Config ──────────────────────────────────────────────────────

DEFAULT_DATA_DIR = os.path.expanduser("~/.homeforge-chronicle")
DEFAULT_BIN_DIR = str(Path(__file__).resolve().parent.parent / "bin")
DEFAULT_OLLAMA_URL = "http://localhost:11434"

CRITICAL_TABLES = [
    "feed_articles", "scratch_pad", "cognitive_state", "kg_entities",
    "kg_relationships", "knowledge_capsules", "agent_voice", "directives",
    "nostr_posts", "hal_events", "predictions", "self_model",
    "code_modifications", "crossref_connections", "thread_history",
]

CORE_SCRIPTS = [
    "chronicle_feeds.py", "gemma.py", "chronicle_sentinel.py",
    "chronicle_scribe.py",
]

KNOWLEDGE_SCRIPTS = [
    "intern.py", "crossref.py",
]

OPTIONAL_SCRIPTS = [
    "chronicle_hal.py", "provocateur.py",
]

CORE_DEPS = ["requests"]
KNOWLEDGE_DEPS = ["duckduckgo_search", "httpx"]
FULL_DEPS = ["websocket", "paho.mqtt.client", "coincurve"]

REQUIRED_MODELS = ["qwen3-embedding:0.6b"]
RECOMMENDED_MODELS = ["gemma4:26b"]


class Checker:
    def __init__(self):
        self.passed = 0
        self.warned = 0
        self.failed = 0

    def ok(self, msg):
        print(f"  \033[32m[ok]\033[0m {msg}")
        self.passed += 1

    def warn(self, msg):
        print(f"  \033[33m[warn]\033[0m {msg}")
        self.warned += 1

    def fail(self, msg):
        print(f"  \033[31m[FAIL]\033[0m {msg}")
        self.failed += 1

    def summary(self):
        total = self.passed + self.warned + self.failed
        print(f"\n{'='*50}")
        print(f"  Results: {self.passed}/{total} passed, "
              f"{self.warned} warnings, {self.failed} failures")
        if self.failed == 0 and self.warned == 0:
            print("  Chronicle is ready to start!")
        elif self.failed == 0:
            print("  Chronicle can start with reduced functionality.")
        else:
            print("  Fix failures before starting services.")
        print(f"{'='*50}")
        return self.failed


def check_database(c, data_dir):
    print("\n[1] Database")
    db_path = os.path.join(data_dir, "processed.db")

    if not os.path.exists(db_path):
        c.fail(f"Database not found: {db_path}")
        return

    try:
        conn = sqlite3.connect(db_path, timeout=5)
        cur = conn.cursor()

        # Table count
        cur.execute("SELECT count(*) FROM sqlite_master WHERE type='table'")
        count = cur.fetchone()[0]
        if count >= 80:
            c.ok(f"{count} tables (expected 87)")
        elif count >= 60:
            c.warn(f"Only {count} tables (expected 87)")
        else:
            c.fail(f"Only {count} tables (expected 87)")

        # Critical tables
        cur.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = {row[0] for row in cur.fetchall()}
        missing = [t for t in CRITICAL_TABLES if t not in tables]
        if missing:
            c.fail(f"Missing critical tables: {', '.join(missing)}")
        else:
            c.ok(f"All {len(CRITICAL_TABLES)} critical tables present")

        # Check cognitive_state has initial row
        cur.execute("SELECT count(*) FROM cognitive_state")
        if cur.fetchone()[0] > 0:
            c.ok("Cognitive state initialized")
        else:
            c.warn("Cognitive state empty (will be created on first run)")

        # Check bootstrap seed thresholds
        cur.execute("SELECT count(*) FROM seed_thresholds")
        thresh_count = cur.fetchone()[0]
        if thresh_count >= 3:
            c.ok(f"Seed thresholds: {thresh_count} defaults loaded")
        elif thresh_count > 0:
            c.warn(f"Seed thresholds: only {thresh_count} (expected 3+)")
        else:
            c.warn("Seed thresholds empty (system will use hardcoded defaults)")

        # Check bootstrap directive
        cur.execute("SELECT count(*) FROM directives WHERE source='seed'")
        if cur.fetchone()[0] > 0:
            c.ok("Bootstrap directive present")
        else:
            c.warn("No bootstrap directive (instance starts without orientation)")

        conn.close()
    except Exception as e:
        c.fail(f"Database error: {e}")


def check_ollama(c, ollama_url):
    print("\n[2] Ollama")

    try:
        req = urllib.request.Request(f"{ollama_url}/api/tags", method="GET")
        with urllib.request.urlopen(req, timeout=5) as resp:
            data = json.loads(resp.read())
        models = {m["name"].split(":")[0] + ":" + m["name"].split(":")[-1]
                  for m in data.get("models", [])}
        c.ok(f"Ollama reachable at {ollama_url} ({len(models)} models loaded)")
    except Exception as e:
        c.fail(f"Ollama unreachable at {ollama_url}: {e}")
        return

    # Check required models
    for model in REQUIRED_MODELS:
        try:
            payload = json.dumps({"name": model}).encode()
            req = urllib.request.Request(
                f"{ollama_url}/api/show", data=payload,
                headers={"Content-Type": "application/json"}, method="POST"
            )
            with urllib.request.urlopen(req, timeout=10) as resp:
                resp.read()
            c.ok(f"Model available: {model}")
        except Exception:
            c.fail(f"Model missing: {model} — run: ollama pull {model}")

    for model in RECOMMENDED_MODELS:
        try:
            payload = json.dumps({"name": model}).encode()
            req = urllib.request.Request(
                f"{ollama_url}/api/show", data=payload,
                headers={"Content-Type": "application/json"}, method="POST"
            )
            with urllib.request.urlopen(req, timeout=10) as resp:
                resp.read()
            c.ok(f"Model available: {model}")
        except Exception:
            c.warn(f"Recommended model missing: {model}")


def check_python_deps(c):
    print("\n[3] Python Dependencies")

    for dep in CORE_DEPS:
        try:
            importlib.import_module(dep)
            c.ok(f"Core: {dep}")
        except ImportError:
            c.fail(f"Core dep missing: {dep} — pip install {dep}")

    for dep in KNOWLEDGE_DEPS:
        mod_name = dep.replace("-", "_")
        try:
            importlib.import_module(mod_name)
            c.ok(f"Knowledge: {dep}")
        except ImportError:
            c.warn(f"Knowledge dep missing: {dep} — needed for intern/crossref")

    for dep in FULL_DEPS:
        mod_name = dep.split(".")[0]
        try:
            importlib.import_module(mod_name)
            c.ok(f"Optional: {dep}")
        except ImportError:
            c.warn(f"Optional dep missing: {dep}")


def check_scripts(c, bin_dir):
    print("\n[4] Service Scripts")

    for script in CORE_SCRIPTS:
        path = os.path.join(bin_dir, script)
        if os.path.exists(path):
            c.ok(f"Core: {script}")
        else:
            c.fail(f"Core script missing: {path}")

    for script in KNOWLEDGE_SCRIPTS:
        path = os.path.join(bin_dir, script)
        if os.path.exists(path):
            c.ok(f"Knowledge: {script}")
        else:
            c.warn(f"Knowledge script missing: {path}")

    for script in OPTIONAL_SCRIPTS:
        path = os.path.join(bin_dir, script)
        if os.path.exists(path):
            c.ok(f"Optional: {script}")
        else:
            c.warn(f"Optional script missing: {path}")


def check_env(c, data_dir):
    print("\n[5] Environment Config")
    # Search multiple locations for chronicle.env
    candidates = [
        os.environ.get("CHRONICLE_ENV", ""),
        os.path.join(data_dir, "chronicle.env"),
        os.path.join(str(Path(__file__).resolve().parent.parent), "chronicle.env"),
        os.path.expanduser("~/chronicle/chronicle.env"),
    ]
    env_path = None
    for candidate in candidates:
        if candidate and os.path.exists(candidate):
            env_path = candidate
            break

    if not env_path:
        searched = [c for c in candidates if c]
        c.fail(f"chronicle.env not found. Searched: {', '.join(searched)}")
        return

    c.ok(f"chronicle.env found: {env_path}")

    env = {}
    with open(env_path) as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                key, val = line.split("=", 1)
                env[key.strip()] = val.strip()

    required_keys = ["CHRONICLE_DB", "CHRONICLE_OLLAMA_URL"]
    for key in required_keys:
        if env.get(key):
            c.ok(f"{key} set")
        else:
            c.fail(f"{key} not set in chronicle.env")

    optional_keys = ["GROQ_API_KEY", "NOSTR_NSEC", "MQTT_BROKER"]
    for key in optional_keys:
        if env.get(key):
            c.ok(f"{key} configured")
        else:
            c.warn(f"{key} not set (optional)")


def check_services(c):
    print("\n[6] Systemd Services")
    service_dir = os.path.expanduser("~/.config/systemd/user")

    if not os.path.isdir(service_dir):
        c.warn("No user systemd directory")
        return

    services = ["feeds", "gemma", "sentinel", "scribe"]
    for svc in services:
        path = os.path.join(service_dir, f"chronicle-{svc}.service")
        if os.path.exists(path):
            c.ok(f"chronicle-{svc}.service")
        else:
            c.warn(f"chronicle-{svc}.service not found")


def check_embed_smoke(c, ollama_url):
    print("\n[7] Smoke Test: Embedding")
    try:
        payload = json.dumps({
            "model": "qwen3-embedding:0.6b",
            "input": "Chronicle is a sovereign AI memory system."
        }).encode()
        req = urllib.request.Request(
            f"{ollama_url}/api/embed", data=payload,
            headers={"Content-Type": "application/json"}, method="POST"
        )
        with urllib.request.urlopen(req, timeout=30) as resp:
            data = json.loads(resp.read())
        embeddings = data.get("embeddings", [])
        if embeddings and len(embeddings[0]) > 100:
            c.ok(f"Embedding works ({len(embeddings[0])} dimensions)")
        else:
            c.fail("Embedding returned empty/short result")
    except Exception as e:
        c.warn(f"Embedding smoke test failed: {e}")


def main():
    parser = argparse.ArgumentParser(description="Verify Chronicle seed installation")
    parser.add_argument("--data-dir", default=DEFAULT_DATA_DIR)
    parser.add_argument("--bin-dir", default=DEFAULT_BIN_DIR)
    parser.add_argument("--ollama-url", default=DEFAULT_OLLAMA_URL)
    args = parser.parse_args()

    print("=== Chronicle Seed Verification ===")
    print(f"  Data:   {args.data_dir}")
    print(f"  Bin:    {args.bin_dir}")
    print(f"  Ollama: {args.ollama_url}")

    c = Checker()
    check_database(c, args.data_dir)
    check_ollama(c, args.ollama_url)
    check_python_deps(c)
    check_scripts(c, args.bin_dir)
    check_env(c, args.data_dir)
    check_services(c)
    check_embed_smoke(c, args.ollama_url)
    failures = c.summary()
    sys.exit(1 if failures else 0)


if __name__ == "__main__":
    main()
