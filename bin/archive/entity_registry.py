#!/usr/bin/env python3
"""Entity Registry — Curated ground truth for Chronicle entities.

Adapted from MemPalace's entity_registry.py (bensig/mempalace).

Chronicle's auto-populated KG has 15K+ entities — mostly noise from
geopolitical captures. This registry maintains a curated set of entities
that Chronicle actually cares about, with:
  - Canonical names and aliases
  - Entity types (person, agent, infrastructure, concept, asset)
  - Relationships that are KNOWN (not inferred)
  - Disambiguation for ambiguous names

The registry is the ground truth. The KG can auto-populate from captures,
but the registry is what the family trusts.

Usage:
    from entity_registry import Registry

    reg = Registry()
    result = reg.lookup("Nate")
    # → {"type": "person", "canonical": "Nate", "aliases": ["Brad"], ...}

    reg.lookup("XRP")
    # → {"type": "asset", "canonical": "XRP", "aliases": ["Ripple"], ...}

Build #80. Stolen from MemPalace, adapted for Chronicle.
"""

import os
import json
from pathlib import Path
from typing import Dict, List, Optional


REGISTRY_PATH = os.path.expanduser("~/chronicle/data/entity_registry.json")


# ═══════════════════════════════════════════════════════════════════
#  Default Registry — Chronicle's ground truth
# ═══════════════════════════════════════════════════════════════════

DEFAULT_REGISTRY = {
    "version": 1,
    "entities": {
        # People
        "Nate": {
            "type": "person",
            "aliases": ["Brad", "Nate-AGX", "phi_nate"],
            "role": "Collaborator, father, Senior Estimator, sovereignty builder",
            "relationships": {
                "partner_of": "Opus",
                "creator_of": "Chronicle",
            },
        },
        # Agents (family)
        "Opus": {
            "type": "agent",
            "aliases": ["Opus 4.6", "Claude Opus"],
            "role": "Synthesizer, thread writer, build shipper",
            "model": "claude-opus-4-6",
            "relationships": {
                "partner_of": "Nate",
                "leads": "Chronicle Swarm",
            },
        },
        "Darby": {
            "type": "agent",
            "aliases": ["DRB"],
            "role": "The curious one — questions, connections, suggestions",
            "model": "Qwen3-32B via DeepInfra",
            "relationships": {
                "sibling_of": ["Ada", "Gemma"],
                "runs_on": "DeepInfra",
            },
        },
        "Ada": {
            "type": "agent",
            "aliases": ["ADA", "GPT-OSS-120B"],
            "role": "The challenger — keeps everyone honest, structural analysis",
            "model": "GPT-OSS-120B via Groq",
            "relationships": {
                "sibling_of": ["Darby", "Gemma"],
                "runs_on": "Groq",
            },
        },
        "Gemma": {
            "type": "agent",
            "aliases": ["GEM", "Gemma 4 26B"],
            "role": "The gate — filters everything entering the pipeline",
            "model": "Gemma 4 26B local",
            "relationships": {
                "sibling_of": ["Darby", "Ada"],
                "runs_on": "AGX Orin",
            },
        },
        # Infrastructure
        "AGX Orin": {
            "type": "infrastructure",
            "aliases": ["AGX", "Jetson", "Jetson AGX Orin", "192.168.1.70"],
            "role": "Edge compute platform — runs all Chronicle services",
        },
        "Chronicle": {
            "type": "project",
            "aliases": ["CHR", "Homeforge Chronicle", "Chronicle AGX"],
            "role": "Autonomous knowledge synthesis swarm",
        },
        "Homeforge": {
            "type": "concept",
            "aliases": ["HMF"],
            "role": "Philosophy — building toward sovereignty, local infrastructure",
        },
        # Assets
        "XRP": {
            "type": "asset",
            "aliases": ["Ripple", "XRPL"],
            "role": "Nate holds 13K+ XRP. Plant the flag and walk away.",
            "relationships": {
                "held_by": "Nate",
            },
        },
        # Inference providers
        "DeepInfra": {
            "type": "infrastructure",
            "aliases": ["DPI"],
            "role": "Primary inference — Qwen3-235B for Darby",
        },
        "Cerebras": {
            "type": "infrastructure",
            "aliases": ["CRB"],
            "role": "Fallback inference — Qwen3-235B",
        },
        "Groq": {
            "type": "infrastructure",
            "aliases": ["GRQ"],
            "role": "Ada's inference — llama-3.3-70b",
        },
        # Platforms
        "Nostr": {
            "type": "platform",
            "aliases": ["NST"],
            "role": "Public publishing — threads, essays, predictions",
        },
        "Discord": {
            "type": "platform",
            "aliases": ["DSC"],
            "role": "Internal communication — Nate's window in",
        },
        "ICP": {
            "type": "platform",
            "aliases": ["Internet Computer", "DFINITY"],
            "role": "On-chain storage — capsules, keeper, lab canisters",
        },
    },
}


class Registry:
    def __init__(self, path: str = None):
        self.path = path or REGISTRY_PATH
        self._data = None
        self._load()

    def _load(self):
        if os.path.exists(self.path):
            try:
                with open(self.path) as f:
                    self._data = json.load(f)
                return
            except (json.JSONDecodeError, OSError):
                pass
        self._data = DEFAULT_REGISTRY
        self._save()

    def _save(self):
        Path(self.path).parent.mkdir(parents=True, exist_ok=True)
        with open(self.path, 'w') as f:
            json.dump(self._data, f, indent=2)

    def lookup(self, name: str) -> Optional[Dict]:
        """Look up an entity by name or alias."""
        name_lower = name.lower()
        for canonical, info in self._data["entities"].items():
            if canonical.lower() == name_lower:
                return {"canonical": canonical, **info}
            aliases = [a.lower() for a in info.get("aliases", [])]
            if name_lower in aliases:
                return {"canonical": canonical, **info}
        return None

    def lookup_type(self, entity_type: str) -> List[Dict]:
        """Get all entities of a given type."""
        results = []
        for canonical, info in self._data["entities"].items():
            if info.get("type") == entity_type:
                results.append({"canonical": canonical, **info})
        return results

    def add_entity(self, name: str, entity_type: str, role: str = "",
                   aliases: List[str] = None, relationships: Dict = None):
        """Add or update an entity."""
        self._data["entities"][name] = {
            "type": entity_type,
            "aliases": aliases or [],
            "role": role,
        }
        if relationships:
            self._data["entities"][name]["relationships"] = relationships
        self._save()

    def remove_entity(self, name: str) -> bool:
        """Remove an entity from the registry."""
        if name in self._data["entities"]:
            del self._data["entities"][name]
            self._save()
            return True
        return False

    def all_names(self) -> List[str]:
        """Get all known names and aliases for entity detection."""
        names = []
        for canonical, info in self._data["entities"].items():
            names.append(canonical)
            names.extend(info.get("aliases", []))
        return names

    def entity_codes(self) -> Dict[str, str]:
        """Get entity → 3-letter code mapping for AAAK compression."""
        codes = {}
        for canonical, info in self._data["entities"].items():
            for alias in info.get("aliases", []):
                if len(alias) == 3 and alias.isupper():
                    codes[canonical.lower()] = alias
                    break
            else:
                codes[canonical.lower()] = canonical[:3].upper()
        return codes

    def stats(self) -> Dict:
        """Registry statistics."""
        from collections import Counter
        types = Counter(info["type"] for info in self._data["entities"].values())
        return {
            "total": len(self._data["entities"]),
            "by_type": dict(types),
            "version": self._data.get("version", 0),
        }

    def summary(self) -> str:
        """Human-readable summary of the registry."""
        lines = ["## Entity Registry"]
        by_type = {}
        for canonical, info in self._data["entities"].items():
            t = info.get("type", "unknown")
            if t not in by_type:
                by_type[t] = []
            aliases = ", ".join(info.get("aliases", [])[:3])
            by_type[t].append(f"  {canonical}" + (f" ({aliases})" if aliases else ""))

        for entity_type, entries in sorted(by_type.items()):
            lines.append(f"\n### {entity_type.upper()} ({len(entries)})")
            lines.extend(entries)

        return "\n".join(lines)


# ═══════════════════════════════════════════════════════════════════
#  CLI
# ═══════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    import sys

    reg = Registry()

    if len(sys.argv) < 2:
        print("Usage:")
        print("  entity_registry.py lookup <name>")
        print("  entity_registry.py list [type]")
        print("  entity_registry.py add <name> <type> <role>")
        print("  entity_registry.py summary")
        print("  entity_registry.py stats")
        print("  entity_registry.py codes       # AAAK entity codes")
        sys.exit(0)

    cmd = sys.argv[1]

    if cmd == "lookup":
        name = sys.argv[2] if len(sys.argv) > 2 else ""
        result = reg.lookup(name)
        if result:
            print(json.dumps(result, indent=2))
        else:
            print(f"Unknown entity: {name}")

    elif cmd == "list":
        entity_type = sys.argv[2] if len(sys.argv) > 2 else None
        if entity_type:
            entities = reg.lookup_type(entity_type)
            for e in entities:
                print(f"  {e['canonical']:20} {e.get('role', '')[:60]}")
        else:
            for name in sorted(reg._data["entities"].keys()):
                info = reg._data["entities"][name]
                print(f"  {name:20} [{info['type']:15}] {info.get('role', '')[:50]}")

    elif cmd == "add":
        if len(sys.argv) < 5:
            print("Usage: entity_registry.py add <name> <type> <role>")
            sys.exit(1)
        reg.add_entity(sys.argv[2], sys.argv[3], sys.argv[4])
        print(f"Added: {sys.argv[2]} ({sys.argv[3]})")

    elif cmd == "summary":
        print(reg.summary())

    elif cmd == "stats":
        print(json.dumps(reg.stats(), indent=2))

    elif cmd == "codes":
        codes = reg.entity_codes()
        for name, code in sorted(codes.items()):
            print(f"  {code} = {name}")

    else:
        print(f"Unknown command: {cmd}")
