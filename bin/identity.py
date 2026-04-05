"""Canonical identity mapping for Chronicle agents.

Directive #96 — Thread #128 F3.

Every agent has ONE canonical name. All tables should use this name
or a structured derivative (e.g., activity:intern:brief).

Cross-table mapping:
  activity_feed.source  = canonical name (e.g., "intern")
  seed_observations.source = "activity:{canonical}:{action}" (e.g., "activity:intern:brief")
  feedback_events.source_agent = canonical name
  seed_thresholds.category = seed_observations.source format
"""

# The family names. If you're writing source/agent anywhere, use these.
CANONICAL_AGENTS = {
    'seed',
    'intern',
    'crossref',
    'provocateur',
    'sentinel',
    'feeds',
    'opus',
    'darby',       # Qwen3-32B researcher (runs through crossref/intern)
    'ada',         # GPT-OSS challenger (runs through crossref)
    'hal',         # Home Awareness Layer
    'atom',        # M5 sensor bridge
    'scribe',      # Verified record keeper
    'watchdog',    # Mind behavior monitor
    'dashboard',   # Visibility layer
    'engine',      # Inference server
}

# Historical names that should be normalized on write
ALIAS_MAP = {
    'nostr': 'opus',
    'nostr_post': 'opus',
    'opus:nostr': 'opus',
    'opus-cycle': 'opus',
    'opus:autonomous': 'opus',
    'opus:cycle': 'opus',
    # 'mind' is historical (old cognitive loop, not Opus). 654 entries. Keep as-is.
    # 'phi' is historical (nate-phi4 Discord bot, stopped 2026-03-21). Keep as-is.
    # 'operator:capture' is Nate. Keep as-is.
}

def canonical_source(raw_source: str) -> str:
    """Normalize a source string to its canonical form."""
    return ALIAS_MAP.get(raw_source, raw_source)

def seed_obs_to_agent(seed_source: str) -> str:
    """Extract canonical agent name from seed_observations source.
    
    'activity:intern:brief' → 'intern'
    'canister:capsule' → 'canister'
    'mqtt:homeforge/...' → 'mqtt'
    """
    parts = seed_source.split(':')
    if len(parts) >= 2 and parts[0] == 'activity':
        return parts[1]
    return parts[0]
