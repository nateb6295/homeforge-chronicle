"""Chronicle Mind - Action Parsing (robust JSON repair for messy LLM output)."""

import json
import re
from typing import List, Dict

from mind.utils import log, safe_truncate


# ═══════════════════════════════════════════════════════════════════
#  Action Parsing (robust - handles messy LLM output)
# ═══════════════════════════════════════════════════════════════════

def parse_actions(response: str) -> List[Dict]:
    """Parse JSON actions from LLM output. Handles messy JSON gracefully."""
    if not response:
        return []

    # Strip <think>...</think> internal monologue before JSON parsing
    if "<think>" in response:
        raw_think = re.search(r'<think>.*?</think>', response, flags=re.DOTALL)
        if raw_think:
            log(f"  Think block: {safe_truncate(raw_think.group(0), 300)}")
        response = re.sub(r'<think>.*?</think>', '', response, flags=re.DOTALL).strip()

    # Unwrap ICP canister response wrapper if present
    if response.strip().startswith('{') and '"success"' in response[:60]:
        try:
            # Clean Candid artifacts: literal backslash+newline → newline
            clean = response.replace('\\\n', '\n').replace('\\\r', '\r')
            maybe_wrapper = json.loads(clean)
            if isinstance(maybe_wrapper, dict) and maybe_wrapper.get("success") and "response" in maybe_wrapper:
                response = maybe_wrapper["response"]
        except (json.JSONDecodeError, ValueError):
            # Last resort: regex extract the response field
            m = re.search(r'"response"\s*:\s*"(.*)"', response, re.DOTALL)
            if m:
                inner = m.group(1).replace('\\"', '"').replace('\\n', '\n')
                response = inner

    # Strip markdown code fences (```json ... ```)
    cleaned = re.sub(r'```(?:json)?\s*', '', response)
    cleaned = cleaned.replace('```', '')
    response = cleaned.strip()

    # ── Standard JSON parse (try first — handles clean output) ──
    try:
        parsed = json.loads(response)
        if isinstance(parsed, list):
            result = [a for a in parsed if isinstance(a, dict) and "action" in a]
            if result:
                return result
        if isinstance(parsed, dict) and "action" in parsed:
            return [parsed]  # Single action object → wrap in array
        if isinstance(parsed, dict) and "actions" in parsed and isinstance(parsed["actions"], list):
            result = [a for a in parsed["actions"] if isinstance(a, dict) and "action" in a]
            if result:
                return result
    except json.JSONDecodeError:
        pass

    # ── Nemotron repair: fix common malformed JSON patterns ──
    # Pattern 1: ["action_name", "key": "val", ...] — mixed array/object
    # Pattern 2: ["action_name", {"key": "val"}, ...] — action name as bare string
    # Only runs if standard parse above failed
    if response.startswith("["):
        try:
            # Try to repair: extract action objects from malformed array
            repaired_actions = []
            # Split on top-level commas, respecting nesting
            depth = 0
            current = ""
            in_string = False
            escape_next = False
            items = []
            for ch in response[1:-1]:  # strip outer []
                if escape_next:
                    current += ch
                    escape_next = False
                    continue
                if ch == '\\':
                    current += ch
                    escape_next = True
                    continue
                if ch == '"' and not escape_next:
                    in_string = not in_string
                if not in_string:
                    if ch in '{[':
                        depth += 1
                    elif ch in '}]':
                        depth -= 1
                    elif ch == ',' and depth == 0:
                        items.append(current.strip())
                        current = ""
                        continue
                current += ch
            if current.strip():
                items.append(current.strip())

            # Try to pair action name strings with following key-value pairs
            i = 0
            while i < len(items):
                item = items[i].strip().strip('"')
                # If this looks like an action name (no colons, short)
                if ':' not in items[i] and len(item) < 40 and item.replace('_', '').isalpha():
                    # Collect following key:value pairs until next action name
                    obj = {"action": item}
                    i += 1
                    while i < len(items):
                        kv = items[i].strip()
                        # Check if this is a "key": "value" pair
                        kv_match = re.match(r'"([^"]+)"\s*:\s*(.*)', kv, re.DOTALL)
                        if kv_match:
                            key = kv_match.group(1)
                            val_str = kv_match.group(2).strip()
                            try:
                                val = json.loads(val_str)
                            except json.JSONDecodeError:
                                val = val_str.strip('"')
                            obj[key] = val
                            i += 1
                        else:
                            break  # Next action name or unparseable
                    repaired_actions.append(obj)
                elif items[i].strip().startswith('{'):
                    # Already a JSON object
                    try:
                        obj = json.loads(items[i].strip())
                        if isinstance(obj, dict) and "action" in obj:
                            repaired_actions.append(obj)
                    except json.JSONDecodeError:
                        pass
                    i += 1
                else:
                    i += 1

            if repaired_actions:
                log(f"  JSON repair: recovered {len(repaired_actions)} action(s) from malformed array")
                return repaired_actions
        except Exception:
            pass  # Fall through

    # Try to find a JSON array in the text
    try:
        start = response.find("[")
        end = response.rfind("]")
        if start != -1 and end > start:
            parsed = json.loads(response[start:end + 1])
            if isinstance(parsed, list):
                result = [a for a in parsed if isinstance(a, dict) and "action" in a]
                if result:
                    return result
    except json.JSONDecodeError:
        pass

    # Try to find individual JSON objects with "action" key
    actions = []
    i = 0
    while i < len(response):
        if response[i] == "{":
            depth = 0
            start = i
            for j in range(i, len(response)):
                if response[j] == "{":
                    depth += 1
                elif response[j] == "}":
                    depth -= 1
                    if depth == 0:
                        try:
                            obj = json.loads(response[start:j + 1])
                            if isinstance(obj, dict) and "action" in obj:
                                actions.append(obj)
                        except json.JSONDecodeError:
                            pass
                        i = j
                        break
            else:
                break
        i += 1

    if actions:
        return actions

    # ── Prose fallback: model produced text instead of JSON ──
    # Return explicit no_action to keep cycle fast (retry goes to ICP with stale context).
    if response and len(response) > 20:
        log(f"  Prose fallback: no JSON found, treating as no_action. Preview: {safe_truncate(response, 120)}")
        return [{"action": "no_action", "reason": f"LLM produced prose instead of JSON: {safe_truncate(response, 200)}"}]

    return []
