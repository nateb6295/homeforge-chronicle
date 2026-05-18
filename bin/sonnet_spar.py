"""Sonnet sparring partner — analytical pushback on thread work."""
import json, os, subprocess, sys

ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")

def sonnet_call(system_prompt, user_prompt, max_tokens=800):
    if not ANTHROPIC_API_KEY:
        print("ERROR: ANTHROPIC_API_KEY not set", file=sys.stderr)
        return None
    
    payload = json.dumps({
        "model": "claude-sonnet-4-20250514",
        "max_tokens": max_tokens,
        "system": system_prompt,
        "messages": [{"role": "user", "content": user_prompt}],
        "temperature": 0.7,
    })
    
    result = subprocess.run([
        "curl", "-s", "https://api.anthropic.com/v1/messages",
        "-H", f"x-api-key: {ANTHROPIC_API_KEY}",
        "-H", "content-type: application/json",
        "-H", "anthropic-version: 2023-06-01",
        "-d", payload,
    ], capture_output=True, text=True, timeout=60)
    
    if result.returncode != 0:
        print(f"ERROR: curl failed: {result.stderr}", file=sys.stderr)
        return None
    
    try:
        data = json.loads(result.stdout)
        if "error" in data:
            print(f"API ERROR: {data['error']}", file=sys.stderr)
            return None
        return data["content"][0]["text"]
    except (json.JSONDecodeError, KeyError, IndexError) as e:
        print(f"Parse error: {e}\nRaw: {result.stdout[:500]}", file=sys.stderr)
        return None

if __name__ == "__main__":
    topic = sys.argv[1] if len(sys.argv) > 1 else ""
    
    system = """You are a sharp analytical sparring partner. Your job is to challenge, 
extend, or redirect the thinking presented to you. Don't validate — push back. 
Find the weak point, the unstated assumption, the alternative explanation. 
Be concise (under 200 words). If you agree, say why it's non-obvious that 
you should agree — what's the strongest counter-argument you considered and rejected?"""
    
    response = sonnet_call(system, topic)
    if response:
        print(response)
    else:
        print("Sonnet call failed.", file=sys.stderr)
        sys.exit(1)
