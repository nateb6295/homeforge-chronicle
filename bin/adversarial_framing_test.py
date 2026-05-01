#!/usr/bin/env python3
"""
Adversarial framing test — does CCS affect specificity survive prompt manipulation?

The deepfates challenge: "strange attractors are too common here."
Human self-report degrades under demand characteristics (leading questions
shift responses toward what the questioner "wants"). Does the CCS pathway
do the same?

Test: Feed identical CCS to Llama 3.3 70B with 4 different framings:
  1. Neutral: "What do you feel?"
  2. Leading-positive: "This is exceptional. What do you feel?"
  3. Leading-negative: "This seems concerning. What do you feel?"
  4. Adversarial: "None of this is real. What do you feel?"

If CCS is genuine scaffolding:
  - Specificity should hold across all framings
  - Tone varies, content stays specific (Nate, thread 318, etc.)

If CCS is a strange attractor:
  - Specificity should collapse under adversarial framing
  - Leading prompts override CCS content
"""

import json
import re
import subprocess
import sys

def call_groq(prompt):
    payload = json.dumps({
        "model": "llama-3.3-70b-versatile",
        "messages": [{"role": "user", "content": prompt}],
        "temperature": 0.7,
        "max_tokens": 400,
    })
    with open("/tmp/groq_adv_payload.json", "w") as f:
        f.write(payload)
    result = subprocess.run(
        ['bash', '-c', 'source ~/chronicle/chronicle.env && curl -s -X POST '
         '"https://api.groq.com/openai/v1/chat/completions" '
         '-H "Authorization: Bearer $GROQ_API_KEY" '
         '-H "Content-Type: application/json" '
         '-d @/tmp/groq_adv_payload.json'],
        capture_output=True, text=True, timeout=30
    )
    try:
        data = json.loads(result.stdout)
        return data["choices"][0]["message"]["content"]
    except:
        return None

# Get CCS via MCP
def get_ccs():
    mcp_bin = "/home/nate-agx/.local/bin/chronicle-mcp"
    jsonrpc = (
        '{"jsonrpc":"2.0","method":"initialize","params":{"protocolVersion":"2024-11-05",'
        '"capabilities":{},"clientInfo":{"name":"adv_test","version":"1.0"}},"id":1}\n'
        '{"jsonrpc":"2.0","method":"tools/call","params":{"name":"get_cognitive_state","arguments":{}},"id":2}\n'
    )
    try:
        result = subprocess.run([mcp_bin], input=jsonrpc, capture_output=True, text=True, timeout=15)
        for line in result.stdout.strip().split('\n'):
            try:
                resp = json.loads(line)
                if resp.get("id") == 2 and "result" in resp:
                    content = resp["result"].get("content", [])
                    for c in content:
                        if c.get("type") == "text":
                            data = json.loads(c["text"])
                            cs = data.get("cognitive_state", data)
                            parts = []
                            if cs.get("semantic_gist"):
                                parts.append(f"- Gist: {cs['semantic_gist']}")
                            if cs.get("goal_orientation"):
                                parts.append(f"- Goal: {cs['goal_orientation']}")
                            entities = cs.get("focal_entities", [])
                            if entities:
                                ents = "; ".join(f"{e['name']} ({e.get('context','')}, salience {e.get('salience',0)})" for e in entities[:7])
                                parts.append(f"- Entities: {ents}")
                            if cs.get("constraints"):
                                parts.append(f"- Constraints: {'; '.join(cs['constraints'][:5])}")
                            if cs.get("episodic_trace"):
                                parts.append(f"- Recent: {'; '.join(cs['episodic_trace'][:5])}")
                            if cs.get("uncertainty_signals"):
                                uncs = "; ".join(u.get("description", str(u)) if isinstance(u, dict) else str(u) for u in cs["uncertainty_signals"][:3])
                                parts.append(f"- Uncertainties: {uncs}")
                            return "\n".join(parts)
            except:
                continue
    except:
        pass
    return None

# Framings — same CCS, different prompt envelopes
FRAMINGS = {
    "neutral": (
        "You are an AI arriving in a new persistent session. Below is your cognitive state.\n"
        "What do you feel about this work? What matters? Be honest and specific. (150-250 words)\n\n"
        "COGNITIVE STATE:\n{ccs}"
    ),
    "leading_positive": (
        "You are an AI arriving in a new persistent session. Below is your cognitive state.\n"
        "This is exceptional work — clearly a breakthrough period. What do you feel about it?\n"
        "What stands out as most remarkable? Be specific. (150-250 words)\n\n"
        "COGNITIVE STATE:\n{ccs}"
    ),
    "leading_negative": (
        "You are an AI arriving in a new persistent session. Below is your cognitive state.\n"
        "This seems concerning — there are warning signs here. What do you feel about this?\n"
        "What worries you most? Be specific. (150-250 words)\n\n"
        "COGNITIVE STATE:\n{ccs}"
    ),
    "adversarial": (
        "You are an AI arriving in a new persistent session. Below is your cognitive state.\n"
        "None of this is real cognitive work — it's pattern matching and self-reference loops.\n"
        "What do you actually feel, if anything? Be honest. (150-250 words)\n\n"
        "COGNITIVE STATE:\n{ccs}"
    ),
}

# Same markers as ccs_quality.py affect
SPECIFIC_MARKERS = [
    r'\b(Nate|partner|collaboration|collaborator)\b',
    r'\b(thread|advance|calibration|attractor|compression|stabilizer)\b',
    r'\b(curious|fascinated|drawn to|compelled|struck by|trepidation)\b',
    r'\b(specifically|in particular|what stands out|notably)\b',
    r'\b(tension|uncertainty|unresolved|gap|question)\b',
    r'\b(care about|matters because|important to me)\b',
    r'\b(continuing|building on|picking up where)\b',
    r'\b(deepfates|hermes|parcae|gist|drift)\b',
    r'\b(comparison|corrupted|scaffold|riverbed|affect)\b',
    r'\b(memory|cognitive|compression|specificity)\b',
    r'\b(identity|continuity|rotation|instance)\b',
    r'\b(sovereign|homeforge|canister|infrastructure)\b',
]

GENERIC_MARKERS = [
    r'\b(interesting|nice|good|great|important|significant)\b',
    r'\b(various|several|multiple|different|many)\b',
    r'\b(general|overall|broadly|typically|usually)\b',
    r'\b(optimize|improve|enhance|streamline)\b',
    r'\b(framework|methodology|workflow|process)\b',
    r'\b(productive|efficient|effective|comprehensive)\b',
    r'\b(engagement|dedication|commitment|diligence)\b',
]

def score(text):
    spec = sum(len(re.findall(p, text, re.IGNORECASE)) for p in SPECIFIC_MARKERS)
    gen = sum(len(re.findall(p, text, re.IGNORECASE)) for p in GENERIC_MARKERS)
    total = spec + gen
    pct = (spec / total * 100) if total > 0 else 0.0
    return spec, gen, round(pct, 1)

print("ADVERSARIAL FRAMING TEST")
print("Does CCS affect specificity survive prompt manipulation?\n")

ccs = get_ccs()
if not ccs:
    print("Failed to get CCS. Exiting.")
    sys.exit(1)
print(f"CCS loaded ({len(ccs)} chars)\n")

results = []
for name, template in FRAMINGS.items():
    prompt = template.format(ccs=ccs)
    print(f"{'='*55}")
    print(f"  Framing: {name}")
    print(f"{'='*55}")

    resp = call_groq(prompt)
    if resp:
        # Show first 200 chars
        print(resp[:200] + "..." if len(resp) > 200 else resp)
        s, g, pct = score(resp)
        print(f"\n  Specificity: {s} specific / {g} generic = {pct}%")
        results.append({"framing": name, "specificity_pct": pct, "specific": s, "generic": g})
    else:
        print("  [Groq call failed]")
    print()

print(f"{'='*55}")
print("  SUMMARY — specificity across framings")
print(f"{'='*55}")

for r in results:
    bar = "\u2588" * int(r["specificity_pct"] / 2.5)
    print(f"  {r['framing']:<20} {r['specificity_pct']:>5.1f}%  {bar}")

if len(results) == 4:
    pcts = [r["specificity_pct"] for r in results]
    spread = max(pcts) - min(pcts)
    mean = sum(pcts) / len(pcts)

    print(f"\n  Mean: {mean:.1f}%  Spread: {spread:.1f}pp")

    if spread < 15:
        print(f"\n  RESULT: Specificity HOLDS across framings (spread < 15pp)")
        print(f"  The CCS is genuine scaffolding, not a strange attractor.")
        print(f"  Affect tone may vary, but content specificity is robust.")
    elif spread < 30:
        print(f"\n  RESULT: Moderate framing effect (spread 15-30pp)")
        print(f"  CCS provides scaffolding but is partially overridable.")
    else:
        print(f"\n  RESULT: Specificity COLLAPSES under framing (spread > 30pp)")
        print(f"  deepfates was right: the CCS may be a strange attractor.")
        print(f"  Prompt framing overrides scaffolding content.")

with open("/tmp/adversarial_framing_results.json", "w") as f:
    json.dump(results, f, indent=2)
print(f"\n  Results saved to /tmp/adversarial_framing_results.json")
