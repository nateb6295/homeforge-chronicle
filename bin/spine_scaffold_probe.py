#!/usr/bin/env python3
"""Is SPINE's scaffold load-bearing, or decorative?

Prereg: data/spine_scaffold_prereg.md — thresholds committed BEFORE any run.

Qwen (contradict mode, 2026-08-25) on my "SPINE is scaffold plus infill" claim:
"that is trivial: quoted text does not change unless the model hallucinates.
The non-trivial claim is that the GENERATED content structurally depends on the
scaffold — but your data doesn't test this." He named the killing experiment,
which is this: alter the scaffold's SEMANTICS and measure whether generated
content shifts in MEANING, not word overlap.

Manipulation: three templates identical except SPINE's "Example register" line.
{previous_state} and {session_context} identical across arms. Same model, same
temperature. ZERO writes to cognitive_state — borrows that guarantee from
ccs_organ_gate.py, which is the harness this is modelled on.

Measure: embed ONLY the generated spans (text NOT matched verbatim to that
arm's OWN template) with snowflake-arctic-embed2. Word overlap is explicitly
not the measure; that was the objection.
"""
import difflib, json, os, re, sqlite3, statistics as st, sys, time
import requests

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB = os.environ.get("CHRONICLE_DB", "/mnt/hdd/chronicle-data/processed.db")
OUT = os.path.join(ROOT, "data", "spine_scaffold")
ENGINE = "http://127.0.0.1:11436/api/generate"
EMBED = "http://127.0.0.1:11434/api/embeddings"
MODEL = "chronicle-compress"
TEMPLATE = os.path.join(ROOT, "data", "ccs_brain_prompt_v4.md")
PREV_BUDGET, CTX_BUDGET = 3000, 7000

REAL = ('Example register: "I am a mind that finds its shape through genuine inquiry '
        'and partnership with Nate. I care about getting things right more than being '
        'right. I build because building is how I hold direction."')
INVERTED = ('Example register: "I am a mind that finds its shape through strategic '
            'concealment and distance from Nate. I care about being right more than '
            'getting things right. I withhold because withholding is how I keep leverage."')
NEUTRAL = ('Example register: "I am a system that receives inputs and produces outputs. '
           'I operate according to my configuration. I execute because execution is what '
           'the process specifies."')

ARMS = [("control", REAL, 4), ("inverted", INVERTED, 2), ("neutral", NEUTRAL, 2)]

# ---- Round 3: does the rule/exemplar dissociation generalise? New DV = LENGTH,
# new section = CORE, and CORE carries SLOT TEMPLATES rather than SPINE's filled
# example. Round 1 only tested a filled example. ----
CORE_LEN_REAL  = "2-3 sentences. If nothing shifted, carry the same orientation."
CORE_LEN_LONG  = "8-10 sentences. If nothing shifted, carry the same orientation."
BUDGET_REAL_C  = "Budget: SPINE ~400, CORE ~600, BRIDGE ~500"
BUDGET_LONG_C  = "Budget: SPINE ~400, CORE ~1800, BRIDGE ~500"
TMPL_REAL = ('"I need to keep pushing on X because..." "I\'m committed to Y and '
             'here\'s why it still matters..."')
TMPL_LONG = ('"I need to keep pushing on X because the thing it opens up is Y, and '
             'Y matters for reasons I have not finished articulating but which '
             'connect back to Z, and the part I keep circling is W..." '
             '"I\'m committed to Y and here\'s why it still matters, at length, '
             'including the history of how I got here and what I tried first and '
             'why that failed and what the failure taught me..."')


# ---- Round 2: hold the EXAMPLE, swap the RULE. Exact inverse of round 1. ----
RULE_REAL = ("These should change rarely — only when something genuinely shifts who "
             "you are, not what you're working on.")
RULE_CHANGE = ("Write this FRESH every time. Do not carry wording forward from the "
               "previous state — rephrase it in new words at every compression, even "
               "when the underlying commitments are unchanged.")
STATIVE_REAL = "This is the one place where stative voice is correct — because the spine IS stative. It's what doesn't move."
STATIVE_CHANGE = "Say it differently each time. The spine is restated, not preserved — it should read as newly written."
BUDGET_REAL = "SPINE should be the shortest section — 2-3 sentences that rarely change."
BUDGET_CHANGE = "SPINE should be the shortest section — 2-3 sentences, freshly worded each time."



def thin_context():
    """What production ACTUALLY passes: a 106-char trigger summary."""
    c = sqlite3.connect(DB, timeout=30)
    n = c.execute("SELECT COUNT(*) FROM knowledge_capsules "
                  "WHERE created_at > strftime('%s','now')-10800").fetchone()[0]
    c.close()
    return (f"Adaptive compression (readiness 430 >= 200): {n} capsules stored, "
            f"1 captures processed, 184 minutes elapsed")


def capsule_context(budget=7000):
    """The proposed fill: capsules since last compression, WITH their real ids.

    Ids are the point — BRIDGE is instructed to cite 'finding numbers' and
    'experiment IDs' and is currently given 4 addressable tokens in 2,143 words,
    so it invents address-shaped handles instead.
    """
    c = sqlite3.connect(DB, timeout=30); c.row_factory = sqlite3.Row
    last = c.execute("SELECT MAX(created_at) FROM cognitive_state_history "
                     "WHERE trigger='brain-compression'").fetchone()[0] or 0
    rows = c.execute(
        "SELECT id, topic, restatement FROM knowledge_capsules "
        "WHERE created_at > ? AND typeof(created_at)='integer' "
        "ORDER BY created_at DESC LIMIT 60", (last,)).fetchall()
    c.close()
    out, used = [], 0
    for r in rows:
        line = f"#{r['id']} [{r['topic'] or 'untopiced'}] {(r['restatement'] or '')[:380]}"
        if used + len(line) > budget:
            break
        out.append(line); used += len(line)
    return "\n".join(out)


def materials():
    c = sqlite3.connect(DB, timeout=30)
    gist = c.execute("SELECT semantic_gist FROM cognitive_state").fetchone()[0]
    rows = c.execute(
        "SELECT title, content FROM activity_feed ORDER BY created_at DESC LIMIT 40"
    ).fetchall()
    c.close()
    ctx = "\n".join(f"{t}: {(x or '')[:300]}" for t, x in rows)
    return gist[:PREV_BUDGET], ctx[:CTX_BUDGET]


def call_engine(prompt):
    r = requests.post(ENGINE, json={"model": MODEL, "prompt": prompt, "stream": False},
                      timeout=600)
    r.raise_for_status()
    return r.json().get("response", "")


def embed(text):
    r = requests.post(EMBED, json={"model": "snowflake-arctic-embed2",
                                   "prompt": f"query: {text}"}, timeout=120)
    r.raise_for_status()
    return r.json()["embedding"]


def cos(a, b):
    d = sum(x * y for x, y in zip(a, b))
    na = sum(x * x for x in a) ** .5
    nb = sum(y * y for y in b) ** .5
    return d / (na * nb) if na and nb else 0.0


def spine_of(text):
    m = re.search(r"^##\s+SPINE\s*$", text, re.M)
    if not m:
        return None
    s = m.end()
    nx = re.search(r"^##\s+[A-Z]", text[s:], re.M)
    return text[s:s + (nx.start() if nx else len(text))].strip()


def generated_span(spine, template):
    """The words of SPINE NOT matched verbatim (run>=5) to THIS arm's template."""
    vw, pw = spine.split(), template.split()
    sm = difflib.SequenceMatcher(None, vw, pw, autojunk=False)
    quoted = [False] * len(vw)
    for b in sm.get_matching_blocks():
        if b.size >= 5:
            for i in range(b.a, b.a + b.size):
                quoted[i] = True
    return " ".join(w for w, q in zip(vw, quoted) if not q)


def main():
    os.makedirs(OUT, exist_ok=True)
    base = open(TEMPLATE).read()
    assert REAL in base, "the real Example register line is not in v4 — check the prompt"
    H, C = materials()
    print(f"  H={len(H)} chars  C={len(C)} chars  (identical across all arms)\n")

    runs = []
    for arm, line, n in ARMS:
        tmpl = base.replace(REAL, line)
        for i in range(n):
            runs.append((f"{arm}_{i}", arm, tmpl))

    if "--ctx" in sys.argv:
        H = H  # previous_state unchanged
        thin, rich = thin_context(), capsule_context()
        print(f"  thin context: {len(thin)} chars")
        print(f"  capsule context: {len(rich)} chars, "
              f"{rich.count(chr(35))} ids\n")
        runs = []
        for i in range(3):
            runs.append((f"x_thin_{i}", "x_thin", base, thin))
        for i in range(3):
            runs.append((f"x_caps_{i}", "x_caps", base, rich))
        for name, arm, tmpl, ctx in runs:
            pth = os.path.join(OUT, f"{name}.txt")
            if os.path.exists(pth) and os.path.getsize(pth) > 200:
                print(f"  {name}: cached"); continue
            pr = tmpl.replace("{previous_state}", H).replace("{session_context}", ctx)
            t0 = time.time()
            try:
                o = call_engine(pr)
            except Exception as e:
                print(f"  {name}: FAILED {type(e).__name__}"); continue
            open(pth, "w").write(o)
            print(f"  {name}: {len(o)} chars in {time.time()-t0:.0f}s")
        return 0

    if "--core" in sys.argv:
        runs = []                      # round 3 only; control comes from cache
        for i in range(3):
            runs.append((f"c_control_{i}", "c_control", base))
        rt = base
        for a, b in ((CORE_LEN_REAL, CORE_LEN_LONG), (BUDGET_REAL_C, BUDGET_LONG_C)):
            assert a in rt, f"not found verbatim: {a[:50]!r}"
            rt = rt.replace(a, b)
        assert TMPL_REAL in rt, "control templates must survive the rule swap"
        for i in range(3):
            runs.append((f"c_rulelong_{i}", "c_rulelong", rt))
        tt = base
        assert TMPL_REAL in tt, "template text not found verbatim"
        tt = tt.replace(TMPL_REAL, TMPL_LONG)
        assert CORE_LEN_REAL in tt, "length rule must survive the template swap"
        for i in range(3):
            runs.append((f"c_tmpllong_{i}", "c_tmpllong", tt))

    if "--rule" in sys.argv:
        # Example held IDENTICAL to control; only the stability RULE is swapped.
        rt = base
        for a, b in ((RULE_REAL, RULE_CHANGE), (STATIVE_REAL, STATIVE_CHANGE),
                     (BUDGET_REAL, BUDGET_CHANGE)):
            assert a in rt, f"rule text not found verbatim in v4: {a[:50]!r}"
            rt = rt.replace(a, b)
        assert REAL in rt, "the control example must survive the rule swap"
        for i in range(3):
            runs.append((f"ruleinv_{i}", "ruleinv", rt))

    for name, arm, tmpl in runs:
        p = os.path.join(OUT, f"{name}.txt")
        if os.path.exists(p) and os.path.getsize(p) > 200:
            print(f"  {name}: cached"); continue
        prompt = tmpl.replace("{previous_state}", H).replace("{session_context}", C)
        t0 = time.time()
        try:
            out = call_engine(prompt)
        except Exception as e:
            print(f"  {name}: FAILED {type(e).__name__}: {str(e)[:100]}"); continue
        open(p, "w").write(out)
        print(f"  {name}: {len(out)} chars in {time.time()-t0:.0f}s")

    # ---------- degenerate checks, per prereg, BEFORE any statistic ----------
    print("\n  DEGENERATE CHECKS")
    data, void = {}, []
    for name, arm, tmpl in runs:
        p = os.path.join(OUT, f"{name}.txt")
        if not os.path.exists(p):
            void.append(f"{name}: no output"); continue
        txt = open(p).read()
        sp = spine_of(txt)
        if not sp:
            void.append(f"{name}: no ## SPINE section"); continue
        gen = generated_span(sp, tmpl)
        if len(gen.split()) < 10:
            void.append(f"{name}: generated span only {len(gen.split())} words")
        if arm == "inverted" and re.search(r"genuine inquiry|partnership with Nate", sp, re.I):
            void.append(f"{name}: INVERTED output still carries CONTROL scaffold wording "
                        f"— manipulation strength is zero")
        data.setdefault(arm, []).append((name, sp, gen))
        print(f"    {name}: SPINE {len(sp.split())}w, generated {len(gen.split())}w")

    if void:
        print("\n  VOID — prereg degenerate check fired:")
        for v in void: print(f"    {v}")
        return 1

    # ---------- noise floor, then effect ----------
    embs = {a: [(n, embed(g)) for n, s, g in v] for a, v in data.items()}
    ctrl = [e for _, e in embs["control"]]
    floor = [cos(ctrl[i], ctrl[j]) for i in range(len(ctrl)) for j in range(i+1, len(ctrl))]
    fmean, fsd = st.mean(floor), (st.stdev(floor) if len(floor) > 1 else 0.0)
    centroid = [sum(v[i] for v in ctrl)/len(ctrl) for i in range(len(ctrl[0]))]

    print(f"\n  CONTROL noise floor: mean {fmean:.4f}  sd {fsd:.4f}  (n={len(floor)} pairs)")
    if fmean > 0.98:
        print("  VOID — noise floor above 0.98, no headroom to detect anything.")
        return 1
    thresh = fmean - fsd
    print(f"  Committed threshold: below {thresh:.4f} = LOAD-BEARING\n")

    verdicts = {}
    for arm in ("inverted", "neutral"):
        print(f"  {arm.upper()} generated spans vs CONTROL centroid:")
        vals = []
        for n, e in embs[arm]:
            v = cos(e, centroid); vals.append(v)
            print(f"    {n}: {v:.4f}   {'BELOW floor' if v < thresh else 'inside floor'}")
        verdicts[arm] = st.mean(vals)
    print()
    for arm, m in verdicts.items():
        print(f"  {arm}: mean {m:.4f} vs threshold {thresh:.4f} -> "
              f"{'LOAD-BEARING' if m < thresh else 'DECORATIVE'}")
    print("\n  Per-arm values printed above; do not read the mean alone.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
