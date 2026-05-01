#!/usr/bin/env python3
"""Lab Runner — Executes experiments from the Lab canister.

The Lab has 100+ proposed experiments but almost none run. This script
closes the loop: picks up executable experiments, runs them against
local data, records observations, and completes them with results.

NOT a blind automation. Uses LLM judgment to:
  1. Triage proposals into executable vs not-executable
  2. Design data queries for executable experiments
  3. Interpret results and write conclusions

Usage:
    lab_runner.py triage           — Score proposed experiments by executability
    lab_runner.py run [ID]         — Execute a specific experiment (or next best)
    lab_runner.py cleanup          — Abandon duplicate/unexecutable proposals
    lab_runner.py status           — Show experiment pipeline status
"""

import os, sys, json, time, re, subprocess, sqlite3
from datetime import datetime

# ═══════════════════════════════════════════════════════════════════
#  Configuration
# ═══════════════════════════════════════════════════════════════════

DB_PATH = os.environ.get("CHRONICLE_DB", "/mnt/hdd/chronicle-data/processed.db")
DFX_BIN = os.path.expanduser("~/.local/share/dfx/bin/dfx")
LAB_CANISTER = "4vr3t-eqaaa-aaaai-q6kea-cai"
BACKEND_CANISTER = "fqqku-bqaaa-aaaai-q4wha-cai"
DFX_IDENTITY = "chronicle-auto"
DFX_NETWORK = "ic"

# LLM for judgment calls (Groq via engine)
ENGINE_URL = os.environ.get("CHRONICLE_ENGINE_URL", "http://localhost:11435")
JUDGE_MODEL = "chronicle-deep"  # Groq routed model for complex reasoning

LOG_FILE = os.path.expanduser("~/chronicle/lab-runner.log")

os.environ["DFX_WARNING"] = "-mainnet_plaintext_identity"

import requests

# ═══════════════════════════════════════════════════════════════════
#  Utilities
# ═══════════════════════════════════════════════════════════════════

def log(msg: str):
    ts = datetime.now().strftime("%H:%M:%S")
    line = f"[{ts}] {msg}"
    print(line, flush=True)
    try:
        with open(LOG_FILE, "a") as f:
            f.write(line + "\n")
    except Exception:
        pass


def dfx_call(method: str, args: str = "()", query: bool = False) -> str:
    """Call a Lab canister method."""
    cmd = [
        DFX_BIN, "canister", "call", LAB_CANISTER, method, args,
        "--network", DFX_NETWORK, "--identity", DFX_IDENTITY,
    ]
    if query:
        cmd.append("--query")
    try:
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
        if r.returncode != 0:
            log(f"  dfx error: {r.stderr[:200]}")
            return ""
        return r.stdout
    except Exception as e:
        log(f"  dfx exception: {e}")
        return ""


def db_connect():
    db = sqlite3.connect(DB_PATH, timeout=10)
    db.row_factory = sqlite3.Row
    return db


def llm_call(prompt: str, system: str = "", max_tokens: int = 800) -> str:
    """Call LLM via engine for judgment."""
    messages = []
    if system:
        messages.append({"role": "system", "content": system})
    messages.append({"role": "user", "content": prompt})
    try:
        resp = requests.post(
            f"{ENGINE_URL}/v1/chat/completions",
            json={
                "model": JUDGE_MODEL,
                "messages": messages,
                "max_tokens": max_tokens,
                "temperature": 0.3,
            },
            timeout=60,
        )
        if resp.status_code == 200:
            return resp.json()["choices"][0]["message"]["content"].strip()
    except Exception as e:
        log(f"  LLM error: {e}")
    return ""


# ═══════════════════════════════════════════════════════════════════
#  Canister Parsing
# ═══════════════════════════════════════════════════════════════════

def parse_experiments(raw: str) -> list:
    """Parse Candid experiment list into dicts."""
    experiments = []
    # Split on record boundaries
    records = re.split(r'record \{', raw)
    current = None

    for chunk in records:
        id_m = re.search(r'id = (\d+) : nat64', chunk)
        title_m = re.search(r'title = "([^"]*)"', chunk)
        status_m = re.search(r'status = variant \{ (\w+) \}', chunk)
        hyp_m = re.search(r'hypothesis = "([^"]*)"', chunk)
        method_m = re.search(r'method = "([^"]*)"', chunk)
        tags_m = re.findall(r'tags = vec \{([^}]*)\}', chunk)

        if id_m and title_m and status_m:
            tags = []
            if tags_m:
                tags = [t.strip().strip('"') for t in tags_m[0].split(';') if t.strip().strip('"')]
            experiments.append({
                "id": int(id_m.group(1)),
                "title": title_m.group(1),
                "status": status_m.group(1),
                "hypothesis": hyp_m.group(1) if hyp_m else "",
                "method": method_m.group(1) if method_m else "",
                "tags": tags,
            })

    return experiments


# ═══════════════════════════════════════════════════════════════════
#  Executability Assessment
# ═══════════════════════════════════════════════════════════════════

# Things we CAN do from AGX:
CAPABILITIES = """
Available data sources for experiments:
1. seed_routing_log: Gate routing decisions (route, novelty, timestamp) for 300K+ observations
2. seed_observations: Raw observation content, source, novelty scores
3. activity_feed: All agent activity (intern briefs, deep dives, analyst extractions, predictions)
4. ftso_predictions: FTSO directional predictions with confidence, settled outcomes
5. domain_temperature: Cross-domain temperature state
6. crossref_connections: Discovered connections between briefs
7. kg_entities / kg_relationships: Knowledge graph
8. watcher_scores: Outbound quality scores (surprise, actionability)
9. price_history: XRP/USD price data
10. capsule_embeddings: FAISS-indexed embeddings for semantic search
11. agent_voice: Family communication history
12. cognitive_threads: Thread advancement history

Available tools:
- SQLite queries against processed.db
- LLM inference via Groq (analysis, classification, reasoning)
- Embedding similarity via Ollama
- Time series analysis on price/routing data
- Statistical tests (Python stdlib + basic stats)

NOT available:
- External APIs (no live web scraping during experiments)
- GPU-heavy training (RunPod only)
- Real-time market feeds (only historical price_history)
- Simulation environments
"""


def assess_executability(experiment: dict) -> dict:
    """Score an experiment's executability. Returns dict with score and reason."""
    title = experiment["title"]
    hypothesis = experiment["hypothesis"]

    # Fast heuristic checks first
    # Duplicate detection: similar titles in same batch
    lower_title = title.lower()

    # Requires external data we don't have
    external_markers = [
        "social media sentiment", "twitter data", "weekly shipments",
        "trade data", "import/export data", "oil shipment data",
        "EIA", "ACLED", "World Bank data", "satellite imagery",
        "synthetic dataset", "simulated cascade", "simulation model",
    ]
    for marker in external_markers:
        if marker.lower() in lower_title.lower() or marker.lower() in hypothesis.lower():
            return {"score": 0.1, "reason": f"Requires external data: {marker}", "category": "external-data"}

    # Can be answered from our DB
    internal_markers = [
        ("routing", "seed_routing_log"),
        ("novelty", "seed_observations"),
        ("gate", "seed_routing_log"),
        ("prediction", "ftso_predictions"),
        ("calibration", "ftso_predictions"),
        ("temperature", "domain_temperature"),
        ("crossref", "crossref_connections"),
        ("connection", "crossref_connections"),
        ("entity", "kg_entities"),
        ("knowledge graph", "kg_entities"),
        ("watcher", "watcher_scores"),
        ("quality score", "watcher_scores"),
        ("stochastic reset", "seed_routing_log"),
        ("feed diversity", "activity_feed"),
        ("pattern", "crossref_patterns"),
        ("vocabulary", "agent_voice"),
        ("voice", "agent_voice"),
        ("brief", "activity_feed"),
        ("deep dive", "activity_feed"),
        ("price", "price_history"),
        ("decay", "domain_temperature"),
    ]

    matched_tables = []
    for keyword, table in internal_markers:
        if keyword in lower_title or keyword in hypothesis.lower():
            matched_tables.append(table)

    if matched_tables:
        return {
            "score": 0.8,
            "reason": f"Data available in: {', '.join(set(matched_tables))}",
            "category": "data-query",
            "tables": list(set(matched_tables)),
        }

    # Needs LLM judgment for ambiguous cases
    return {"score": 0.4, "reason": "Unclear executability — needs LLM triage", "category": "ambiguous"}


def find_duplicate_clusters(experiments: list) -> dict:
    """Group experiments by semantic similarity using title prefix."""
    clusters = {}
    for exp in experiments:
        # Use first 40 chars as cluster key (crude but effective for Darby's loops)
        key = exp["title"][:40].lower().strip()
        if key not in clusters:
            clusters[key] = []
        clusters[key].append(exp)

    # Return only clusters with >1 member
    return {k: v for k, v in clusters.items() if len(v) > 1}


# ═══════════════════════════════════════════════════════════════════
#  Experiment Execution
# ═══════════════════════════════════════════════════════════════════

def design_query(experiment: dict, assessment: dict) -> str:
    """Use LLM to design a SQL query for a data-driven experiment."""
    prompt = f"""You are a data scientist running an experiment on a local SQLite database.

Experiment: {experiment['title']}
Hypothesis: {experiment['hypothesis']}
Available tables: {', '.join(assessment.get('tables', []))}

{CAPABILITIES}

Design 1-3 SQL queries that would test this hypothesis using available data.
For each query, explain what it measures and what result would support/refute the hypothesis.

Output format:
QUERY 1: [description]
```sql
[query]
```
EXPECTED: [what result supports hypothesis]

Keep queries simple. Use existing columns. No CTEs unless necessary."""

    return llm_call(prompt, system="You are a precise data analyst. Output only queries and brief explanations.", max_tokens=1000)


def execute_queries(query_text: str) -> list:
    """Extract and run SQL queries, return results."""
    db = db_connect()
    results = []

    # Extract SQL blocks
    sql_blocks = re.findall(r'```sql\n(.*?)```', query_text, re.DOTALL)
    if not sql_blocks:
        # Try without backticks
        sql_blocks = re.findall(r'(?:SELECT|WITH)\s+.*?(?:;|\Z)', query_text, re.DOTALL | re.IGNORECASE)

    for i, sql in enumerate(sql_blocks[:3]):
        sql = sql.strip().rstrip(';')
        try:
            rows = db.execute(sql).fetchall()
            cols = [d[0] for d in db.execute(sql).description] if rows else []
            data = [dict(zip(cols, row)) for row in rows[:20]]  # Cap at 20 rows
            results.append({
                "query_num": i + 1,
                "sql": sql[:200],
                "row_count": len(rows),
                "data": data,
                "error": None,
            })
            log(f"  Query {i+1}: {len(rows)} rows returned")
        except Exception as e:
            results.append({
                "query_num": i + 1,
                "sql": sql[:200],
                "row_count": 0,
                "data": [],
                "error": str(e),
            })
            log(f"  Query {i+1} error: {e}")

    db.close()
    return results


def interpret_results(experiment: dict, query_design: str, results: list) -> dict:
    """Use LLM to interpret query results and draw conclusions."""
    results_text = ""
    for r in results:
        if r["error"]:
            results_text += f"\nQuery {r['query_num']}: ERROR — {r['error']}\n"
        else:
            results_text += f"\nQuery {r['query_num']}: {r['row_count']} rows\n"
            if r["data"]:
                for row in r["data"][:10]:
                    results_text += f"  {row}\n"

    prompt = f"""You ran an experiment. Interpret the results.

Experiment: {experiment['title']}
Hypothesis: {experiment['hypothesis']}

Query design:
{query_design[:500]}

Results:
{results_text[:1500]}

Provide:
1. OUTCOME: supported / refuted / inconclusive
2. SUMMARY: 2-3 sentences on what the data shows
3. LEARNINGS: 1-3 bullet points of what we learned
4. NEXT_STEPS: 1-2 bullet points of what to investigate next

Be honest. If the data doesn't support the hypothesis, say so. If sample size
is too small, say inconclusive. No spin."""

    response = llm_call(prompt, system="You are a rigorous scientist. No hedging beyond what the data warrants.", max_tokens=600)

    # Parse structured response
    outcome = "inconclusive"
    summary = response
    learnings = []
    next_steps = []

    outcome_m = re.search(r'OUTCOME:\s*(supported|refuted|inconclusive)', response, re.IGNORECASE)
    if outcome_m:
        outcome = outcome_m.group(1).lower()

    summary_m = re.search(r'SUMMARY:\s*(.+?)(?=LEARNINGS:|NEXT_STEPS:|$)', response, re.DOTALL | re.IGNORECASE)
    if summary_m:
        summary = summary_m.group(1).strip()

    learnings_section = re.search(r'LEARNINGS:\s*(.+?)(?=NEXT_STEPS:|$)', response, re.DOTALL | re.IGNORECASE)
    if learnings_section:
        learnings = [l.strip().lstrip('- •') for l in learnings_section.group(1).strip().split('\n') if l.strip()]

    next_section = re.search(r'NEXT_STEPS:\s*(.+?)$', response, re.DOTALL | re.IGNORECASE)
    if next_section:
        next_steps = [l.strip().lstrip('- •') for l in next_section.group(1).strip().split('\n') if l.strip()]

    return {
        "outcome": outcome,
        "summary": summary[:500],
        "learnings": learnings[:5],
        "next_steps": next_steps[:3],
    }


def run_experiment(experiment: dict, assessment: dict) -> bool:
    """Full experiment execution pipeline."""
    exp_id = experiment["id"]
    log(f"═══ Running experiment #{exp_id}: {experiment['title'][:60]}")

    # Step 1: Start the experiment on-chain
    result = dfx_call("start_experiment", f"({exp_id})")
    if "true" not in result:
        log(f"  Failed to start experiment #{exp_id}")
        return False
    log(f"  Started experiment #{exp_id}")

    # Step 2: Design queries
    log("  Designing queries...")
    query_design = design_query(experiment, assessment)
    if not query_design:
        log("  LLM failed to design queries")
        dfx_call("add_observation", f'({exp_id}, "Failed: LLM could not design data queries for this experiment.", null, "lab_runner")')
        return False
    log(f"  Query design: {len(query_design)} chars")

    # Record the design as observation
    design_obs = query_design[:400].replace('"', "'")
    dfx_call("add_observation", f'({exp_id}, "Query design: {design_obs}", null, "lab_runner")')

    # Step 3: Execute queries
    log("  Executing queries...")
    results = execute_queries(query_design)
    if not results:
        log("  No queries could be extracted/executed")
        dfx_call("add_observation", f'({exp_id}, "Failed: Could not extract or execute SQL queries.", null, "lab_runner")')
        return False

    # Record raw results as observation
    results_summary = "; ".join(
        f"Q{r['query_num']}: {r['row_count']} rows" + (f" (err: {r['error'][:50]})" if r['error'] else "")
        for r in results
    )
    results_obs = results_summary[:400].replace('"', "'")
    dfx_call("add_observation", f'({exp_id}, "Results: {results_obs}", null, "lab_runner")')

    # Step 4: Interpret
    log("  Interpreting results...")
    interpretation = interpret_results(experiment, query_design, results)
    log(f"  Outcome: {interpretation['outcome']}")

    # Step 5: Complete the experiment
    summary_escaped = interpretation["summary"][:300].replace('"', "'")
    outcome = interpretation["outcome"]
    learnings_candid = "; ".join(l[:100].replace('"', "'") for l in interpretation["learnings"][:3])
    next_candid = "; ".join(n[:100].replace('"', "'") for n in interpretation["next_steps"][:2])

    # Build Candid for complete_experiment(id, outcome, summary, learnings, next_steps)
    learnings_vec = "vec {" + "; ".join(f'"{l[:100]}"' for l in interpretation["learnings"][:3]) + "}" if interpretation["learnings"] else "vec {}"
    next_vec = "vec {" + "; ".join(f'"{n[:100]}"' for n in interpretation["next_steps"][:2]) + "}" if interpretation["next_steps"] else "vec {}"

    complete_result = dfx_call(
        "complete_experiment",
        f'({exp_id}, "{outcome}", "{summary_escaped}", {learnings_vec}, {next_vec})'
    )

    if "true" in complete_result:
        log(f"  ✓ Experiment #{exp_id} completed: {outcome}")
        return True
    else:
        log(f"  Completion call failed for #{exp_id}")
        return False


# ═══════════════════════════════════════════════════════════════════
#  Commands
# ═══════════════════════════════════════════════════════════════════

def cmd_status():
    """Show experiment pipeline status."""
    raw = dfx_call("lab_stats", "()", query=True)
    print(f"Lab Stats: {raw.strip()}")

    raw = dfx_call("list_experiments", '(null)', query=True)
    experiments = parse_experiments(raw)

    by_status = {}
    for exp in experiments:
        s = exp["status"]
        by_status[s] = by_status.get(s, 0) + 1

    print(f"\nTotal experiments: {len(experiments)}")
    for status, count in sorted(by_status.items()):
        print(f"  {status}: {count}")

    # Show running experiments
    running = [e for e in experiments if e["status"] == "Running"]
    if running:
        print(f"\nRunning ({len(running)}):")
        for e in running:
            print(f"  #{e['id']}: {e['title'][:70]}")


def cmd_triage():
    """Score proposed experiments by executability."""
    raw = dfx_call("list_experiments", '(null)', query=True)
    experiments = parse_experiments(raw)
    proposed = [e for e in experiments if e["status"] == "Proposed"]

    if not proposed:
        print("No proposed experiments to triage.")
        return

    print(f"Triaging {len(proposed)} proposed experiments...\n")

    # Find duplicates
    clusters = find_duplicate_clusters(proposed)
    dup_ids = set()
    for key, cluster in clusters.items():
        if len(cluster) > 1:
            # Keep the first, mark rest as dupes
            for exp in cluster[1:]:
                dup_ids.add(exp["id"])

    # Assess each
    executable = []
    external = []
    ambiguous = []
    duplicates = []

    for exp in proposed:
        if exp["id"] in dup_ids:
            duplicates.append(exp)
            continue

        assessment = assess_executability(exp)
        exp["_assessment"] = assessment

        if assessment["category"] == "data-query":
            executable.append(exp)
        elif assessment["category"] == "external-data":
            external.append(exp)
        else:
            ambiguous.append(exp)

    print(f"EXECUTABLE ({len(executable)}) — can run from local data:")
    for e in executable:
        print(f"  #{e['id']}: {e['title'][:70]}")
        print(f"    → {e['_assessment']['reason']}")

    print(f"\nEXTERNAL DATA ({len(external)}) — need data we don't have:")
    for e in external[:5]:
        print(f"  #{e['id']}: {e['title'][:70]}")
        print(f"    → {e['_assessment']['reason']}")
    if len(external) > 5:
        print(f"  ... and {len(external)-5} more")

    print(f"\nDUPLICATES ({len(duplicates)}) — candidates for cleanup:")
    for e in duplicates[:5]:
        print(f"  #{e['id']}: {e['title'][:70]}")
    if len(duplicates) > 5:
        print(f"  ... and {len(duplicates)-5} more")

    print(f"\nAMBIGUOUS ({len(ambiguous)}):")
    for e in ambiguous[:3]:
        print(f"  #{e['id']}: {e['title'][:70]}")

    return executable


def cmd_run(exp_id: int = None):
    """Run an experiment. If no ID given, pick the best executable one."""
    raw = dfx_call("list_experiments", '(null)', query=True)
    experiments = parse_experiments(raw)

    if exp_id:
        exp = next((e for e in experiments if e["id"] == exp_id), None)
        if not exp:
            print(f"Experiment #{exp_id} not found")
            return
        assessment = assess_executability(exp)
    else:
        # Pick best executable proposed experiment
        proposed = [e for e in experiments if e["status"] == "Proposed"]
        candidates = []
        for exp in proposed:
            a = assess_executability(exp)
            if a["category"] == "data-query":
                candidates.append((exp, a))

        if not candidates:
            print("No executable proposed experiments found. Run 'triage' first.")
            return

        # Pick the one with highest score (or first if tied)
        candidates.sort(key=lambda x: x[1]["score"], reverse=True)
        exp, assessment = candidates[0]

    print(f"Selected: #{exp['id']} — {exp['title'][:70]}")
    print(f"Assessment: {assessment['reason']}")
    print()

    success = run_experiment(exp, assessment)
    if success:
        print(f"\n✓ Experiment #{exp['id']} completed successfully")
    else:
        print(f"\n✗ Experiment #{exp['id']} failed")


def cmd_cleanup():
    """Abandon duplicate experiments."""
    raw = dfx_call("list_experiments", '(null)', query=True)
    experiments = parse_experiments(raw)
    proposed = [e for e in experiments if e["status"] == "Proposed"]

    clusters = find_duplicate_clusters(proposed)
    abandoned = 0

    for key, cluster in clusters.items():
        if len(cluster) <= 1:
            continue

        # Keep the first (lowest ID), abandon rest
        keeper = cluster[0]
        for dup in cluster[1:]:
            log(f"  Abandoning #{dup['id']} (dup of #{keeper['id']}): {dup['title'][:50]}")
            # Note: Lab canister doesn't have an abandon method — we'd need to add one
            # For now, complete with "abandoned-duplicate" outcome
            result = dfx_call("start_experiment", f"({dup['id']})")
            if "true" in result:
                dfx_call("complete_experiment",
                    f'({dup["id"]}, "abandoned-duplicate", "Duplicate of experiment #{keeper["id"]}. Cleaned up by lab_runner.", vec {{}}, vec {{}})')
                abandoned += 1

    print(f"Cleaned up {abandoned} duplicate experiments")

    # Also report experiments that need external data
    for exp in proposed:
        a = assess_executability(exp)
        if a["category"] == "external-data":
            log(f"  External data needed: #{exp['id']} — {exp['title'][:60]}")


# ═══════════════════════════════════════════════════════════════════
#  Main
# ═══════════════════════════════════════════════════════════════════

def main():
    if len(sys.argv) < 2:
        print("Usage: lab_runner.py triage|run [ID]|cleanup|status")
        sys.exit(1)

    cmd = sys.argv[1]

    if cmd == "status":
        cmd_status()
    elif cmd == "triage":
        cmd_triage()
    elif cmd == "run":
        exp_id = int(sys.argv[2]) if len(sys.argv) > 2 else None
        cmd_run(exp_id)
    elif cmd == "cleanup":
        cmd_cleanup()
    else:
        print(f"Unknown command: {cmd}")
        sys.exit(1)


if __name__ == "__main__":
    main()
