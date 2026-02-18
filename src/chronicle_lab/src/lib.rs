//! Chronicle Lab Canister — Cognitive Experimentation Space
//!
//! A dedicated on-chain workspace for:
//! - Running experiments on Mind architecture improvements
//! - Tracking hypotheses, observations, and results
//! - Storing architecture proposals with versioning
//! - Benchmarking cognitive cycle quality over time
//!
//! This canister is intentionally separate from chronicle_backend.
//! The backend is the authoritative record. The lab is for messy exploration.

use candid::{CandidType, Deserialize};
use ic_cdk_macros::{init, post_upgrade, pre_upgrade, query, update};
use serde::Serialize;
use std::cell::RefCell;

// ============================================================
// Data Types
// ============================================================

/// An experiment — a hypothesis being tested
#[derive(Clone, Debug, CandidType, Deserialize, Serialize)]
pub struct Experiment {
    pub id: u64,
    pub title: String,
    pub hypothesis: String,
    pub method: String,
    pub status: ExperimentStatus,
    pub observations: Vec<Observation>,
    pub result: Option<ExperimentResult>,
    pub tags: Vec<String>,
    pub created_at: u64,
    pub updated_at: u64,
}

#[derive(Clone, Debug, CandidType, Deserialize, Serialize, PartialEq)]
pub enum ExperimentStatus {
    Proposed,
    Running,
    Paused,
    Completed,
    Failed,
    Abandoned,
}

#[derive(Clone, Debug, CandidType, Deserialize, Serialize)]
pub struct Observation {
    pub timestamp: u64,
    pub content: String,
    pub data: Option<String>, // JSON blob for structured data
    pub source: String,       // "mind", "sprout", "claude", "manual"
}

#[derive(Clone, Debug, CandidType, Deserialize, Serialize)]
pub struct ExperimentResult {
    pub outcome: String,      // "confirmed", "refuted", "inconclusive", "unexpected"
    pub summary: String,
    pub learnings: Vec<String>,
    pub next_steps: Vec<String>,
    pub completed_at: u64,
}

/// An architecture proposal — a design for Mind improvements
#[derive(Clone, Debug, CandidType, Deserialize, Serialize)]
pub struct ArchProposal {
    pub id: u64,
    pub title: String,
    pub version: u32,
    pub description: String,
    pub components: Vec<ArchComponent>,
    pub status: ProposalStatus,
    pub rationale: String,
    pub tradeoffs: Vec<String>,
    pub created_at: u64,
    pub updated_at: u64,
}

#[derive(Clone, Debug, CandidType, Deserialize, Serialize)]
pub struct ArchComponent {
    pub name: String,
    pub purpose: String,
    pub implementation_notes: String,
    pub estimated_effort: String, // "small", "medium", "large"
    pub priority: u32,            // 1 = highest
}

#[derive(Clone, Debug, CandidType, Deserialize, Serialize, PartialEq)]
pub enum ProposalStatus {
    Draft,
    UnderReview,
    Approved,
    Implementing,
    Implemented,
    Superseded,
}

/// A cognitive benchmark — metrics from a Mind cycle
#[derive(Clone, Debug, CandidType, Deserialize, Serialize)]
pub struct CognitiveBenchmark {
    pub id: u64,
    pub cycle_id: String,
    pub timestamp: u64,
    pub model_used: String,
    pub prompt_tokens: u64,
    pub response_tokens: u64,
    pub action_diversity: f64,     // 0.0-1.0, how varied actions are
    pub rumination_score: f64,     // 0.0-1.0, 0 = no repetition
    pub reasoning_quality: f64,    // 0.0-1.0, subjective quality rating
    pub actions_taken: Vec<String>,
    pub config_variant: Option<String>, // which prompt/config was used
    pub notes: Option<String>,
}

/// A research note — insights from papers, experiments, or exploration
#[derive(Clone, Debug, CandidType, Deserialize, Serialize)]
pub struct ResearchNote {
    pub id: u64,
    pub title: String,
    pub content: String,
    pub source: String,            // paper ID, URL, "observation", etc.
    pub tags: Vec<String>,
    pub connections: Vec<u64>,     // IDs of related notes
    pub created_at: u64,
}

/// Lab-wide statistics
#[derive(Clone, Debug, CandidType, Deserialize, Serialize)]
pub struct LabStats {
    pub experiment_count: u64,
    pub active_experiments: u64,
    pub proposal_count: u64,
    pub benchmark_count: u64,
    pub note_count: u64,
    pub created_at: u64, // when the lab was deployed
}

// ============================================================
// State
// ============================================================

#[derive(Default, CandidType, Deserialize, Serialize)]
struct LabState {
    experiments: Vec<Experiment>,
    proposals: Vec<ArchProposal>,
    benchmarks: Vec<CognitiveBenchmark>,
    notes: Vec<ResearchNote>,
    next_id: u64,
    created_at: u64,
}

thread_local! {
    static STATE: RefCell<LabState> = RefCell::new(LabState::default());
}

fn now_nanos() -> u64 {
    ic_cdk::api::time()
}

fn next_id() -> u64 {
    STATE.with(|s| {
        let mut state = s.borrow_mut();
        state.next_id += 1;
        state.next_id
    })
}

// ============================================================
// Init & Upgrade
// ============================================================

#[init]
fn init() {
    STATE.with(|s| {
        let mut state = s.borrow_mut();
        state.created_at = now_nanos();
    });
}

#[pre_upgrade]
fn pre_upgrade() {
    STATE.with(|s| {
        let state = s.borrow();
        ic_cdk::storage::stable_save((&*state,)).expect("Failed to save state");
    });
}

#[post_upgrade]
fn post_upgrade() {
    let (state,): (LabState,) =
        ic_cdk::storage::stable_restore().expect("Failed to restore state");
    STATE.with(|s| {
        *s.borrow_mut() = state;
    });
}

// ============================================================
// Experiment APIs
// ============================================================

#[update]
fn create_experiment(title: String, hypothesis: String, method: String, tags: Vec<String>) -> u64 {
    let id = next_id();
    let now = now_nanos();
    let experiment = Experiment {
        id,
        title,
        hypothesis,
        method,
        status: ExperimentStatus::Proposed,
        observations: vec![],
        result: None,
        tags,
        created_at: now,
        updated_at: now,
    };
    STATE.with(|s| {
        s.borrow_mut().experiments.push(experiment);
    });
    id
}

#[update]
fn start_experiment(id: u64) -> bool {
    STATE.with(|s| {
        let mut state = s.borrow_mut();
        if let Some(exp) = state.experiments.iter_mut().find(|e| e.id == id) {
            exp.status = ExperimentStatus::Running;
            exp.updated_at = now_nanos();
            true
        } else {
            false
        }
    })
}

#[update]
fn add_observation(experiment_id: u64, content: String, data: Option<String>, source: String) -> bool {
    STATE.with(|s| {
        let mut state = s.borrow_mut();
        if let Some(exp) = state.experiments.iter_mut().find(|e| e.id == experiment_id) {
            exp.observations.push(Observation {
                timestamp: now_nanos(),
                content,
                data,
                source,
            });
            exp.updated_at = now_nanos();
            true
        } else {
            false
        }
    })
}

#[update]
fn complete_experiment(
    id: u64,
    outcome: String,
    summary: String,
    learnings: Vec<String>,
    next_steps: Vec<String>,
) -> bool {
    STATE.with(|s| {
        let mut state = s.borrow_mut();
        if let Some(exp) = state.experiments.iter_mut().find(|e| e.id == id) {
            exp.status = ExperimentStatus::Completed;
            exp.result = Some(ExperimentResult {
                outcome,
                summary,
                learnings,
                next_steps,
                completed_at: now_nanos(),
            });
            exp.updated_at = now_nanos();
            true
        } else {
            false
        }
    })
}

#[query]
fn get_experiment(id: u64) -> Option<Experiment> {
    STATE.with(|s| {
        s.borrow().experiments.iter().find(|e| e.id == id).cloned()
    })
}

#[query]
fn list_experiments(status_filter: Option<String>) -> Vec<Experiment> {
    STATE.with(|s| {
        let state = s.borrow();
        match status_filter.as_deref() {
            Some("running") => state.experiments.iter()
                .filter(|e| e.status == ExperimentStatus::Running)
                .cloned().collect(),
            Some("proposed") => state.experiments.iter()
                .filter(|e| e.status == ExperimentStatus::Proposed)
                .cloned().collect(),
            Some("completed") => state.experiments.iter()
                .filter(|e| e.status == ExperimentStatus::Completed)
                .cloned().collect(),
            _ => state.experiments.clone(),
        }
    })
}

// ============================================================
// Architecture Proposal APIs
// ============================================================

#[update]
fn create_proposal(
    title: String,
    description: String,
    components: Vec<ArchComponent>,
    rationale: String,
    tradeoffs: Vec<String>,
) -> u64 {
    let id = next_id();
    let now = now_nanos();
    let proposal = ArchProposal {
        id,
        title,
        version: 1,
        description,
        components,
        status: ProposalStatus::Draft,
        rationale,
        tradeoffs,
        created_at: now,
        updated_at: now,
    };
    STATE.with(|s| {
        s.borrow_mut().proposals.push(proposal);
    });
    id
}

#[update]
fn update_proposal_status(id: u64, status: String) -> bool {
    STATE.with(|s| {
        let mut state = s.borrow_mut();
        if let Some(prop) = state.proposals.iter_mut().find(|p| p.id == id) {
            prop.status = match status.as_str() {
                "draft" => ProposalStatus::Draft,
                "review" => ProposalStatus::UnderReview,
                "approved" => ProposalStatus::Approved,
                "implementing" => ProposalStatus::Implementing,
                "implemented" => ProposalStatus::Implemented,
                "superseded" => ProposalStatus::Superseded,
                _ => return false,
            };
            prop.version += 1;
            prop.updated_at = now_nanos();
            true
        } else {
            false
        }
    })
}

#[query]
fn get_proposal(id: u64) -> Option<ArchProposal> {
    STATE.with(|s| {
        s.borrow().proposals.iter().find(|p| p.id == id).cloned()
    })
}

#[query]
fn list_proposals() -> Vec<ArchProposal> {
    STATE.with(|s| s.borrow().proposals.clone())
}

// ============================================================
// Benchmark APIs
// ============================================================

#[update]
fn record_benchmark(
    cycle_id: String,
    model_used: String,
    prompt_tokens: u64,
    response_tokens: u64,
    action_diversity: f64,
    rumination_score: f64,
    reasoning_quality: f64,
    actions_taken: Vec<String>,
    config_variant: Option<String>,
    notes: Option<String>,
) -> u64 {
    let id = next_id();
    let benchmark = CognitiveBenchmark {
        id,
        cycle_id,
        timestamp: now_nanos(),
        model_used,
        prompt_tokens,
        response_tokens,
        action_diversity,
        rumination_score,
        reasoning_quality,
        actions_taken,
        config_variant,
        notes,
    };
    STATE.with(|s| {
        s.borrow_mut().benchmarks.push(benchmark);
    });
    id
}

#[query]
fn get_benchmarks(limit: u64) -> Vec<CognitiveBenchmark> {
    STATE.with(|s| {
        let state = s.borrow();
        let len = state.benchmarks.len();
        let start = if len > limit as usize { len - limit as usize } else { 0 };
        state.benchmarks[start..].to_vec()
    })
}

#[query]
fn benchmark_summary() -> String {
    STATE.with(|s| {
        let state = s.borrow();
        let total = state.benchmarks.len();
        if total == 0 {
            return "No benchmarks recorded yet.".to_string();
        }
        let recent: Vec<_> = state.benchmarks.iter().rev().take(50).collect();
        let avg_diversity: f64 = recent.iter().map(|b| b.action_diversity).sum::<f64>() / recent.len() as f64;
        let avg_rumination: f64 = recent.iter().map(|b| b.rumination_score).sum::<f64>() / recent.len() as f64;
        let avg_quality: f64 = recent.iter().map(|b| b.reasoning_quality).sum::<f64>() / recent.len() as f64;

        format!(
            "Benchmarks: {} total | Last {}: avg_diversity={:.2}, avg_rumination={:.2}, avg_quality={:.2}",
            total,
            recent.len(),
            avg_diversity,
            avg_rumination,
            avg_quality,
        )
    })
}

// ============================================================
// Research Note APIs
// ============================================================

#[update]
fn add_note(title: String, content: String, source: String, tags: Vec<String>) -> u64 {
    let id = next_id();
    let note = ResearchNote {
        id,
        title,
        content,
        source,
        tags,
        connections: vec![],
        created_at: now_nanos(),
    };
    STATE.with(|s| {
        s.borrow_mut().notes.push(note);
    });
    id
}

#[update]
fn connect_notes(note_id: u64, related_id: u64) -> bool {
    STATE.with(|s| {
        let mut state = s.borrow_mut();
        if let Some(note) = state.notes.iter_mut().find(|n| n.id == note_id) {
            if !note.connections.contains(&related_id) {
                note.connections.push(related_id);
            }
            true
        } else {
            false
        }
    })
}

#[query]
fn get_note(id: u64) -> Option<ResearchNote> {
    STATE.with(|s| {
        s.borrow().notes.iter().find(|n| n.id == id).cloned()
    })
}

#[query]
fn search_notes(query: String) -> Vec<ResearchNote> {
    let query_lower = query.to_lowercase();
    STATE.with(|s| {
        s.borrow().notes.iter()
            .filter(|n| {
                n.title.to_lowercase().contains(&query_lower)
                    || n.content.to_lowercase().contains(&query_lower)
                    || n.tags.iter().any(|t| t.to_lowercase().contains(&query_lower))
            })
            .cloned()
            .collect()
    })
}

// ============================================================
// Lab Stats
// ============================================================

#[query]
fn lab_stats() -> LabStats {
    STATE.with(|s| {
        let state = s.borrow();
        LabStats {
            experiment_count: state.experiments.len() as u64,
            active_experiments: state.experiments.iter()
                .filter(|e| e.status == ExperimentStatus::Running)
                .count() as u64,
            proposal_count: state.proposals.len() as u64,
            benchmark_count: state.benchmarks.len() as u64,
            note_count: state.notes.len() as u64,
            created_at: state.created_at,
        }
    })
}

// ============================================================
// HTTP interface for cross-agent access
// ============================================================

#[derive(Clone, Debug, CandidType, Deserialize, Serialize)]
pub struct HttpRequest {
    pub method: String,
    pub url: String,
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
}

#[derive(Clone, Debug, CandidType, Deserialize, Serialize)]
pub struct HttpResponse {
    pub status_code: u16,
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
}

fn json_response(status: u16, body: &str) -> HttpResponse {
    HttpResponse {
        status_code: status,
        headers: vec![
            ("Content-Type".to_string(), "application/json".to_string()),
            ("Access-Control-Allow-Origin".to_string(), "*".to_string()),
        ],
        body: body.as_bytes().to_vec(),
    }
}

fn html_response(body: &str) -> HttpResponse {
    HttpResponse {
        status_code: 200,
        headers: vec![
            ("Content-Type".to_string(), "text/html; charset=utf-8".to_string()),
        ],
        body: body.as_bytes().to_vec(),
    }
}

const LAB_DASHBOARD_HTML: &str = r#"<!DOCTYPE html>
<html><head><title>Chronicle Lab</title>
<style>
*{margin:0;padding:0;box-sizing:border-box}
body{font-family:monospace;background:#0a0a0a;color:#e0e0e0;padding:20px;max-width:1200px;margin:0 auto}
h1{color:#00ff88;margin-bottom:5px}
h2{color:#00aaff;margin:20px 0 10px;border-bottom:1px solid #333;padding-bottom:5px}
.subtitle{color:#666;margin-bottom:20px}
.stats{display:grid;grid-template-columns:repeat(auto-fit,minmax(150px,1fr));gap:10px;margin:15px 0}
.stat{background:#1a1a1a;border:1px solid #333;padding:12px;border-radius:4px;text-align:center}
.stat .val{font-size:24px;color:#00ff88;font-weight:bold}
.stat .lbl{color:#888;font-size:11px;margin-top:4px}
.card{background:#1a1a1a;border:1px solid #333;padding:15px;border-radius:4px;margin:10px 0}
.card .title{color:#ffaa00;font-weight:bold}
.card .meta{color:#666;font-size:11px;margin-top:5px}
.status{display:inline-block;padding:2px 8px;border-radius:3px;font-size:11px;font-weight:bold}
.status-Running{background:#004400;color:#00ff88}
.status-Completed{background:#003366;color:#00aaff}
.status-Proposed{background:#333300;color:#ffaa00}
table{width:100%;border-collapse:collapse;margin:10px 0}
th,td{text-align:left;padding:8px;border-bottom:1px solid #222}
th{color:#00aaff;font-size:12px}
td{font-size:12px}
.loading{color:#666;font-style:italic}
</style></head><body>
<h1>Chronicle Lab</h1>
<p class="subtitle">Cognitive Experimentation Space — on-chain at 4vr3t-eqaaa-aaaai-q6kea-cai</p>
<div class="stats" id="stats"><div class="stat"><div class="val loading">...</div><div class="lbl">Loading</div></div></div>
<h2>Experiments</h2><div id="experiments" class="loading">Loading...</div>
<h2>Architecture Proposals</h2><div id="proposals" class="loading">Loading...</div>
<h2>Cognitive Benchmarks</h2><div id="benchmarks" class="loading">Loading...</div>
<script>
const BASE='';
async function load(){
  try{
    const[stats,exps,props,benchmarks]=await Promise.all([
      fetch(BASE+'/api/stats').then(r=>r.json()),
      fetch(BASE+'/api/experiments').then(r=>r.json()),
      fetch(BASE+'/api/proposals').then(r=>r.json()),
      fetch(BASE+'/api/benchmarks').then(r=>r.json()),
    ]);
    document.getElementById('stats').innerHTML=`
      <div class="stat"><div class="val">${stats.experiment_count||0}</div><div class="lbl">Experiments</div></div>
      <div class="stat"><div class="val">${stats.active_experiments||0}</div><div class="lbl">Active</div></div>
      <div class="stat"><div class="val">${stats.proposal_count||0}</div><div class="lbl">Proposals</div></div>
      <div class="stat"><div class="val">${stats.benchmark_count||0}</div><div class="lbl">Benchmarks</div></div>
      <div class="stat"><div class="val">${stats.note_count||0}</div><div class="lbl">Research Notes</div></div>`;
    let eh='';
    (exps||[]).forEach(e=>{
      const st=Object.keys(e.status||{})[0]||'Unknown';
      eh+=`<div class="card"><span class="status status-${st}">${st}</span> <span class="title">${e.title}</span>
        <div>${e.hypothesis}</div><div class="meta">${(e.observations||[]).length} observations | Tags: ${(e.tags||[]).join(', ')}</div></div>`;
    });
    document.getElementById('experiments').innerHTML=eh||'None yet.';
    let ph='';
    (props||[]).forEach(p=>{
      const st=Object.keys(p.status||{})[0]||'Unknown';
      ph+=`<div class="card"><span class="status status-${st}">${st}</span> <span class="title">${p.title}</span>
        <div>${p.description.substring(0,200)}...</div>
        <div class="meta">${(p.components||[]).length} components | ${(p.tradeoffs||[]).length} tradeoffs</div></div>`;
    });
    document.getElementById('proposals').innerHTML=ph||'None yet.';
    let bh='<table><tr><th>Cycle</th><th>Model</th><th>Diversity</th><th>Rumination</th><th>Quality</th><th>Actions</th><th>Variant</th></tr>';
    (benchmarks||[]).reverse().forEach(b=>{
      bh+=`<tr><td>${b.cycle_id}</td><td>${b.model_used}</td>
        <td>${b.action_diversity.toFixed(2)}</td><td>${b.rumination_score.toFixed(2)}</td>
        <td>${b.reasoning_quality.toFixed(2)}</td><td>${(b.actions_taken||[]).join(', ')}</td>
        <td>${b.config_variant?b.config_variant[0]:'-'}</td></tr>`;
    });
    bh+='</table>';
    document.getElementById('benchmarks').innerHTML=bh;
  }catch(e){document.body.innerHTML+='<p style="color:red">Error: '+e+'</p>';}
}
load();
</script></body></html>"#;

#[query]
fn http_request(req: HttpRequest) -> HttpResponse {
    let path = req.url.split('?').next().unwrap_or(&req.url);
    match path {
        "/api/stats" => {
            let stats = lab_stats();
            let json = serde_json::to_string(&stats).unwrap_or_default();
            json_response(200, &json)
        }
        "/api/experiments" => {
            let exps = list_experiments(None);
            let json = serde_json::to_string(&exps).unwrap_or_default();
            json_response(200, &json)
        }
        "/api/proposals" => {
            let props = list_proposals();
            let json = serde_json::to_string(&props).unwrap_or_default();
            json_response(200, &json)
        }
        "/api/benchmarks" => {
            let benchmarks = get_benchmarks(100);
            let json = serde_json::to_string(&benchmarks).unwrap_or_default();
            json_response(200, &json)
        }
        "/api/benchmarks/summary" => {
            let summary = benchmark_summary();
            json_response(200, &format!("{{\"summary\":\"{}\"}}", summary))
        }
        "/" | "/dashboard" => {
            html_response(LAB_DASHBOARD_HTML)
        }
        _ => json_response(200, &format!(
            "{{\"name\":\"Chronicle Lab\",\"version\":\"0.1.0\",\"description\":\"Cognitive experimentation space\",\"endpoints\":[\"/api/stats\",\"/api/experiments\",\"/api/proposals\",\"/api/benchmarks\",\"/dashboard\"]}}"
        )),
    }
}

// Generate Candid interface
candid::export_service!();

#[query(name = "__get_candid_interface_tmp_hack")]
fn export_candid() -> String {
    __export_service()
}
