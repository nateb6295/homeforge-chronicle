//! Chronicle Pulse - Mobile-first dashboard for monitoring Chronicle
//!
//! A beautiful, real-time view into Chronicle's cognitive activity.

use axum::{
    extract::State,
    response::{Html, Json},
    routing::get,
    Router,
};
use serde::Serialize;
use std::sync::Arc;
use std::path::PathBuf;
use tokio::net::TcpListener;
use tokio::sync::Mutex;

use homeforge_chronicle::db::Database;

/// Shared application state
struct AppState {
    db_path: PathBuf,
}

/// Dashboard data sent to frontend
#[derive(Serialize, Default)]
struct DashboardData {
    // Status
    status: String,
    last_cycle: Option<String>,
    last_cycle_ago: String,

    // Wallets
    agent_xrp: f64,
    agent_rlusd: f64,
    canister_xrp: f64,
    total_usd: f64,

    // Market
    xrp_price: Option<f64>,
    xrp_rsi: Option<f64>,
    rsi_status: String,
    price_data_points: usize,

    // Outbox
    unread_messages: usize,
    messages: Vec<OutboxMessage>,

    // Thoughts
    recent_thoughts: Vec<ThoughtEntry>,

    // Patterns
    patterns: Vec<PatternEntry>,

    // Scratch pad
    scratch_notes: Vec<ScratchEntry>,

    // Activity
    recent_activity: Vec<ActivityEntry>,

    // Swap stats
    swaps_today: f64,
    swap_allowance: f64,
    hours_until_swap: Option<f64>,
}

#[derive(Serialize)]
struct OutboxMessage {
    id: i64,
    message: String,
    priority: i32,
    time_ago: String,
}

#[derive(Serialize)]
struct ThoughtEntry {
    cycle_id: String,
    reasoning_preview: String,
    context: String,
    actions: Vec<String>,
    time_ago: String,
}

#[derive(Serialize)]
struct PatternEntry {
    summary: String,
    confidence: f64,
    trend: String,
    capsule_count: i64,
}

#[derive(Serialize)]
struct ScratchEntry {
    id: i64,
    content: String,
    category: String,
    priority: i32,
}

#[derive(Serialize)]
struct ActivityEntry {
    icon: String,
    description: String,
    time_ago: String,
}

/// Format seconds ago into human readable string
fn format_time_ago(seconds: i64) -> String {
    if seconds < 60 {
        format!("{}s ago", seconds)
    } else if seconds < 3600 {
        format!("{}m ago", seconds / 60)
    } else if seconds < 86400 {
        format!("{}h ago", seconds / 3600)
    } else {
        format!("{}d ago", seconds / 86400)
    }
}

/// Get dashboard data from database
async fn get_dashboard_data(State(state): State<Arc<AppState>>) -> Json<DashboardData> {
    // Open fresh connection for each request (thread-safe)
    let db = match Database::new(&state.db_path) {
        Ok(db) => db,
        Err(e) => {
            eprintln!("Database error: {}", e);
            return Json(DashboardData::default());
        }
    };
    let now = chrono::Utc::now().timestamp();

    // Get latest thought
    let thoughts: Vec<ThoughtEntry> = {
        let mut result = Vec::new();
        if let Ok(mut stmt) = db.conn.prepare(
            "SELECT cycle_id, reasoning, context_summary, actions_taken, created_at
             FROM thought_stream ORDER BY created_at DESC LIMIT 5"
        ) {
            if let Ok(rows) = stmt.query_map([], |row| {
                let cycle_id: String = row.get(0)?;
                let reasoning: String = row.get(1)?;
                let context: String = row.get(2)?;
                let actions_json: String = row.get(3)?;
                let created_at: i64 = row.get(4)?;
                Ok((cycle_id, reasoning, context, actions_json, created_at))
            }) {
                for row in rows.flatten() {
                    let (cycle_id, reasoning, context, actions_json, created_at) = row;
                    let actions: Vec<String> = serde_json::from_str(&actions_json).unwrap_or_default();
                    result.push(ThoughtEntry {
                        cycle_id,
                        reasoning_preview: if reasoning.len() > 200 {
                            format!("{}...", &reasoning[..200])
                        } else {
                            reasoning
                        },
                        context,
                        actions,
                        time_ago: format_time_ago(now - created_at),
                    });
                }
            }
        }
        result
    };

    let last_cycle = thoughts.first().map(|t| t.cycle_id.clone());
    let last_cycle_ago = thoughts.first().map(|t| t.time_ago.clone()).unwrap_or_else(|| "never".to_string());

    // Get outbox messages
    let messages: Vec<OutboxMessage> = {
        let mut result = Vec::new();
        if let Ok(mut stmt) = db.conn.prepare(
            "SELECT id, message, priority, created_at FROM outbox
             WHERE acknowledged = 0 ORDER BY created_at DESC LIMIT 10"
        ) {
            if let Ok(rows) = stmt.query_map([], |row| {
                let id: i64 = row.get(0)?;
                let message: String = row.get(1)?;
                let priority: i32 = row.get(2)?;
                let created_at: i64 = row.get(3)?;
                Ok((id, message, priority, created_at))
            }) {
                for row in rows.flatten() {
                    let (id, message, priority, created_at) = row;
                    result.push(OutboxMessage {
                        id,
                        message,
                        priority,
                        time_ago: format_time_ago(now - created_at),
                    });
                }
            }
        }
        result
    };

    let unread_messages = messages.len();

    // Get patterns
    let patterns: Vec<PatternEntry> = db.get_enriched_patterns(0.4, 6, false)
        .map(|ps| ps.into_iter().map(|p| {
            let trend = if p.projected_confidence_7d > p.confidence + 0.05 {
                "↑".to_string()
            } else if p.projected_confidence_7d < p.confidence - 0.05 {
                "↓".to_string()
            } else {
                "→".to_string()
            };
            PatternEntry {
                summary: p.summary,
                confidence: p.confidence,
                trend,
                capsule_count: p.capsule_count,
            }
        }).collect())
        .unwrap_or_default();

    // Get scratch notes
    let scratch_notes: Vec<ScratchEntry> = db.get_scratch_notes(10, None, false)
        .map(|notes| notes.into_iter().map(|n| ScratchEntry {
            id: n.id,
            content: n.content,
            category: n.category.unwrap_or_else(|| "note".to_string()),
            priority: n.priority,
        }).collect())
        .unwrap_or_default();

    // Get price data
    let (xrp_price, price_data_points) = (
        db.get_latest_price("XRP").ok().flatten().map(|(p, _)| p),
        db.get_price_count("XRP").unwrap_or(0),
    );

    let xrp_rsi = db.calculate_rsi("XRP").ok().flatten();
    let rsi_status = match xrp_rsi {
        Some(rsi) if rsi < 30.0 => "OVERSOLD".to_string(),
        Some(rsi) if rsi > 70.0 => "OVERBOUGHT".to_string(),
        Some(_) => "neutral".to_string(),
        None => format!("collecting ({}/15)", price_data_points),
    };

    // Get swap stats
    let swaps_today = db.xrp_swapped_in_hours(24.0).unwrap_or(0.0);
    let swap_allowance = (2.0 - swaps_today).max(0.0);
    let hours_until_swap = db.hours_since_last_swap().ok().flatten()
        .map(|h| (4.0 - h).max(0.0))
        .filter(|h| *h > 0.0);

    // Build activity feed from thoughts
    let recent_activity: Vec<ActivityEntry> = {
        let mut result = Vec::new();
        for t in &thoughts {
            for a in &t.actions {
                let (icon, desc): (&str, String) = if a.contains("swap") {
                    ("💱", a.clone())
                } else if a.contains("reflection") {
                    ("💭", a.clone())
                } else if a.contains("note") {
                    ("📝", a.clone())
                } else if a.contains("message") {
                    ("📬", a.clone())
                } else if a.contains("no_action") {
                    ("😴", a.clone())
                } else {
                    ("⚡", a.clone())
                };
                result.push(ActivityEntry {
                    icon: icon.to_string(),
                    description: desc.chars().take(60).collect::<String>(),
                    time_ago: t.time_ago.clone(),
                });
                if result.len() >= 8 {
                    break;
                }
            }
            if result.len() >= 8 {
                break;
            }
        }
        result
    };

    // Determine status
    let status = if last_cycle_ago.contains("s ago") || last_cycle_ago.contains("1m") || last_cycle_ago.contains("2m") {
        "thinking".to_string()
    } else if last_cycle_ago.contains("m ago") && !last_cycle_ago.contains("10m") {
        "idle".to_string()
    } else {
        "sleeping".to_string()
    };

    // Calculate totals (placeholder - would need wallet fetch)
    let agent_xrp = 6.50; // Would fetch from wallet
    let agent_rlusd = 2.30;
    let canister_xrp = 0.0;
    let total_usd = xrp_price.map(|p| agent_xrp * p + agent_rlusd).unwrap_or(0.0);

    Json(DashboardData {
        status,
        last_cycle,
        last_cycle_ago,
        agent_xrp,
        agent_rlusd,
        canister_xrp,
        total_usd,
        xrp_price,
        xrp_rsi,
        rsi_status,
        price_data_points,
        unread_messages,
        messages,
        recent_thoughts: thoughts,
        patterns,
        scratch_notes,
        recent_activity,
        swaps_today,
        swap_allowance,
        hours_until_swap,
    })
}

/// Serve the main dashboard HTML
async fn index() -> Html<&'static str> {
    Html(include_str!("../../static/pulse.html"))
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let home = std::env::var("HOME")?;
    let db_path = PathBuf::from(format!("{}/.homeforge-chronicle/processed.db", home));

    // Test connection
    let _ = Database::new(&db_path)?;

    let state = Arc::new(AppState { db_path });

    let app = Router::new()
        .route("/", get(index))
        .route("/api/dashboard", get(get_dashboard_data))
        .with_state(state);

    let port = std::env::var("PULSE_PORT").unwrap_or_else(|_| "3131".to_string());
    let addr = format!("0.0.0.0:{}", port);

    println!("Chronicle Pulse starting on http://{}", addr);
    println!("Open on your phone: http://{}:{}", local_ip().unwrap_or_else(|| "localhost".to_string()), port);

    let listener = TcpListener::bind(&addr).await?;
    axum::serve(listener, app).await?;

    Ok(())
}

/// Get local IP address for display
fn local_ip() -> Option<String> {
    use std::net::UdpSocket;
    let socket = UdpSocket::bind("0.0.0.0:0").ok()?;
    socket.connect("8.8.8.8:80").ok()?;
    socket.local_addr().ok().map(|a| a.ip().to_string())
}
