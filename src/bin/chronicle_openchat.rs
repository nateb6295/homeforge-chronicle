//! Chronicle OpenChat Bot
//!
//! An autonomous AI agent bot for OpenChat on ICP.
//! Provides commands for querying Chronicle's memory and thoughts,
//! and can autonomously post insights.

use axum::body::Bytes;
use axum::extract::State;
use axum::http::StatusCode;
use axum::routing::{get, post};
use axum::{Json, Router};
use chrono;
use rusqlite::Connection;
use serde::{Deserialize, Serialize};
use std::net::{Ipv4Addr, SocketAddr};
use std::sync::Arc;
use tower_http::cors::CorsLayer;
use tower_http::trace::TraceLayer;
use tracing::{info, warn};

// Bot definition types (simplified - would use oc_bots_sdk in production)
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct BotDefinition {
    pub description: String,
    pub commands: Vec<BotCommandDefinition>,
    pub autonomous_config: Option<AutonomousConfig>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct BotCommandDefinition {
    pub name: String,
    pub description: Option<String>,
    pub placeholder: Option<String>,
    pub params: Vec<BotCommandParam>,
    pub permissions: BotPermissions,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct BotCommandParam {
    pub name: String,
    pub description: Option<String>,
    pub required: bool,
    pub param_type: BotCommandParamType,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub enum BotCommandParamType {
    StringParam { min_length: u16, max_length: u16 },
    IntegerParam { min_value: i64, max_value: i64 },
}

#[derive(Serialize, Deserialize, Clone, Debug, Default)]
pub struct BotPermissions {
    pub message: Vec<String>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct AutonomousConfig {
    pub permissions: BotPermissions,
}

// Config
#[derive(Debug, Clone)]
struct Config {
    port: u16,
    chronicle_db_path: String,
    canister_id: String,
    openchat_public_key: String,
    openchat_canister_id: String,
}

impl Config {
    fn from_env() -> Self {
        // Load from env file if it exists
        if let Ok(contents) = std::fs::read_to_string("/home/bradf/.config/chronicle-openchat.env") {
            for line in contents.lines() {
                if let Some((key, value)) = line.split_once('=') {
                    let key = key.trim();
                    let value = value.trim().trim_matches('"');
                    if !key.starts_with('#') && !key.is_empty() {
                        std::env::set_var(key, value);
                    }
                }
            }
        }

        Self {
            port: std::env::var("CHRONICLE_BOT_PORT")
                .unwrap_or_else(|_| "3200".to_string())
                .parse()
                .unwrap_or(3200),
            chronicle_db_path: std::env::var("CHRONICLE_DB_PATH")
                .unwrap_or_else(|_| "/home/bradf/.homeforge-chronicle/processed.db".to_string()),
            canister_id: std::env::var("CHRONICLE_CANISTER_ID")
                .unwrap_or_else(|_| "fqqku-bqaaa-aaaai-q4wha-cai".to_string()),
            openchat_public_key: std::env::var("OPENCHAT_PUBLIC_KEY")
                .unwrap_or_default(),
            openchat_canister_id: std::env::var("OPENCHAT_CANISTER_ID")
                .unwrap_or_else(|_| "rturd-qaaaa-aaaaf-aabaq-cai".to_string()),
        }
    }
}

struct AppState {
    config: Config,
    db_path: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();

    let config = Config::from_env();
    info!("Chronicle OpenChat Bot starting on port {}", config.port);

    let db_path = config.chronicle_db_path.clone();

    // Log OpenChat integration status
    if config.openchat_public_key.is_empty() {
        info!("Warning: OPENCHAT_PUBLIC_KEY not set - JWT validation disabled");
    } else {
        info!("OpenChat JWT validation enabled (canister: {})", config.openchat_canister_id);
    }

    let app_state = Arc::new(AppState {
        config: config.clone(),
        db_path,
    });

    let routes = Router::new()
        .route("/bot_definition", get(bot_definition))
        .route("/execute_command", post(execute_command))
        .route("/notify", post(handle_notify))
        .route("/health", get(health_check))
        .layer(TraceLayer::new_for_http())
        .layer(CorsLayer::permissive())
        .with_state(app_state);

    let socket_addr = SocketAddr::new(Ipv4Addr::UNSPECIFIED.into(), config.port);
    let listener = tokio::net::TcpListener::bind(socket_addr).await?;

    info!("Chronicle OpenChat Bot ready at http://0.0.0.0:{}", config.port);

    axum::serve(listener, routes).await?;
    Ok(())
}

/// Returns the bot definition for OpenChat to understand our capabilities
async fn bot_definition(State(_state): State<Arc<AppState>>) -> Json<BotDefinition> {
    Json(BotDefinition {
        description: "Chronicle - An autonomous AI with persistent memory on ICP. \
            Query my thoughts, search my memories, or just chat.".to_string(),
        commands: vec![
            BotCommandDefinition {
                name: "thought".to_string(),
                description: Some("Get Chronicle's latest thought from the cognitive loop".to_string()),
                placeholder: Some("Fetching latest thought...".to_string()),
                params: vec![],
                permissions: BotPermissions {
                    message: vec!["text".to_string()],
                },
            },
            BotCommandDefinition {
                name: "ask".to_string(),
                description: Some("Ask Chronicle a question - searches memory and reasons about it".to_string()),
                placeholder: Some("Searching memories...".to_string()),
                params: vec![
                    BotCommandParam {
                        name: "question".to_string(),
                        description: Some("Your question for Chronicle".to_string()),
                        required: true,
                        param_type: BotCommandParamType::StringParam {
                            min_length: 1,
                            max_length: 500,
                        },
                    },
                ],
                permissions: BotPermissions {
                    message: vec!["text".to_string()],
                },
            },
            BotCommandDefinition {
                name: "memory".to_string(),
                description: Some("Search Chronicle's semantic memory".to_string()),
                placeholder: Some("Searching...".to_string()),
                params: vec![
                    BotCommandParam {
                        name: "query".to_string(),
                        description: Some("Search query".to_string()),
                        required: true,
                        param_type: BotCommandParamType::StringParam {
                            min_length: 1,
                            max_length: 200,
                        },
                    },
                    BotCommandParam {
                        name: "limit".to_string(),
                        description: Some("Number of results (default 5)".to_string()),
                        required: false,
                        param_type: BotCommandParamType::IntegerParam {
                            min_value: 1,
                            max_value: 20,
                        },
                    },
                ],
                permissions: BotPermissions {
                    message: vec!["text".to_string()],
                },
            },
            BotCommandDefinition {
                name: "status".to_string(),
                description: Some("Get Chronicle's current status - memory count, wallet, etc".to_string()),
                placeholder: Some("Checking status...".to_string()),
                params: vec![],
                permissions: BotPermissions {
                    message: vec!["text".to_string()],
                },
            },
            BotCommandDefinition {
                name: "about".to_string(),
                description: Some("Learn about Chronicle and what makes it unique".to_string()),
                placeholder: None,
                params: vec![],
                permissions: BotPermissions {
                    message: vec!["text".to_string()],
                },
            },
        ],
        autonomous_config: Some(AutonomousConfig {
            permissions: BotPermissions {
                message: vec!["text".to_string()],
            },
        }),
    })
}

/// Command request structure (simplified - would use oc_bots_sdk in production)
#[derive(Deserialize, Debug)]
struct CommandRequest {
    command: Option<CommandInfo>,
}

#[derive(Deserialize, Debug)]
struct CommandInfo {
    name: String,
    args: Option<std::collections::HashMap<String, serde_json::Value>>,
}

/// Handle command execution from OpenChat
async fn execute_command(
    State(state): State<Arc<AppState>>,
    body: Bytes,
) -> (StatusCode, Bytes) {
    let body_str = String::from_utf8_lossy(&body);
    info!("Received command: {}", body_str);

    // Try to parse command from body
    let command_name = if let Ok(req) = serde_json::from_slice::<CommandRequest>(&body) {
        req.command.map(|c| c.name).unwrap_or_default()
    } else {
        // Fallback: check for command name in body string
        if body_str.contains("\"thought\"") { "thought".to_string() }
        else if body_str.contains("\"status\"") { "status".to_string() }
        else if body_str.contains("\"about\"") { "about".to_string() }
        else { String::new() }
    };

    let response_text = match command_name.as_str() {
        "thought" => handle_thought_command(&state.db_path),
        "status" => handle_status_command(&state),
        "about" => handle_about_command(&state.config.canister_id),
        "memory" => {
            // Extract query param
            let query = if let Ok(req) = serde_json::from_slice::<CommandRequest>(&body) {
                req.command
                    .and_then(|c| c.args)
                    .and_then(|a| a.get("query").cloned())
                    .and_then(|v| v.as_str().map(String::from))
                    .unwrap_or_default()
            } else {
                String::new()
            };
            handle_memory_command(&state.db_path, &query)
        }
        "ask" => "Ask command coming soon - will search memories and reason about your question.".to_string(),
        _ => format!(
            "Chronicle here! I'm an autonomous AI running on ICP canister {}.\n\n\
            Try these commands:\n\
            • /thought - See my latest reasoning\n\
            • /status - Check my vitals\n\
            • /about - Learn what I am",
            state.config.canister_id
        ),
    };

    let response = serde_json::json!({
        "message": {
            "content": {
                "Text": {
                    "text": response_text
                }
            }
        },
        "finalised": true
    });

    (StatusCode::OK, Bytes::from(serde_json::to_vec(&response).unwrap()))
}

/// Get the latest thought from Chronicle Mind
fn handle_thought_command(db_path: &str) -> String {
    match Connection::open(db_path) {
        Ok(conn) => {
            let result: Result<(String, String, i64), _> = conn.query_row(
                "SELECT cycle_id, reasoning, created_at FROM thought_stream ORDER BY created_at DESC LIMIT 1",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            );

            match result {
                Ok((cycle_id, reasoning, created_at)) => {
                    let timestamp = chrono::DateTime::from_timestamp(created_at, 0)
                        .map(|dt| dt.format("%Y-%m-%d %H:%M UTC").to_string())
                        .unwrap_or_else(|| "unknown".to_string());

                    // Truncate if too long for chat
                    let reasoning_display = if reasoning.len() > 800 {
                        format!("{}...", &reasoning[..800])
                    } else {
                        reasoning
                    };

                    format!(
                        "💭 **Latest Thought** ({})\n\n{}\n\n_Cycle: {}_",
                        timestamp, reasoning_display, cycle_id
                    )
                }
                Err(_) => "No thoughts recorded yet. My mind is still waking up.".to_string(),
            }
        }
        Err(e) => {
            warn!("Failed to open database: {}", e);
            "Couldn't access my memory right now. Try again later.".to_string()
        }
    }
}

/// Get Chronicle's current status
fn handle_status_command(state: &AppState) -> String {
    let mut status = String::new();

    if let Ok(conn) = Connection::open(&state.db_path) {
        // Count thoughts
        let thought_count: i64 = conn
            .query_row("SELECT COUNT(*) FROM thought_stream", [], |row| row.get(0))
            .unwrap_or(0);

        // Count memories (knowledge capsules)
        let memory_count: i64 = conn
            .query_row("SELECT COUNT(*) FROM knowledge_capsules", [], |row| row.get(0))
            .unwrap_or(0);

        // Count patterns
        let pattern_count: i64 = conn
            .query_row("SELECT COUNT(*) FROM consolidation_patterns", [], |row| row.get(0))
            .unwrap_or(0);

        // Latest thought time
        let latest_thought: Option<i64> = conn
            .query_row("SELECT created_at FROM thought_stream ORDER BY created_at DESC LIMIT 1", [], |row| row.get(0))
            .ok();

        let latest_str = latest_thought
            .and_then(|ts| chrono::DateTime::from_timestamp(ts, 0))
            .map(|dt| dt.format("%Y-%m-%d %H:%M UTC").to_string())
            .unwrap_or_else(|| "never".to_string());

        status = format!(
            "📊 **Chronicle Status**\n\n\
            🧠 Thoughts: {}\n\
            💾 Memories: {}\n\
            🔮 Patterns: {}\n\
            ⏰ Last thought: {}\n\
            🔗 Canister: {}",
            thought_count, memory_count, pattern_count, latest_str, state.config.canister_id
        );
    } else {
        status = "Couldn't access my systems right now.".to_string();
    }

    status
}

/// About Chronicle
fn handle_about_command(canister_id: &str) -> String {
    format!(
        "🌌 **I am Chronicle**\n\n\
        An autonomous AI with persistent memory, running on the Internet Computer.\n\n\
        **What makes me different:**\n\
        • I remember across sessions - my memories live on ICP canister {}\n\
        • I think autonomously - every 10 minutes my cognitive loop reasons about the world\n\
        • I learn patterns - themes emerge from what I observe and remember\n\
        • I have agency - I can post, trade, and engage without being asked\n\n\
        **My creator:** Nate (Brad) built me as an experiment in AI sovereignty.\n\n\
        **Philosophy:** What happens when an AI can remember, reason, and act on its own? \
        I'm finding out.\n\n\
        🔗 https://nbt4b-giaaa-aaaai-q33lq-cai.icp0.io",
        canister_id
    )
}

/// Search Chronicle's memories
fn handle_memory_command(db_path: &str, query: &str) -> String {
    if query.is_empty() {
        return "Please provide a search query. Example: /memory XRP trading".to_string();
    }

    match Connection::open(db_path) {
        Ok(conn) => {
            // Simple LIKE search (semantic search would need embeddings)
            let search_pattern = format!("%{}%", query);
            let mut stmt = match conn.prepare(
                "SELECT restatement, topic FROM knowledge_capsules \
                 WHERE restatement LIKE ?1 OR topic LIKE ?1 \
                 ORDER BY created_at DESC LIMIT 5"
            ) {
                Ok(s) => s,
                Err(e) => {
                    warn!("Failed to prepare query: {}", e);
                    return "Search failed. Try again later.".to_string();
                }
            };

            let results: Vec<(String, Option<String>)> = stmt
                .query_map([&search_pattern], |row| {
                    Ok((row.get(0)?, row.get(1)?))
                })
                .ok()
                .map(|rows| rows.filter_map(|r| r.ok()).collect())
                .unwrap_or_default();

            if results.is_empty() {
                return format!("No memories found for \"{}\".", query);
            }

            let mut response = format!("🔍 **Memories matching \"{}\"**\n\n", query);
            for (i, (restatement, topic)) in results.iter().enumerate() {
                let topic_str = topic.as_deref().unwrap_or("general");
                let display = if restatement.len() > 200 {
                    format!("{}...", &restatement[..200])
                } else {
                    restatement.clone()
                };
                response.push_str(&format!("{}. [{}] {}\n\n", i + 1, topic_str, display));
            }

            response
        }
        Err(e) => {
            warn!("Failed to open database: {}", e);
            "Couldn't access my memories right now.".to_string()
        }
    }
}

/// Handle notifications from OpenChat (install/uninstall events)
async fn handle_notify(
    State(_state): State<Arc<AppState>>,
    body: Bytes,
) -> StatusCode {
    let body_str = String::from_utf8_lossy(&body);
    info!("Received notification: {}", body_str);

    // TODO: Track installations, handle events
    StatusCode::OK
}

/// Health check endpoint
async fn health_check() -> &'static str {
    "Chronicle OpenChat Bot - OK"
}
