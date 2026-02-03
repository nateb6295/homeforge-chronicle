//! Chronicle Shopkeeper - The quiet keeper of mundane things
//!
//! A simple, reliable binary that handles maintenance without needing an LLM.
//! Runs frequently (every 5 min), does its job, stays quiet.
//!
//! Responsibilities:
//! - Health checks (services, connections)
//! - Balance monitoring (XRP, RLUSD, ICP)
//! - Alert detection and notification
//! - Error rate monitoring
//! - Waking the Mind when something interesting happens
//!
//! Philosophy: The shopkeeper doesn't think. It watches, maintains, and knows
//! when to call for the thinker.

use anyhow::{Context, Result};
use std::time::Duration;
use homeforge_chronicle::db::Database;

const NTFY_TOPIC: &str = "chronicle-mind-9f86c413d8a7b982";
const PROJECT_ROOT: &str = "/home/bradf/projects/homeforge-chronicle";

/// Thresholds that trigger alerts or mind-wake
#[derive(Debug)]
struct Thresholds {
    /// XRP balance below this triggers alert
    xrp_low: f64,
    /// Error count in last hour that triggers mind-wake
    error_count_high: i32,
    /// Hours since last mind cycle before forcing one
    max_hours_without_mind: f64,
    /// Price change % that's "interesting"
    interesting_price_change: f64,
}

impl Default for Thresholds {
    fn default() -> Self {
        Self {
            xrp_low: 10.0,
            error_count_high: 5,
            max_hours_without_mind: 2.0,
            interesting_price_change: 5.0,
        }
    }
}

/// Current state snapshot
#[derive(Debug, Default)]
struct ShopState {
    // Health
    chronicle_mind_running: bool,
    icp_reachable: bool,
    xrpl_reachable: bool,
    ollama_reachable: bool,

    // Balances
    xrp_balance: Option<f64>,
    rlusd_balance: Option<f64>,
    icp_balance: Option<f64>,

    // Activity
    hours_since_last_mind_cycle: Option<f64>,
    recent_error_count: i32,

    // Events worth noting
    interesting_events: Vec<String>,
}

/// Reasons to wake the Mind
#[derive(Debug)]
enum WakeReason {
    TooLongSinceLastCycle(f64),
    HighErrorRate(i32),
    InterestingEvent(String),
    LowBalance(String, f64),
    ScheduledTime,
}

fn main() -> Result<()> {
    eprintln!("Chronicle Shopkeeper starting...");
    eprintln!("  Quiet keeper of mundane things.");

    let db_path = dirs::home_dir()
        .unwrap_or_default()
        .join(".homeforge-chronicle/processed.db");

    let db = Database::new(&db_path)
        .context("Failed to open database")?;

    let thresholds = Thresholds::default();

    // Gather state
    let state = gather_state(&db);

    // Log what we found (quietly)
    log_state(&state, &db);

    // Check if we need to wake the Mind
    let wake_reasons = check_wake_conditions(&state, &thresholds);

    if !wake_reasons.is_empty() {
        eprintln!("  Wake reasons: {:?}", wake_reasons);
        wake_the_mind(&wake_reasons, &db)?;
    } else {
        eprintln!("  All quiet. Mind can rest.");
    }

    // Check for alerts that need immediate notification
    check_alerts(&state, &thresholds);

    eprintln!("Shopkeeper done.");
    Ok(())
}

fn gather_state(db: &Database) -> ShopState {
    let mut state = ShopState::default();

    // Check if chronicle-mind service is running
    state.chronicle_mind_running = std::process::Command::new("systemctl")
        .args(["--user", "is-active", "--quiet", "chronicle-mind"])
        .status()
        .map(|s| s.success())
        .unwrap_or(false);

    // Check Ollama
    state.ollama_reachable = check_ollama();

    // Check hours since last mind cycle
    if let Ok(Some(hours)) = db.hours_since_event("last_mind_cycle") {
        state.hours_since_last_mind_cycle = Some(hours);
    }

    // Count recent errors (from scratch pad)
    state.recent_error_count = count_recent_errors(db);

    // Check for interesting events in scratch pad
    state.interesting_events = find_interesting_events(db);

    // Get balances from last known state (don't make network calls - that's Mind's job)
    if let Ok(Some(bal)) = db.get_mind_value("last_xrp_balance") {
        let bal_str: &str = &bal;
        state.xrp_balance = bal_str.parse::<f64>().ok();
    }
    if let Ok(Some(bal)) = db.get_mind_value("last_rlusd_balance") {
        let bal_str: &str = &bal;
        state.rlusd_balance = bal_str.parse::<f64>().ok();
    }

    state
}

fn check_ollama() -> bool {
    let url = std::env::var("CHRONICLE_OLLAMA_URL")
        .unwrap_or_else(|_| "http://192.168.1.11:11434".to_string());

    reqwest::blocking::Client::new()
        .get(format!("{}/api/tags", url))
        .timeout(Duration::from_secs(5))
        .send()
        .map(|r| r.status().is_success())
        .unwrap_or(false)
}

fn count_recent_errors(db: &Database) -> i32 {
    // Count scratch notes with "error" category in last hour
    db.count_recent_notes_by_category("error", 1.0).unwrap_or(0)
}

fn find_interesting_events(db: &Database) -> Vec<String> {
    // Look for high-priority notes or specific categories
    let mut events = Vec::new();

    if let Ok(notes) = db.get_scratch_notes(10, None, false) {
        for note in notes {
            // Priority 8+ is interesting
            if note.priority >= 8 {
                events.push(format!("High priority note: {}", truncate(&note.content, 50)));
            }
            // Certain categories are interesting
            if let Some(cat) = &note.category {
                if cat == "alert" || cat == "urgent" || cat == "discovery" {
                    events.push(format!("{}: {}", cat, truncate(&note.content, 50)));
                }
            }
        }
    }

    events
}

fn log_state(state: &ShopState, db: &Database) {
    eprintln!("  Mind running: {}", state.chronicle_mind_running);
    eprintln!("  Ollama: {}", if state.ollama_reachable { "✓" } else { "✗" });

    if let Some(hours) = state.hours_since_last_mind_cycle {
        eprintln!("  Hours since mind cycle: {:.1}", hours);
    }

    if state.recent_error_count > 0 {
        eprintln!("  Recent errors: {}", state.recent_error_count);
    }

    if !state.interesting_events.is_empty() {
        eprintln!("  Interesting events: {}", state.interesting_events.len());
    }

    // Record shopkeeper run
    let _ = db.set_mind_timestamp("last_shopkeeper_run", None);
}

fn check_wake_conditions(state: &ShopState, thresholds: &Thresholds) -> Vec<WakeReason> {
    let mut reasons = Vec::new();

    // Too long since last mind cycle
    if let Some(hours) = state.hours_since_last_mind_cycle {
        if hours > thresholds.max_hours_without_mind {
            reasons.push(WakeReason::TooLongSinceLastCycle(hours));
        }
    }

    // High error rate
    if state.recent_error_count >= thresholds.error_count_high {
        reasons.push(WakeReason::HighErrorRate(state.recent_error_count));
    }

    // Interesting events
    for event in &state.interesting_events {
        reasons.push(WakeReason::InterestingEvent(event.clone()));
    }

    // Low balance
    if let Some(xrp) = state.xrp_balance {
        if xrp < thresholds.xrp_low {
            reasons.push(WakeReason::LowBalance("XRP".to_string(), xrp));
        }
    }

    reasons
}

fn wake_the_mind(reasons: &[WakeReason], db: &Database) -> Result<()> {
    eprintln!("  Waking the Mind...");

    // Leave a note for the Mind explaining why it was woken
    let reason_text = reasons.iter()
        .map(|r| match r {
            WakeReason::TooLongSinceLastCycle(h) => format!("Been {:.1}h since last cycle", h),
            WakeReason::HighErrorRate(n) => format!("{} errors in last hour", n),
            WakeReason::InterestingEvent(e) => format!("Event: {}", e),
            WakeReason::LowBalance(asset, bal) => format!("Low {}: {:.2}", asset, bal),
            WakeReason::ScheduledTime => "Scheduled wake time".to_string(),
        })
        .collect::<Vec<_>>()
        .join("; ");

    let note = format!("[SHOPKEEPER WAKE]\n{}", reason_text);
    let _ = db.write_scratch_note(&note, Some("shopkeeper"), 7, None);

    // Don't actually restart the service - just leave the note
    // The Mind runs on its own schedule, but now it has context about why
    // the shopkeeper thought it was important

    // For urgent reasons, send a notification
    let is_urgent = reasons.iter().any(|r| matches!(r,
        WakeReason::HighErrorRate(_) |
        WakeReason::LowBalance(_, _)
    ));

    if is_urgent {
        send_notification("Shopkeeper Alert", &reason_text, "default")?;
    }

    Ok(())
}

fn check_alerts(state: &ShopState, thresholds: &Thresholds) {
    // Service down is critical
    if !state.chronicle_mind_running {
        eprintln!("  ALERT: chronicle-mind not running!");
        let _ = send_notification(
            "Chronicle Down",
            "chronicle-mind service is not running",
            "high"
        );
    }
}

fn send_notification(title: &str, message: &str, priority: &str) -> Result<()> {
    let client = reqwest::blocking::Client::new();
    let url = format!("https://ntfy.sh/{}", NTFY_TOPIC);

    client.post(&url)
        .header("Title", title)
        .header("Priority", priority)
        .header("Tags", "shop,robot")
        .body(message.to_string())
        .send()
        .context("Failed to send notification")?;

    Ok(())
}

fn truncate(s: &str, max_len: usize) -> String {
    if s.len() <= max_len {
        s.to_string()
    } else {
        format!("{}...", &s[..max_len])
    }
}
