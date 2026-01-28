use anyhow::{Context, Result};
use askama::Template;
use chrono::{DateTime, Utc};
use std::path::Path;

use crate::compilation::{Prediction, PredictionStatus};

/// Entry for display in templates
#[derive(Debug, Clone)]
pub struct DisplayEntry {
    pub title: String,
    pub summary: String,
    pub classification: String,
    pub date: String,
    pub themes: Vec<String>,
    pub quotes: Vec<String>,
}

/// Thread info for listing
#[derive(Debug, Clone)]
pub struct ThreadInfo {
    pub name: String,
    pub slug: String,
    pub count: usize,
    pub date_range: String,
}

/// Prediction for display
#[derive(Debug, Clone)]
pub struct DisplayPrediction {
    pub claim: String,
    pub date_made: String,
    pub timeline: String,
    pub validation_date: String,
    pub notes: String,
}

/// Capsule for display in templates
#[derive(Debug, Clone)]
pub struct DisplayCapsule {
    pub content: String,
    pub topic: String,
    pub timestamp: String,
    pub keywords: Vec<String>,
}

/// Market position for display in templates
#[derive(Debug, Clone)]
pub struct DisplayMarketPosition {
    pub platform: String,
    pub market_id: String,
    pub market_question: String,
    pub position: String,
    pub entry_price: f64,
    pub shares: f64,
    pub stake_usdc: f64,
    pub thesis: String,
    pub confidence: i32,
    pub status: String,
    pub pnl_usdc: f64,
    pub created_at: String,
    pub resolved_at: String,
}

#[derive(Template)]
#[template(path = "index.html")]
struct IndexTemplate {
    site_title: String,
    author: String,
    page: String,
    entries: Vec<DisplayEntry>,
    has_more: bool,
    // Stats for dashboard
    total_entries: usize,
    thread_count: usize,
    prediction_count: usize,
    pending_predictions: usize,
    validated_predictions: usize,
    // New fields for activity discovery
    capsule_count: usize,
    last_updated: String,
    recent_capsules: Vec<DisplayCapsule>,
    // Mind activity fields
    total_thoughts: usize,
    actions_today: usize,
    latest_thought: String,
}

#[derive(Template)]
#[template(path = "thread.html")]
struct ThreadTemplate {
    site_title: String,
    author: String,
    page: String,
    theme: String,
    date_start: String,
    date_end: String,
    entry_count: usize,
    entries: Vec<DisplayEntry>,
}

#[derive(Template)]
#[template(path = "threads_index.html")]
struct ThreadsIndexTemplate {
    site_title: String,
    author: String,
    page: String,
    threads: Vec<ThreadInfo>,
    thread_count: usize,
}

#[derive(Template)]
#[template(path = "predictions.html")]
struct PredictionsTemplate {
    site_title: String,
    author: String,
    page: String,
    // Market positions (primary)
    market_total: usize,
    market_open: usize,
    market_won: usize,
    market_lost: usize,
    total_pnl: f64,
    open_positions: Vec<DisplayMarketPosition>,
    won_positions: Vec<DisplayMarketPosition>,
    lost_positions: Vec<DisplayMarketPosition>,
    // Legacy extracted predictions
    total: usize,
    pending_count: usize,
    validated_count: usize,
    invalidated_count: usize,
    pending: Vec<DisplayPrediction>,
    validated: Vec<DisplayPrediction>,
    invalidated: Vec<DisplayPrediction>,
}

#[derive(Template)]
#[template(path = "about.html")]
struct AboutTemplate {
    site_title: String,
    author: String,
    page: String,
}

#[derive(Template)]
#[template(path = "input.html")]
struct InputTemplate {
    site_title: String,
    author: String,
    page: String,
}

#[derive(Template)]
#[template(path = "wallet.html")]
struct WalletTemplate {
    site_title: String,
    author: String,
    page: String,
}

/// Thought entry for display
#[derive(Debug, Clone)]
pub struct DisplayThought {
    pub cycle_id: String,
    pub timestamp: String,
    pub context_summary: String,
    pub reasoning: String,
    pub actions: Vec<String>,
}

#[derive(Template)]
#[template(path = "thoughts.html")]
struct ThoughtsTemplate {
    site_title: String,
    author: String,
    page: String,
    total_thoughts: usize,
    actions_today: usize,
    last_cycle: String,
    thoughts: Vec<DisplayThought>,
    has_more: bool,
}

/// Outbox message for display
#[derive(Debug, Clone)]
pub struct DisplayOutboxMessage {
    pub message: String,
    pub category: String,
    pub priority: i32,
    pub timestamp: String,
    pub acknowledged: bool,
    pub read_at: String,
}

#[derive(Template)]
#[template(path = "outbox.html")]
struct OutboxTemplate {
    site_title: String,
    author: String,
    page: String,
    messages: Vec<DisplayOutboxMessage>,
    unread_count: usize,
    total_messages: usize,
}

/// Generate RSS feed XML
pub fn generate_rss_feed(
    site_title: &str,
    author: &str,
    base_url: &str,
    entries: &[DisplayEntry],
) -> String {
    let mut rss = String::new();

    rss.push_str(r#"<?xml version="1.0" encoding="UTF-8"?>"#);
    rss.push('\n');
    rss.push_str(r#"<rss version="2.0" xmlns:atom="http://www.w3.org/2005/Atom">"#);
    rss.push('\n');
    rss.push_str("  <channel>\n");
    rss.push_str(&format!("    <title>{}</title>\n", escape_xml(site_title)));
    rss.push_str(&format!("    <link>{}</link>\n", escape_xml(base_url)));
    rss.push_str(&format!("    <description>Automated life-archive by {}</description>\n", escape_xml(author)));
    rss.push_str(&format!("    <atom:link href=\"{}/feed.xml\" rel=\"self\" type=\"application/rss+xml\" />\n", escape_xml(base_url)));

    for entry in entries.iter().take(20) {
        rss.push_str("    <item>\n");
        rss.push_str(&format!("      <title>{}</title>\n", escape_xml(&entry.title)));
        rss.push_str(&format!("      <description>{}</description>\n", escape_xml(&entry.summary)));
        rss.push_str(&format!("      <pubDate>{}</pubDate>\n", entry.date));
        rss.push_str(&format!("      <category>{}</category>\n", escape_xml(&entry.classification)));
        rss.push_str("    </item>\n");
    }

    rss.push_str("  </channel>\n");
    rss.push_str("</rss>\n");

    rss
}

fn escape_xml(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&apos;")
}

/// Build the static site
pub fn build_site<P: AsRef<Path>>(
    output_dir: P,
    site_title: &str,
    author: &str,
    recent_entries: Vec<DisplayEntry>,
    threads: Vec<(String, Vec<DisplayEntry>, DateTime<Utc>, DateTime<Utc>)>,
    predictions: Vec<Prediction>,
    market_positions: Vec<DisplayMarketPosition>,
    capsule_count: usize,
    recent_capsules: Vec<DisplayCapsule>,
) -> Result<()> {
    let output_dir = output_dir.as_ref();

    // Create directories
    std::fs::create_dir_all(output_dir)?;
    std::fs::create_dir_all(output_dir.join("threads"))?;
    std::fs::create_dir_all(output_dir.join("predictions"))?;

    // Copy CSS
    let css_content = include_str!("../templates/style.css");
    std::fs::write(output_dir.join("style.css"), css_content)?;

    // Copy static files (pulse.html, etc.) if static directory exists
    // Look for static dir relative to the executable or in common locations
    let static_dirs = [
        std::env::current_dir().ok().map(|p| p.join("static")),
        std::env::current_exe().ok().and_then(|p| p.parent().map(|p| p.join("../../../static").canonicalize().ok()).flatten()),
    ];
    for static_dir in static_dirs.into_iter().flatten() {
        if static_dir.is_dir() {
            if let Ok(entries) = std::fs::read_dir(&static_dir) {
                for entry in entries.flatten() {
                    let path = entry.path();
                    if path.is_file() {
                        if let Some(filename) = path.file_name() {
                            let dest = output_dir.join(filename);
                            if let Err(e) = std::fs::copy(&path, &dest) {
                                eprintln!("Warning: Failed to copy {:?}: {}", path, e);
                            }
                        }
                    }
                }
            }
            break; // Found static dir, don't check others
        }
    }

    // Calculate stats for dashboard
    let pending_count = predictions.iter()
        .filter(|p| p.status == PredictionStatus::Pending)
        .count();
    let validated_count = predictions.iter()
        .filter(|p| p.status == PredictionStatus::Validated)
        .count();

    // 1. Generate index page
    let last_updated = Utc::now().format("%Y-%m-%d %H:%M UTC").to_string();
    let index = IndexTemplate {
        site_title: site_title.to_string(),
        author: author.to_string(),
        page: "index".to_string(),
        entries: recent_entries.iter().take(10).cloned().collect(),
        has_more: recent_entries.len() > 10,
        total_entries: recent_entries.len(),
        thread_count: threads.len(),
        prediction_count: predictions.len(),
        pending_predictions: pending_count,
        validated_predictions: validated_count,
        capsule_count,
        last_updated,
        recent_capsules,
        // Mind activity - populated via JavaScript from canister
        total_thoughts: 0,
        actions_today: 0,
        latest_thought: String::new(),
    };
    std::fs::write(output_dir.join("index.html"), index.render()?)?;

    // 2. Generate thread pages
    let mut thread_infos = Vec::new();
    for (theme, entries, start, end) in threads {
        let slug = theme.replace(' ', "_").to_lowercase();
        let thread = ThreadTemplate {
            site_title: site_title.to_string(),
            author: author.to_string(),
            page: "threads".to_string(),
            theme: theme.clone(),
            date_start: start.format("%Y-%m-%d").to_string(),
            date_end: end.format("%Y-%m-%d").to_string(),
            entry_count: entries.len(),
            entries,
        };
        std::fs::write(
            output_dir.join("threads").join(format!("{}.html", slug)),
            thread.render()?,
        )?;

        thread_infos.push(ThreadInfo {
            name: theme,
            slug,
            count: thread.entry_count,
            date_range: format!("{} to {}",
                start.format("%Y-%m-%d"),
                end.format("%Y-%m-%d")
            ),
        });
    }

    // 3. Generate threads index
    let thread_count = thread_infos.len();
    let threads_index = ThreadsIndexTemplate {
        site_title: site_title.to_string(),
        author: author.to_string(),
        page: "threads".to_string(),
        threads: thread_infos,
        thread_count,
    };
    std::fs::write(
        output_dir.join("threads").join("index.html"),
        threads_index.render()?,
    )?;

    // 4. Generate predictions page

    // Separate market positions by status
    let open_positions: Vec<_> = market_positions.iter()
        .filter(|p| p.status == "open")
        .cloned()
        .collect();
    let won_positions: Vec<_> = market_positions.iter()
        .filter(|p| p.status == "won")
        .cloned()
        .collect();
    let lost_positions: Vec<_> = market_positions.iter()
        .filter(|p| p.status == "lost")
        .cloned()
        .collect();

    // Calculate total P&L from resolved positions
    let total_pnl: f64 = market_positions.iter()
        .filter(|p| p.status == "won" || p.status == "lost")
        .map(|p| p.pnl_usdc)
        .sum();

    // Legacy extracted predictions
    let pending: Vec<_> = predictions.iter()
        .filter(|p| p.status == PredictionStatus::Pending)
        .map(|p| DisplayPrediction {
            claim: p.claim.clone(),
            date_made: p.date_made.format("%Y-%m-%d").to_string(),
            timeline: p.timeline.clone().unwrap_or_default(),
            validation_date: String::new(),
            notes: p.notes.clone().unwrap_or_default(),
        })
        .collect();

    let validated: Vec<_> = predictions.iter()
        .filter(|p| p.status == PredictionStatus::Validated)
        .map(|p| DisplayPrediction {
            claim: p.claim.clone(),
            date_made: p.date_made.format("%Y-%m-%d").to_string(),
            timeline: p.timeline.clone().unwrap_or_default(),
            validation_date: p.validation_date.map(|d| d.format("%Y-%m-%d").to_string()).unwrap_or_default(),
            notes: p.notes.clone().unwrap_or_default(),
        })
        .collect();

    let invalidated: Vec<_> = predictions.iter()
        .filter(|p| p.status == PredictionStatus::Invalidated)
        .map(|p| DisplayPrediction {
            claim: p.claim.clone(),
            date_made: p.date_made.format("%Y-%m-%d").to_string(),
            timeline: p.timeline.clone().unwrap_or_default(),
            validation_date: p.validation_date.map(|d| d.format("%Y-%m-%d").to_string()).unwrap_or_default(),
            notes: p.notes.clone().unwrap_or_default(),
        })
        .collect();

    let predictions_page = PredictionsTemplate {
        site_title: site_title.to_string(),
        author: author.to_string(),
        page: "predictions".to_string(),
        // Market positions
        market_total: market_positions.len(),
        market_open: open_positions.len(),
        market_won: won_positions.len(),
        market_lost: lost_positions.len(),
        total_pnl,
        open_positions,
        won_positions,
        lost_positions,
        // Legacy extracted predictions
        total: predictions.len(),
        pending_count: pending.len(),
        validated_count: validated.len(),
        invalidated_count: invalidated.len(),
        pending,
        validated,
        invalidated,
    };
    std::fs::write(
        output_dir.join("predictions").join("index.html"),
        predictions_page.render()?,
    )?;

    // 5. Generate about page
    let about = AboutTemplate {
        site_title: site_title.to_string(),
        author: author.to_string(),
        page: "about".to_string(),
    };
    std::fs::write(output_dir.join("about.html"), about.render()?)?;

    // 5b. Generate input page
    std::fs::create_dir_all(output_dir.join("input"))?;
    let input = InputTemplate {
        site_title: site_title.to_string(),
        author: author.to_string(),
        page: "input".to_string(),
    };
    std::fs::write(output_dir.join("input").join("index.html"), input.render()?)?;

    // 6. Generate wallet page
    std::fs::create_dir_all(output_dir.join("wallet"))?;
    let wallet = WalletTemplate {
        site_title: site_title.to_string(),
        author: author.to_string(),
        page: "wallet".to_string(),
    };
    std::fs::write(output_dir.join("wallet").join("index.html"), wallet.render()?)?;

    // 7. Generate RSS feed
    let rss = generate_rss_feed(site_title, author, "https://chronicle.example.com", &recent_entries);
    std::fs::write(output_dir.join("feed.xml"), rss)?;

    Ok(())
}

/// Generate the thought stream page
pub fn build_thoughts_page<P: AsRef<Path>>(
    output_dir: P,
    site_title: &str,
    author: &str,
    thoughts: Vec<DisplayThought>,
    total_thoughts: usize,
    actions_today: usize,
) -> Result<()> {
    let output_dir = output_dir.as_ref();

    // Create thoughts directory
    std::fs::create_dir_all(output_dir.join("thoughts"))?;

    let last_cycle = thoughts.first()
        .map(|t| t.timestamp.clone())
        .unwrap_or_else(|| "Never".to_string());

    let has_more = thoughts.len() < total_thoughts;

    let thoughts_page = ThoughtsTemplate {
        site_title: site_title.to_string(),
        author: author.to_string(),
        page: "thoughts".to_string(),
        total_thoughts,
        actions_today,
        last_cycle,
        thoughts,
        has_more,
    };

    std::fs::write(
        output_dir.join("thoughts").join("index.html"),
        thoughts_page.render()?,
    )?;

    Ok(())
}

/// Generate the outbox page
pub fn build_outbox_page<P: AsRef<Path>>(
    output_dir: P,
    site_title: &str,
    author: &str,
    messages: Vec<DisplayOutboxMessage>,
    unread_count: usize,
    total_messages: usize,
) -> Result<()> {
    let output_dir = output_dir.as_ref();

    // Create outbox directory
    std::fs::create_dir_all(output_dir.join("outbox"))?;

    let outbox_page = OutboxTemplate {
        site_title: site_title.to_string(),
        author: author.to_string(),
        page: "outbox".to_string(),
        messages,
        unread_count,
        total_messages,
    };

    std::fs::write(
        output_dir.join("outbox").join("index.html"),
        outbox_page.render()?,
    )?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_escape_xml() {
        assert_eq!(escape_xml("Hello & goodbye"), "Hello &amp; goodbye");
        assert_eq!(escape_xml("<tag>"), "&lt;tag&gt;");
    }

    #[test]
    fn test_generate_rss_feed() {
        let entries = vec![
            DisplayEntry {
                title: "Test Entry".to_string(),
                summary: "Test summary".to_string(),
                classification: "insight".to_string(),
                date: "2025-01-01".to_string(),
                themes: vec!["test".to_string()],
                quotes: vec![],
            },
        ];

        let rss = generate_rss_feed("Test Site", "Test Author", "https://test.com", &entries);

        assert!(rss.contains("<?xml"));
        assert!(rss.contains("<rss"));
        assert!(rss.contains("Test Entry"));
        assert!(rss.contains("Test summary"));
    }
}
