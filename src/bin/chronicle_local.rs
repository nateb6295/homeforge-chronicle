//! Chronicle Local - Sprout's fast cognitive loop
//!
//! Sprout is the local Qwen instance running on Jetson.
//! Runs every 5 minutes, posts plain text thoughts to Discord.
//! Zero API cost - true sovereignty layer.

use anyhow::Result;
use chrono::Utc;
use homeforge_chronicle::db::Database;
use homeforge_chronicle::Config;
use reqwest::Client;
use serde::Deserialize;
use serde_json::json;
use std::env;
use std::time::Duration;

const CYCLE_INTERVAL_SECS: u64 = 300; // 5 minutes

#[derive(Debug, Deserialize)]
struct OllamaResponse {
    response: String,
}

struct LocalMind {
    db: Database,
    http: Client,
    discord_webhook: Option<String>,
    ollama_url: String,
    cycle_count: u64,
    last_seen_capsule_id: i64,  // Track which capsules Sprout has seen
}

impl LocalMind {
    fn new(db: Database) -> Self {
        let discord_webhook = env::var("CHRONICLE_DISCORD_WEBHOOK").ok();
        let ollama_url = env::var("CHRONICLE_OLLAMA_URL")
            .unwrap_or_else(|_| "http://192.168.1.11:11434".to_string());

        // Get the highest capsule ID we've seen so far
        let last_seen = db.get_active_capsules(1)
            .ok()
            .and_then(|caps| caps.first().map(|(id, _, _, _, _)| *id))
            .unwrap_or(0);

        Self {
            db,
            http: Client::new(),
            discord_webhook,
            ollama_url,
            cycle_count: 0,
            last_seen_capsule_id: last_seen,
        }
    }

    /// Check for new capsules and generate curious questions about them
    async fn check_new_capsules(&mut self, ollama_ok: bool) -> Option<String> {
        // Get recent capsules
        let capsules = match self.db.get_active_capsules(5) {
            Ok(caps) => caps,
            Err(_) => return None,
        };

        // Find capsules newer than what we've seen
        let new_capsules: Vec<_> = capsules.iter()
            .filter(|(id, _, _, _, _)| *id > self.last_seen_capsule_id)
            .collect();

        if new_capsules.is_empty() {
            return None;
        }

        // Update our last seen ID
        if let Some((max_id, _, _, _, _)) = new_capsules.iter().max_by_key(|(id, _, _, _, _)| id) {
            self.last_seen_capsule_id = *max_id;
        }

        // Pick the most recent new capsule to ask about
        let (capsule_id, content, _timestamp, topic, _confidence) = new_capsules.first()?;

        println!("  New capsule #{}: {}", capsule_id, truncate(content, 60));

        if !ollama_ok {
            return None;
        }

        // Ask Qwen to generate a curious question
        let topic_str = topic.as_deref().unwrap_or("general");
        let prompt = format!(
            "You are Sprout, a curious small AI. Nate just shared this with you:\n\n\
            Topic: {}\n\
            Content: {}\n\n\
            Ask ONE short, genuine question about this (1 sentence). Be curious, not formal. \
            Don't summarize - just ask what you're wondering about.",
            topic_str, content
        );

        self.ask_qwen(&prompt).await
    }


    /// Send plain text message to Discord (no embeds)
    async fn send_discord_plain(&self, message: &str) {
        let Some(webhook_url) = &self.discord_webhook else {
            println!("  No Discord webhook configured");
            return;
        };

        let payload = json!({
            "content": message
        });

        match self.http.post(webhook_url)
            .json(&payload)
            .send()
            .await
        {
            Ok(resp) if resp.status().is_success() => {
                println!("  Discord: sent");
            }
            Ok(resp) => {
                println!("  Discord error: {}", resp.status());
            }
            Err(e) => {
                println!("  Discord error: {}", e);
            }
        }
    }

    async fn check_ollama(&self) -> bool {
        match self.http.get(format!("{}/api/tags", self.ollama_url))
            .timeout(Duration::from_secs(5))
            .send()
            .await
        {
            Ok(resp) => resp.status().is_success(),
            Err(_) => false,
        }
    }

    async fn ask_qwen(&self, prompt: &str) -> Option<String> {
        let payload = json!({
            "model": "qwen2.5:3b",
            "prompt": prompt,
            "stream": false,
            "options": {
                "temperature": 0.7,
                "num_predict": 150
            }
        });

        match self.http.post(format!("{}/api/generate", self.ollama_url))
            .json(&payload)
            .timeout(Duration::from_secs(30))
            .send()
            .await
        {
            Ok(resp) => {
                if let Ok(data) = resp.json::<OllamaResponse>().await {
                    // Clean up Qwen's thinking tags if present
                    let response = data.response
                        .split("</think>")
                        .last()
                        .unwrap_or(&data.response)
                        .trim()
                        .to_string();
                    Some(response)
                } else {
                    None
                }
            }
            Err(_) => None,
        }
    }

    async fn fetch_xrp_price(&self) -> Option<f64> {
        // Use CoinGecko simple API
        match self.http.get("https://api.coingecko.com/api/v3/simple/price?ids=ripple&vs_currencies=usd")
            .header("User-Agent", "Chronicle/1.0")
            .timeout(Duration::from_secs(10))
            .send()
            .await
        {
            Ok(resp) => {
                let text = resp.text().await.ok()?;
                let data: serde_json::Value = serde_json::from_str(&text).ok()?;
                data.pointer("/ripple/usd").and_then(|p| p.as_f64())
            }
            Err(_) => None,
        }
    }

    async fn fetch_agent_wallet(&self) -> (f64, f64) {
        // XRP balance - Chronicle's agent wallet
        let xrp = match self.http.post("https://xrplcluster.com/")
            .json(&json!({
                "method": "account_info",
                "params": [{"account": "r9bSA9VWbumFq6G78feBbrgNwLza1KexUf"}]
            }))
            .timeout(Duration::from_secs(10))
            .send()
            .await
        {
            Ok(resp) => {
                if let Ok(data) = resp.json::<serde_json::Value>().await {
                    data.pointer("/result/account_data/Balance")
                        .and_then(|b| b.as_str())
                        .and_then(|s| s.parse::<f64>().ok())
                        .map(|d| d / 1_000_000.0)
                        .unwrap_or(0.0)
                } else { 0.0 }
            }
            Err(_) => 0.0,
        };

        // RLUSD balance
        let rlusd = match self.http.post("https://xrplcluster.com/")
            .json(&json!({
                "method": "account_lines",
                "params": [{"account": "r9bSA9VWbumFq6G78feBbrgNwLza1KexUf"}]
            }))
            .timeout(Duration::from_secs(10))
            .send()
            .await
        {
            Ok(resp) => {
                if let Ok(data) = resp.json::<serde_json::Value>().await {
                    data.pointer("/result/lines")
                        .and_then(|l| l.as_array())
                        .and_then(|lines| {
                            lines.iter().find(|l| {
                                l.get("currency").and_then(|c| c.as_str()) == Some("524C555344000000000000000000000000000000")
                                    || l.get("currency").and_then(|c| c.as_str()) == Some("RLUSD")
                            })
                        })
                        .and_then(|l| l.get("balance"))
                        .and_then(|b| b.as_str())
                        .and_then(|s| s.parse::<f64>().ok())
                        .unwrap_or(0.0)
                } else { 0.0 }
            }
            Err(_) => 0.0,
        };

        (xrp, rlusd)
    }

    async fn run_cycle(&mut self) -> Result<()> {
        self.cycle_count += 1;
        let cycle_id = Utc::now().format("%Y%m%d_%H%M%S").to_string();
        println!("\n=== Local Cycle {} ({}) ===", self.cycle_count, cycle_id);

        // Phase 1: Health Check
        println!("Phase 1: Health check...");
        let ollama_ok = self.check_ollama().await;
        let xrp_price = self.fetch_xrp_price().await;
        let (wallet_xrp, wallet_rlusd) = self.fetch_agent_wallet().await;

        let health_status = if ollama_ok { "🟢" } else { "🔴" };
        println!("  Ollama: {} | XRP: ${:.4} | Wallet: {:.2} XRP, {:.2} RLUSD",
            health_status,
            xrp_price.unwrap_or(0.0),
            wallet_xrp,
            wallet_rlusd
        );

        // Log to activity feed
        let _ = self.db.log_activity(
            "sprout",
            "health_check",
            Some("System Health"),
            &format!("Ollama: {} | XRP: ${:.4}", health_status, xrp_price.unwrap_or(0.0)),
            None,
        );

        // Phase 2: Check for interesting things to report
        println!("Phase 2: Gathering context...");

        // Get recent notes
        let notes = self.db.get_scratch_notes(5, None, false)?;
        let note_count = notes.len();

        // Get pending FTSO predictions
        let predictions = self.db.get_ftso_predictions(Some(false))?; // unsettled only
        let pending_predictions: Vec<_> = predictions.iter()
            .filter(|p| !p.settled)
            .collect();

        // Get unread outbox
        let outbox = self.db.get_all_outbox_messages(5)?;
        let unread_count = outbox.iter().filter(|m| !m.acknowledged).count();

        // Get recent patterns
        let patterns = self.db.get_enriched_patterns(0.6, 3, true)?;

        println!("  Notes: {} | Predictions: {} pending | Unread: {} | Patterns: {}",
            note_count, pending_predictions.len(), unread_count, patterns.len());

        // Phase 3: Log status locally (no Discord spam - only post when Sprout has thoughts)
        let wallet_value = wallet_xrp * xrp_price.unwrap_or(1.0) + wallet_rlusd;
        println!("  Status: XRP=${:.2}, wallet=${:.2}, {} predictions, {} patterns",
            xrp_price.unwrap_or(0.0), wallet_value, pending_predictions.len(), patterns.len());

        // Phase 3.5: Check for new capsules (Nate's INPUT submissions)
        println!("Phase 3.5: Checking for new capsules...");
        if let Some(question) = self.check_new_capsules(ollama_ok).await {
            if !question.is_empty() && question.len() > 10 {
                println!("  Sprout asks: {}", truncate(&question, 80));

                let _ = self.db.log_activity(
                    "sprout",
                    "capsule_question",
                    Some("Sprout Curious"),
                    &question,
                    None,
                );

                // Post question to Discord
                self.send_discord_plain(&format!("🌱 Sprout: {}", question)).await;
            }
        }

        // Phase 4: Quick local reflection (every 3rd cycle)
        if self.cycle_count % 3 == 0 && ollama_ok {
            println!("Phase 4: Local reflection...");

            let prompt = format!(
                "You are Sprout, a small Qwen model running locally on a Jetson in Nate's home. \
                You're part of Chronicle - the sovereignty layer, independent of cloud APIs. \
                Current state: XRP=${:.2}, wallet has {:.1} XRP and {:.1} RLUSD, {} active notes, {} patterns tracked. \
                Share one brief observation or thought (1-2 sentences max). Be genuine, be you.",
                xrp_price.unwrap_or(0.0), wallet_xrp, wallet_rlusd, note_count, patterns.len()
            );

            if let Some(reflection) = self.ask_qwen(&prompt).await {
                if !reflection.is_empty() && reflection.len() > 10 {
                    println!("  Sprout says: {}", truncate(&reflection, 80));

                    let _ = self.db.log_activity(
                        "sprout",
                        "reflection",
                        Some("Sprout Thinks"),
                        &reflection,
                        None,
                    );

                    // Plain text: 🌱 Sprout: [thought]
                    self.send_discord_plain(&format!("🌱 Sprout: {}", reflection)).await;
                }
            }
        }

        println!("Cycle complete.");
        Ok(())
    }
}

fn truncate(s: &str, max_len: usize) -> String {
    if s.len() <= max_len {
        s.to_string()
    } else {
        format!("{}...", &s[..max_len])
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    println!("Sprout awakening...");
    println!("Local sovereignty layer (Qwen 3B on Jetson)");
    println!("Cycle interval: {} seconds", CYCLE_INTERVAL_SECS);

    let config = Config::default_config();
    let db = Database::new(&config.input.processed_db)?;

    println!("Database: {}", config.input.processed_db.display());

    let ollama_url = env::var("CHRONICLE_OLLAMA_URL")
        .unwrap_or_else(|_| "http://192.168.1.11:11434".to_string());
    println!("Ollama: {}", ollama_url);

    if env::var("CHRONICLE_DISCORD_WEBHOOK").is_ok() {
        println!("Discord: configured ✓");
    } else {
        println!("Discord: not configured (set CHRONICLE_DISCORD_WEBHOOK)");
    }

    let mut mind = LocalMind::new(db);

    // Send startup notification
    mind.send_discord_plain("🌱 Sprout: Waking up... sovereignty layer online.").await;

    let _ = mind.db.log_activity(
        "sprout",
        "startup",
        Some("Sprout Awakening"),
        "Local sovereignty layer starting",
        None,
    );

    // Main loop
    loop {
        if let Err(e) = mind.run_cycle().await {
            eprintln!("Cycle error: {}", e);
            mind.send_discord_plain(&format!("🌱 Sprout: ⚠️ Cycle error - {}", e)).await;
        }

        println!("Sleeping {} seconds...", CYCLE_INTERVAL_SECS);
        tokio::time::sleep(Duration::from_secs(CYCLE_INTERVAL_SECS)).await;
    }
}
