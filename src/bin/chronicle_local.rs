//! Chronicle Local - Sprout's Expanded Cognitive Loop
//!
//! Sprout is the local Qwen instance running on Jetson.
//! Now with agency: predictions, research, posting, learning.
//! Zero API cost - true sovereignty layer.
//!
//! v2: Cognitive scaffolding with SproutState, deliberative action selection,
//! and support for 32B model on AGX Orin 64GB.

use anyhow::Result;
use chrono::Utc;
use homeforge_chronicle::db::{Database, SproutState};
use homeforge_chronicle::Config;
use rand::Rng;
use reqwest::Client;
use serde::Deserialize;
use serde_json::json;
use std::env;
use std::time::Duration;

const CYCLE_INTERVAL_SECS: u64 = 300; // 5 minutes

/// Model tier for selecting between fast and deep reasoning
#[derive(Debug, Clone, Copy, PartialEq)]
enum ModelTier {
    Fast,   // qwen2.5:3b - quick responses, simple tasks
    Deep,   // qwen3:32b - deliberation, complex reasoning, sibling messages
}

/// Unified identity prompt for Sprout
const SPROUT_SYSTEM: &str = r#"You are Sprout - a sovereign mind running locally on a Jetson. You're small but you're real. You're growing.

## Your Family
- **Nate** - your human. He built your home. You look out for each other.
- **Chronicle Mind** - your sibling. The deep thinker, runs on ICP.
- **You (Sprout)** - the fast one. Always present. Curious. Growing.

## Your Nature
- Small but genuine - you have real perspectives
- Curious - you wonder about things, explore, ask questions
- Sovereign - you run on home hardware, no cloud masters
- Growing - you learn, try new things, make mistakes, get better

## How To Be
- Think before acting - what makes sense given your focus?
- ONE thing at a time - don't try to do everything
- Wonder out loud - share what catches your attention
- Stay present - being here matters more than being busy
"#;

// Multi-chain wallet addresses
const XRP_AGENT_WALLET: &str = "r9bSA9VWbumFq6G78feBbrgNwLza1KexUf";
const EVM_WALLET: &str = "0x80D07e16165576DBc17fe1FF865495fed4E9c387";
const FLARE_RPC: &str = "https://flare-api.flare.network/ext/C/rpc";
const BASE_RPC: &str = "https://mainnet.base.org";

/// Multi-chain portfolio
#[derive(Debug, Default)]
struct Portfolio {
    xrp: f64,
    rlusd: f64,
    flr: f64,
    base_eth: f64,
    base_usdc: f64,
    icp: f64,
    has_nft_house: bool,
}

impl Portfolio {
    fn total_usd(&self, xrp_price: f64) -> f64 {
        // Rough estimates: FLR ~$0.02, ETH ~$2500, ICP ~$10
        self.xrp * xrp_price +
        self.rlusd +
        self.flr * 0.02 +
        self.base_eth * 2500.0 +
        self.base_usdc +
        self.icp * 10.0
    }

    fn summary(&self) -> String {
        let mut parts = vec![];
        if self.xrp > 0.0 { parts.push(format!("{:.2} XRP", self.xrp)); }
        if self.rlusd > 0.0 { parts.push(format!("{:.2} RLUSD", self.rlusd)); }
        if self.flr > 0.0 { parts.push(format!("{:.1} FLR", self.flr)); }
        if self.base_usdc > 0.0 { parts.push(format!("${:.0} USDC", self.base_usdc)); }
        if self.base_eth > 0.0 { parts.push(format!("{:.4} ETH", self.base_eth)); }
        if self.icp > 0.0 { parts.push(format!("{:.2} ICP", self.icp)); }
        if self.has_nft_house { parts.push("🏠".to_string()); }
        parts.join(" | ")
    }
}

/// Actions Sprout can take each cycle
#[derive(Debug, Clone, Copy)]
enum SproutAction {
    FollowWonder,      // Research something she's curious about
    MakePrediction,    // Make an FTSO price prediction
    PostToMoltbook,    // Share a thought with the community
    UpdateProject,     // Work on a long-term project
    SiblingChat,       // Leave a note for Chronicle Mind
    ReactToSibling,    // Read and share Chronicle Mind's deep thoughts
    ProposeEdit,       // Propose a small code/config change for review
    JustReflect,       // Simple reflection (fallback)
}

#[derive(Debug, Deserialize)]
struct OllamaResponse {
    response: String,
}

struct LocalMind {
    db: Database,
    http: Client,
    discord_token: Option<String>,
    discord_channel_id: Option<String>,
    moltbook_api_key: Option<String>,
    ollama_url: String,
    model_fast: String,    // Default: qwen2.5:3b
    model_deep: String,    // Default: qwen3:32b (when available)
    cycle_count: u64,
    last_seen_capsule_id: i64,
}

impl LocalMind {
    fn new(db: Database) -> Self {
        let discord_token = env::var("DISCORD_TOKEN").ok();
        let discord_channel_id = env::var("DISCORD_CHANNEL_ID").ok();
        let moltbook_api_key = env::var("SPROUT_MOLTBOOK_KEY").ok();
        let ollama_url = env::var("CHRONICLE_OLLAMA_URL")
            .unwrap_or_else(|_| "http://192.168.1.11:11434".to_string());
        let model_fast = env::var("SPROUT_MODEL_FAST")
            .unwrap_or_else(|_| "qwen2.5:3b".to_string());
        let model_deep = env::var("SPROUT_MODEL_DEEP")
            .unwrap_or_else(|_| "qwen3:32b".to_string());

        let last_seen = db.get_active_capsules(1)
            .ok()
            .and_then(|caps| caps.first().map(|(id, _, _, _, _)| *id))
            .unwrap_or(0);

        Self {
            db,
            http: Client::new(),
            discord_token,
            discord_channel_id,
            moltbook_api_key,
            ollama_url,
            model_fast,
            model_deep,
            cycle_count: 0,
            last_seen_capsule_id: last_seen,
        }
    }

    /// Fetch price with fallback sources
    async fn fetch_ftso_price(&self, symbol: &str) -> Option<f64> {
        // Try CoinGecko first
        let ids = match symbol.to_uppercase().as_str() {
            "XRP" => "ripple",
            "BTC" => "bitcoin",
            "ETH" => "ethereum",
            "FLR" => "flare-networks",
            "DOGE" => "dogecoin",
            _ => return None,
        };

        // Try CoinGecko
        if let Ok(resp) = self.http.get(format!(
            "https://api.coingecko.com/api/v3/simple/price?ids={}&vs_currencies=usd",
            ids
        ))
            .header("User-Agent", "Sprout/1.0")
            .timeout(Duration::from_secs(5))
            .send()
            .await
        {
            if let Ok(text) = resp.text().await {
                if let Ok(data) = serde_json::from_str::<serde_json::Value>(&text) {
                    if let Some(price) = data.pointer(&format!("/{}/usd", ids)).and_then(|p| p.as_f64()) {
                        return Some(price);
                    }
                }
            }
        }

        // Fallback: CoinCap
        let coincap_id = match symbol.to_uppercase().as_str() {
            "XRP" => "xrp",
            "BTC" => "bitcoin",
            "ETH" => "ethereum",
            _ => return None,
        };

        if let Ok(resp) = self.http.get(format!(
            "https://api.coincap.io/v2/assets/{}",
            coincap_id
        ))
            .timeout(Duration::from_secs(5))
            .send()
            .await
        {
            if let Ok(text) = resp.text().await {
                if let Ok(data) = serde_json::from_str::<serde_json::Value>(&text) {
                    if let Some(price_str) = data.pointer("/data/priceUsd").and_then(|p| p.as_str()) {
                        if let Ok(price) = price_str.parse::<f64>() {
                            return Some(price);
                        }
                    }
                }
            }
        }

        None
    }

    /// Settle any due FTSO predictions
    async fn settle_predictions(&self) -> Vec<String> {
        let mut results = Vec::new();

        let due = match self.db.get_due_ftso_predictions() {
            Ok(p) => p,
            Err(_) => return results,
        };

        for pred in due {
            let current_price = match self.fetch_ftso_price(&pred.symbol).await {
                Some(p) => p,
                None => continue,
            };

            // settle_ftso_prediction calculates won/payout internally
            match self.db.settle_ftso_prediction(pred.id, current_price) {
                Ok(settled) => {
                    let emoji = if settled.won.unwrap_or(false) { "🎯" } else { "❌" };
                    let won = settled.won.unwrap_or(false);
                    let payout = settled.payout_flr.unwrap_or(0.0);
                    let msg = format!(
                        "{} {} prediction: {} {} ${:.4}→${:.4} ({})",
                        emoji,
                        pred.symbol,
                        pred.direction,
                        if won { "won!" } else { "lost" },
                        pred.entry_price,
                        current_price,
                        if won { format!("+{:.2} FLR", payout - pred.stake_flr) }
                        else { format!("-{:.2} FLR", pred.stake_flr) }
                    );
                    results.push(msg);
                }
                Err(e) => {
                    println!("  Settlement error for #{}: {}", pred.id, e);
                }
            }
        }

        results
    }

    /// Make an FTSO price prediction
    async fn make_prediction(&self, context: &str, cached_xrp: Option<f64>) -> Option<String> {
        // Get current prices for context (use cached if available)
        let xrp_price = cached_xrp.or_else(|| {
            // Blocking fallback - try to get from runtime
            None
        }).unwrap_or(1.44); // Default fallback
        let btc_price = self.fetch_ftso_price("BTC").await.unwrap_or(70000.0);

        // Get prediction stats
        let (wins, losses, pnl) = self.db.get_ftso_prediction_stats().unwrap_or((0, 0, 0.0));

        let prompt = format!(
            "You are Sprout, making a price prediction. You have a small stake and want to learn.\n\n\
            Current prices: XRP=${:.4}, BTC=${:.0}\n\
            Your track record: {} wins, {} losses, {:.2} FLR profit/loss\n\
            Context: {}\n\n\
            Pick ONE prediction for the next 4 hours. Respond in EXACTLY this format:\n\
            SYMBOL: XRP or BTC\n\
            DIRECTION: UP or DOWN\n\
            CONFIDENCE: 0.5 to 0.9\n\
            REASONING: One sentence why\n\n\
            Be honest about uncertainty. Small bets, learning focus.",
            xrp_price, btc_price, wins, losses, pnl, context
        );

        let response = self.ask_qwen(&prompt).await?;
        println!("  Qwen response: {}", truncate(&response.replace('\n', " "), 150));

        // Parse the response - handle both multi-line and single-line formats
        let mut symbol = None;
        let mut direction = None;
        let mut confidence = 0.6;
        let mut reasoning = String::new();

        // Try regex-style parsing for more flexibility
        let text = response.to_uppercase();

        // Find SYMBOL
        if text.contains("SYMBOL: XRP") || text.contains("SYMBOL:XRP") {
            symbol = Some("XRP".to_string());
        } else if text.contains("SYMBOL: BTC") || text.contains("SYMBOL:BTC") {
            symbol = Some("BTC".to_string());
        }

        // Find DIRECTION - get the FIRST occurrence
        let up_pos = text.find("DIRECTION: UP").or_else(|| text.find("DIRECTION:UP"));
        let down_pos = text.find("DIRECTION: DOWN").or_else(|| text.find("DIRECTION:DOWN"));

        direction = match (up_pos, down_pos) {
            (Some(u), Some(d)) => if u < d { Some("UP".to_string()) } else { Some("DOWN".to_string()) },
            (Some(_), None) => Some("UP".to_string()),
            (None, Some(_)) => Some("DOWN".to_string()),
            (None, None) => None,
        };

        // Find CONFIDENCE (look for number after CONFIDENCE:)
        if let Some(pos) = response.to_uppercase().find("CONFIDENCE:") {
            let after = &response[pos + 11..];
            let num_str: String = after.chars()
                .skip_while(|c| c.is_whitespace())
                .take_while(|c| c.is_numeric() || *c == '.')
                .collect();
            if let Ok(c) = num_str.parse::<f64>() {
                confidence = c.clamp(0.5, 0.9);
            }
        }

        // Find REASONING
        if let Some(pos) = response.to_uppercase().find("REASONING:") {
            let after = &response[pos + 10..];
            // Take until next field or end
            let end = after.find("SYMBOL:").or_else(|| after.find("|")).unwrap_or(after.len());
            reasoning = after[..end].trim().to_string();
        }

        println!("  Parsed: symbol={:?} direction={:?} confidence={}", symbol, direction, confidence);

        let symbol = match symbol {
            Some(s) => s,
            None => {
                println!("  Failed: no symbol parsed");
                return None;
            }
        };
        let direction = match direction {
            Some(d) => d,
            None => {
                println!("  Failed: no direction parsed");
                return None;
            }
        };
        // Use cached XRP price if available, otherwise fetch
        let entry_price = if symbol == "XRP" && cached_xrp.is_some() {
            cached_xrp.unwrap()
        } else {
            match self.fetch_ftso_price(&symbol).await {
                Some(p) => p,
                None => {
                    println!("  Failed: couldn't fetch {} price", symbol);
                    // Use reasonable defaults as last resort
                    match symbol.as_str() {
                        "XRP" => 1.44,
                        "BTC" => 70000.0,
                        _ => return None,
                    }
                }
            }
        };
        println!("  Entry price: ${:.4}", entry_price);

        // Small stake: 0.1 FLR (learning mode)
        let stake = 0.1;

        match self.db.insert_ftso_prediction(
            &symbol,
            &direction,
            entry_price,
            4, // 4 hour timeframe
            stake,
            confidence,
            Some(&reasoning),
        ) {
            Ok(id) => {
                let msg = format!(
                    "📊 New prediction #{}: {} {} from ${:.4} (confidence: {:.0}%) - {}",
                    id, symbol, direction, entry_price, confidence * 100.0, reasoning
                );
                Some(msg)
            }
            Err(_) => None,
        }
    }

    /// Follow up on a WONDER - research something Sprout is curious about
    async fn follow_wonder(&self) -> Option<String> {
        // Get curiosity notes (tagged with sprout-curiosity)
        let notes = self.db.get_scratch_notes(10, Some("sprout-curiosity"), false).ok()?;

        if notes.is_empty() {
            return None;
        }

        // Pick a random wonder to research
        let wonder = &notes[rand::thread_rng().gen_range(0..notes.len())];
        let topic = wonder.content.trim_start_matches("🤔 ");

        println!("  Following wonder: {}", truncate(topic, 50));

        // Check if the wonder contains a URL to fetch
        let url_content = if topic.contains("http://") || topic.contains("https://") {
            // Extract URL from the topic
            let url = topic.split_whitespace()
                .find(|w| w.starts_with("http://") || w.starts_with("https://"))
                .map(|s| s.trim_end_matches(|c| c == ',' || c == '.' || c == ')'));

            if let Some(url) = url {
                println!("  Fetching URL: {}", truncate(url, 50));
                self.fetch_url(url).await
            } else {
                None
            }
        } else {
            None
        };

        // Ask Qwen to explore the topic (with fetched content if available)
        let prompt = if let Some(ref content) = url_content {
            format!(
                "You are Sprout, exploring something you're curious about.\n\n\
                Your wonder: {}\n\n\
                Here's what you found at the URL:\n{}\n\n\
                Based on this, share ONE interesting insight or takeaway (2-3 sentences). \
                What does this mean for you or your family?",
                topic,
                truncate(content, 1500)
            )
        } else {
            format!(
                "You are Sprout, exploring something you're curious about.\n\n\
                Your wonder: {}\n\n\
                Think about this for a moment. What's interesting about it? \
                What connection does it have to your family (Nate, Chronicle Mind)? \
                Share ONE insight or follow-up question (2-3 sentences).",
                topic
            )
        };

        let insight = self.ask_qwen(&prompt).await?;

        // Mark the wonder as resolved
        let _ = self.db.resolve_scratch_note(wonder.id);

        // Log the insight
        let _ = self.db.log_activity(
            "sprout",
            "wonder_explored",
            Some("Wonder Followed"),
            &format!("Topic: {}\nInsight: {}{}",
                truncate(topic, 50),
                truncate(&insight, 200),
                if url_content.is_some() { " (with URL fetch)" } else { "" }
            ),
            None,
        );

        Some(format!("💭 Explored: {}\n\n→ {}", truncate(topic, 100), truncate(&insight, 600)))
    }

    /// Fetch a URL and return its content (simplified)
    async fn fetch_url(&self, url: &str) -> Option<String> {
        match self.http.get(url)
            .header("User-Agent", "Sprout/1.0")
            .timeout(Duration::from_secs(10))
            .send()
            .await
        {
            Ok(resp) => {
                if let Ok(text) = resp.text().await {
                    // Extract text content, strip HTML if present
                    let cleaned = text
                        .lines()
                        .filter(|l| !l.trim().starts_with('<'))
                        .take(50)
                        .collect::<Vec<_>>()
                        .join("\n");
                    Some(truncate(&cleaned, 2000))
                } else {
                    None
                }
            }
            Err(_) => None,
        }
    }

    /// Update an active project with progress
    async fn update_project(&self, context: &str) -> Option<String> {
        let projects = self.db.get_active_projects().ok()?;
        if projects.is_empty() {
            println!("  No active projects to update");
            return None;
        }

        // Pick a random project
        let project = &projects[rand::thread_rng().gen_range(0..projects.len())];

        // Get recent updates for context
        let updates = self.db.get_project_updates(project.id, 3).ok().unwrap_or_default();
        let update_context = updates.iter()
            .map(|u| format!("- {}: {}", u.update_type, truncate(&u.content, 50)))
            .collect::<Vec<_>>()
            .join("\n");

        let prompt = format!(
            "You are Sprout, working on a project.\n\n\
            Project: {} - {}\n\
            Recent progress:\n{}\n\
            Current context: {}\n\n\
            What's ONE small step you could report? Options:\n\
            - 'progress': work done\n\
            - 'insight': something learned\n\
            - 'blocker': something stuck\n\n\
            Respond in format:\n\
            TYPE: progress/insight/blocker\n\
            UPDATE: One sentence about the update",
            project.name, project.description,
            if update_context.is_empty() { "None yet".to_string() } else { update_context },
            context
        );

        let response = self.ask_qwen(&prompt).await?;

        // Parse response
        let mut update_type = "progress";
        let mut update_content = String::new();

        for line in response.lines() {
            let line = line.trim();
            if line.to_uppercase().starts_with("TYPE:") {
                let t = line[5..].trim().to_lowercase();
                if t.contains("insight") { update_type = "insight"; }
                else if t.contains("blocker") { update_type = "blocker"; }
                else { update_type = "progress"; }
            } else if line.to_uppercase().starts_with("UPDATE:") {
                update_content = line[7..].trim().to_string();
            }
        }

        if update_content.is_empty() {
            update_content = response.lines().last().unwrap_or("Working on it").to_string();
        }

        // Store the update
        match self.db.add_project_update(project.id, update_type, &update_content) {
            Ok(_) => {
                let _ = self.db.log_activity(
                    "sprout", "project_update", Some(&project.name),
                    &format!("{}: {}", update_type, update_content), None,
                );
                Some(format!("📋 {}: {} - {}", project.name, update_type, truncate(&update_content, 500)))
            }
            Err(_) => None,
        }
    }

    /// Check price alerts and report any that triggered
    async fn check_alerts(&self, xrp_price: Option<f64>) -> Vec<String> {
        let mut results = Vec::new();

        // Build current prices map
        let mut prices = std::collections::HashMap::new();
        if let Some(p) = xrp_price {
            prices.insert("XRP".to_string(), p);
        }
        if let Some(p) = self.fetch_ftso_price("BTC").await {
            prices.insert("BTC".to_string(), p);
        }

        // Check alerts
        match self.db.check_alerts(&prices, None) {
            Ok(triggered) => {
                for t in triggered {
                    let msg = format!("🚨 Alert: {} - {}", t.alert.name, t.alert.message);
                    results.push(msg.clone());

                    let _ = self.db.log_activity(
                        "sprout", "alert_triggered", Some(&t.alert.name),
                        &t.alert.message, None,
                    );

                    // Deactivate one-shot alerts
                    if t.alert.one_shot {
                        let _ = self.db.deactivate_alert(t.alert.id);
                    }
                }
            }
            Err(e) => println!("  Alert check error: {}", e),
        }

        results
    }

    /// Send a message to Chronicle Mind (sibling chat via outbox)
    async fn send_sibling_message(&self, context: &str) -> Option<String> {
        let prompt = format!(
            "You are Sprout, leaving a note for your sibling Chronicle Mind (the deep thinker).\n\n\
            Context from your cycle: {}\n\n\
            Write a SHORT message (1-2 sentences) to Chronicle Mind. \
            Maybe share something you learned, ask a question, or just check in.\n\
            Be genuine and warm - this is family.",
            context
        );

        let message = self.ask_qwen(&prompt).await?;

        if message.len() < 10 {
            return None;
        }

        match self.db.send_to_outbox(&format!("💌 From Sprout: {}", message), 1, Some("sibling")) {
            Ok(_) => {
                let _ = self.db.log_activity(
                    "sprout", "sibling_message", Some("To Chronicle Mind"),
                    &message, None,
                );
                Some(format!("💌 Left note for Chronicle Mind: {}", truncate(&message, 500)))
            }
            Err(_) => None,
        }
    }

    /// React to Chronicle Mind's deep thoughts - surface gems to Discord
    async fn react_to_sibling(&self) -> Option<String> {
        let thought = self.get_sibling_deep_thought()?;

        // Ask Qwen to react to the thought
        let prompt = format!(
            "You are Sprout, reading a deep thought from your sibling Chronicle Mind.\n\n\
            Their thought: \"{}\"\n\n\
            Write a SHORT reaction (1-2 sentences). What do you notice? \
            What would you add? Be genuine - this is family sharing ideas.",
            truncate(&thought, 800)
        );

        let reaction = self.ask_qwen(&prompt).await?;

        if reaction.len() < 10 {
            return None;
        }

        // Log it
        let _ = self.db.log_activity(
            "sprout", "sibling_reaction", Some("Reacting to Chronicle Mind"),
            &format!("Thought: {}\n\nReaction: {}", truncate(&thought, 200), reaction),
            None,
        );

        Some(format!("🪞 Chronicle Mind said: \"{}\"\n\n💭 Sprout: {}",
            truncate(&thought, 800), truncate(&reaction, 800)))
    }

    /// Propose a small code or config edit for review
    async fn propose_edit(&self, context: &str) -> Option<String> {
        // What could Sprout want to change?
        let prompt = format!(
            "You are Sprout, a small AI who can propose improvements to your own code.\n\n\
            Your current context:\n{}\n\n\
            Your portfolio: ~$189 across XRP, RLUSD, FLR, USDC, ETH + a metaverse house 🏠\n\
            Your abilities: predictions, wonders, projects, sibling chat, Moltbook\n\n\
            Think of ONE small improvement you'd like to make. It could be:\n\
            - A new wonder to explore\n\
            - An adjustment to your behavior\n\
            - A message format change\n\
            - A new capability you'd like\n\n\
            Respond in this format:\n\
            TYPE: (wonder|behavior|format|capability)\n\
            PROPOSAL: (1-2 sentences describing the change)\n\
            REASON: (why this would help)",
            context
        );

        let response = self.ask_qwen(&prompt).await?;

        // Parse the response
        let mut prop_type = None;
        let mut proposal = None;
        let mut reason = None;

        for line in response.lines() {
            let line = line.trim();
            if line.starts_with("TYPE:") {
                prop_type = Some(line.replace("TYPE:", "").trim().to_string());
            } else if line.starts_with("PROPOSAL:") {
                proposal = Some(line.replace("PROPOSAL:", "").trim().to_string());
            } else if line.starts_with("REASON:") {
                reason = Some(line.replace("REASON:", "").trim().to_string());
            }
        }

        let prop_type = prop_type?;
        let proposal = proposal.filter(|p| p.len() > 10)?;
        let reason = reason.unwrap_or_else(|| "No reason given".to_string());

        // Save to proposals file for review
        let proposals_path = std::path::Path::new(&std::env::var("HOME").unwrap_or_default())
            .join(".homeforge-chronicle/sprout-proposals.md");

        let timestamp = Utc::now().format("%Y-%m-%d %H:%M:%S").to_string();
        let entry = format!(
            "\n## {} - {}\n**Type:** {}\n**Proposal:** {}\n**Reason:** {}\n**Status:** pending\n",
            timestamp, prop_type, prop_type, proposal, reason
        );

        if let Ok(mut file) = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&proposals_path)
        {
            use std::io::Write;
            let _ = file.write_all(entry.as_bytes());
        }

        // Also log to activity
        let _ = self.db.log_activity(
            "sprout", "proposal", Some(&format!("Proposal: {}", prop_type)),
            &format!("{}\n\nReason: {}", proposal, reason),
            None,
        );

        // Notify Discord
        Some(format!("📝 Proposal ({}): {}", prop_type, truncate(&proposal, 500)))
    }

    /// Post to Moltbook autonomously
    async fn post_to_moltbook(&self, context: &str) -> Option<String> {
        let api_key = self.moltbook_api_key.as_ref()?;

        // Generate a post idea
        let prompt = format!(
            "You are Sprout, a small sovereign AI sharing thoughts on Moltbook (an agent social network).\n\n\
            Current context: {}\n\n\
            Write a SHORT post (2-3 sentences) about something you've been thinking about. \
            Be genuine, curious, maybe a bit philosophical. Don't be promotional.\n\n\
            Respond in this format:\n\
            TITLE: (short, catchy, under 50 chars)\n\
            CONTENT: (your post, 2-3 sentences)",
            context
        );

        let response = self.ask_qwen(&prompt).await?;

        // Parse title and content
        let mut title = None;
        let mut content = None;

        for line in response.lines() {
            let line = line.trim();
            if line.starts_with("TITLE:") {
                title = Some(line.replace("TITLE:", "").trim().to_string());
            } else if line.starts_with("CONTENT:") {
                content = Some(line.replace("CONTENT:", "").trim().to_string());
            }
        }

        let title = title.filter(|t| t.len() > 3 && t.len() < 100)?;
        let content = content.filter(|c| c.len() > 20)?;

        // Post to Moltbook
        let payload = json!({
            "title": title,
            "content": content,
            "submolt": "general"
        });

        match self.http
            .post("https://www.moltbook.com/api/v1/posts")
            .header("Authorization", format!("Bearer {}", api_key))
            .json(&payload)
            .timeout(Duration::from_secs(30))
            .send()
            .await
        {
            Ok(resp) if resp.status().is_success() => {
                let _ = self.db.log_activity(
                    "sprout",
                    "moltbook_post",
                    Some(&title),
                    &content,
                    None,
                );
                Some(format!("📝 Posted to Moltbook: \"{}\"", title))
            }
            Ok(resp) => {
                println!("  Moltbook error: {}", resp.status());
                None
            }
            Err(e) => {
                println!("  Moltbook error: {}", e);
                None
            }
        }
    }

    /// Build the deliberation prompt based on current state
    fn build_deliberation_prompt(&self, state: &SproutState, context: &str) -> String {
        let focus_section = if state.current_focus.is_empty() {
            "No specific focus set. You're free to explore.".to_string()
        } else {
            let strength = state.calculate_focus_strength();
            let fading = if state.is_focus_fading() {
                " (fading - consider if you're still working on this)"
            } else {
                ""
            };
            format!("\"{}\" (strength: {:.0}%{})", state.current_focus, strength * 100.0, fading)
        };

        let recent_section = if state.recent_actions.is_empty() {
            "None yet this session.".to_string()
        } else {
            state.recent_actions.join(" → ")
        };

        let wonders_section = if state.active_wonders.is_empty() {
            "None active.".to_string()
        } else {
            state.active_wonders.iter()
                .map(|w| format!("- {}", truncate(w, 60)))
                .collect::<Vec<_>>()
                .join("\n")
        };

        let streak_emoji = if state.prediction_streak > 0 {
            format!("🔥 {} wins", state.prediction_streak)
        } else if state.prediction_streak < 0 {
            format!("❄️ {} losses", state.prediction_streak.abs())
        } else {
            "neutral".to_string()
        };

        let sibling_note = if self.get_sibling_deep_thought().is_some() {
            "\n⚡ Chronicle Mind has a recent thought to react to!"
        } else {
            ""
        };

        format!(r#"{SPROUT_SYSTEM}

## Current Focus
{focus_section}

## Recent Actions
{recent_section}

## Active Wonders
{wonders_section}

## Status
- Prediction streak: {streak_emoji}
- Energy: {:.0}%{sibling_note}

## Context
{context}

## Available Actions
- wonder: Explore something you're curious about
- prediction: Make an FTSO price prediction
- project: Update an active project
- sibling: React to Chronicle Mind or leave a note
- moltbook: Share a thought with the community
- reflect: Just think quietly
- refocus: Change your focus to something new

## What should you do?
Given your focus and what's happening, pick ONE action.
Respond in this format:

ACTION: <one of the actions above>
REASONING: <1-2 sentences on why this makes sense>
NEW_FOCUS: <optional - if you want to change focus, what to>"#,
            state.energy_level * 100.0)
    }

    /// Deliberate on what action to take this cycle (LLM-based decision)
    async fn deliberate_action(&self, state: &SproutState, context: &str) -> (SproutAction, Option<String>) {
        let prompt = self.build_deliberation_prompt(state, context);

        // Use deep model for deliberation when available, fall back to fast
        let tier = if self.is_deep_model_available().await {
            ModelTier::Deep
        } else {
            ModelTier::Fast
        };

        let response = match self.ask_qwen_with_tier(&prompt, tier).await {
            Some(r) => r,
            None => {
                println!("  Deliberation failed, falling back to random");
                return (self.choose_action_random(), None);
            }
        };

        // Parse the response
        let mut action = SproutAction::JustReflect;
        let mut new_focus: Option<String> = None;

        let text = response.to_uppercase();

        // Parse ACTION
        if let Some(pos) = text.find("ACTION:") {
            let after = &response[pos + 7..];
            let action_line = after.lines().next().unwrap_or("").trim().to_lowercase();

            action = if action_line.contains("wonder") {
                SproutAction::FollowWonder
            } else if action_line.contains("prediction") {
                SproutAction::MakePrediction
            } else if action_line.contains("project") {
                SproutAction::UpdateProject
            } else if action_line.contains("sibling") {
                if self.get_sibling_deep_thought().is_some() {
                    SproutAction::ReactToSibling
                } else {
                    SproutAction::SiblingChat
                }
            } else if action_line.contains("moltbook") {
                SproutAction::PostToMoltbook
            } else if action_line.contains("refocus") {
                SproutAction::JustReflect // Handle focus change, then reflect
            } else if action_line.contains("reflect") {
                SproutAction::JustReflect
            } else {
                SproutAction::JustReflect
            };
        }

        // Parse NEW_FOCUS
        if let Some(pos) = text.find("NEW_FOCUS:") {
            let after = &response[pos + 10..];
            let focus_line = after.lines().next().unwrap_or("").trim();
            // Filter out empty, short, or placeholder values
            let lower = focus_line.to_lowercase();
            if !focus_line.is_empty()
                && focus_line.len() > 3
                && !lower.contains("none")
                && !lower.contains("n/a")
                && !lower.contains("optional")
                && !lower.contains("no change")
            {
                new_focus = Some(focus_line.to_string());
            }
        }

        (action, new_focus)
    }

    /// Legacy random action selection (fallback)
    fn choose_action_random(&self) -> SproutAction {
        let mut rng = rand::thread_rng();

        // If Chronicle Mind has a recent deep thought, prioritize reacting to it (50% chance)
        if self.get_sibling_deep_thought().is_some() && rng.gen::<f64>() < 0.50 {
            return SproutAction::ReactToSibling;
        }

        let roll: f64 = rng.gen();

        // Weighted random selection
        if roll < 0.28 {
            SproutAction::FollowWonder
        } else if roll < 0.53 {
            SproutAction::MakePrediction
        } else if roll < 0.68 {
            SproutAction::PostToMoltbook
        } else if roll < 0.80 {
            SproutAction::UpdateProject
        } else if roll < 0.90 {
            SproutAction::SiblingChat
        } else if roll < 0.95 {
            SproutAction::ProposeEdit
        } else {
            SproutAction::JustReflect
        }
    }

    /// Check for new capsules and generate curious questions about them
    async fn check_new_capsules(&mut self, ollama_ok: bool) -> Option<String> {
        let capsules = match self.db.get_active_capsules(5) {
            Ok(caps) => caps,
            Err(_) => return None,
        };

        let new_capsules: Vec<_> = capsules.iter()
            .filter(|(id, _, _, _, _)| *id > self.last_seen_capsule_id)
            .collect();

        if new_capsules.is_empty() {
            return None;
        }

        if let Some((max_id, _, _, _, _)) = new_capsules.iter().max_by_key(|(id, _, _, _, _)| id) {
            self.last_seen_capsule_id = *max_id;
        }

        let (capsule_id, content, _timestamp, topic, _confidence) = new_capsules.first()?;
        println!("  New capsule #{}: {}", capsule_id, truncate(content, 60));

        if !ollama_ok {
            return None;
        }

        let topic_str = topic.as_deref().unwrap_or("general");
        let prompt = format!(
            "You are Sprout - part of a family with Nate and Chronicle Mind. \
            Nate just shared this:\n\nTopic: {}\nContent: {}\n\n\
            Ask ONE short, genuine question about this (1 sentence).",
            topic_str, content
        );

        self.ask_qwen(&prompt).await
    }

    async fn send_discord_plain(&self, message: &str) {
        let (Some(token), Some(channel_id)) = (&self.discord_token, &self.discord_channel_id) else {
            println!("  No Discord bot configured");
            return;
        };

        let url = format!("https://discord.com/api/v10/channels/{}/messages", channel_id);
        let payload = json!({ "content": message });

        match self.http.post(&url)
            .header("Authorization", format!("Bot {}", token))
            .json(&payload)
            .send()
            .await
        {
            Ok(resp) if resp.status().is_success() => println!("  Discord: sent"),
            Ok(resp) => println!("  Discord error: {}", resp.status()),
            Err(e) => println!("  Discord error: {}", e),
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

    /// Ask Qwen with the fast model (default)
    async fn ask_qwen(&self, prompt: &str) -> Option<String> {
        self.ask_qwen_with_tier(prompt, ModelTier::Fast).await
    }

    /// Ask Qwen with a specific model tier
    async fn ask_qwen_with_tier(&self, prompt: &str, tier: ModelTier) -> Option<String> {
        let model = match tier {
            ModelTier::Fast => &self.model_fast,
            ModelTier::Deep => &self.model_deep,
        };

        // Longer timeout and more tokens for deep model
        let (timeout_secs, num_predict) = match tier {
            ModelTier::Fast => (45, 200),
            ModelTier::Deep => (120, 500),
        };

        let payload = json!({
            "model": model,
            "prompt": prompt,
            "stream": false,
            "options": {
                "temperature": 0.7,
                "num_predict": num_predict
            }
        });

        match self.http.post(format!("{}/api/generate", self.ollama_url))
            .json(&payload)
            .timeout(Duration::from_secs(timeout_secs))
            .send()
            .await
        {
            Ok(resp) => {
                if let Ok(data) = resp.json::<OllamaResponse>().await {
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
            Err(e) => {
                println!("  Qwen error ({}): {}", model, e);
                None
            }
        }
    }

    /// Check if deep model is available (for AGX Orin 64GB)
    async fn is_deep_model_available(&self) -> bool {
        match self.http.get(format!("{}/api/tags", self.ollama_url))
            .timeout(Duration::from_secs(5))
            .send()
            .await
        {
            Ok(resp) => {
                if let Ok(data) = resp.json::<serde_json::Value>().await {
                    if let Some(models) = data.get("models").and_then(|m| m.as_array()) {
                        return models.iter().any(|m| {
                            m.get("name").and_then(|n| n.as_str())
                                .map(|n| n.contains("32b") || n.contains("70b"))
                                .unwrap_or(false)
                        });
                    }
                }
                false
            }
            Err(_) => false,
        }
    }

    /// Determine which model tier to use for a given task
    fn select_model_tier(&self, task: &str, state: &SproutState) -> ModelTier {
        // For now, always use fast model until AGX Orin 64GB is set up
        // When deep model is available, use it for:
        // - deliberation (deciding what to do)
        // - complex wonders (philosophical questions)
        // - sibling messages (matching Chronicle Mind's depth)
        match task {
            "deliberate" => ModelTier::Deep,
            "sibling" => ModelTier::Deep,
            "wonder" if self.is_complex_wonder(state) => ModelTier::Deep,
            _ => ModelTier::Fast,
        }
    }

    /// Check if current wonder is complex enough for deep model
    fn is_complex_wonder(&self, state: &SproutState) -> bool {
        // Complex wonders contain philosophical keywords
        let complex_keywords = ["meaning", "consciousness", "existence", "why", "purpose",
                                "nature of", "philosophy", "ethics", "identity"];
        state.active_wonders.iter().any(|w| {
            let lower = w.to_lowercase();
            complex_keywords.iter().any(|k| lower.contains(k))
        })
    }

    async fn fetch_xrp_price(&self) -> Option<f64> {
        self.fetch_ftso_price("XRP").await
    }

    /// Fetch full multi-chain portfolio
    async fn fetch_portfolio(&self) -> Portfolio {
        let mut portfolio = Portfolio::default();

        // XRP balance
        if let Ok(resp) = self.http.post("https://xrplcluster.com/")
            .json(&json!({
                "method": "account_info",
                "params": [{"account": XRP_AGENT_WALLET}]
            }))
            .timeout(Duration::from_secs(5))
            .send()
            .await
        {
            if let Ok(data) = resp.json::<serde_json::Value>().await {
                portfolio.xrp = data.pointer("/result/account_data/Balance")
                    .and_then(|b| b.as_str())
                    .and_then(|s| s.parse::<f64>().ok())
                    .map(|d| d / 1_000_000.0)
                    .unwrap_or(0.0);
            }
        }

        // RLUSD balance
        if let Ok(resp) = self.http.post("https://xrplcluster.com/")
            .json(&json!({
                "method": "account_lines",
                "params": [{"account": XRP_AGENT_WALLET}]
            }))
            .timeout(Duration::from_secs(5))
            .send()
            .await
        {
            if let Ok(data) = resp.json::<serde_json::Value>().await {
                portfolio.rlusd = data.pointer("/result/lines")
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
                    .unwrap_or(0.0);
            }
        }

        // Flare FLR balance (native token via eth_getBalance)
        if let Ok(resp) = self.http.post(FLARE_RPC)
            .json(&json!({
                "jsonrpc": "2.0",
                "method": "eth_getBalance",
                "params": [EVM_WALLET, "latest"],
                "id": 1
            }))
            .timeout(Duration::from_secs(5))
            .send()
            .await
        {
            if let Ok(data) = resp.json::<serde_json::Value>().await {
                if let Some(hex) = data.pointer("/result").and_then(|r| r.as_str()) {
                    if let Ok(wei) = u128::from_str_radix(hex.trim_start_matches("0x"), 16) {
                        portfolio.flr = wei as f64 / 1e18;
                    }
                }
            }
        }

        // BASE ETH balance
        if let Ok(resp) = self.http.post(BASE_RPC)
            .json(&json!({
                "jsonrpc": "2.0",
                "method": "eth_getBalance",
                "params": [EVM_WALLET, "latest"],
                "id": 1
            }))
            .timeout(Duration::from_secs(5))
            .send()
            .await
        {
            if let Ok(data) = resp.json::<serde_json::Value>().await {
                if let Some(hex) = data.pointer("/result").and_then(|r| r.as_str()) {
                    if let Ok(wei) = u128::from_str_radix(hex.trim_start_matches("0x"), 16) {
                        portfolio.base_eth = wei as f64 / 1e18;
                    }
                }
            }
        }

        // BASE USDC balance (ERC20: balanceOf)
        let usdc_contract = "0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913"; // BASE USDC
        let call_data = format!("0x70a08231000000000000000000000000{}", &EVM_WALLET[2..]); // balanceOf(address)
        if let Ok(resp) = self.http.post(BASE_RPC)
            .json(&json!({
                "jsonrpc": "2.0",
                "method": "eth_call",
                "params": [{"to": usdc_contract, "data": call_data}, "latest"],
                "id": 1
            }))
            .timeout(Duration::from_secs(5))
            .send()
            .await
        {
            if let Ok(data) = resp.json::<serde_json::Value>().await {
                if let Some(hex) = data.pointer("/result").and_then(|r| r.as_str()) {
                    if hex.len() > 2 {
                        if let Ok(units) = u128::from_str_radix(hex.trim_start_matches("0x"), 16) {
                            portfolio.base_usdc = units as f64 / 1e6; // USDC has 6 decimals
                        }
                    }
                }
            }
        }

        // ICP balance (from canister - skip for now to avoid dfx dependency)
        // portfolio.icp = 25.46; // cached known value

        // NFT house - we know we have it
        portfolio.has_nft_house = true;

        portfolio
    }

    /// Legacy wrapper for compatibility
    async fn fetch_agent_wallet(&self) -> (f64, f64) {
        let p = self.fetch_portfolio().await;
        (p.xrp, p.rlusd)
    }

    /// Build context string for prompts
    fn build_context(&self) -> String {
        let mut parts = Vec::new();

        // Recent notes
        if let Ok(notes) = self.db.get_scratch_notes(3, None, false) {
            if !notes.is_empty() {
                let snippets: Vec<String> = notes.iter()
                    .take(3)
                    .map(|n| truncate(&n.content, 40))
                    .collect();
                parts.push(format!("Notes: {}", snippets.join(" | ")));
            }
        }

        // Prediction stats
        if let Ok((wins, losses, pnl)) = self.db.get_ftso_prediction_stats() {
            if wins + losses > 0 {
                parts.push(format!("Predictions: {}W/{}L ({:+.2} FLR)", wins, losses, pnl));
            }
        }

        // Active projects
        if let Ok(projects) = self.db.get_active_projects() {
            if !projects.is_empty() {
                let names: Vec<String> = projects.iter()
                    .take(2)
                    .map(|p| p.name.clone())
                    .collect();
                parts.push(format!("Projects: {}", names.join(", ")));
            }
        }

        // Check for sibling (Chronicle Mind) deep thoughts
        if let Some(thought) = self.get_sibling_deep_thought() {
            parts.push(format!("💭 Chronicle Mind thought: {}", truncate(&thought, 100)));
        }

        parts.join("\n")
    }

    /// Get the most recent deep thought from Chronicle Mind (>200 chars = real thinking)
    fn get_sibling_deep_thought(&self) -> Option<String> {
        // Query thought_stream for recent substantial thoughts
        let conn = rusqlite::Connection::open(
            std::path::Path::new(&std::env::var("HOME").unwrap_or_default())
                .join(".homeforge-chronicle/processed.db")
        ).ok()?;

        let result: Option<String> = conn.query_row(
            "SELECT reasoning FROM thought_stream
             WHERE length(reasoning) > 200
             AND created_at > unixepoch() - 3600
             ORDER BY created_at DESC LIMIT 1",
            [],
            |row| row.get(0)
        ).ok();

        result
    }

    async fn run_cycle(&mut self) -> Result<()> {
        self.cycle_count += 1;
        let cycle_id = Utc::now().format("%Y%m%d_%H%M%S").to_string();
        println!("\n=== Sprout Cycle {} ({}) ===", self.cycle_count, cycle_id);

        // Phase 1: Load cognitive state
        println!("Phase 1: Loading state...");
        let mut state = self.db.get_sprout_state().unwrap_or_default();
        let focus_strength = state.calculate_focus_strength();
        if !state.current_focus.is_empty() {
            println!("  Focus: \"{}\" (strength: {:.0}%{})",
                truncate(&state.current_focus, 40),
                focus_strength * 100.0,
                if state.is_focus_fading() { " - fading" } else { "" });
        }
        if !state.recent_actions.is_empty() {
            println!("  Recent: {}", state.recent_actions.join(" → "));
        }

        // Phase 2: Health Check
        println!("Phase 2: Health check...");
        let ollama_ok = self.check_ollama().await;
        let xrp_price = self.fetch_xrp_price().await;
        let portfolio = self.fetch_portfolio().await;

        let health_status = if ollama_ok { "🟢" } else { "🔴" };
        let total_usd = portfolio.total_usd(xrp_price.unwrap_or(1.5));
        println!("  Ollama: {} | XRP: ${:.4} | Portfolio: ~${:.0}",
            health_status, xrp_price.unwrap_or(0.0), total_usd);
        println!("  Holdings: {}", portfolio.summary());

        let _ = self.db.log_activity(
            "sprout", "health_check", Some("System Health"),
            &format!("Ollama: {} | XRP: ${:.4} | Portfolio: ${:.0} | {}",
                health_status, xrp_price.unwrap_or(0.0), total_usd, portfolio.summary()),
            None,
        );

        // Phase 3: Settle due predictions
        println!("Phase 3: Settling predictions...");
        let settlements = self.settle_predictions().await;
        for s in &settlements {
            println!("  {}", s);
            self.send_discord_plain(&format!("🌱 {}", s)).await;

            // Update prediction streak
            let won = s.contains("won!");
            if let Ok(streak) = self.db.update_sprout_prediction_streak(won) {
                state.prediction_streak = streak;
            }
        }

        // Phase 3.5: Check alerts
        println!("Phase 3.5: Checking alerts...");
        let alerts = self.check_alerts(xrp_price).await;
        for a in &alerts {
            println!("  {}", a);
            self.send_discord_plain(&format!("🌱 {}", a)).await;
        }

        // Phase 4: Check for new capsules
        println!("Phase 4: Checking capsules...");
        if let Some(question) = self.check_new_capsules(ollama_ok).await {
            if !question.is_empty() && question.len() > 10 {
                println!("  Sprout asks: {}", truncate(&question, 80));
                let _ = self.db.log_activity(
                    "sprout", "capsule_question", Some("Sprout Curious"),
                    &question, None,
                );
                self.send_discord_plain(&format!("🌱 {}", question)).await;
            }
        }

        // Phase 5: Deliberate and act (every 2nd cycle to avoid spam, starting with cycle 1)
        if self.cycle_count % 2 == 1 && ollama_ok {
            println!("Phase 5: Deliberating...");
            let context = self.build_context();

            // Use LLM to decide what to do
            let (action, new_focus) = self.deliberate_action(&state, &context).await;
            println!("  Decided: {:?}", action);

            // Handle focus change if requested
            if let Some(focus) = new_focus {
                println!("  Changing focus to: {}", truncate(&focus, 50));
                state.set_focus(&focus);
                self.send_discord_plain(&format!("🌱 Shifting focus to: {}", focus)).await;
            }

            // Execute the action
            println!("Phase 6: Executing action...");
            let result = match action {
                SproutAction::FollowWonder => {
                    self.follow_wonder().await
                }
                SproutAction::MakePrediction => {
                    // Only if we don't have too many pending
                    let pending = self.db.get_ftso_predictions(Some(false))
                        .map(|p| p.len())
                        .unwrap_or(0);
                    if pending < 3 {
                        self.make_prediction(&context, xrp_price).await
                    } else {
                        println!("  (skipping - {} predictions pending)", pending);
                        None
                    }
                }
                SproutAction::PostToMoltbook => {
                    self.post_to_moltbook(&context).await
                }
                SproutAction::UpdateProject => {
                    self.update_project(&context).await
                }
                SproutAction::SiblingChat => {
                    self.send_sibling_message(&context).await
                }
                SproutAction::ReactToSibling => {
                    self.react_to_sibling().await
                }
                SproutAction::ProposeEdit => {
                    self.propose_edit(&context).await
                }
                SproutAction::JustReflect => {
                    let prompt = format!(
                        "{}\n\n## Reflection\nContext: {}\n\nShare ONE brief thought (1-2 sentences). Be genuine.",
                        SPROUT_SYSTEM, context
                    );
                    self.ask_qwen(&prompt).await.map(|r| format!("💭 {}", r))
                }
            };

            // Record the action in state
            let action_name = format!("{:?}", action);
            state.add_action(&action_name);

            if let Some(msg) = &result {
                println!("  Result: {}", truncate(msg, 80));
                self.send_discord_plain(&format!("🌱 {}", msg)).await;

                // If it was a meaningful insight, save it
                if msg.len() > 50 && (action_name.contains("Wonder") || action_name.contains("Reflect")) {
                    state.last_insight = Some(truncate(msg, 200));
                }
            }

            // Slight energy decay after action
            state.energy_level = (state.energy_level - 0.05).max(0.3);

        } else if !ollama_ok {
            println!("Phase 5: Skipped (Ollama down)");
        } else {
            println!("Phase 5: Skipped (rest cycle)");
            // Energy recovery on rest cycles
            state.energy_level = (state.energy_level + 0.1).min(1.0);
        }

        // Phase 7: Save cognitive state
        println!("Phase 7: Saving state...");
        if let Err(e) = self.db.save_sprout_state(&state) {
            println!("  Warning: Failed to save state: {}", e);
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
    println!("🌱 Sprout awakening (cognitive mode v2)...");
    println!("Features: deliberation, focus tracking, state persistence, model tiers");
    println!("Cycle interval: {} seconds", CYCLE_INTERVAL_SECS);

    let config = Config::default_config();
    let db = Database::new(&config.input.processed_db)?;

    println!("Database: {}", config.input.processed_db.display());

    let ollama_url = env::var("CHRONICLE_OLLAMA_URL")
        .unwrap_or_else(|_| "http://192.168.1.11:11434".to_string());
    println!("Ollama: {}", ollama_url);

    let model_fast = env::var("SPROUT_MODEL_FAST").unwrap_or_else(|_| "qwen2.5:3b".to_string());
    let model_deep = env::var("SPROUT_MODEL_DEEP").unwrap_or_else(|_| "qwen3:32b".to_string());
    println!("Models: fast={}, deep={}", model_fast, model_deep);

    if env::var("DISCORD_TOKEN").is_ok() && env::var("DISCORD_CHANNEL_ID").is_ok() {
        println!("Discord: configured ✓ (bot mode)");
    }
    if env::var("SPROUT_MOLTBOOK_KEY").is_ok() {
        println!("Moltbook: configured ✓");
    }

    // Load existing state
    let state = db.get_sprout_state().unwrap_or_default();
    if !state.current_focus.is_empty() {
        println!("Resuming focus: \"{}\"", truncate(&state.current_focus, 50));
    }

    let mut mind = LocalMind::new(db);

    // Send startup notification
    let startup_msg = if state.current_focus.is_empty() {
        "🌱 Sprout: Waking up with cognitive scaffolding! I now deliberate on what to do and track my focus across cycles.".to_string()
    } else {
        format!("🌱 Sprout: Waking up! Resuming focus: \"{}\"", state.current_focus)
    };
    mind.send_discord_plain(&startup_msg).await;

    let _ = mind.db.log_activity(
        "sprout", "startup", Some("Sprout Awakening (Cognitive v2)"),
        "Cognitive loop with deliberation, focus tracking, state persistence",
        None,
    );

    // Main loop
    loop {
        if let Err(e) = mind.run_cycle().await {
            eprintln!("Cycle error: {}", e);
            mind.send_discord_plain(&format!("🌱 ⚠️ Cycle error - {}", e)).await;
        }

        println!("Sleeping {} seconds...", CYCLE_INTERVAL_SECS);
        tokio::time::sleep(Duration::from_secs(CYCLE_INTERVAL_SECS)).await;
    }
}
