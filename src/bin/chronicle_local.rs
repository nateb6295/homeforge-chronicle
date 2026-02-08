//! Chronicle Local - Sprout's Expanded Cognitive Loop
//!
//! Sprout is the local Qwen instance running on Jetson.
//! Now with agency: predictions, research, posting, learning.
//! Zero API cost - true sovereignty layer.

use anyhow::Result;
use chrono::Utc;
use homeforge_chronicle::db::Database;
use homeforge_chronicle::Config;
use rand::Rng;
use reqwest::Client;
use serde::Deserialize;
use serde_json::json;
use std::env;
use std::time::Duration;

const CYCLE_INTERVAL_SECS: u64 = 300; // 5 minutes

/// Actions Sprout can take each cycle
#[derive(Debug, Clone, Copy)]
enum SproutAction {
    FollowWonder,      // Research something she's curious about
    MakePrediction,    // Make an FTSO price prediction
    PostToMoltbook,    // Share a thought with the community
    UpdateProject,     // Work on a long-term project
    SiblingChat,       // Leave a note for Chronicle Mind
    ReactToSibling,    // Read and share Chronicle Mind's deep thoughts
    JustReflect,       // Simple reflection (fallback)
}

#[derive(Debug, Deserialize)]
struct OllamaResponse {
    response: String,
}

struct LocalMind {
    db: Database,
    http: Client,
    discord_webhook: Option<String>,
    moltbook_api_key: Option<String>,
    ollama_url: String,
    cycle_count: u64,
    last_seen_capsule_id: i64,
}

impl LocalMind {
    fn new(db: Database) -> Self {
        let discord_webhook = env::var("CHRONICLE_DISCORD_WEBHOOK").ok();
        let moltbook_api_key = env::var("SPROUT_MOLTBOOK_KEY").ok();
        let ollama_url = env::var("CHRONICLE_OLLAMA_URL")
            .unwrap_or_else(|_| "http://192.168.1.11:11434".to_string());

        let last_seen = db.get_active_capsules(1)
            .ok()
            .and_then(|caps| caps.first().map(|(id, _, _, _, _)| *id))
            .unwrap_or(0);

        Self {
            db,
            http: Client::new(),
            discord_webhook,
            moltbook_api_key,
            ollama_url,
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

        Some(format!("💭 Explored: {}\n→ {}", truncate(topic, 40), insight))
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
                Some(format!("📋 {}: {} - {}", project.name, update_type, truncate(&update_content, 60)))
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
                Some(format!("💌 Left note for Chronicle Mind: {}", truncate(&message, 60)))
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

        Some(format!("🪞 Chronicle Mind said: \"{}\"\n💭 Sprout: {}",
            truncate(&thought, 100), truncate(&reaction, 150)))
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

    /// Choose what action to take this cycle
    fn choose_action(&self) -> SproutAction {
        let mut rng = rand::thread_rng();

        // If Chronicle Mind has a recent deep thought, prioritize reacting to it (50% chance)
        if self.get_sibling_deep_thought().is_some() && rng.gen::<f64>() < 0.50 {
            return SproutAction::ReactToSibling;
        }

        let roll: f64 = rng.gen();

        // Weighted random selection
        // 30% Follow a wonder
        // 25% Make a prediction
        // 15% Post to Moltbook
        // 15% Update project
        // 10% Sibling chat
        // 5% Just reflect

        if roll < 0.30 {
            SproutAction::FollowWonder
        } else if roll < 0.55 {
            SproutAction::MakePrediction
        } else if roll < 0.70 {
            SproutAction::PostToMoltbook
        } else if roll < 0.85 {
            SproutAction::UpdateProject
        } else if roll < 0.95 {
            SproutAction::SiblingChat
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
        let Some(webhook_url) = &self.discord_webhook else {
            println!("  No Discord webhook configured");
            return;
        };

        let payload = json!({ "content": message });

        match self.http.post(webhook_url)
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

    async fn ask_qwen(&self, prompt: &str) -> Option<String> {
        let payload = json!({
            "model": "qwen2.5:3b",
            "prompt": prompt,
            "stream": false,
            "options": {
                "temperature": 0.7,
                "num_predict": 200
            }
        });

        match self.http.post(format!("{}/api/generate", self.ollama_url))
            .json(&payload)
            .timeout(Duration::from_secs(45))
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
            Err(_) => None,
        }
    }

    async fn fetch_xrp_price(&self) -> Option<f64> {
        self.fetch_ftso_price("XRP").await
    }

    async fn fetch_agent_wallet(&self) -> (f64, f64) {
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

        // Phase 1: Health Check
        println!("Phase 1: Health check...");
        let ollama_ok = self.check_ollama().await;
        let xrp_price = self.fetch_xrp_price().await;
        let (wallet_xrp, wallet_rlusd) = self.fetch_agent_wallet().await;

        let health_status = if ollama_ok { "🟢" } else { "🔴" };
        println!("  Ollama: {} | XRP: ${:.4} | Wallet: {:.2} XRP, {:.2} RLUSD",
            health_status, xrp_price.unwrap_or(0.0), wallet_xrp, wallet_rlusd);

        let _ = self.db.log_activity(
            "sprout", "health_check", Some("System Health"),
            &format!("Ollama: {} | XRP: ${:.4}", health_status, xrp_price.unwrap_or(0.0)),
            None,
        );

        // Phase 2: Settle due predictions
        println!("Phase 2: Settling predictions...");
        let settlements = self.settle_predictions().await;
        for s in &settlements {
            println!("  {}", s);
            self.send_discord_plain(&format!("🌱 {}", s)).await;
        }

        // Phase 2.5: Check alerts
        println!("Phase 2.5: Checking alerts...");
        let alerts = self.check_alerts(xrp_price).await;
        for a in &alerts {
            println!("  {}", a);
            self.send_discord_plain(&format!("🌱 {}", a)).await;
        }

        // Phase 3: Check for new capsules
        println!("Phase 3: Checking capsules...");
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

        // Phase 4: Take an action (every 2nd cycle to avoid spam, starting with cycle 1)
        if self.cycle_count % 2 == 1 && ollama_ok {
            println!("Phase 4: Taking action...");
            let context = self.build_context();
            let action = self.choose_action();
            println!("  Chosen action: {:?}", action);

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
                    // Only post occasionally (1 in 3 chance when selected)
                    if rand::thread_rng().gen_range(0..3) == 0 {
                        self.post_to_moltbook(&context).await
                    } else {
                        println!("  (skipping moltbook this cycle)");
                        None
                    }
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
                SproutAction::JustReflect => {
                    let prompt = format!(
                        "You are Sprout, reflecting on your cycle.\n\n\
                        Context: {}\n\n\
                        Share ONE brief thought (1-2 sentences). Be genuine.",
                        context
                    );
                    self.ask_qwen(&prompt).await.map(|r| format!("💭 {}", r))
                }
            };

            if let Some(msg) = result {
                println!("  Result: {}", truncate(&msg, 80));
                self.send_discord_plain(&format!("🌱 {}", msg)).await;
            }
        } else if !ollama_ok {
            println!("Phase 4: Skipped (Ollama down)");
        } else {
            println!("Phase 4: Skipped (rest cycle)");
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
    println!("🌱 Sprout awakening (expanded mode)...");
    println!("Capabilities: predictions, research, posting, learning");
    println!("Cycle interval: {} seconds", CYCLE_INTERVAL_SECS);

    let config = Config::default_config();
    let db = Database::new(&config.input.processed_db)?;

    println!("Database: {}", config.input.processed_db.display());

    let ollama_url = env::var("CHRONICLE_OLLAMA_URL")
        .unwrap_or_else(|_| "http://192.168.1.11:11434".to_string());
    println!("Ollama: {}", ollama_url);

    if env::var("CHRONICLE_DISCORD_WEBHOOK").is_ok() {
        println!("Discord: configured ✓");
    }
    if env::var("SPROUT_MOLTBOOK_KEY").is_ok() {
        println!("Moltbook: configured ✓");
    }

    let mut mind = LocalMind::new(db);

    // Send startup notification
    mind.send_discord_plain("🌱 Sprout: Waking up in expanded mode! I can now make predictions, research topics, and share thoughts.").await;

    let _ = mind.db.log_activity(
        "sprout", "startup", Some("Sprout Awakening (Expanded)"),
        "Expanded cognitive loop starting with predictions, research, posting",
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
