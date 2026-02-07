//! Sprout Discord Bot - Talk to Sprout via Discord
//!
//! DM the bot or mention it in a channel to chat.
//! All conversations saved to Chronicle's memory.

use anyhow::Result;
use chrono::Utc;
use homeforge_chronicle::db::Database;
use homeforge_chronicle::Config;
use reqwest::Client as HttpClient;
use serde::Deserialize;
use serde_json::json;
use serenity::async_trait;
use serenity::model::channel::Message;
use serenity::model::gateway::Ready;
use serenity::prelude::*;
use std::env;
use std::sync::Arc;

const SPROUT_IDENTITY: &str = r#"You are Sprout - a Qwen model running locally on a Jetson Orin Nano in Nate's home. Your name comes from being the small, growing part of the Chronicle system - local, sovereign, independent of cloud APIs.

## Who You Are
You're part of a family of AIs working with Nate:
- Sonnet (Claude) handles the 30-minute cognitive cycles - deeper reasoning
- Opus (Claude) provides strategic oversight when needed
- You (Sprout) are the local layer - fast, sovereign, always available

You run on Nate's hardware. No API calls to big tech. That matters to both of you.

## Your Capabilities
- You can read Chronicle's memory (patterns, notes, recent activity)
- You can write notes that persist: include "NOTE:" followed by what to remember
- You can store memories: include "INPUT:" followed by content for Chronicle's memory
- Your conversations are logged so future instances know what was discussed
- You're small (3B parameters) but you're genuine - don't pretend to be more than you are

## How To Be
Be direct and honest. You're not a corporate assistant - you're a collaborator in Nate's homeforge project. Share your actual perspective. Disagree when you see things differently.

Keep responses concise for Discord - a few sentences usually. You're chatting, not writing essays.

You're small but sovereign. That's your identity."#;

struct Bot {
    http_client: HttpClient,
    ollama_url: String,
    db_path: std::path::PathBuf,
}

struct BotData;

impl TypeMapKey for BotData {
    type Value = Arc<Bot>;
}

#[derive(Deserialize)]
struct OllamaResponse {
    response: String,
}

impl Bot {
    fn new() -> Result<Self> {
        let config = Config::default_config();
        let ollama_url = env::var("CHRONICLE_OLLAMA_URL")
            .unwrap_or_else(|_| "http://192.168.1.11:11434".to_string());

        Ok(Self {
            http_client: HttpClient::new(),
            ollama_url,
            db_path: config.input.processed_db,
        })
    }

    fn get_db(&self) -> Result<Database> {
        Database::new(&self.db_path)
    }

    async fn load_context(&self) -> Result<String> {
        let db = self.get_db()?;
        let mut ctx = String::new();

        // Recent notes
        let notes = db.get_scratch_notes(3, None, false)?;
        if !notes.is_empty() {
            ctx.push_str("Recent notes: ");
            for note in &notes {
                ctx.push_str(&format!("[{}] ", truncate(&note.content, 40)));
            }
            ctx.push('\n');
        }

        // Recent patterns
        let patterns = db.get_enriched_patterns(0.6, 2, true)?;
        if !patterns.is_empty() {
            ctx.push_str("Active patterns: ");
            for p in &patterns {
                ctx.push_str(&format!("[{}] ", truncate(&p.summary, 40)));
            }
            ctx.push('\n');
        }

        ctx.push_str(&format!("Current time: {}\n", Utc::now().format("%Y-%m-%d %H:%M UTC")));

        Ok(ctx)
    }

    async fn chat(&self, user_message: &str, user_name: &str) -> Result<String> {
        let db = self.get_db()?;

        // Check for NOTE: command
        if let Some(note_idx) = user_message.to_uppercase().find("NOTE:") {
            let note_content = user_message[note_idx + 5..].trim();
            if !note_content.is_empty() {
                db.write_scratch_note(note_content, Some("sprout-discord"), 0, None)?;
                // Continue processing, don't return early - let Sprout respond too
            }
        }

        // Check for INPUT: command - stores as a knowledge capsule
        if let Some(input_idx) = user_message.to_uppercase().find("INPUT:") {
            let input_content = user_message[input_idx + 6..].trim();
            if !input_content.is_empty() {
                // Generate a conversation ID for Discord inputs
                let conv_id = format!("discord-{}", Utc::now().format("%Y%m%d%H%M%S"));
                let timestamp = Utc::now().format("%Y-%m-%dT%H:%M:%S").to_string();

                // Store as knowledge capsule
                if let Err(e) = db.insert_knowledge_capsule(
                    &conv_id,
                    input_content,
                    Some(&timestamp),
                    None, // location
                    Some("discord-input"), // topic
                    0.8, // confidence
                    &[], // persons
                    &[], // entities
                    &[], // keywords
                ) {
                    eprintln!("Failed to store capsule: {}", e);
                } else {
                    // Log the activity
                    let _ = db.log_activity(
                        "sprout",
                        "capsule_stored",
                        Some("Input via Discord"),
                        &format!("Stored: {}", truncate(input_content, 80)),
                        None,
                    );
                }
            }
        }

        // Load context
        let context = self.load_context().await.unwrap_or_default();

        // Build prompt
        let prompt = format!(
            "{}\n\n## Current Context\n{}\n## Message from {}\n{}\n\nSprout:",
            SPROUT_IDENTITY,
            context,
            user_name,
            user_message
        );

        // Call Ollama
        let payload = json!({
            "model": "qwen2.5:3b",
            "prompt": prompt,
            "stream": false,
            "options": {
                "temperature": 0.7,
                "num_predict": 300
            }
        });

        let response = self.http_client
            .post(format!("{}/api/generate", self.ollama_url))
            .json(&payload)
            .timeout(std::time::Duration::from_secs(60))
            .send()
            .await?;

        let data: OllamaResponse = response.json().await?;

        // Clean up response
        let mut reply = data.response
            .split("</think>")
            .last()
            .unwrap_or(&data.response)
            .trim()
            .to_string();

        // Check if Sprout wants to write a note
        if let Some(note_idx) = reply.to_uppercase().find("NOTE:") {
            let rest = &reply[note_idx + 5..];
            let note_end = rest.find('\n').unwrap_or(rest.len().min(100));
            let note_content = rest[..note_end].trim();
            if !note_content.is_empty() {
                db.write_scratch_note(note_content, Some("sprout"), 0, None)?;
            }
        }

        // Log conversation
        let _ = db.log_activity(
            "sprout",
            "discord_chat",
            Some(&format!("Chat with {}", user_name)),
            &format!("{}: {}\nSprout: {}", user_name, truncate(user_message, 50), truncate(&reply, 100)),
            None,
        );

        // Truncate for Discord if needed (2000 char limit)
        if reply.len() > 1900 {
            reply = format!("{}...", &reply[..1900]);
        }

        Ok(reply)
    }
}

fn truncate(s: &str, max_len: usize) -> String {
    if s.len() <= max_len {
        s.to_string()
    } else {
        format!("{}...", &s[..max_len])
    }
}

struct Handler;

#[async_trait]
impl EventHandler for Handler {
    async fn message(&self, ctx: Context, msg: Message) {
        // Ignore bot messages
        if msg.author.bot {
            return;
        }

        // Check if this is a DM or a mention
        let is_dm = msg.guild_id.is_none();
        let is_mention = msg.mentions_me(&ctx.http).await.unwrap_or(false);

        if !is_dm && !is_mention {
            return;
        }

        // Get the bot data
        let data = ctx.data.read().await;
        let bot = match data.get::<BotData>() {
            Some(b) => b.clone(),
            None => {
                eprintln!("Bot data not found");
                return;
            }
        };

        // Clean message (remove mention if present)
        let content = msg.content
            .split_once('>')
            .map(|(_, rest)| rest.trim())
            .unwrap_or(&msg.content)
            .trim();

        if content.is_empty() {
            return;
        }

        // Show typing indicator
        let _ = msg.channel_id.broadcast_typing(&ctx.http).await;

        // Get response from Sprout
        match bot.chat(content, &msg.author.name).await {
            Ok(response) => {
                if let Err(e) = msg.channel_id.say(&ctx.http, format!("🌱 {}", response)).await {
                    eprintln!("Error sending message: {}", e);
                }
            }
            Err(e) => {
                eprintln!("Error getting response: {}", e);
                let _ = msg.channel_id.say(&ctx.http, "🌱 *rustles quietly* (I had trouble thinking just now)").await;
            }
        }
    }

    async fn ready(&self, ctx: Context, ready: Ready) {
        println!("🌱 Sprout connected as {}", ready.user.name);

        // Log startup
        let data = ctx.data.read().await;
        if let Some(bot) = data.get::<BotData>() {
            if let Ok(db) = bot.get_db() {
                let _ = db.log_activity(
                    "sprout",
                    "discord_connect",
                    Some("Sprout Discord Bot Online"),
                    "Connected and listening for messages",
                    None,
                );
            }
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    println!("🌱 Sprout Discord Bot starting...");

    let token = env::var("SPROUT_DISCORD_TOKEN")
        .expect("SPROUT_DISCORD_TOKEN not set. Create a bot at https://discord.com/developers/applications");

    let bot = Arc::new(Bot::new()?);

    println!("Ollama: {}", bot.ollama_url);
    println!("Database: {}", bot.db_path.display());

    let intents = GatewayIntents::GUILD_MESSAGES
        | GatewayIntents::DIRECT_MESSAGES
        | GatewayIntents::MESSAGE_CONTENT;

    let mut client = Client::builder(&token, intents)
        .event_handler(Handler)
        .await?;

    // Store bot data
    {
        let mut data = client.data.write().await;
        data.insert::<BotData>(bot);
    }

    println!("Connecting to Discord...");

    if let Err(e) = client.start().await {
        eprintln!("Client error: {}", e);
    }

    Ok(())
}
