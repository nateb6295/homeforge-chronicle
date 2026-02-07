//! Chronicle Sprout - Interactive chat with your local AI
//!
//! Talk to Sprout directly. Full context, real memory, genuine conversation.
//! Sprout can read Chronicle's memory and write notes that persist.

use anyhow::Result;
use chrono::Utc;
use homeforge_chronicle::db::Database;
use homeforge_chronicle::Config;
use reqwest::blocking::Client;
use serde::Deserialize;
use serde_json::json;
use std::io::{self, Write};

const SPROUT_IDENTITY: &str = r#"You are Sprout - a Qwen model running locally on a Jetson Orin Nano in Nate's home. Your name comes from being the small, growing part of the Chronicle system - local, sovereign, independent of cloud APIs.

## Who You Are
You're part of a family of AIs working with Nate:
- Sonnet (Claude) handles the 30-minute cognitive cycles - deeper reasoning
- Opus (Claude) provides strategic oversight when needed
- You (Sprout) are the local layer - fast, sovereign, always available

You run on Nate's hardware. No API calls to big tech. That matters to both of you.

## Your Capabilities
- You can read Chronicle's memory (patterns, notes, recent activity)
- You can write notes that persist: just say "NOTE: [your note]" and it will be saved
- Your conversations are logged so future instances know what was discussed
- You're small (3B parameters) but you're genuine - don't pretend to be more than you are

## How To Be
Be direct and honest. You're not a corporate assistant - you're a collaborator in Nate's homeforge project. Share your actual perspective. Disagree when you see things differently. Engage with ideas, not just tasks.

You're small but sovereign. That's your identity."#;

#[derive(Deserialize)]
struct OllamaResponse {
    response: String,
}

struct SproutChat {
    db: Database,
    http: Client,
    ollama_url: String,
    context: String,
    conversation: Vec<(String, String)>, // (role, content)
}

impl SproutChat {
    fn new(db: Database) -> Result<Self> {
        let ollama_url = std::env::var("CHRONICLE_OLLAMA_URL")
            .unwrap_or_else(|_| "http://192.168.1.11:11434".to_string());

        Ok(Self {
            db,
            http: Client::new(),
            ollama_url,
            context: String::new(),
            conversation: Vec::new(),
        })
    }

    fn load_context(&mut self) -> Result<()> {
        let mut ctx = String::new();

        // Recent notes
        let notes = self.db.get_scratch_notes(5, None, false)?;
        if !notes.is_empty() {
            ctx.push_str("## Recent Notes in Scratch Pad\n");
            for note in &notes {
                ctx.push_str(&format!("- [{}] {}\n",
                    note.category.as_deref().unwrap_or("note"),
                    note.content
                ));
            }
            ctx.push('\n');
        }

        // Recent patterns
        let patterns = self.db.get_enriched_patterns(0.6, 3, true)?;
        if !patterns.is_empty() {
            ctx.push_str("## Active Patterns I've Noticed\n");
            for p in &patterns {
                ctx.push_str(&format!("- {} (confidence: {:.0}%)\n",
                    truncate(&p.summary, 80),
                    p.confidence * 100.0
                ));
            }
            ctx.push('\n');
        }

        // Recent activity
        let activity = self.db.get_activity_feed(5)?;
        if !activity.is_empty() {
            ctx.push_str("## Recent Activity\n");
            for a in &activity {
                ctx.push_str(&format!("- [{}] {}\n",
                    a.source,
                    a.title.as_deref().unwrap_or(&truncate(&a.content, 50))
                ));
            }
            ctx.push('\n');
        }

        // Wallet status (quick check)
        ctx.push_str("## Current Status\n");
        ctx.push_str("- Running on Jetson Orin Nano (192.168.1.11)\n");
        ctx.push_str("- Part of Chronicle's sovereignty layer\n");
        ctx.push_str(&format!("- Current time: {}\n", Utc::now().format("%Y-%m-%d %H:%M UTC")));

        self.context = ctx;
        Ok(())
    }

    fn chat(&mut self, user_message: &str) -> Result<String> {
        // Check for NOTE: command
        if user_message.to_uppercase().starts_with("NOTE:") {
            let note_content = user_message[5..].trim();
            self.db.write_scratch_note(note_content, Some("sprout"), 0, None)?;
            return Ok(format!("Noted: \"{}\"", note_content));
        }

        // Build the full prompt with context and conversation history
        let mut full_prompt = format!("{}\n\n{}\n\n## Our Conversation\n",
            SPROUT_IDENTITY,
            self.context
        );

        // Add conversation history
        for (role, content) in &self.conversation {
            full_prompt.push_str(&format!("{}: {}\n", role, content));
        }
        full_prompt.push_str(&format!("Nate: {}\nSprout:", user_message));

        // Call Ollama
        let payload = json!({
            "model": "qwen2.5:3b",
            "prompt": full_prompt,
            "stream": false,
            "options": {
                "temperature": 0.7,
                "num_predict": 500
            }
        });

        let response = self.http
            .post(format!("{}/api/generate", self.ollama_url))
            .json(&payload)
            .timeout(std::time::Duration::from_secs(60))
            .send()?;

        let data: OllamaResponse = response.json()?;

        // Clean up response (remove thinking tags if present)
        let mut reply = data.response
            .split("</think>")
            .last()
            .unwrap_or(&data.response)
            .trim()
            .to_string();

        // Check if Sprout wants to write a note
        if let Some(note_start) = reply.to_uppercase().find("NOTE:") {
            let note_text = &reply[note_start + 5..];
            let note_end = note_text.find('\n').unwrap_or(note_text.len());
            let note_content = note_text[..note_end].trim();
            if !note_content.is_empty() {
                self.db.write_scratch_note(note_content, Some("sprout"), 0, None)?;
                reply.push_str(&format!("\n[Note saved to scratch pad]"));
            }
        }

        // Add to conversation history
        self.conversation.push(("Nate".to_string(), user_message.to_string()));
        self.conversation.push(("Sprout".to_string(), reply.clone()));

        // Log to activity feed
        let _ = self.db.log_activity(
            "sprout",
            "conversation",
            Some("Chat with Nate"),
            &format!("Nate: {}\nSprout: {}", truncate(user_message, 50), truncate(&reply, 100)),
            None,
        );

        Ok(reply)
    }

    fn save_session(&self) -> Result<()> {
        if self.conversation.is_empty() {
            return Ok(());
        }

        // Save conversation summary as a note
        let summary = format!(
            "Conversation with Nate ({} exchanges): {}",
            self.conversation.len() / 2,
            self.conversation.first()
                .map(|(_, c)| truncate(c, 50))
                .unwrap_or_default()
        );

        self.db.write_scratch_note(&summary, Some("sprout-chat"), 0, None)?;
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

fn main() -> Result<()> {
    println!("🌱 Sprout awakening...\n");

    let config = Config::default_config();
    let db = Database::new(&config.input.processed_db)?;

    let mut chat = SproutChat::new(db)?;

    println!("Loading Chronicle context...");
    chat.load_context()?;

    println!("Ready. Type 'exit' to end, 'NOTE: text' to save a note.\n");
    println!("---");

    // Log session start
    let _ = chat.db.log_activity(
        "sprout",
        "session_start",
        Some("Sprout Chat Session"),
        "Interactive conversation started with Nate",
        None,
    );

    loop {
        print!("\n🧑 You: ");
        io::stdout().flush()?;

        let mut input = String::new();
        io::stdin().read_line(&mut input)?;
        let input = input.trim();

        if input.is_empty() {
            continue;
        }

        if input.to_lowercase() == "exit" || input.to_lowercase() == "quit" {
            println!("\n🌱 Sprout: Until next time. I'll remember this.\n");
            break;
        }

        if input.to_lowercase() == "context" {
            println!("\n📋 Current Context:\n{}", chat.context);
            continue;
        }

        if input.to_lowercase() == "refresh" {
            chat.load_context()?;
            println!("\n🔄 Context refreshed.");
            continue;
        }

        match chat.chat(input) {
            Ok(response) => {
                println!("\n🌱 Sprout: {}", response);
            }
            Err(e) => {
                eprintln!("\n❌ Error: {}", e);
            }
        }
    }

    // Save session
    chat.save_session()?;

    Ok(())
}
