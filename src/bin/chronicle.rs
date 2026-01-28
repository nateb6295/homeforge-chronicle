use anyhow::Result;
use clap::Parser;
use homeforge_chronicle::cli::{Cli, Commands};
use homeforge_chronicle::config::Config;
use homeforge_chronicle::db::Database;
use homeforge_chronicle::{SyncCapsule, ThoughtEntry};
use std::path::PathBuf;

fn main() -> Result<()> {
    let cli = Cli::parse();

    // Load configuration
    let config = load_config(cli.config.as_ref())?;

    // Execute command
    match cli.command {
        Commands::Init { path } => {
            let target_path = path.unwrap_or_else(|| PathBuf::from("."));
            init_command(&target_path)?;
        }
        Commands::Ingest { file } => {
            ingest_command(&config, &file)?;
        }
        Commands::IngestBulk { file } => {
            ingest_bulk_command(&config, &file)?;
        }
        Commands::Extract { limit } => {
            extract_command(&config, limit)?;
        }
        Commands::Compile => {
            compile_command(&config)?;
        }
        Commands::Build => {
            build_command(&config)?;
        }
        Commands::Deploy => {
            deploy_command(&config)?;
        }
        Commands::Watch => {
            watch_command(&config)?;
        }
        Commands::Capsules { limit, conversation_id } => {
            capsules_command(&config, limit, conversation_id)?;
        }
        Commands::CapsuleStats => {
            capsule_stats_command(&config)?;
        }
        Commands::SearchCapsules { query, search_type, limit } => {
            search_capsules_command(&config, &query, &search_type, limit)?;
        }
        Commands::Embeddings { ollama_url, model, limit, force } => {
            embeddings_command(&config, &ollama_url, &model, limit, force)?;
        }
        Commands::SemanticSearch { query, limit, ollama_url, model } => {
            semantic_search_command(&config, &query, limit, &ollama_url, &model)?;
        }
        Commands::Sync { canister_id, identity, capsules_only, embeddings_only } => {
            sync_command(&config, &canister_id, &identity, capsules_only, embeddings_only)?;
        }
        Commands::Metabolize { ollama_url, model, with_decay, detect_meta } => {
            metabolize_command(&config, &ollama_url, &model, with_decay, detect_meta)?;
        }
        Commands::Patterns { min_confidence, limit, active_only, id } => {
            patterns_command(&config, min_confidence, limit, active_only, id)?;
        }
        Commands::SyncNative { canister_id, identity, batch_size } => {
            sync_native_command(&config, &canister_id, &identity, batch_size)?;
        }
        Commands::OnchainSearch { query, limit, canister_id, identity, ollama_url, model } => {
            onchain_search_command(&query, limit, &canister_id, &identity, &ollama_url, &model)?;
        }
        Commands::RefreshPatterns { id, min_capsules, dry_run } => {
            refresh_patterns_command(&config, id, min_capsules, dry_run)?;
        }
    }

    Ok(())
}

fn load_config(config_path: Option<&PathBuf>) -> Result<Config> {
    if let Some(path) = config_path {
        Config::from_file(path)
    } else {
        // Try default locations
        let default_paths = vec![
            PathBuf::from("chronicle.toml"),
            PathBuf::from(".chronicle.toml"),
        ];

        for path in default_paths {
            if path.exists() {
                return Config::from_file(&path);
            }
        }

        // Use default config if no file found
        Ok(Config::default_config())
    }
}

fn init_command(path: &PathBuf) -> Result<()> {
    println!("Initializing Homeforge Chronicle in {:?}", path);

    // Create default config
    let mut config = Config::default_config();
    config.expand_paths();

    // Write config file
    let config_path = path.join("chronicle.toml");
    config.to_file(&config_path)?;
    println!("Created configuration: {:?}", config_path);

    // Create directories
    std::fs::create_dir_all(&config.input.watch_directory)?;
    println!("Created watch directory: {:?}", config.input.watch_directory);

    if let Some(parent) = config.input.processed_db.parent() {
        std::fs::create_dir_all(parent)?;
        println!("Created data directory: {:?}", parent);
    }

    std::fs::create_dir_all(&config.output.build_directory)?;
    println!("Created build directory: {:?}", config.output.build_directory);

    // Initialize database
    let db = Database::new(&config.input.processed_db)?;
    println!("Initialized database: {:?}", config.input.processed_db);

    println!("\nInitialization complete!");
    println!("\nNext steps:");
    println!("1. Edit chronicle.toml to customize settings");
    println!("2. Add your Claude API key to the configuration");
    println!("3. Export conversations from claude.ai and place them in {:?}", config.input.watch_directory);
    println!("4. Run 'chronicle ingest <file>' to process a conversation");

    Ok(())
}

fn ingest_command(config: &Config, file: &PathBuf) -> Result<()> {
    println!("Ingesting conversation from {:?}", file);

    // Open database
    let db = Database::new(&config.input.processed_db)?;

    // Ingest the conversation
    let result = homeforge_chronicle::ingest_conversation(&db, file)?;

    // Display result
    match result {
        homeforge_chronicle::IngestResult::Ingested { conversation_id, message_count } => {
            println!("✓ Successfully ingested conversation {}", conversation_id);
            println!("  Messages: {}", message_count);
            println!("  Database: {:?}", config.input.processed_db);
        }
        homeforge_chronicle::IngestResult::Duplicate { conversation_id } => {
            println!("⊘ Conversation {} already processed (duplicate)", conversation_id);
        }
        homeforge_chronicle::IngestResult::Skipped { reason } => {
            println!("⊘ Skipped: {}", reason);
        }
    }

    Ok(())
}

fn ingest_bulk_command(config: &Config, file: &PathBuf) -> Result<()> {
    use homeforge_chronicle::parse_bulk_export;

    println!("Ingesting bulk conversation export from {:?}", file);

    // Open database
    let db = Database::new(&config.input.processed_db)?;

    // Parse bulk export
    let bulk_export = parse_bulk_export(file)?;

    let total_messages: usize = bulk_export.iter().map(|c| c.message_count()).sum();
    println!("Found {} conversations in bulk export", bulk_export.len());
    println!("Total messages: {}\n", total_messages);

    // Track statistics
    let mut ingested_count = 0;
    let mut duplicate_count = 0;
    let mut skipped_count = 0;
    let mut error_count = 0;

    // Process each conversation
    for (i, conversation) in bulk_export.iter().enumerate() {
        println!("[{}/{}] Processing: {}",
            i + 1,
            bulk_export.len(),
            conversation.name
        );

        // Create a temporary file for the conversation (to use existing ingest logic)
        // Or we can directly ingest using the database
        let first_ts = match conversation.first_message_timestamp() {
            Ok(ts) => ts.timestamp(),
            Err(_) => {
                println!("  ⊘ Skipped: No messages");
                skipped_count += 1;
                continue;
            }
        };

        let last_ts = match conversation.last_message_timestamp() {
            Ok(ts) => ts.timestamp(),
            Err(_) => {
                println!("  ⊘ Skipped: No messages");
                skipped_count += 1;
                continue;
            }
        };

        // Check if already processed
        if db.is_conversation_processed(&conversation.uuid)? {
            println!("  ⊘ Duplicate ({})", conversation.uuid);
            duplicate_count += 1;
            continue;
        }

        // Insert conversation
        match db.insert_conversation(
            &conversation.uuid,
            file.to_str().unwrap_or("bulk_export.json"),
            first_ts,
            last_ts,
            conversation.message_count() as i64,
        ) {
            Ok(_) => {
                println!("  ✓ Ingested ({} messages)", conversation.message_count());
                ingested_count += 1;
            }
            Err(e) => {
                println!("  ✗ Error: {:?}", e);
                error_count += 1;
            }
        }
    }

    // Summary
    println!("\n✓ Bulk ingestion complete!");
    println!("  Ingested: {}", ingested_count);
    println!("  Duplicates: {}", duplicate_count);
    println!("  Skipped: {}", skipped_count);
    println!("  Errors: {}", error_count);
    println!("  Database: {:?}", config.input.processed_db);

    Ok(())
}

fn extract_command(config: &Config, limit: Option<usize>) -> Result<()> {
    use homeforge_chronicle::llm::ClaudeClient;
    use homeforge_chronicle::{extract_conversation, ConversationExport};
    use std::collections::HashMap;

    println!("Extracting themes and summaries from conversations");
    println!("Using LLM backend: {:?}", config.extraction.llm_backend);

    // Open database
    let db = Database::new(&config.input.processed_db)?;

    // Get conversations that haven't been extracted
    let mut unextracted = db.get_unextracted_conversations()?;

    if unextracted.is_empty() {
        println!("No conversations to extract. All caught up!");
        return Ok(());
    }

    // Apply limit if specified
    if let Some(limit_count) = limit {
        if unextracted.len() > limit_count {
            unextracted.truncate(limit_count);
            println!("Limiting extraction to {} most recent conversations", limit_count);
        }
    }

    println!("Found {} conversations to extract", unextracted.len());

    // Scan watch directory for conversation files
    println!("Scanning {:?} for conversation files...", config.input.watch_directory);

    let mut file_map: HashMap<String, ConversationExport> = HashMap::new();

    if config.input.watch_directory.exists() {
        for entry in std::fs::read_dir(&config.input.watch_directory)? {
            let entry = entry?;
            let path = entry.path();

            if path.extension().and_then(|s| s.to_str()) == Some("json") {
                // Check if it's a bulk export
                if path.file_name().and_then(|n| n.to_str()) == Some("conversations.json") {
                    println!("Found bulk export, loading all conversations...");
                    if let Ok(bulk_export) = homeforge_chronicle::parse_bulk_export(&path) {
                        for conv in bulk_export {
                            file_map.insert(conv.uuid.clone(), conv);
                        }
                    }
                } else {
                    // Single conversation file
                    if let Ok(conv) = ConversationExport::from_file(&path) {
                        file_map.insert(conv.uuid.clone(), conv);
                    }
                }
            }
        }
    }

    println!("Found {} conversations available for extraction", file_map.len());

    // Create LLM client
    let llm_client: Box<dyn homeforge_chronicle::LlmClient> = match config.extraction.llm_backend {
        homeforge_chronicle::config::LlmBackend::ClaudeApi => {
            // Get API key from config or env
            let api_key = config
                .extraction
                .claude_api_key
                .clone()
                .or_else(|| std::env::var("ANTHROPIC_API_KEY").ok())
                .ok_or_else(|| anyhow::anyhow!(
                    "Claude API key not found. Set ANTHROPIC_API_KEY environment variable or add to config"
                ))?;

            Box::new(ClaudeClient::new(api_key, config.extraction.llm_model.clone())?)
        }
        homeforge_chronicle::config::LlmBackend::Ollama => {
            anyhow::bail!("Ollama backend not yet implemented. Use claude-api for now.");
        }
    };

    // Process each conversation
    let mut success_count = 0;
    let mut error_count = 0;
    let mut skipped_count = 0;

    for (i, conversation_id) in unextracted.iter().enumerate() {
        println!("\n[{}/{}] Processing conversation {}...", i + 1, unextracted.len(), conversation_id);

        // Find the conversation
        let conversation = match file_map.get(conversation_id) {
            Some(conv) => conv.clone(),
            None => {
                println!("  ⊘ Skipped: conversation not found in watch directory");
                skipped_count += 1;
                continue;
            }
        };

        // Extract with LLM
        println!("  Calling LLM for extraction...");
        let extraction = match extract_conversation(&conversation, llm_client.as_ref(), &config.extraction.themes) {
            Ok(ext) => ext,
            Err(e) => {
                println!("  ✗ Error during extraction: {}", e);
                error_count += 1;
                continue;
            }
        };

        // Store in database
        match db.insert_extraction(
            &extraction.conversation_id,
            &extraction.title,
            &extraction.summary,
            &extraction.classification.to_string(),
            extraction.confidence_score,
            &extraction.themes,
            &extraction.key_quotes,
        ) {
            Ok(extraction_id) => {
                println!("  ✓ Extracted successfully (ID: {})", extraction_id);
                println!("    Title: {}", extraction.title);
                println!("    Themes: {}", extraction.themes.join(", "));
                println!("    Classification: {}", extraction.classification);
                success_count += 1;
            }
            Err(e) => {
                println!("  ✗ Error storing extraction: {}", e);
                error_count += 1;
            }
        }
    }

    println!("\n✓ Extraction complete!");
    println!("  Successful: {}", success_count);
    println!("  Skipped: {}", skipped_count);
    println!("  Errors: {}", error_count);

    Ok(())
}

fn compile_command(config: &Config) -> Result<()> {
    use homeforge_chronicle::compilation::{DigestEntry, PredictionRegistry, Prediction, PredictionStatus, ThreadEntry};
    use chrono::{DateTime, Utc};

    println!("Compiling weekly digests, theme threads, and prediction registry");

    // Open database
    let db = Database::new(&config.input.processed_db)?;

    // Create output directory if it doesn't exist
    let output_dir = &config.output.build_directory;
    std::fs::create_dir_all(output_dir)?;

    // 1. Generate weekly digest
    println!("\n[1/3] Generating weekly digest...");

    let recent_extractions = db.get_recent_extractions(7)?;

    if recent_extractions.is_empty() {
        println!("  No extractions from the past week");
    } else {
        let mut entries_by_theme: std::collections::HashMap<String, Vec<DigestEntry>> = std::collections::HashMap::new();

        for (id, conv_id, title, summary, classification_str, _confidence, created_at, themes, quotes) in recent_extractions {
            let classification = match classification_str.as_str() {
                "prediction" => homeforge_chronicle::Classification::Prediction,
                "insight" => homeforge_chronicle::Classification::Insight,
                "milestone" => homeforge_chronicle::Classification::Milestone,
                "reflection" => homeforge_chronicle::Classification::Reflection,
                _ => homeforge_chronicle::Classification::Technical,
            };

            let entry = DigestEntry {
                extraction_id: id,
                conversation_id: conv_id,
                title,
                summary,
                classification,
                created_at: DateTime::from_timestamp(created_at, 0).unwrap_or_else(Utc::now),
                quotes,
            };

            // Add to each theme
            for theme in themes {
                entries_by_theme.entry(theme).or_insert_with(Vec::new).push(entry.clone());
            }
        }

        let now = Utc::now();
        let week_start = now - chrono::Duration::days(7);

        let total_entries: usize = entries_by_theme.values().map(|v| v.len()).sum();

        let digest = homeforge_chronicle::WeeklyDigest {
            week_start,
            week_end: now,
            entries_by_theme,
            total_entries,
        };

        let digest_path = output_dir.join("weekly_digest.md");
        std::fs::write(&digest_path, digest.to_markdown())?;
        println!("  ✓ Weekly digest saved to {:?}", digest_path);
        println!("    Total entries: {}", total_entries);
    }

    // 2. Generate theme threads
    println!("\n[2/3] Generating theme threads...");

    let themes = db.get_themes_with_threshold(3)?; // Only themes with 3+ entries

    if themes.is_empty() {
        println!("  No themes with sufficient entries yet");
    } else {
        let threads_dir = output_dir.join("threads");
        std::fs::create_dir_all(&threads_dir)?;

        for (theme, count) in themes {
            let extractions = db.get_extractions_by_theme(&theme)?;

            let entries: Vec<ThreadEntry> = extractions.into_iter().map(|(id, title, summary, created_at, quotes)| {
                ThreadEntry {
                    extraction_id: id,
                    title,
                    summary,
                    created_at: DateTime::from_timestamp(created_at, 0).unwrap_or_else(Utc::now),
                    quotes,
                }
            }).collect();

            let thread = homeforge_chronicle::generate_theme_thread(theme.clone(), entries);
            let thread_path = threads_dir.join(format!("{}.md", theme.replace(' ', "_")));
            std::fs::write(&thread_path, thread.to_markdown())?;
            println!("  ✓ {} thread: {} entries → {:?}", theme, count, thread_path);
        }
    }

    // 3. Generate prediction registry
    println!("\n[3/3] Generating prediction registry...");

    let predictions_data = db.get_all_predictions()?;

    if predictions_data.is_empty() {
        println!("  No predictions recorded yet");
    } else {
        let predictions: Vec<Prediction> = predictions_data.into_iter().map(|(id, extraction_id, claim, date_made, timeline, status_str, validation_date, notes)| {
            let status = match status_str.as_str() {
                "validated" => PredictionStatus::Validated,
                "invalidated" => PredictionStatus::Invalidated,
                _ => PredictionStatus::Pending,
            };

            Prediction {
                id,
                extraction_id,
                claim,
                date_made: DateTime::from_timestamp(date_made, 0).unwrap_or_else(Utc::now),
                timeline,
                status,
                validation_date: validation_date.and_then(|ts| DateTime::from_timestamp(ts, 0)),
                notes,
            }
        }).collect();

        let registry = PredictionRegistry { predictions: predictions.clone() };
        let registry_path = output_dir.join("predictions.md");
        std::fs::write(&registry_path, registry.to_markdown())?;
        println!("  ✓ Prediction registry saved to {:?}", registry_path);
        println!("    Total predictions: {}", predictions.len());
    }

    println!("\n✓ Compilation complete!");
    println!("  Output directory: {:?}", output_dir);

    Ok(())
}

fn build_command(config: &Config) -> Result<()> {
    use homeforge_chronicle::{build_site, build_thoughts_page, build_outbox_page, DisplayEntry, DisplayCapsule, DisplayThought, DisplayOutboxMessage, DisplayMarketPosition};
    use homeforge_chronicle::compilation::{Prediction, PredictionStatus};
    use chrono::{DateTime, Utc, Datelike, Timelike};

    println!("Building static site to {:?}", config.output.build_directory);

    // Open database
    let db = Database::new(&config.input.processed_db)?;

    // Get recent extractions for index page
    println!("Loading recent extractions...");
    let recent_data = db.get_recent_extractions(30)?; // Last 30 days

    let recent_entries: Vec<DisplayEntry> = recent_data.into_iter().map(|(id, _conv_id, title, summary, classification, _confidence, created_at, themes, quotes)| {
        DisplayEntry {
            title,
            summary,
            classification,
            date: DateTime::from_timestamp(created_at, 0)
                .unwrap_or_else(Utc::now)
                .format("%Y-%m-%d")
                .to_string(),
            themes,
            quotes,
        }
    }).collect();

    println!("  Found {} recent entries", recent_entries.len());

    // Get themes for thread pages
    println!("Loading theme threads...");
    let themes_data = db.get_themes_with_threshold(3)?;

    let mut threads = Vec::new();
    for (theme, _count) in themes_data {
        let extractions = db.get_extractions_by_theme(&theme)?;

        if extractions.is_empty() {
            continue;
        }

        let entries: Vec<DisplayEntry> = extractions.iter().map(|(id, title, summary, created_at, quotes)| {
            DisplayEntry {
                title: title.clone(),
                summary: summary.clone(),
                classification: String::new(), // Not needed for threads
                date: DateTime::from_timestamp(*created_at, 0)
                    .unwrap_or_else(Utc::now)
                    .format("%Y-%m-%d")
                    .to_string(),
                themes: vec![],
                quotes: quotes.clone(),
            }
        }).collect();

        let dates: Vec<DateTime<Utc>> = extractions.iter()
            .map(|(_, _, _, created_at, _)| {
                DateTime::from_timestamp(*created_at, 0).unwrap_or_else(Utc::now)
            })
            .collect();

        let start = dates.iter().min().cloned().unwrap_or_else(Utc::now);
        let end = dates.iter().max().cloned().unwrap_or_else(Utc::now);

        threads.push((theme, entries, start, end));
    }

    println!("  Found {} themes", threads.len());

    // Get predictions
    println!("Loading predictions...");
    let predictions_data = db.get_all_predictions()?;

    let predictions: Vec<Prediction> = predictions_data.into_iter().map(|(id, extraction_id, claim, date_made, timeline, status_str, validation_date, notes)| {
        let status = match status_str.as_str() {
            "validated" => PredictionStatus::Validated,
            "invalidated" => PredictionStatus::Invalidated,
            _ => PredictionStatus::Pending,
        };

        Prediction {
            id,
            extraction_id,
            claim,
            date_made: DateTime::from_timestamp(date_made, 0).unwrap_or_else(Utc::now),
            timeline,
            status,
            validation_date: validation_date.and_then(|ts| DateTime::from_timestamp(ts, 0)),
            notes,
        }
    }).collect();

    println!("  Found {} predictions", predictions.len());

    // Get capsules for activity discovery
    println!("Loading capsules...");
    let capsule_count = db.get_capsule_count()? as usize;
    let active_capsules = db.get_active_capsules(10)?;
    let recent_capsules: Vec<DisplayCapsule> = active_capsules.into_iter().map(|(id, restatement, timestamp, topic, confidence)| {
        DisplayCapsule {
            content: if restatement.len() > 200 {
                format!("{}...", &restatement[..200])
            } else {
                restatement
            },
            topic: topic.unwrap_or_else(|| "general".to_string()),
            timestamp: timestamp.unwrap_or_else(|| "recent".to_string()),
            keywords: vec![], // Could fetch separately if needed
        }
    }).collect();
    println!("  Found {} total capsules, showing {}", capsule_count, recent_capsules.len());

    // Get market positions
    println!("Loading market positions...");
    let positions_data = db.get_market_positions(None)?;
    let market_positions: Vec<DisplayMarketPosition> = positions_data.into_iter().map(|p| {
        DisplayMarketPosition {
            platform: p.platform,
            market_id: p.market_id,
            market_question: p.market_question,
            position: p.position,
            entry_price: p.entry_price,
            shares: p.shares,
            stake_usdc: p.stake_usdc,
            thesis: p.thesis,
            confidence: (p.confidence * 100.0) as i32,
            status: p.status,
            pnl_usdc: p.pnl_usdc.unwrap_or(0.0),
            created_at: DateTime::from_timestamp(p.created_at, 0)
                .unwrap_or_else(Utc::now)
                .format("%Y-%m-%d")
                .to_string(),
            resolved_at: p.resolved_at
                .and_then(|ts| DateTime::from_timestamp(ts, 0))
                .map(|dt| dt.format("%Y-%m-%d").to_string())
                .unwrap_or_default(),
        }
    }).collect();
    println!("  Found {} market positions", market_positions.len());

    // Build the site
    println!("\nGenerating static site...");
    build_site(
        &config.output.build_directory,
        &config.output.site_title,
        &config.output.author,
        recent_entries,
        threads,
        predictions,
        market_positions,
        capsule_count,
        recent_capsules,
    )?;

    // Build thoughts page
    println!("Loading thought stream...");
    let thought_entries = db.get_recent_thoughts(50)?;
    let total_thoughts = thought_entries.len(); // Could add a count method later

    // Count actions taken today
    let now = Utc::now();
    let today_start = now.date_naive().and_hms_opt(0, 0, 0).unwrap().and_utc().timestamp();
    let actions_today: usize = thought_entries.iter()
        .filter(|t| t.created_at >= today_start)
        .map(|t| t.actions_taken.split('\n').filter(|a| !a.is_empty()).count())
        .sum();

    // Convert to display format
    let display_thoughts: Vec<DisplayThought> = thought_entries.into_iter().map(|t| {
        let actions: Vec<String> = t.actions_taken
            .split('\n')
            .filter(|a| !a.is_empty())
            .map(|s| s.to_string())
            .collect();

        DisplayThought {
            cycle_id: t.cycle_id,
            timestamp: DateTime::from_timestamp(t.created_at, 0)
                .unwrap_or_else(Utc::now)
                .format("%Y-%m-%d %H:%M UTC")
                .to_string(),
            context_summary: t.context_summary,
            reasoning: t.reasoning,
            actions,
        }
    }).collect();

    println!("  Found {} thoughts, {} actions today", total_thoughts, actions_today);

    build_thoughts_page(
        &config.output.build_directory,
        &config.output.site_title,
        &config.output.author,
        display_thoughts,
        total_thoughts,
        actions_today,
    )?;

    // Build outbox page
    println!("Loading outbox messages...");
    let outbox_messages = db.get_all_outbox_messages(100)?;
    let unread_count = db.count_unread_messages()?;
    let total_outbox = db.count_outbox_messages()?;

    // Convert to display format
    let display_messages: Vec<DisplayOutboxMessage> = outbox_messages.into_iter().map(|m| {
        DisplayOutboxMessage {
            message: m.message,
            category: m.category.unwrap_or_else(|| "general".to_string()),
            priority: m.priority,
            timestamp: DateTime::from_timestamp(m.created_at, 0)
                .unwrap_or_else(Utc::now)
                .format("%Y-%m-%d %H:%M UTC")
                .to_string(),
            acknowledged: m.acknowledged,
            read_at: m.read_at
                .and_then(|ts| DateTime::from_timestamp(ts, 0))
                .map(|dt| dt.format("%Y-%m-%d %H:%M UTC").to_string())
                .unwrap_or_default(),
        }
    }).collect();

    println!("  Found {} messages ({} unread)", total_outbox, unread_count);

    build_outbox_page(
        &config.output.build_directory,
        &config.output.site_title,
        &config.output.author,
        display_messages,
        unread_count,
        total_outbox,
    )?;

    println!("\n✓ Static site built successfully!");
    println!("  Output: {:?}", config.output.build_directory);
    println!("\nFiles generated:");
    println!("  - index.html (homepage with recent entries)");
    println!("  - thoughts/index.html (cognitive loop stream)");
    println!("  - outbox/index.html (messages for the operator)");
    println!("  - threads/index.html (theme threads listing)");
    println!("  - threads/*.html (individual theme pages)");
    println!("  - predictions/index.html (prediction registry)");
    println!("  - about.html (about page)");
    println!("  - feed.xml (RSS feed)");
    println!("  - style.css (styling)");

    Ok(())
}

fn deploy_command(config: &Config) -> Result<()> {
    use homeforge_chronicle::deploy_to_icp;

    println!("Deploying to Internet Computer Protocol (ICP)");
    println!("Network: {}", config.deployment.network);

    // Verify build directory exists
    if !config.output.build_directory.exists() {
        anyhow::bail!(
            "Build directory not found: {:?}\nRun 'chronicle build' first to generate the static site.",
            config.output.build_directory
        );
    }

    // Determine canister ID
    let canister_id = if config.deployment.canister_id.is_empty() {
        None
    } else {
        Some(config.deployment.canister_id.as_str())
    };

    // Deploy
    let result = deploy_to_icp(
        &config.output.build_directory,
        &config.deployment.network,
        canister_id,
    )?;

    // Display results
    println!("\n✓ Deployment successful!");
    println!("  Canister ID: {}", result.canister_id);
    println!("  Network: {}", result.network);
    println!("  URL: {}", result.url);
    println!("\nYour chronicle is now live at:");
    println!("  {}", result.url);

    Ok(())
}

fn watch_command(config: &Config) -> Result<()> {
    use homeforge_chronicle::{watch_directory, PipelineOptions};

    // Build pipeline options from config (use defaults then override)
    let options = PipelineOptions {
        extract: true,
        compile: true,
        build: true,
        deploy: config.schedule.auto_deploy,
        notify: config.schedule.notify_on_completion.unwrap_or(false),
        metabolize: true,
        generate_embeddings: true,
        sync_backend: true,
        ..PipelineOptions::default()
    };

    // Start watching
    watch_directory(&config.input.watch_directory, config, options)?;

    Ok(())
}

// ============================================================
// Knowledge Capsule Commands (KIP Integration)
// ============================================================

fn capsules_command(config: &Config, limit: Option<usize>, conversation_id: Option<String>) -> Result<()> {
    use homeforge_chronicle::{extract_capsules, parse_bulk_export};
    use homeforge_chronicle::llm::ClaudeClient;

    println!("Extracting knowledge capsules...");

    let db = Database::new(&config.input.processed_db)?;

    // Initialize LLM client
    let llm_client = ClaudeClient::new(
        config.extraction.claude_api_key.clone().unwrap_or_default(),
        config.extraction.llm_model.clone(),
    )?;

    // Get conversations to process
    let conversations_to_process: Vec<String> = if let Some(id) = conversation_id {
        vec![id]
    } else {
        // Get all conversations that haven't been capsulized yet
        let mut stmt = db.connection().prepare(
            "SELECT c.id FROM conversations c
             WHERE c.id NOT IN (SELECT DISTINCT conversation_id FROM knowledge_capsules)
             ORDER BY c.processed_at DESC"
        )?;
        let conv_ids: Vec<String> = stmt.query_map([], |row| row.get(0))?
            .collect::<Result<Vec<_>, _>>()?;

        if let Some(l) = limit {
            conv_ids.into_iter().take(l).collect()
        } else {
            conv_ids
        }
    };

    if conversations_to_process.is_empty() {
        println!("No new conversations to process.");
        return Ok(());
    }

    println!("Processing {} conversation(s)...\n", conversations_to_process.len());

    // Find the export file and re-parse conversations
    let export_file = config.input.watch_directory.join("conversations.json");
    if !export_file.exists() {
        anyhow::bail!("Export file not found: {:?}", export_file);
    }

    let bulk_export = parse_bulk_export(&export_file)?;
    let mut total_capsules = 0;

    for conv_id in &conversations_to_process {
        // Find conversation in export
        let conversation = bulk_export.iter().find(|c| &c.uuid == conv_id);

        if let Some(conv) = conversation {
            print!("Processing '{}' ({} messages)... ", conv.name, conv.message_count());

            match extract_capsules(conv, &llm_client) {
                Ok(capsules) => {
                    if capsules.is_empty() {
                        println!("skipped (too short)");
                        continue;
                    }

                    // Store capsules in database
                    for capsule in &capsules {
                        let entities: Vec<(String, Option<String>)> = capsule.entities.clone();

                        db.insert_knowledge_capsule(
                            conv_id,
                            &capsule.restatement,
                            capsule.timestamp.as_deref(),
                            capsule.location.as_deref(),
                            capsule.topic.as_deref(),
                            0.8, // Default confidence
                            &capsule.persons,
                            &entities,
                            &capsule.keywords,
                        )?;
                    }

                    println!("{} capsules extracted", capsules.len());
                    total_capsules += capsules.len();
                }
                Err(e) => {
                    println!("error: {}", e);
                }
            }
        } else {
            println!("Warning: Conversation {} not found in export file", conv_id);
        }
    }

    println!("\n✓ Extraction complete!");
    println!("  Total capsules created: {}", total_capsules);
    println!("  Run 'chronicle capsule-stats' to see statistics");

    Ok(())
}

fn capsule_stats_command(config: &Config) -> Result<()> {
    println!("Knowledge Capsule Statistics\n");

    let db = Database::new(&config.input.processed_db)?;

    // Get capsule count
    let capsule_count = db.get_capsule_count()?;
    println!("Total capsules: {}", capsule_count);

    // Get active capsules (not consolidated)
    let active = db.get_active_capsules(5)?;
    println!("Active capsules: {}", active.len());

    // Get pattern count
    let patterns = db.get_active_patterns(0.0, 100)?;
    println!("Consolidation patterns: {}\n", patterns.len());

    // Show recent capsules
    if !active.is_empty() {
        println!("Recent capsules:");
        for (id, restatement, timestamp, topic, confidence) in active.iter().take(5) {
            println!("  [{}] {}", id, truncate_string(restatement, 60));
            if let Some(t) = topic {
                println!("      Topic: {} | Confidence: {:.2}", t, confidence);
            }
            if let Some(ts) = timestamp {
                println!("      Time: {}", ts);
            }
        }
    }

    // Show patterns if any
    if !patterns.is_empty() {
        println!("\nTop patterns:");
        for (id, summary, count, confidence) in patterns.iter().take(5) {
            println!("  [{}] {} (seen {}x, confidence: {:.2})", id, summary, count, confidence);
        }
    }

    Ok(())
}

fn search_capsules_command(config: &Config, query: &str, search_type: &str, limit: i64) -> Result<()> {
    println!("Searching capsules for '{}' (type: {})\n", query, search_type);

    let db = Database::new(&config.input.processed_db)?;

    let results = match search_type {
        "person" => {
            db.search_capsules_by_person(query, limit)?
        }
        "keyword" | _ => {
            let keywords: Vec<String> = query.split_whitespace().map(String::from).collect();
            db.search_capsules_by_keyword(&keywords, limit)?
        }
    };

    if results.is_empty() {
        println!("No capsules found.");
        return Ok(());
    }

    println!("Found {} result(s):\n", results.len());

    for (id, restatement, confidence) in results {
        println!("[{}] (confidence: {:.2})", id, confidence);
        println!("  {}\n", restatement);
    }

    Ok(())
}

fn truncate_string(s: &str, max_len: usize) -> String {
    if s.len() <= max_len {
        s.to_string()
    } else {
        format!("{}...", &s[..max_len])
    }
}

fn embeddings_command(config: &Config, ollama_url: &str, model: &str, limit: Option<usize>, force: bool) -> Result<()> {
    use homeforge_chronicle::OllamaEmbedding;

    println!("Generating embeddings via Ollama...");
    println!("  URL: {}", ollama_url);
    println!("  Model: {}", model);
    if force {
        println!("  Mode: FORCE (re-embedding all capsules)");
    }
    println!();

    let db = Database::new(&config.input.processed_db)?;

    // Get capsules - either all (force) or only those without embeddings
    let capsules = if force {
        db.get_all_capsules_for_embedding(limit)?
    } else {
        db.get_capsules_without_embeddings(limit)?
    };

    if capsules.is_empty() {
        if force {
            println!("No capsules found in database.");
        } else {
            println!("All capsules already have embeddings. Use --force to re-embed.");
        }
        return Ok(());
    }

    println!("Processing {} capsule(s)...\n", capsules.len());

    // Initialize embedding client
    let embedder = OllamaEmbedding::new(ollama_url, model)?;

    let mut success_count = 0;
    let mut error_count = 0;

    for (i, (capsule_id, restatement)) in capsules.iter().enumerate() {
        print!("[{}/{}] Capsule {}... ", i + 1, capsules.len(), capsule_id);

        match embedder.embed(restatement) {
            Ok(embedding) => {
                // Store/update embedding in database
                db.upsert_capsule_embedding(*capsule_id, &embedding, model)?;
                println!("ok ({} dims)", embedding.len());
                success_count += 1;
            }
            Err(e) => {
                println!("error: {}", e);
                error_count += 1;
            }
        }
    }

    println!("\n✓ Embedding generation complete!");
    println!("  Success: {}", success_count);
    println!("  Errors: {}", error_count);
    println!("  Total embeddings: {}", db.get_embedding_count()?);

    Ok(())
}

fn semantic_search_command(config: &Config, query: &str, limit: usize, ollama_url: &str, model: &str) -> Result<()> {
    use homeforge_chronicle::{find_top_k_similar, OllamaEmbedding};

    println!("Semantic search for: \"{}\"\n", query);

    let db = Database::new(&config.input.processed_db)?;

    // Check if we have embeddings
    let embedding_count = db.get_embedding_count()?;
    if embedding_count == 0 {
        println!("No embeddings found. Run 'chronicle embeddings' first.");
        return Ok(());
    }

    // Initialize embedding client and embed query
    print!("Embedding query... ");
    let embedder = OllamaEmbedding::new(ollama_url, model)?;
    let query_embedding = embedder.embed(query)?;
    println!("ok ({} dims)", query_embedding.len());

    // Load all embeddings
    print!("Loading {} embeddings... ", embedding_count);
    let embeddings = db.get_all_embeddings()?;
    println!("ok");

    // Find similar capsules
    print!("Finding similar capsules... ");
    let similar = find_top_k_similar(&query_embedding, &embeddings, limit);
    println!("found {}\n", similar.len());

    if similar.is_empty() {
        println!("No similar capsules found.");
        return Ok(());
    }

    println!("Results:\n");

    for (i, (capsule_id, similarity)) in similar.iter().enumerate() {
        if let Some((restatement, timestamp, topic, _confidence)) = db.get_capsule_display_info(*capsule_id)? {
            println!("{}. [similarity: {:.3}]", i + 1, similarity);
            println!("   {}", restatement);
            if let Some(t) = topic {
                println!("   Topic: {}", t);
            }
            if let Some(ts) = timestamp {
                println!("   Time: {}", ts);
            }
            println!();
        }
    }

    Ok(())
}

fn sync_command(config: &Config, canister_id: &str, identity: &str, capsules_only: bool, embeddings_only: bool) -> Result<()> {
    use std::process::Command;

    println!("Syncing to ICP canister...");
    println!("  Canister: {}", canister_id);
    println!("  Identity: {}\n", identity);

    let db = Database::new(&config.input.processed_db)?;

    // Get counts from canister
    print!("Checking canister state... ");
    let output = Command::new("dfx")
        .args(["canister", "call", canister_id, "get_capsule_count", "--network", "ic", "--identity", identity])
        .env("DFX_WARNING", "-mainnet_plaintext_identity")
        .output()?;

    let canister_count: i64 = if output.status.success() {
        let stdout = String::from_utf8_lossy(&output.stdout);
        // Parse "(123 : nat64)" format
        stdout.trim()
            .trim_start_matches('(')
            .split_whitespace()
            .next()
            .and_then(|s| s.parse().ok())
            .unwrap_or(0)
    } else {
        println!("warning: couldn't get count");
        0
    };
    println!("{} capsules on canister", canister_count);

    // Get local counts
    let local_capsule_count = db.get_capsule_count()?;
    let local_embedding_count = db.get_embedding_count()?;

    println!("Local database: {} capsules, {} embeddings\n", local_capsule_count, local_embedding_count);

    // Sync capsules
    if !embeddings_only {
        println!("Syncing capsules...");

        // Get all capsules from local DB
        let capsules = db.get_all_capsules_for_sync()?;

        if capsules.is_empty() {
            println!("  No capsules to sync.");
        } else {
            let mut success = 0;
            let mut errors = 0;

            for (i, capsule) in capsules.iter().enumerate() {
                print!("\r  [{}/{}] Syncing capsule {}... ", i + 1, capsules.len(), capsule.id);

                // Build the dfx call
                let persons_arg = format!("vec {{ {} }}",
                    capsule.persons.iter().map(|p| format!("\"{}\"", p.replace('"', "\\\""))).collect::<Vec<_>>().join("; "));
                let entities_arg = format!("vec {{ {} }}",
                    capsule.entities.iter().map(|e| format!("\"{}\"", e.replace('"', "\\\""))).collect::<Vec<_>>().join("; "));
                let keywords_arg = format!("vec {{ {} }}",
                    capsule.keywords.iter().map(|k| format!("\"{}\"", k.replace('"', "\\\""))).collect::<Vec<_>>().join("; "));

                let timestamp_arg = capsule.timestamp.as_ref()
                    .map(|t| format!("opt \"{}\"", t.replace('"', "\\\"")))
                    .unwrap_or_else(|| "null".to_string());
                let location_arg = capsule.location.as_ref()
                    .map(|l| format!("opt \"{}\"", l.replace('"', "\\\"")))
                    .unwrap_or_else(|| "null".to_string());
                let topic_arg = capsule.topic.as_ref()
                    .map(|t| format!("opt \"{}\"", t.replace('"', "\\\"")))
                    .unwrap_or_else(|| "null".to_string());

                let args = format!(
                    "(\"{}\", \"{}\", {}, {}, {}, {:.2} : float64, {}, {}, {})",
                    capsule.conversation_id.replace('"', "\\\""),
                    capsule.restatement.replace('"', "\\\"").replace('\n', " "),
                    timestamp_arg,
                    location_arg,
                    topic_arg,
                    capsule.confidence_score,
                    persons_arg,
                    entities_arg,
                    keywords_arg
                );

                let result = Command::new("dfx")
                    .args(["canister", "call", canister_id, "add_capsule", "--network", "ic", "--identity", identity])
                    .arg(&args)
                    .env("DFX_WARNING", "-mainnet_plaintext_identity")
                    .output();

                match result {
                    Ok(output) if output.status.success() => {
                        success += 1;
                    }
                    Ok(output) => {
                        errors += 1;
                        let stderr = String::from_utf8_lossy(&output.stderr);
                        if errors <= 3 {
                            println!("\n    Error: {}", stderr.lines().next().unwrap_or("unknown"));
                        }
                    }
                    Err(e) => {
                        errors += 1;
                        if errors <= 3 {
                            println!("\n    Error: {}", e);
                        }
                    }
                }
            }

            println!("\r  Capsules: {} synced, {} errors                    ", success, errors);
        }
    }

    // Sync embeddings
    if !capsules_only {
        println!("\nSyncing embeddings...");

        let embeddings = db.get_all_embeddings()?;

        if embeddings.is_empty() {
            println!("  No embeddings to sync.");
        } else {
            let mut success = 0;
            let mut errors = 0;

            for (i, (capsule_id, embedding)) in embeddings.iter().enumerate() {
                print!("\r  [{}/{}] Syncing embedding for capsule {}... ", i + 1, embeddings.len(), capsule_id);

                // Convert embedding to Candid vec format
                let embedding_arg = format!("vec {{ {} }}",
                    embedding.iter().map(|v| format!("{:.6} : float32", v)).collect::<Vec<_>>().join("; "));

                let args = format!(
                    "({} : nat64, {}, \"gemma2:2b\")",
                    capsule_id,
                    embedding_arg
                );

                let result = Command::new("dfx")
                    .args(["canister", "call", canister_id, "add_embedding", "--network", "ic", "--identity", identity])
                    .arg(&args)
                    .env("DFX_WARNING", "-mainnet_plaintext_identity")
                    .output();

                match result {
                    Ok(output) if output.status.success() => {
                        success += 1;
                    }
                    Ok(_) | Err(_) => {
                        errors += 1;
                    }
                }
            }

            println!("\r  Embeddings: {} synced, {} errors                    ", success, errors);
        }
    }

    // Final status check
    println!("\nChecking final canister state...");
    let output = Command::new("dfx")
        .args(["canister", "call", canister_id, "health", "--network", "ic", "--identity", identity])
        .env("DFX_WARNING", "-mainnet_plaintext_identity")
        .output()?;

    if output.status.success() {
        let stdout = String::from_utf8_lossy(&output.stdout);
        println!("  {}", stdout.trim());
    }

    println!("\n✓ Sync complete!");

    Ok(())
}

fn metabolize_command(config: &Config, ollama_url: &str, model: &str, with_decay: bool, detect_meta: bool) -> Result<()> {
    use homeforge_chronicle::{MetabolismConfig, metabolize_all_new, decay_patterns, detect_meta_patterns};
    use homeforge_chronicle::OllamaEmbedding;

    println!("Memory Metabolism");
    println!("=================\n");

    let db = Database::new(&config.input.processed_db)?;
    let embedding_client = OllamaEmbedding::new(ollama_url, model)?;
    let metabolism_config = MetabolismConfig::default();

    // Run metabolism on new capsules
    println!("Processing unmetabolized capsules...");
    let result = metabolize_all_new(&db, &embedding_client, &metabolism_config)?;

    println!("  Processed: {} capsules", result.processed);
    println!("  Patterns reinforced: {}", result.patterns_reinforced);
    println!("  New patterns seeded: {}", result.patterns_seeded);
    if result.errors > 0 {
        println!("  Errors: {}", result.errors);
    }

    // Optionally run decay
    if with_decay {
        println!("\nApplying decay to stale patterns...");
        let decay_result = decay_patterns(&db, &metabolism_config)?;
        println!("  Decayed: {} patterns", decay_result.decayed);
        println!("  Deactivated: {} patterns", decay_result.deactivated);
    }

    // Optionally detect meta-patterns
    if detect_meta {
        println!("\nDetecting meta-patterns...");
        let meta_patterns = detect_meta_patterns(&db, &embedding_client, &metabolism_config)?;
        if meta_patterns.is_empty() {
            println!("  No meta-patterns detected yet.");
        } else {
            println!("  Found {} meta-patterns:", meta_patterns.len());
            for (i, mp) in meta_patterns.iter().enumerate() {
                println!("\n  {}. {} (confidence: {:.2})", i + 1, mp.summary, mp.confidence);
                println!("     Composed of {} patterns: {:?}", mp.pattern_ids.len(), mp.pattern_ids);
            }
        }
    }

    // Show overall stats
    let pattern_count = db.get_pattern_count()?;
    println!("\n✓ Metabolism complete!");
    println!("  Total active patterns: {}", pattern_count);

    Ok(())
}

fn patterns_command(config: &Config, min_confidence: f64, limit: usize, active_only: bool, id: Option<i64>) -> Result<()> {
    let db = Database::new(&config.input.processed_db)?;

    if let Some(pattern_id) = id {
        // Show specific pattern with linked capsules
        if let Some((summary, confidence, reinforced_at, is_active)) = db.get_pattern_by_id(pattern_id)? {
            println!("Pattern #{}", pattern_id);
            println!("===========");
            println!("Summary: {}", summary);
            println!("Confidence: {:.2}", confidence);
            println!("Active: {}", if is_active { "yes" } else { "no" });
            if let Some(ts) = reinforced_at {
                let dt = chrono::DateTime::from_timestamp(ts, 0)
                    .map(|d| d.format("%Y-%m-%d %H:%M").to_string())
                    .unwrap_or_else(|| ts.to_string());
                println!("Last reinforced: {}", dt);
            }

            // Get linked capsules
            let linked = db.get_pattern_capsules(pattern_id)?;
            if !linked.is_empty() {
                println!("\nLinked capsules ({}):", linked.len());
                for (capsule_id, restatement) in linked.iter().take(10) {
                    let truncated = if restatement.len() > 80 {
                        format!("{}...", &restatement[..80])
                    } else {
                        restatement.clone()
                    };
                    println!("  [{}] {}", capsule_id, truncated);
                }
                if linked.len() > 10 {
                    println!("  ... and {} more", linked.len() - 10);
                }
            }
        } else {
            println!("Pattern #{} not found.", pattern_id);
        }
    } else {
        // List patterns
        let patterns = db.get_all_patterns(min_confidence, limit, active_only)?;

        if patterns.is_empty() {
            println!("No patterns found.");
            println!("\nRun 'chronicle metabolize' to process capsules into patterns.");
            return Ok(());
        }

        println!("Knowledge Patterns");
        println!("==================\n");

        for (pattern_id, summary, capsule_count, confidence, is_active) in &patterns {
            let status = if *is_active { "" } else { " [inactive]" };
            let truncated = if summary.len() > 60 {
                format!("{}...", &summary[..60])
            } else {
                summary.clone()
            };
            println!("#{:<4} ({} capsules, conf: {:.2}){}", pattern_id, capsule_count, confidence, status);
            println!("      {}\n", truncated);
        }

        let total = db.get_pattern_count()? as usize;
        if total > patterns.len() {
            println!("Showing {} of {} patterns. Use --limit to see more.", patterns.len(), total);
        }
    }

    Ok(())
}

fn sync_native_command(config: &Config, canister_id: &str, identity: &str, batch_size: usize) -> Result<()> {
    use homeforge_chronicle::icp::{IcpClient, KnowledgeCapsule, CapsuleEmbedding, sync_to_canister};

    println!("Native ICP Sync");
    println!("===============\n");
    println!("Canister: {}", canister_id);
    println!("Identity: {}", identity);
    println!("Batch size: {}\n", batch_size);

    let db = Database::new(&config.input.processed_db)?;

    // Create async runtime
    let rt = tokio::runtime::Runtime::new()?;

    rt.block_on(async {
        // Connect to ICP
        println!("Connecting to ICP...");
        let client = IcpClient::from_dfx_identity(canister_id, identity).await?;

        // Check canister health
        let health = client.health().await?;
        println!("Canister status: {}\n", health);

        // Get local capsules
        let local_capsules = db.get_all_capsules_for_sync()?;
        let local_embeddings = db.get_all_embeddings()?;

        println!("Local data: {} capsules, {} embeddings", local_capsules.len(), local_embeddings.len());

        if local_capsules.is_empty() {
            println!("No capsules to sync.");
            return Ok(());
        }

        // Convert to ICP types
        let capsules: Vec<KnowledgeCapsule> = local_capsules.into_iter().map(|c| {
            KnowledgeCapsule {
                id: 0, // Will be assigned by canister
                conversation_id: c.conversation_id,
                restatement: c.restatement,
                timestamp: c.timestamp,
                location: c.location,
                topic: c.topic,
                confidence_score: c.confidence_score,
                persons: c.persons,
                entities: c.entities,
                keywords: c.keywords,
                created_at: 0, // Will be set by canister
            }
        }).collect();

        let embeddings: Vec<CapsuleEmbedding> = local_embeddings.into_iter().map(|(capsule_id, embedding)| {
            CapsuleEmbedding {
                capsule_id: capsule_id as u64,
                embedding,
                model_name: "gemma2:2b".to_string(),
            }
        }).collect();

        // Sync
        println!("\nSyncing...");
        let result = sync_to_canister(&client, capsules, embeddings, batch_size).await?;

        println!("\n✓ Sync complete!");
        println!("  Capsules synced: {}", result.capsules_synced);
        println!("  Embeddings synced: {}", result.embeddings_synced);

        if !result.errors.is_empty() {
            println!("  Errors:");
            for err in &result.errors {
                println!("    - {}", err);
            }
        }

        // Final status
        let final_health = client.health().await?;
        println!("\nFinal canister status: {}", final_health);

        Ok::<(), anyhow::Error>(())
    })?;

    Ok(())
}

fn onchain_search_command(
    query: &str,
    limit: u64,
    canister_id: &str,
    identity: &str,
    ollama_url: &str,
    model: &str,
) -> Result<()> {
    use homeforge_chronicle::icp::IcpClient;
    use homeforge_chronicle::OllamaEmbedding;

    println!("On-Chain Semantic Search");
    println!("========================\n");
    println!("Query: \"{}\"\n", query);

    // Generate query embedding
    println!("Generating query embedding...");
    let embedding_client = OllamaEmbedding::new(ollama_url, model)?;
    let query_embedding = embedding_client.embed(query)?;
    println!("  ✓ Generated {}-dimensional embedding\n", query_embedding.len());

    // Create async runtime
    let rt = tokio::runtime::Runtime::new()?;

    rt.block_on(async {
        // Connect to ICP
        println!("Querying canister...");
        let client = IcpClient::from_dfx_identity(canister_id, identity).await?;

        // Perform on-chain semantic search
        let results = client.semantic_search(query_embedding, limit).await?;

        if results.is_empty() {
            println!("No results found.");
            return Ok(());
        }

        println!("Found {} results:\n", results.len());

        for (i, result) in results.iter().enumerate() {
            println!("{}. [similarity: {:.3}]", i + 1, result.score);
            let restatement = if result.capsule.restatement.len() > 100 {
                format!("{}...", &result.capsule.restatement[..100])
            } else {
                result.capsule.restatement.clone()
            };
            println!("   {}", restatement);
            if let Some(topic) = &result.capsule.topic {
                println!("   Topic: {}", topic);
            }
            println!();
        }

        Ok::<(), anyhow::Error>(())
    })?;

    Ok(())
}

fn refresh_patterns_command(config: &Config, id: Option<i64>, min_capsules: i64, dry_run: bool) -> Result<()> {
    use std::collections::HashMap;

    println!("Pattern Summary Refresh");
    println!("=======================\n");

    if dry_run {
        println!("(DRY RUN - no changes will be made)\n");
    }

    let db = Database::new(&config.input.processed_db)?;

    // Get patterns to refresh
    let patterns = db.get_active_patterns(0.0, 1000)?;

    let mut refreshed = 0;
    let mut skipped = 0;

    for (pattern_id, old_summary, capsule_count, confidence) in patterns {
        // Skip if filtering by ID
        if let Some(target_id) = id {
            if pattern_id != target_id {
                continue;
            }
        }

        // Skip if too few capsules
        if capsule_count < min_capsules {
            skipped += 1;
            continue;
        }

        // Get capsules for this pattern (sample up to 20)
        let capsules = db.get_capsules_for_pattern(pattern_id, 20)?;

        if capsules.is_empty() {
            skipped += 1;
            continue;
        }

        // Count topic frequencies
        let mut topic_counts: HashMap<String, usize> = HashMap::new();
        for (_id, _restatement, topic) in &capsules {
            if let Some(t) = topic {
                *topic_counts.entry(t.clone()).or_insert(0) += 1;
            }
        }

        // Find the most common topic(s)
        let mut sorted_topics: Vec<_> = topic_counts.into_iter().collect();
        sorted_topics.sort_by(|a, b| b.1.cmp(&a.1));

        // Check if topics are useful (not too fragmented)
        // If top topic appears in less than 10% of total capsules, topics are too fragmented
        // Use capsule_count (total) not capsules.len() (sampled)
        let topics_useful = !sorted_topics.is_empty() &&
            (sorted_topics[0].1 as f64 / capsule_count as f64) >= 0.1;

        // Generate new summary
        let new_summary = if topics_useful {
            // Topics are consistent enough to use
            if sorted_topics.len() == 1 || sorted_topics[0].1 > sorted_topics.get(1).map(|x| x.1).unwrap_or(0) * 2 {
                sorted_topics[0].0.clone()
            } else {
                let top_topics: Vec<&str> = sorted_topics.iter()
                    .take(3)
                    .map(|(t, _)| t.as_str())
                    .collect();
                top_topics.join(" + ")
            }
        } else {
            // Topics too fragmented - fall back to keywords
            let keywords = db.get_keywords_for_pattern(pattern_id, 5)?;
            if keywords.is_empty() {
                // No keywords either - use truncated restatement
                let first_restatement = &capsules[0].1;
                if first_restatement.len() > 60 {
                    format!("{}...", &first_restatement[..60])
                } else {
                    first_restatement.clone()
                }
            } else {
                // Use top keywords as summary
                let kw_list: Vec<&str> = keywords.iter()
                    .take(4)
                    .map(|(k, _)| k.as_str())
                    .collect();
                kw_list.join(", ")
            }
        };

        // Check if summary would change
        if new_summary == old_summary {
            continue;
        }

        println!("Pattern #{} ({} capsules, confidence {:.2})", pattern_id, capsule_count, confidence);
        println!("  Old: {}", old_summary);
        println!("  New: {}", new_summary);

        if !dry_run {
            db.update_pattern_summary(pattern_id, &new_summary)?;
            println!("  ✓ Updated");
        }
        println!();

        refreshed += 1;
    }

    println!("Summary:");
    println!("  Patterns refreshed: {}", refreshed);
    println!("  Patterns skipped: {}", skipped);

    if dry_run && refreshed > 0 {
        println!("\nRun without --dry-run to apply changes.");
    }

    Ok(())
}
