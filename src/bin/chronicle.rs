use anyhow::Result;
use clap::Parser;
use homeforge_chronicle::cli::{Cli, Commands};
use homeforge_chronicle::config::Config;
use homeforge_chronicle::db::Database;
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
    use homeforge_chronicle::{build_site, DisplayEntry};
    use homeforge_chronicle::compilation::{Prediction, PredictionStatus};
    use chrono::{DateTime, Utc};

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

    // Build the site
    println!("\nGenerating static site...");
    build_site(
        &config.output.build_directory,
        &config.output.site_title,
        &config.output.author,
        recent_entries,
        threads,
        predictions,
    )?;

    println!("\n✓ Static site built successfully!");
    println!("  Output: {:?}", config.output.build_directory);
    println!("\nFiles generated:");
    println!("  - index.html (homepage with recent entries)");
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

    // Build pipeline options from config
    let options = PipelineOptions {
        extract: true,
        compile: true,
        build: true,
        deploy: config.schedule.auto_deploy,
        notify: config.schedule.notify_on_completion.unwrap_or(false),
    };

    // Start watching
    watch_directory(&config.input.watch_directory, config, options)?;

    Ok(())
}
