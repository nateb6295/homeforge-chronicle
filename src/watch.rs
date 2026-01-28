use anyhow::{Context, Result};
use notify_debouncer_mini::{new_debouncer, notify::RecursiveMode, DebounceEventResult};
use std::path::{Path, PathBuf};
use std::sync::mpsc::channel;
use std::time::Duration;

use crate::config::Config;
use crate::db::Database;

/// Pipeline orchestration options
#[derive(Debug, Clone)]
pub struct PipelineOptions {
    /// Run extraction after ingestion
    pub extract: bool,
    /// Run compilation after extraction
    pub compile: bool,
    /// Build static site after compilation
    pub build: bool,
    /// Deploy to ICP after build
    pub deploy: bool,
    /// Send notifications
    pub notify: bool,
    /// Run metabolism after ingestion (pattern evolution)
    pub metabolize: bool,
    /// Ollama URL for embedding generation
    pub ollama_url: String,
    /// Embedding model name
    pub embedding_model: String,
    /// Generate embeddings for unembedded capsules
    pub generate_embeddings: bool,
    /// Sync capsules and embeddings to backend canister
    pub sync_backend: bool,
    /// Backend canister ID for sync
    pub backend_canister_id: String,
    /// DFX identity for backend sync
    pub backend_identity: String,
}

impl Default for PipelineOptions {
    fn default() -> Self {
        Self {
            extract: true,
            compile: true,
            build: true,
            deploy: false, // Don't auto-deploy frontend by default
            notify: false,
            metabolize: true, // Run metabolism by default
            ollama_url: "http://localhost:11434".to_string(),
            embedding_model: "mxbai-embed-large".to_string(), // Better model
            generate_embeddings: true, // Generate embeddings for capsules
            sync_backend: true, // Sync to backend canister
            backend_canister_id: "fqqku-bqaaa-aaaai-q4wha-cai".to_string(),
            backend_identity: "chronicle-auto".to_string(),
        }
    }
}

/// Watch result
#[derive(Debug)]
pub enum WatchResult {
    FileProcessed {
        file_path: PathBuf,
        conversation_id: String,
    },
    BulkProcessed {
        file_path: PathBuf,
        ingested: usize,
        duplicates: usize,
        skipped: usize,
    },
    FileIgnored {
        file_path: PathBuf,
        reason: String,
    },
    PipelineError {
        file_path: PathBuf,
        error: String,
    },
}

/// Start watching the export directory for new conversation files
pub fn watch_directory<P: AsRef<Path>>(
    watch_dir: P,
    config: &Config,
    options: PipelineOptions,
) -> Result<()> {
    let watch_dir = watch_dir.as_ref();

    println!("Starting Chronicle file watcher");
    println!("Watching directory: {:?}", watch_dir);
    println!("Pipeline settings:");
    println!("  Extract: {}", options.extract);
    println!("  Compile: {}", options.compile);
    println!("  Build: {}", options.build);
    println!("  Deploy (frontend): {}", options.deploy);
    println!("  Notify: {}", options.notify);
    println!("  Metabolize: {}", options.metabolize);
    println!("  Generate embeddings: {}", options.generate_embeddings);
    println!("  Sync backend: {}", options.sync_backend);
    if options.sync_backend {
        println!("    Canister: {}", options.backend_canister_id);
        println!("    Identity: {}", options.backend_identity);
    }
    println!("\nPress Ctrl+C to stop watching...\n");

    // Ensure watch directory exists
    if !watch_dir.exists() {
        std::fs::create_dir_all(watch_dir)
            .context("Failed to create watch directory")?;
        println!("Created watch directory: {:?}", watch_dir);
    }

    // Create channel for file system events
    let (tx, rx) = channel();

    // Create debouncer to avoid processing files multiple times
    let mut debouncer = new_debouncer(Duration::from_secs(2), tx)
        .context("Failed to create file watcher")?;

    // Start watching the directory
    debouncer
        .watcher()
        .watch(watch_dir, RecursiveMode::NonRecursive)
        .context("Failed to watch directory")?;

    println!("Watcher initialized. Monitoring for new .json files...\n");

    // Process events
    loop {
        match rx.recv() {
            Ok(result) => {
                let events = match result {
                    Ok(events) => events,
                    Err(error) => {
                        eprintln!("Watch error: {:?}", error);
                        continue;
                    }
                };

                for event in events {
                    // Only process create and modify events for JSON files
                    if is_json_file(&event.path) {
                        match process_file(&event.path, config, &options) {
                            Ok(WatchResult::FileProcessed {
                                file_path,
                                conversation_id,
                            }) => {
                                println!(
                                    "\n✓ Successfully processed {:?} ({})",
                                    file_path.file_name().unwrap_or_default(),
                                    conversation_id
                                );

                                if options.notify {
                                    send_notification(
                                        "Chronicle Processing Complete",
                                        &format!("Processed conversation {}", conversation_id),
                                    );
                                }
                            }
                            Ok(WatchResult::BulkProcessed {
                                file_path,
                                ingested,
                                duplicates,
                                skipped,
                            }) => {
                                println!(
                                    "\n✓ Bulk processed {:?}: {} new, {} duplicates, {} skipped",
                                    file_path.file_name().unwrap_or_default(),
                                    ingested,
                                    duplicates,
                                    skipped
                                );

                                if options.notify && ingested > 0 {
                                    send_notification(
                                        "Chronicle Bulk Processing Complete",
                                        &format!("Ingested {} new conversations", ingested),
                                    );
                                }
                            }
                            Ok(WatchResult::FileIgnored { file_path, reason }) => {
                                println!(
                                    "⊘ Ignored {:?}: {}",
                                    file_path.file_name().unwrap_or_default(),
                                    reason
                                );
                            }
                            Ok(WatchResult::PipelineError { file_path, error }) => {
                                eprintln!(
                                    "\n✗ Error processing {:?}: {}",
                                    file_path.file_name().unwrap_or_default(),
                                    error
                                );

                                if options.notify {
                                    send_notification(
                                        "Chronicle Processing Error",
                                        &format!("Failed to process conversation: {}", error),
                                    );
                                }
                            }
                            Err(e) => {
                                eprintln!("Error handling file event: {}", e);
                            }
                        }
                    }
                }
            }
            Err(e) => {
                eprintln!("Watch channel error: {}", e);
                break;
            }
        }
    }

    Ok(())
}

/// Check if a path is a conversation JSON file (filters out non-conversation exports)
fn is_json_file(path: &Path) -> bool {
    let is_json = path.extension().and_then(|s| s.to_str()) == Some("json");
    if !is_json {
        return false;
    }

    // Filter out known non-conversation files from Claude exports
    let filename = path.file_name().and_then(|s| s.to_str()).unwrap_or("");

    // Skip Zone.Identifier files (Windows security markers)
    if filename.contains(":Zone.Identifier") || filename.ends_with(".Identifier") {
        return false;
    }

    // Skip other Claude export files that aren't conversations
    let skip_files = ["memories.json", "projects.json", "users.json"];
    if skip_files.contains(&filename) {
        return false;
    }

    true
}

/// Check if a JSON file is a bulk export (array of conversations)
fn is_bulk_export(path: &Path) -> Result<bool> {
    let mut file = std::fs::File::open(path)?;
    let mut first_byte = [0u8; 1];
    use std::io::Read;
    file.read_exact(&mut first_byte)?;
    Ok(first_byte[0] == b'[')
}

/// Process a single file through the pipeline
fn process_file(
    file_path: &Path,
    config: &Config,
    options: &PipelineOptions,
) -> Result<WatchResult> {
    println!("\n[{}] Detected new file: {:?}",
        chrono::Local::now().format("%Y-%m-%d %H:%M:%S"),
        file_path.file_name().unwrap_or_default()
    );

    // Check if this is a bulk export
    let is_bulk = is_bulk_export(file_path).unwrap_or(false);

    if is_bulk {
        process_bulk_file(file_path, config, options)
    } else {
        process_single_file(file_path, config, options)
    }
}

/// Process a bulk export file (array of conversations)
fn process_bulk_file(
    file_path: &Path,
    config: &Config,
    options: &PipelineOptions,
) -> Result<WatchResult> {
    use crate::parser::parse_bulk_export;

    // Open database
    let db = Database::new(&config.input.processed_db)?;

    // Step 1: Parse and ingest all conversations
    println!("  [1/{}] Ingesting bulk export...", if options.deploy { 5 } else { 4 });

    let conversations = parse_bulk_export(file_path)
        .context("Failed to parse bulk conversation export")?;

    let total = conversations.len();
    println!("    Found {} conversations", total);

    let filename = file_path
        .file_name()
        .and_then(|f| f.to_str())
        .unwrap_or("conversations.json");

    let mut ingested = 0;
    let mut duplicates = 0;
    let mut skipped = 0;

    for (i, conversation) in conversations.iter().enumerate() {
        let result = crate::ingest_conversation_export(&db, conversation, filename)?;
        match result {
            crate::IngestResult::Ingested { message_count, .. } => {
                println!("    [{}/{}] ✓ {} ({} messages)",
                    i + 1, total,
                    conversation.name.chars().take(40).collect::<String>(),
                    message_count
                );
                ingested += 1;
            }
            crate::IngestResult::Duplicate { .. } => {
                duplicates += 1;
            }
            crate::IngestResult::Skipped { .. } => {
                skipped += 1;
            }
        }
    }

    println!("    Summary: {} ingested, {} duplicates, {} skipped", ingested, duplicates, skipped);

    // If nothing new was ingested, skip the rest of the pipeline
    if ingested == 0 {
        return Ok(WatchResult::BulkProcessed {
            file_path: file_path.to_path_buf(),
            ingested,
            duplicates,
            skipped,
        });
    }

    // Step 2: Metabolize (if enabled) - run pattern evolution on new capsules
    if options.metabolize {
        println!("  [2/{}] Running metabolism (pattern evolution)...", pipeline_steps(options));

        match run_metabolism(&db, &options.ollama_url, &options.embedding_model) {
            Ok((reinforced, seeded)) => {
                println!("    ✓ Metabolized: {} patterns reinforced, {} new patterns", reinforced, seeded);
            }
            Err(e) => {
                // Metabolism errors are non-fatal - log and continue
                eprintln!("    ⚠ Metabolism warning: {}", e);
            }
        }
    }

    // Step 3: Generate embeddings (if enabled) - embed any capsules without embeddings
    if options.generate_embeddings {
        let mut step = 2;
        if options.metabolize { step += 1; }
        println!("  [{}/{}] Generating embeddings for capsules...", step, pipeline_steps(options));

        match run_embedding_generation(&db, &options.ollama_url, &options.embedding_model) {
            Ok(count) => {
                if count > 0 {
                    println!("    ✓ Generated {} embeddings", count);
                } else {
                    println!("    ✓ No new capsules to embed");
                }
            }
            Err(e) => {
                // Embedding errors are non-fatal - log and continue
                eprintln!("    ⚠ Embedding warning: {}", e);
            }
        }
    }

    // Step 4: Sync to backend canister (if enabled)
    if options.sync_backend {
        let mut step = 2;
        if options.metabolize { step += 1; }
        if options.generate_embeddings { step += 1; }
        println!("  [{}/{}] Syncing to backend canister...", step, pipeline_steps(options));

        match run_backend_sync(&db, &options.backend_canister_id, &options.backend_identity) {
            Ok((capsules, embeddings)) => {
                println!("    ✓ Synced {} capsules, {} embeddings", capsules, embeddings);
            }
            Err(e) => {
                // Sync errors are non-fatal - log and continue
                eprintln!("    ⚠ Sync warning: {}", e);
            }
        }
    }

    // Step 5: Compile (if enabled) - skip extraction for bulk (too expensive)
    if options.compile {
        let mut step = 2;
        if options.metabolize { step += 1; }
        if options.generate_embeddings { step += 1; }
        if options.sync_backend { step += 1; }
        println!("  [{}/{}] Compiling digests and threads...", step, pipeline_steps(options));

        if let Err(e) = run_compilation(config) {
            return Ok(WatchResult::PipelineError {
                file_path: file_path.to_path_buf(),
                error: format!("Compilation failed: {}", e),
            });
        }
        println!("    ✓ Compiled");
    }

    // Step 6: Build (if enabled)
    if options.build {
        let mut step = 2;
        if options.metabolize { step += 1; }
        if options.generate_embeddings { step += 1; }
        if options.sync_backend { step += 1; }
        if options.compile { step += 1; }
        println!("  [{}/{}] Building static site...", step, pipeline_steps(options));

        if let Err(e) = run_build(config) {
            return Ok(WatchResult::PipelineError {
                file_path: file_path.to_path_buf(),
                error: format!("Build failed: {}", e),
            });
        }
        println!("    ✓ Built");
    }

    // Step 7: Deploy frontend (if enabled)
    if options.deploy {
        println!("  [{}/{}] Deploying frontend to ICP...", pipeline_steps(options), pipeline_steps(options));

        match run_deployment(config) {
            Ok(url) => {
                println!("    ✓ Deployed to {}", url);
            }
            Err(e) => {
                return Ok(WatchResult::PipelineError {
                    file_path: file_path.to_path_buf(),
                    error: format!("Frontend deployment failed: {}", e),
                });
            }
        }
    }

    Ok(WatchResult::BulkProcessed {
        file_path: file_path.to_path_buf(),
        ingested,
        duplicates,
        skipped,
    })
}

/// Process a single conversation file
fn process_single_file(
    file_path: &Path,
    config: &Config,
    options: &PipelineOptions,
) -> Result<WatchResult> {
    // Open database
    let db = Database::new(&config.input.processed_db)?;

    // Step 1: Ingest
    println!("  [1/{}] Ingesting...", pipeline_steps(options));
    let ingest_result = crate::ingest_conversation(&db, file_path)?;

    let conversation_id = match ingest_result {
        crate::IngestResult::Ingested {
            conversation_id,
            message_count,
        } => {
            println!("    ✓ Ingested {} messages", message_count);
            conversation_id
        }
        crate::IngestResult::Duplicate { conversation_id } => {
            return Ok(WatchResult::FileIgnored {
                file_path: file_path.to_path_buf(),
                reason: format!("Already processed ({})", conversation_id),
            });
        }
        crate::IngestResult::Skipped { reason } => {
            return Ok(WatchResult::FileIgnored {
                file_path: file_path.to_path_buf(),
                reason,
            });
        }
    };

    // Step 2: Extract (if enabled)
    if options.extract {
        println!("  [2/{}] Extracting themes and summaries...", pipeline_steps(options));

        match run_extraction(file_path, &db, config) {
            Ok(extraction_id) => {
                println!("    ✓ Extracted (ID: {})", extraction_id);
            }
            Err(e) => {
                return Ok(WatchResult::PipelineError {
                    file_path: file_path.to_path_buf(),
                    error: format!("Extraction failed: {}", e),
                });
            }
        }
    }

    // Step 3: Metabolize (if enabled) - run pattern evolution
    if options.metabolize {
        let step = if options.extract { 3 } else { 2 };
        println!("  [{}/{}] Running metabolism (pattern evolution)...", step, pipeline_steps(options));

        match run_metabolism(&db, &options.ollama_url, &options.embedding_model) {
            Ok((reinforced, seeded)) => {
                println!("    ✓ Metabolized: {} patterns reinforced, {} new patterns", reinforced, seeded);
            }
            Err(e) => {
                // Metabolism errors are non-fatal - log and continue
                eprintln!("    ⚠ Metabolism warning: {}", e);
            }
        }
    }

    // Step 4: Compile (if enabled)
    if options.compile {
        let mut step = 2;
        if options.extract { step += 1; }
        if options.metabolize { step += 1; }
        println!("  [{}/{}] Compiling digests and threads...", step, pipeline_steps(options));

        if let Err(e) = run_compilation(config) {
            return Ok(WatchResult::PipelineError {
                file_path: file_path.to_path_buf(),
                error: format!("Compilation failed: {}", e),
            });
        }
        println!("    ✓ Compiled");
    }

    // Step 5: Build (if enabled)
    if options.build {
        let mut step = 2;
        if options.extract { step += 1; }
        if options.metabolize { step += 1; }
        if options.compile { step += 1; }
        println!("  [{}/{}] Building static site...", step, pipeline_steps(options));

        if let Err(e) = run_build(config) {
            return Ok(WatchResult::PipelineError {
                file_path: file_path.to_path_buf(),
                error: format!("Build failed: {}", e),
            });
        }
        println!("    ✓ Built");
    }

    // Step 6: Deploy (if enabled)
    if options.deploy {
        println!("  [{}/{}] Deploying to ICP...", pipeline_steps(options), pipeline_steps(options));

        match run_deployment(config) {
            Ok(url) => {
                println!("    ✓ Deployed to {}", url);
            }
            Err(e) => {
                return Ok(WatchResult::PipelineError {
                    file_path: file_path.to_path_buf(),
                    error: format!("Deployment failed: {}", e),
                });
            }
        }
    }

    Ok(WatchResult::FileProcessed {
        file_path: file_path.to_path_buf(),
        conversation_id,
    })
}

/// Calculate total pipeline steps based on options
fn pipeline_steps(options: &PipelineOptions) -> usize {
    let mut steps = 1; // Always have ingest
    if options.extract { steps += 1; }
    if options.metabolize { steps += 1; }
    if options.generate_embeddings { steps += 1; }
    if options.sync_backend { steps += 1; }
    if options.compile { steps += 1; }
    if options.build { steps += 1; }
    if options.deploy { steps += 1; }
    steps
}

/// Run metabolism on unprocessed capsules
fn run_metabolism(db: &Database, ollama_url: &str, model: &str) -> Result<(usize, usize)> {
    use crate::embedding::OllamaEmbedding;
    use crate::metabolism::{metabolize_all_new, MetabolismConfig};

    let embedding_client = OllamaEmbedding::new(ollama_url, model)?;
    let config = MetabolismConfig::default();

    let result = metabolize_all_new(db, &embedding_client, &config)?;

    Ok((result.patterns_reinforced, result.patterns_seeded))
}

/// Run extraction for a single conversation
fn run_extraction(
    file_path: &Path,
    db: &Database,
    config: &Config,
) -> Result<i64> {
    use crate::llm::ClaudeClient;
    use crate::{extract_conversation, ConversationExport};

    // Parse conversation
    let conversation = ConversationExport::from_file(file_path)?;

    // Create LLM client
    let llm_client: Box<dyn crate::LlmClient> = match config.extraction.llm_backend {
        crate::config::LlmBackend::ClaudeApi => {
            let api_key = config
                .extraction
                .claude_api_key
                .clone()
                .or_else(|| std::env::var("ANTHROPIC_API_KEY").ok())
                .ok_or_else(|| {
                    anyhow::anyhow!(
                        "Claude API key not found. Set ANTHROPIC_API_KEY or add to config"
                    )
                })?;

            Box::new(ClaudeClient::new(api_key, config.extraction.llm_model.clone())?)
        }
        crate::config::LlmBackend::Ollama => {
            anyhow::bail!("Ollama backend not yet implemented");
        }
    };

    // Extract
    let extraction = extract_conversation(&conversation, llm_client.as_ref(), &config.extraction.themes)?;

    // Store in database
    let extraction_id = db.insert_extraction(
        &extraction.conversation_id,
        &extraction.title,
        &extraction.summary,
        &extraction.classification.to_string(),
        extraction.confidence_score,
        &extraction.themes,
        &extraction.key_quotes,
    )?;

    Ok(extraction_id)
}

/// Run compilation (digests, threads, predictions)
fn run_compilation(config: &Config) -> Result<()> {
    use crate::compilation::{DigestEntry, Prediction, PredictionStatus, ThreadEntry};
    use chrono::{DateTime, Utc};

    let db = Database::new(&config.input.processed_db)?;
    let output_dir = &config.output.build_directory;
    std::fs::create_dir_all(output_dir)?;

    // Generate weekly digest
    let recent_extractions = db.get_recent_extractions(7)?;
    if !recent_extractions.is_empty() {
        let mut entries_by_theme: std::collections::HashMap<String, Vec<DigestEntry>> =
            std::collections::HashMap::new();

        for (id, conv_id, title, summary, classification_str, _confidence, created_at, themes, quotes) in recent_extractions {
            let classification = match classification_str.as_str() {
                "prediction" => crate::Classification::Prediction,
                "insight" => crate::Classification::Insight,
                "milestone" => crate::Classification::Milestone,
                "reflection" => crate::Classification::Reflection,
                _ => crate::Classification::Technical,
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

            for theme in themes {
                entries_by_theme.entry(theme).or_insert_with(Vec::new).push(entry.clone());
            }
        }

        let now = Utc::now();
        let week_start = now - chrono::Duration::days(7);
        let total_entries: usize = entries_by_theme.values().map(|v| v.len()).sum();

        let digest = crate::WeeklyDigest {
            week_start,
            week_end: now,
            entries_by_theme,
            total_entries,
        };

        std::fs::write(output_dir.join("weekly_digest.md"), digest.to_markdown())?;
    }

    // Generate theme threads
    let themes = db.get_themes_with_threshold(3)?;
    if !themes.is_empty() {
        let threads_dir = output_dir.join("threads");
        std::fs::create_dir_all(&threads_dir)?;

        for (theme, _count) in themes {
            let extractions = db.get_extractions_by_theme(&theme)?;
            let entries: Vec<ThreadEntry> = extractions
                .into_iter()
                .map(|(id, title, summary, created_at, quotes)| ThreadEntry {
                    extraction_id: id,
                    title,
                    summary,
                    created_at: DateTime::from_timestamp(created_at, 0).unwrap_or_else(Utc::now),
                    quotes,
                })
                .collect();

            let thread = crate::generate_theme_thread(theme.clone(), entries);
            std::fs::write(
                threads_dir.join(format!("{}.md", theme.replace(' ', "_"))),
                thread.to_markdown(),
            )?;
        }
    }

    // Generate prediction registry
    let predictions_data = db.get_all_predictions()?;
    if !predictions_data.is_empty() {
        let predictions: Vec<Prediction> = predictions_data
            .into_iter()
            .map(|(id, extraction_id, claim, date_made, timeline, status_str, validation_date, notes)| {
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
            })
            .collect();

        let registry = crate::PredictionRegistry { predictions };
        std::fs::write(output_dir.join("predictions.md"), registry.to_markdown())?;
    }

    Ok(())
}

/// Run static site build
fn run_build(config: &Config) -> Result<()> {
    use crate::{build_site, DisplayEntry, DisplayCapsule, DisplayMarketPosition};
    use crate::compilation::{Prediction, PredictionStatus};
    use chrono::{DateTime, Utc};

    let db = Database::new(&config.input.processed_db)?;

    // Get recent extractions
    let recent_data = db.get_recent_extractions(30)?;
    let recent_entries: Vec<DisplayEntry> = recent_data
        .into_iter()
        .map(|(_, _, title, summary, classification, _, created_at, themes, quotes)| DisplayEntry {
            title,
            summary,
            classification,
            date: DateTime::from_timestamp(created_at, 0)
                .unwrap_or_else(Utc::now)
                .format("%Y-%m-%d")
                .to_string(),
            themes,
            quotes,
        })
        .collect();

    // Get themes
    let themes_data = db.get_themes_with_threshold(3)?;
    let mut threads = Vec::new();
    for (theme, _) in themes_data {
        let extractions = db.get_extractions_by_theme(&theme)?;
        if extractions.is_empty() {
            continue;
        }

        let entries: Vec<DisplayEntry> = extractions
            .iter()
            .map(|(_, title, summary, created_at, quotes)| DisplayEntry {
                title: title.clone(),
                summary: summary.clone(),
                classification: String::new(),
                date: DateTime::from_timestamp(*created_at, 0)
                    .unwrap_or_else(Utc::now)
                    .format("%Y-%m-%d")
                    .to_string(),
                themes: vec![],
                quotes: quotes.clone(),
            })
            .collect();

        let dates: Vec<DateTime<Utc>> = extractions
            .iter()
            .map(|(_, _, _, created_at, _)| {
                DateTime::from_timestamp(*created_at, 0).unwrap_or_else(Utc::now)
            })
            .collect();

        let start = dates.iter().min().cloned().unwrap_or_else(Utc::now);
        let end = dates.iter().max().cloned().unwrap_or_else(Utc::now);

        threads.push((theme, entries, start, end));
    }

    // Get predictions
    let predictions_data = db.get_all_predictions()?;
    let predictions: Vec<Prediction> = predictions_data
        .into_iter()
        .map(|(id, extraction_id, claim, date_made, timeline, status_str, validation_date, notes)| {
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
        })
        .collect();

    // Get capsule data
    let capsule_count = db.get_capsule_count()? as usize;
    let active_capsules = db.get_active_capsules(10)?;
    let recent_capsules: Vec<DisplayCapsule> = active_capsules
        .into_iter()
        .map(|(_, restatement, timestamp, topic, _)| DisplayCapsule {
            content: if restatement.len() > 200 {
                format!("{}...", &restatement[..200])
            } else {
                restatement
            },
            topic: topic.unwrap_or_else(|| "general".to_string()),
            timestamp: timestamp.unwrap_or_else(|| "recent".to_string()),
            keywords: vec![],
        })
        .collect();

    // Get market positions
    let positions_data = db.get_market_positions(None)?;
    let market_positions: Vec<DisplayMarketPosition> = positions_data
        .into_iter()
        .map(|p| DisplayMarketPosition {
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
        })
        .collect();

    // Build site
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

    Ok(())
}

/// Run deployment to ICP
fn run_deployment(config: &Config) -> Result<String> {
    let canister_id = if config.deployment.canister_id.is_empty() {
        None
    } else {
        Some(config.deployment.canister_id.as_str())
    };

    let result = crate::deploy_to_icp(
        &config.output.build_directory,
        &config.deployment.network,
        canister_id,
    )?;

    Ok(result.url)
}

/// Generate embeddings for capsules without embeddings
fn run_embedding_generation(db: &Database, ollama_url: &str, model: &str) -> Result<usize> {
    use crate::embedding::OllamaEmbedding;

    let embedder = OllamaEmbedding::new(ollama_url, model)?;

    // Get capsules without embeddings (None = no limit)
    let capsules = db.get_capsules_without_embeddings(None)?;
    if capsules.is_empty() {
        return Ok(0);
    }

    let mut generated = 0;
    for (capsule_id, restatement) in capsules {
        match embedder.embed(&restatement) {
            Ok(embedding) => {
                db.insert_capsule_embedding(capsule_id, &embedding, model)?;
                generated += 1;
            }
            Err(e) => {
                eprintln!("      Warning: Failed to embed capsule {}: {}", capsule_id, e);
            }
        }
    }

    Ok(generated)
}

/// Sync capsules and embeddings to backend canister
fn run_backend_sync(db: &Database, canister_id: &str, identity: &str) -> Result<(usize, usize)> {
    use crate::icp::{IcpClient, KnowledgeCapsule, CapsuleEmbedding, sync_to_canister};

    // Get all capsules and embeddings from local DB
    let local_capsules = db.get_all_capsules_for_sync()?;
    let local_embeddings = db.get_all_embeddings_for_sync()?;

    if local_capsules.is_empty() && local_embeddings.is_empty() {
        return Ok((0, 0));
    }

    // Convert to ICP types
    let capsules: Vec<KnowledgeCapsule> = local_capsules
        .into_iter()
        .map(|c| {
            KnowledgeCapsule {
                id: c.id as u64,
                conversation_id: c.conversation_id,
                restatement: c.restatement,
                timestamp: c.timestamp,
                location: c.location,
                topic: c.topic,
                confidence_score: c.confidence_score,
                persons: c.persons,
                entities: c.entities,
                keywords: c.keywords,
                created_at: c.created_at as u64,
            }
        })
        .collect();

    let embeddings: Vec<CapsuleEmbedding> = local_embeddings
        .into_iter()
        .map(|e| {
            CapsuleEmbedding {
                capsule_id: e.capsule_id as u64,
                embedding: e.embedding,
                model_name: e.model_name,
            }
        })
        .collect();

    // Create client and sync
    let rt = tokio::runtime::Runtime::new()?;
    let result = rt.block_on(async {
        let client = IcpClient::from_dfx_identity(canister_id, identity).await?;
        sync_to_canister(&client, capsules, embeddings, 50).await
    })?;

    Ok((result.capsules_synced, result.embeddings_synced))
}

/// Send desktop notification (if notifications feature is enabled)
fn send_notification(title: &str, message: &str) {
    #[cfg(feature = "notifications")]
    {
        use notify_rust::Notification;
        let _ = Notification::new()
            .summary(title)
            .body(message)
            .show();
    }

    #[cfg(not(feature = "notifications"))]
    {
        // Notifications disabled - no-op
        let _ = (title, message);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_json_file() {
        // Valid conversation files
        assert!(is_json_file(Path::new("test.json")));
        assert!(is_json_file(Path::new("/path/to/file.json")));
        assert!(is_json_file(Path::new("conversations.json")));

        // Non-JSON files
        assert!(!is_json_file(Path::new("test.txt")));
        assert!(!is_json_file(Path::new("test.md")));
        assert!(!is_json_file(Path::new("test")));

        // Filtered non-conversation exports
        assert!(!is_json_file(Path::new("memories.json")));
        assert!(!is_json_file(Path::new("projects.json")));
        assert!(!is_json_file(Path::new("users.json")));

        // Zone.Identifier files (Windows security markers)
        assert!(!is_json_file(Path::new("conversations.json:Zone.Identifier")));
        assert!(!is_json_file(Path::new("test.json:Zone.Identifier")));
    }

    #[test]
    fn test_pipeline_options_default() {
        let opts = PipelineOptions::default();
        assert!(opts.extract);
        assert!(opts.compile);
        assert!(opts.build);
        assert!(!opts.deploy); // Deploy should be false by default
        assert!(!opts.notify);
    }

    #[test]
    fn test_watch_result_variants() {
        let processed = WatchResult::FileProcessed {
            file_path: PathBuf::from("test.json"),
            conversation_id: "test-123".to_string(),
        };
        assert!(matches!(processed, WatchResult::FileProcessed { .. }));

        let bulk = WatchResult::BulkProcessed {
            file_path: PathBuf::from("conversations.json"),
            ingested: 5,
            duplicates: 2,
            skipped: 1,
        };
        assert!(matches!(bulk, WatchResult::BulkProcessed { .. }));

        let ignored = WatchResult::FileIgnored {
            file_path: PathBuf::from("test.json"),
            reason: "duplicate".to_string(),
        };
        assert!(matches!(ignored, WatchResult::FileIgnored { .. }));

        let error = WatchResult::PipelineError {
            file_path: PathBuf::from("test.json"),
            error: "failed".to_string(),
        };
        assert!(matches!(error, WatchResult::PipelineError { .. }));
    }
}
