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
}

impl Default for PipelineOptions {
    fn default() -> Self {
        Self {
            extract: true,
            compile: true,
            build: true,
            deploy: false, // Don't auto-deploy by default
            notify: false,
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
    println!("  Deploy: {}", options.deploy);
    println!("  Notify: {}", options.notify);
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

/// Check if a path is a JSON file
fn is_json_file(path: &Path) -> bool {
    path.extension().and_then(|s| s.to_str()) == Some("json")
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

    // Open database
    let db = Database::new(&config.input.processed_db)?;

    // Step 1: Ingest
    println!("  [1/{}] Ingesting...", if options.deploy { 5 } else { 4 });
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
        println!("  [2/{}] Extracting themes and summaries...", if options.deploy { 5 } else { 4 });

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

    // Step 3: Compile (if enabled)
    if options.compile {
        let step = if options.extract { 3 } else { 2 };
        println!("  [{}/{}] Compiling digests and threads...", step, if options.deploy { 5 } else { 4 });

        if let Err(e) = run_compilation(config) {
            return Ok(WatchResult::PipelineError {
                file_path: file_path.to_path_buf(),
                error: format!("Compilation failed: {}", e),
            });
        }
        println!("    ✓ Compiled");
    }

    // Step 4: Build (if enabled)
    if options.build {
        let step = if options.compile && options.extract { 4 } else if options.compile || options.extract { 3 } else { 2 };
        println!("  [{}/{}] Building static site...", step, if options.deploy { 5 } else { 4 });

        if let Err(e) = run_build(config) {
            return Ok(WatchResult::PipelineError {
                file_path: file_path.to_path_buf(),
                error: format!("Build failed: {}", e),
            });
        }
        println!("    ✓ Built");
    }

    // Step 5: Deploy (if enabled)
    if options.deploy {
        println!("  [5/5] Deploying to ICP...");

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
    use crate::{build_site, DisplayEntry};
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

    // Build site
    build_site(
        &config.output.build_directory,
        &config.output.site_title,
        &config.output.author,
        recent_entries,
        threads,
        predictions,
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
        assert!(is_json_file(Path::new("test.json")));
        assert!(is_json_file(Path::new("/path/to/file.json")));
        assert!(!is_json_file(Path::new("test.txt")));
        assert!(!is_json_file(Path::new("test.md")));
        assert!(!is_json_file(Path::new("test")));
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
