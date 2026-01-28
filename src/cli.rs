use clap::{Parser, Subcommand};
use std::path::PathBuf;

#[derive(Parser, Debug)]
#[command(name = "chronicle")]
#[command(version, about = "Homeforge Chronicle - Automated life-archive system", long_about = None)]
pub struct Cli {
    /// Path to configuration file
    #[arg(short, long, value_name = "FILE")]
    pub config: Option<PathBuf>,

    #[command(subcommand)]
    pub command: Commands,
}

#[derive(Subcommand, Debug)]
pub enum Commands {
    /// Ingest a Claude conversation export file
    Ingest {
        /// Path to the conversation export JSON file
        file: PathBuf,
    },

    /// Ingest bulk Claude conversation export file
    IngestBulk {
        /// Path to the bulk conversations.json file
        file: PathBuf,
    },

    /// Extract themes and summaries from ingested conversations
    Extract {
        /// Limit number of conversations to extract (for testing)
        #[arg(short, long)]
        limit: Option<usize>,
    },

    /// Compile weekly digests and thread documents
    Compile,

    /// Build static site from compiled content
    Build,

    /// Deploy to Internet Computer canister
    Deploy,

    /// Watch directory for new exports and run full pipeline
    Watch,

    /// Initialize configuration and database
    Init {
        /// Directory to initialize (defaults to current directory)
        path: Option<PathBuf>,
    },

    /// Extract knowledge capsules from conversations (KIP integration)
    Capsules {
        /// Limit number of conversations to process
        #[arg(short, long)]
        limit: Option<usize>,

        /// Process a specific conversation ID
        #[arg(short = 'i', long)]
        conversation_id: Option<String>,
    },

    /// Show knowledge capsule stats
    CapsuleStats,

    /// Search knowledge capsules
    SearchCapsules {
        /// Search query (keywords, person name, or topic)
        query: String,

        /// Search type: keyword, person, or topic
        #[arg(short = 't', long, default_value = "keyword")]
        search_type: String,

        /// Maximum results to return
        #[arg(short, long, default_value = "10")]
        limit: i64,
    },

    /// Generate embeddings for capsules (via Jetson/Ollama)
    Embeddings {
        /// Ollama base URL
        #[arg(long, default_value = "http://localhost:11434")]
        ollama_url: String,

        /// Embedding model to use
        #[arg(long, default_value = "mxbai-embed-large")]
        model: String,

        /// Limit number of capsules to embed
        #[arg(short, long)]
        limit: Option<usize>,

        /// Force re-embedding of all capsules (even those with existing embeddings)
        #[arg(long)]
        force: bool,
    },

    /// Semantic search using embeddings
    SemanticSearch {
        /// Search query
        query: String,

        /// Maximum results to return
        #[arg(short, long, default_value = "5")]
        limit: usize,

        /// Ollama base URL
        #[arg(long, default_value = "http://localhost:11434")]
        ollama_url: String,

        /// Embedding model to use
        #[arg(long, default_value = "gemma2:2b")]
        model: String,
    },

    /// Sync capsules and embeddings to ICP canister
    Sync {
        /// Backend canister ID
        #[arg(long, default_value = "fqqku-bqaaa-aaaai-q4wha-cai")]
        canister_id: String,

        /// DFX identity to use
        #[arg(long, default_value = "chronicle-auto")]
        identity: String,

        /// Only sync capsules (skip embeddings)
        #[arg(long)]
        capsules_only: bool,

        /// Only sync embeddings (skip capsules)
        #[arg(long)]
        embeddings_only: bool,
    },

    /// Metabolize capsules into patterns (reinforcement-based evolution)
    Metabolize {
        /// Ollama base URL for embeddings
        #[arg(long, default_value = "http://localhost:11434")]
        ollama_url: String,

        /// Embedding model to use
        #[arg(long, default_value = "gemma2:2b")]
        model: String,

        /// Also run decay on stale patterns
        #[arg(long)]
        with_decay: bool,

        /// Detect meta-patterns (clusters of related patterns)
        #[arg(long)]
        detect_meta: bool,
    },

    /// List and inspect knowledge patterns
    Patterns {
        /// Minimum confidence to display (0.0-1.0)
        #[arg(short, long, default_value = "0.0")]
        min_confidence: f64,

        /// Maximum patterns to show
        #[arg(short, long, default_value = "20")]
        limit: usize,

        /// Show only active patterns
        #[arg(long)]
        active_only: bool,

        /// Show pattern with specific ID and its linked capsules
        #[arg(short, long)]
        id: Option<i64>,
    },

    /// Sync using native ICP agent (fast, batched)
    SyncNative {
        /// Backend canister ID
        #[arg(long, default_value = "fqqku-bqaaa-aaaai-q4wha-cai")]
        canister_id: String,

        /// DFX identity to use
        #[arg(long, default_value = "chronicle-auto")]
        identity: String,

        /// Batch size for uploads
        #[arg(long, default_value = "50")]
        batch_size: usize,
    },

    /// Semantic search on-chain (query the canister directly)
    OnchainSearch {
        /// Search query
        query: String,

        /// Maximum results
        #[arg(short, long, default_value = "10")]
        limit: u64,

        /// Canister ID
        #[arg(long, default_value = "fqqku-bqaaa-aaaai-q4wha-cai")]
        canister_id: String,

        /// DFX identity
        #[arg(long, default_value = "chronicle-auto")]
        identity: String,

        /// Ollama URL for query embedding
        #[arg(long, default_value = "http://localhost:11434")]
        ollama_url: String,

        /// Embedding model
        #[arg(long, default_value = "gemma2:2b")]
        model: String,
    },

    /// Refresh pattern summaries based on actual linked capsules
    RefreshPatterns {
        /// Only refresh a specific pattern ID
        #[arg(short, long)]
        id: Option<i64>,

        /// Minimum capsule count to refresh (skip small patterns)
        #[arg(long, default_value = "5")]
        min_capsules: i64,

        /// Dry run - show what would change without updating
        #[arg(long)]
        dry_run: bool,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cli_parses_init_command() {
        let args = vec!["chronicle", "init"];
        let cli = Cli::try_parse_from(args);
        assert!(cli.is_ok());
        let cli = cli.unwrap();
        assert!(matches!(cli.command, Commands::Init { .. }));
    }

    #[test]
    fn test_cli_parses_ingest_command() {
        let args = vec!["chronicle", "ingest", "test.json"];
        let cli = Cli::try_parse_from(args);
        assert!(cli.is_ok());
        let cli = cli.unwrap();
        assert!(matches!(cli.command, Commands::Ingest { .. }));
    }

    #[test]
    fn test_cli_parses_ingest_bulk_command() {
        let args = vec!["chronicle", "ingest-bulk", "conversations.json"];
        let cli = Cli::try_parse_from(args);
        assert!(cli.is_ok());
        let cli = cli.unwrap();
        assert!(matches!(cli.command, Commands::IngestBulk { .. }));
    }

    #[test]
    fn test_cli_parses_config_option() {
        let args = vec!["chronicle", "--config", "custom.toml", "extract"];
        let cli = Cli::try_parse_from(args);
        assert!(cli.is_ok());
        let cli = cli.unwrap();
        assert!(cli.config.is_some());
        assert_eq!(cli.config.unwrap(), PathBuf::from("custom.toml"));
    }

    #[test]
    fn test_cli_all_commands_parse() {
        let commands = vec![
            vec!["chronicle", "extract"],
            vec!["chronicle", "compile"],
            vec!["chronicle", "build"],
            vec!["chronicle", "deploy"],
            vec!["chronicle", "watch"],
        ];

        for args in commands {
            let cli = Cli::try_parse_from(args.clone());
            assert!(cli.is_ok(), "Failed to parse: {:?}", args);
        }
    }
}
