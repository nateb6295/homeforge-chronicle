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
