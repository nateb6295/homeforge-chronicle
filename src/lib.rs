pub mod cli;
pub mod compilation;
pub mod config;
pub mod db;
pub mod deploy;
pub mod extraction;
pub mod ingest;
pub mod llm;
pub mod parser;
pub mod site;
pub mod watch;

pub use compilation::{generate_theme_thread, generate_weekly_digest, PredictionRegistry, ThemeThread, WeeklyDigest};
pub use config::Config;
pub use deploy::{deploy_to_icp, DeploymentResult};
pub use extraction::{extract_conversation, Classification, Extraction};
pub use ingest::{ingest_conversation, IngestResult};
pub use llm::LlmClient;
pub use parser::{parse_bulk_export, BulkConversationExport, ConversationExport};
pub use site::{build_site, DisplayEntry};
pub use watch::{watch_directory, PipelineOptions};

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_lib_compiles() {
        // Basic compilation test
        assert!(true);
    }
}
