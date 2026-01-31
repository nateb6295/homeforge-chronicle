pub mod capsule;
pub mod cli;
pub mod cognitive;
pub mod compilation;
pub mod config;
pub mod db;
pub mod deploy;
pub mod embedding;
pub mod extraction;
pub mod icp;
pub mod ingest;
pub mod llm;
pub mod metabolism;
pub mod parser;
pub mod site;
pub mod watch;

pub use capsule::{extract_capsules, KnowledgeCapsule};
pub use db::{CapsuleInfo, EnrichedPattern, OutboxMessage, ScratchNote, SyncCapsule, ThoughtEntry};
pub use metabolism::{
    decay_patterns, detect_meta_patterns, metabolize_all_new, metabolize_capsule,
    BatchMetabolismResult, DecayResult, MetaPattern, MetabolismConfig, MetabolismResult,
};
pub use embedding::{cosine_similarity, find_top_k_similar, OllamaEmbedding};
pub use compilation::{generate_theme_thread, generate_weekly_digest, PredictionRegistry, ThemeThread, WeeklyDigest};
pub use config::Config;
pub use deploy::{deploy_to_icp, DeploymentResult};
pub use extraction::{extract_conversation, Classification, Extraction};
pub use ingest::{ingest_conversation, ingest_conversation_export, IngestResult};
pub use llm::{LlmClient, ClaudeClient, OllamaClient, FallbackLlmClient};
pub use parser::{parse_bulk_export, BulkConversationExport, ConversationExport};
pub use site::{build_site, build_thoughts_page, build_outbox_page, DisplayEntry, DisplayCapsule, DisplayThought, DisplayOutboxMessage, DisplayMarketPosition};
pub use cognitive::{CognitiveState, FocalEntity, EntityType, RetrievedArtifact, UncertaintySignal, QualificationGate, CognitiveCompressor, CompressionInput};
pub use watch::{watch_directory, PipelineOptions};
pub use icp::{IcpClient, sync_to_canister, SyncResult};

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_lib_compiles() {
        // Basic compilation test
        assert!(true);
    }
}
