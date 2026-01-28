//! Native ICP Integration
//!
//! Direct canister interaction via ic-agent instead of shelling out to dfx.
//! Faster, cleaner, and enables batch operations.

use anyhow::{Context, Result};
use candid::{CandidType, Decode, Encode, Principal};
use ic_agent::Agent;
use ic_agent::identity::{BasicIdentity, Secp256k1Identity};
use serde::Deserialize;
use std::path::Path;

/// Knowledge capsule matching the canister's type
#[derive(Clone, Debug, CandidType, Deserialize)]
pub struct KnowledgeCapsule {
    pub id: u64,
    pub conversation_id: String,
    pub restatement: String,
    pub timestamp: Option<String>,
    pub location: Option<String>,
    pub topic: Option<String>,
    pub confidence_score: f64,
    pub persons: Vec<String>,
    pub entities: Vec<String>,
    pub keywords: Vec<String>,
    pub created_at: u64,
}

/// Capsule embedding matching the canister's type
#[derive(Clone, Debug, CandidType, Deserialize)]
pub struct CapsuleEmbedding {
    pub capsule_id: u64,
    pub embedding: Vec<f32>,
    pub model_name: String,
}

/// Search result from semantic search
#[derive(Clone, Debug, CandidType, Deserialize)]
pub struct SearchResult {
    pub capsule: KnowledgeCapsule,
    pub score: f64,
}

/// Agent identity for messaging
#[derive(Clone, Debug, CandidType, Deserialize)]
pub struct AgentIdentity {
    pub canister_id: Principal,
    pub name: String,
    pub agent_type: String,
    pub description: Option<String>,
    pub capabilities: Vec<String>,
    pub http_endpoint: Option<String>,
}

/// Message type enum
#[derive(Clone, Debug, CandidType, Deserialize)]
pub enum MessageType {
    Query,
    ActionRequest,
    Information,
    Conversation,
    GoalAssignment,
}

/// Agent message from inbox
#[derive(Clone, Debug, CandidType, Deserialize)]
pub struct AgentMessage {
    pub id: u64,
    pub thread_id: u64,
    pub in_reply_to: Option<u64>,
    pub sender: AgentIdentity,
    pub msg_type: MessageType,
    pub subject: Option<String>,
    pub content: String,
    pub metadata: Option<String>,
    pub timestamp: u64,
    pub expects_reply: bool,
    pub read: bool,
    pub replied: bool,
}

/// Native ICP client for Chronicle canister
pub struct IcpClient {
    agent: Agent,
    canister_id: Principal,
}

impl IcpClient {
    /// Create a new ICP client with a PEM identity file
    pub async fn new(canister_id: &str, identity_pem_path: &Path) -> Result<Self> {
        let canister_id = Principal::from_text(canister_id)
            .context("Invalid canister ID")?;

        // Load identity from PEM file - try secp256k1 first, then basic
        let identity: Box<dyn ic_agent::Identity> =
            if let Ok(id) = Secp256k1Identity::from_pem_file(identity_pem_path) {
                Box::new(id)
            } else if let Ok(id) = BasicIdentity::from_pem_file(identity_pem_path) {
                Box::new(id)
            } else {
                return Err(anyhow::anyhow!("Failed to load identity from PEM file"));
            };

        // Create agent for mainnet
        let agent = Agent::builder()
            .with_url("https://icp-api.io")
            .with_identity(identity)
            .build()?;

        // Fetch root key only for local development (not needed for mainnet)
        // agent.fetch_root_key().await?;

        Ok(Self { agent, canister_id })
    }

    /// Create client using dfx identity by name
    pub async fn from_dfx_identity(canister_id: &str, identity_name: &str) -> Result<Self> {
        let home = std::env::var("HOME").context("HOME not set")?;
        let pem_path = format!("{}/.config/dfx/identity/{}/identity.pem", home, identity_name);
        Self::new(canister_id, Path::new(&pem_path)).await
    }

    /// Get canister health status
    pub async fn health(&self) -> Result<String> {
        let response = self.agent
            .query(&self.canister_id, "health")
            .with_arg(Encode!()?)
            .call()
            .await?;

        let result = Decode!(&response, String)?;
        Ok(result)
    }

    /// Get capsule count
    pub async fn get_capsule_count(&self) -> Result<u64> {
        let response = self.agent
            .query(&self.canister_id, "get_capsule_count")
            .with_arg(Encode!()?)
            .call()
            .await?;

        let result = Decode!(&response, u64)?;
        Ok(result)
    }

    /// Get embedding count
    pub async fn get_embedding_count(&self) -> Result<u64> {
        let response = self.agent
            .query(&self.canister_id, "get_embedding_count")
            .with_arg(Encode!()?)
            .call()
            .await?;

        let result = Decode!(&response, u64)?;
        Ok(result)
    }

    /// Add a batch of capsules (much faster than one-by-one)
    pub async fn add_capsules_bulk(&self, capsules: Vec<KnowledgeCapsule>) -> Result<Vec<u64>> {
        let response = self.agent
            .update(&self.canister_id, "add_capsules_bulk")
            .with_arg(Encode!(&capsules)?)
            .call_and_wait()
            .await?;

        let ids = Decode!(&response, Vec<u64>)?;
        Ok(ids)
    }

    /// Add a batch of embeddings
    pub async fn add_embeddings_bulk(&self, embeddings: Vec<CapsuleEmbedding>) -> Result<u64> {
        let response = self.agent
            .update(&self.canister_id, "add_embeddings_bulk")
            .with_arg(Encode!(&embeddings)?)
            .call_and_wait()
            .await?;

        let added = Decode!(&response, u64)?;
        Ok(added)
    }

    /// Perform semantic search on-chain
    pub async fn semantic_search(&self, query_embedding: Vec<f32>, limit: u64) -> Result<Vec<SearchResult>> {
        let response = self.agent
            .query(&self.canister_id, "semantic_search")
            .with_arg(Encode!(&query_embedding, &limit)?)
            .call()
            .await?;

        let results = Decode!(&response, Vec<SearchResult>)?;
        Ok(results)
    }

    /// Get recent capsules
    pub async fn get_recent_capsules(&self, limit: u64) -> Result<Vec<KnowledgeCapsule>> {
        let response = self.agent
            .query(&self.canister_id, "get_recent_capsules")
            .with_arg(Encode!(&limit)?)
            .call()
            .await?;

        let capsules = Decode!(&response, Vec<KnowledgeCapsule>)?;
        Ok(capsules)
    }

    /// Search by keyword
    pub async fn search_by_keyword(&self, keyword: &str, limit: u64) -> Result<Vec<KnowledgeCapsule>> {
        let response = self.agent
            .query(&self.canister_id, "search_by_keyword")
            .with_arg(Encode!(&keyword.to_string(), &limit)?)
            .call()
            .await?;

        let capsules = Decode!(&response, Vec<KnowledgeCapsule>)?;
        Ok(capsules)
    }

    /// Search by person
    pub async fn search_by_person(&self, person: &str, limit: u64) -> Result<Vec<KnowledgeCapsule>> {
        let response = self.agent
            .query(&self.canister_id, "search_by_person")
            .with_arg(Encode!(&person.to_string(), &limit)?)
            .call()
            .await?;

        let capsules = Decode!(&response, Vec<KnowledgeCapsule>)?;
        Ok(capsules)
    }

    /// Write a reflection directly to the canister
    /// Returns the capsule ID of the stored reflection
    pub async fn write_reflection(&self, reflection: &str, source: Option<&str>) -> Result<u64> {
        let source_opt: Option<String> = source.map(|s| s.to_string());
        let response = self.agent
            .update(&self.canister_id, "write_reflection")
            .with_arg(Encode!(&reflection.to_string(), &source_opt)?)
            .call_and_wait()
            .await?;

        let capsule_id = Decode!(&response, u64)?;
        Ok(capsule_id)
    }

    /// Sign an XRP to RLUSD swap transaction
    /// Returns JSON with signed blob and tx hash, or error
    pub async fn sign_swap_xrp_to_rlusd(
        &self,
        xrp_drops: u64,
        min_rlusd: &str,
        fee_drops: u64,
        sequence: u32,
        last_ledger_sequence: u32,
    ) -> Result<String> {
        let response = self.agent
            .update(&self.canister_id, "sign_swap_xrp_to_rlusd")
            .with_arg(Encode!(
                &xrp_drops,
                &min_rlusd.to_string(),
                &fee_drops,
                &sequence,
                &last_ledger_sequence
            )?)
            .call_and_wait()
            .await?;

        let result = Decode!(&response, String)?;
        Ok(result)
    }

    /// Submit a signed transaction to XRPL
    /// Returns JSON with result
    pub async fn submit_transaction(&self, signed_blob: &str) -> Result<String> {
        let response = self.agent
            .update(&self.canister_id, "submit_transaction")
            .with_arg(Encode!(&signed_blob.to_string())?)
            .call_and_wait()
            .await?;

        let result = Decode!(&response, String)?;
        Ok(result)
    }

    /// Get wallet info from canister
    pub async fn get_wallet_info(&self) -> Result<String> {
        let response = self.agent
            .query(&self.canister_id, "get_wallet_info")
            .with_arg(Encode!()?)
            .call()
            .await?;

        let result = Decode!(&response, String)?;
        Ok(result)
    }

    /// Refresh wallet balance from XRPL
    pub async fn refresh_balance(&self) -> Result<String> {
        let response = self.agent
            .update(&self.canister_id, "refresh_balance")
            .with_arg(Encode!()?)
            .call_and_wait()
            .await?;

        let result = Decode!(&response, String)?;
        Ok(result)
    }

    /// Add price data to mind state on canister
    pub async fn add_mind_price(&self, price_usd: f64, source: &str) -> Result<()> {
        self.agent
            .update(&self.canister_id, "add_mind_price")
            .with_arg(Encode!(&price_usd, &source.to_string())?)
            .call_and_wait()
            .await?;
        Ok(())
    }

    /// Record a swap in mind state on canister
    pub async fn record_mind_swap(&self, amount_xrp: f64) -> Result<()> {
        self.agent
            .update(&self.canister_id, "record_mind_swap")
            .with_arg(Encode!(&amount_xrp)?)
            .call_and_wait()
            .await?;
        Ok(())
    }

    /// Store a thought from cognitive cycle to canister
    pub async fn store_mind_thought(
        &self,
        cycle_id: &str,
        reasoning_summary: &str,
        context_summary: &str,
        actions: Vec<String>,
    ) -> Result<()> {
        self.agent
            .update(&self.canister_id, "store_mind_thought")
            .with_arg(Encode!(
                &cycle_id.to_string(),
                &reasoning_summary.to_string(),
                &context_summary.to_string(),
                &actions
            )?)
            .call_and_wait()
            .await?;
        Ok(())
    }

    /// Get inbox messages (agent-to-agent messaging)
    pub async fn get_inbox(&self, include_read: bool, limit: u64) -> Result<Vec<AgentMessage>> {
        let response = self.agent
            .query(&self.canister_id, "agent_get_inbox")
            .with_arg(Encode!(&include_read, &limit)?)
            .call()
            .await?;

        let messages = Decode!(&response, Vec<AgentMessage>)?;
        Ok(messages)
    }

    /// Reply to an inbox message
    pub async fn reply_to_message(&self, message_id: u64, content: &str) -> Result<String> {
        let response = self.agent
            .update(&self.canister_id, "agent_reply")
            .with_arg(Encode!(&message_id, &content.to_string())?)
            .call_and_wait()
            .await?;

        let result = Decode!(&response, String)?;
        Ok(result)
    }

    // ============================================================
    // Research Task System - Claude's on-chain research assistant
    // ============================================================

    /// Submit a research task to the on-chain LLM (Qwen 3 32B)
    /// The canister will process this during its next heartbeat cycle
    /// Can optionally include URLs to fetch for additional web context
    pub async fn submit_research_task(
        &self,
        query: &str,
        focus: Option<&str>,
        max_capsules: u32,
        urls: Option<Vec<String>>,
    ) -> Result<String> {
        let focus_opt: Option<String> = focus.map(|s| s.to_string());
        let max_opt: Option<u32> = Some(max_capsules);
        let urls_opt: Option<Vec<String>> = urls;
        let response = self.agent
            .update(&self.canister_id, "submit_research_task")
            .with_arg(Encode!(&query.to_string(), &focus_opt, &max_opt, &urls_opt)?)
            .call_and_wait()
            .await?;

        let result = Decode!(&response, String)?;
        Ok(result)
    }

    /// Get research findings from the on-chain LLM
    /// If only_new is true, returns only unread findings
    pub async fn get_research_findings(&self, only_new: bool) -> Result<String> {
        let response = self.agent
            .query(&self.canister_id, "get_research_findings")
            .with_arg(Encode!(&only_new)?)
            .call()
            .await?;

        let result = Decode!(&response, String)?;
        Ok(result)
    }

    /// Mark findings as retrieved so they don't show up in "new" queries
    pub async fn mark_findings_retrieved(&self, finding_ids: Vec<u64>) -> Result<String> {
        let response = self.agent
            .update(&self.canister_id, "mark_findings_retrieved")
            .with_arg(Encode!(&finding_ids)?)
            .call_and_wait()
            .await?;

        let result = Decode!(&response, String)?;
        Ok(result)
    }

    /// Get overall research system status
    pub async fn get_research_status(&self) -> Result<String> {
        let response = self.agent
            .query(&self.canister_id, "get_research_status")
            .with_arg(Encode!()?)
            .call()
            .await?;

        let result = Decode!(&response, String)?;
        Ok(result)
    }

    /// Manually trigger research processing (don't wait for heartbeat)
    pub async fn trigger_research(&self) -> Result<String> {
        let response = self.agent
            .update(&self.canister_id, "trigger_research")
            .with_arg(Encode!()?)
            .call_and_wait()
            .await?;

        let result = Decode!(&response, String)?;
        Ok(result)
    }
}

/// Sync result
#[derive(Debug)]
pub struct SyncResult {
    pub capsules_synced: usize,
    pub embeddings_synced: usize,
    pub errors: Vec<String>,
}

/// High-level sync function using native client
pub async fn sync_to_canister(
    client: &IcpClient,
    capsules: Vec<KnowledgeCapsule>,
    embeddings: Vec<CapsuleEmbedding>,
    batch_size: usize,
) -> Result<SyncResult> {
    let mut result = SyncResult {
        capsules_synced: 0,
        embeddings_synced: 0,
        errors: Vec::new(),
    };

    // Sync capsules in batches
    for (i, chunk) in capsules.chunks(batch_size).enumerate() {
        println!("  Syncing capsule batch {}/{}", i + 1, (capsules.len() + batch_size - 1) / batch_size);
        match client.add_capsules_bulk(chunk.to_vec()).await {
            Ok(ids) => {
                result.capsules_synced += ids.len();
            }
            Err(e) => {
                result.errors.push(format!("Capsule batch {} failed: {}", i + 1, e));
            }
        }
    }

    // Sync embeddings in batches
    for (i, chunk) in embeddings.chunks(batch_size).enumerate() {
        println!("  Syncing embedding batch {}/{}", i + 1, (embeddings.len() + batch_size - 1) / batch_size);
        match client.add_embeddings_bulk(chunk.to_vec()).await {
            Ok(added) => {
                result.embeddings_synced += added as usize;
            }
            Err(e) => {
                result.errors.push(format!("Embedding batch {} failed: {}", i + 1, e));
            }
        }
    }

    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_capsule_serialization() {
        let capsule = KnowledgeCapsule {
            id: 0,
            conversation_id: "test".to_string(),
            restatement: "Test fact".to_string(),
            timestamp: None,
            location: None,
            topic: Some("testing".to_string()),
            confidence_score: 0.9,
            persons: vec![],
            entities: vec![],
            keywords: vec!["test".to_string()],
            created_at: 0,
        };

        // Should be able to encode
        let encoded = Encode!(&capsule);
        assert!(encoded.is_ok());
    }
}
