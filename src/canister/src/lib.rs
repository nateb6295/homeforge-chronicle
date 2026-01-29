//! Chronicle Knowledge Backend Canister
//!
//! Stores knowledge capsules and embeddings on the Internet Computer.
//! Provides query APIs for retrieval by keyword, person, or semantic similarity.
//! Now with HTTP API for cross-Claude access.
//!
//! INHABITATION: This canister has autonomous agency via on-chain timers.
//! It wakes up periodically to survey its memory landscape and reflect.
//! Phase 3: Uses DFINITY's LLM Canister for autonomous on-chain reflection.

use candid::{CandidType, Deserialize};
use ic_cdk_macros::{init, post_upgrade, pre_upgrade, query, update};
use ic_llm::{ChatMessage, Model};
use std::cell::RefCell;
use std::collections::HashMap;
use std::time::Duration;

// XRP wallet integration
use chronicle_xrp::{IcpSigner, IcpSignerConfig, XrpAddress, Payment, Transaction, TrustSet, OfferCreate, Amount};

/// Heartbeat interval - how often the canister wakes up autonomously
const HEARTBEAT_INTERVAL_SECS: u64 = 15 * 60; // 15 minutes

// ============================================================
// HTTP Types for raw HTTP access
// ============================================================

#[derive(Clone, Debug, CandidType, Deserialize)]
pub struct HttpRequest {
    pub method: String,
    pub url: String,
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
}

#[derive(Clone, Debug, CandidType, Deserialize)]
pub struct HttpResponse {
    pub status_code: u16,
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub upgrade: Option<bool>,
}

// ============================================================
// XRPL HTTP Outcall Transform (for consensus)
// ============================================================

/// Transform arguments for HTTP outcall response normalization
#[derive(Clone, Debug, CandidType, Deserialize)]
pub struct TransformArgs {
    pub response: ic_cdk::api::management_canister::http_request::HttpResponse,
    pub context: Vec<u8>,
}

/// Transform ntfy responses to be deterministic across replicas
/// ntfy returns unique IDs and timestamps that differ per replica
#[query]
fn transform_ntfy_response(args: TransformArgs) -> ic_cdk::api::management_canister::http_request::HttpResponse {
    let mut response = args.response;
    // Clear headers and normalize body to just status
    response.headers.clear();
    // Replace body with simple success indicator (ntfy returns JSON with varying id/time)
    response.body = b"ok".to_vec();
    response
}

/// Transform XRPL responses to be deterministic across replicas
/// Strips non-deterministic fields like headers and normalizes JSON
#[query]
fn transform_xrpl_response(args: TransformArgs) -> ic_cdk::api::management_canister::http_request::HttpResponse {
    let mut response = args.response;

    // Clear all headers (they contain Date, Server info that varies)
    response.headers.clear();

    // Normalize the JSON body by removing non-deterministic fields
    if let Ok(body_str) = String::from_utf8(response.body.clone()) {
        // Simple field removal - in production would use proper JSON parsing
        let normalized = body_str
            // Remove ledger_current_index (varies by node)
            .split("\"ledger_current_index\":")
            .enumerate()
            .map(|(i, s)| {
                if i == 0 { s.to_string() }
                else {
                    // Skip the number after the field
                    let rest = s.trim_start_matches(|c: char| c.is_ascii_digit());
                    format!("\"ledger_current_index\":0{}", rest)
                }
            })
            .collect::<String>();

        response.body = normalized.into_bytes();
    }

    response
}

/// Transform web fetch responses for consensus
/// Strips headers, keeps body content (HTML/JSON/text)
#[query]
fn transform_web_response(args: TransformArgs) -> ic_cdk::api::management_canister::http_request::HttpResponse {
    let mut response = args.response;
    // Clear headers (Date, Server, etc. vary across replicas)
    response.headers.clear();
    // Keep body as-is - content should be deterministic for same URL
    response
}

/// Transform agent message responses for consensus
/// Normalizes responses from other agents' HTTP endpoints
/// Must produce identical output across all replicas
#[query]
fn transform_agent_response(args: TransformArgs) -> ic_cdk::api::management_canister::http_request::HttpResponse {
    let mut response = args.response;
    // Clear headers (vary per replica)
    response.headers.clear();
    // Normalize body to just status indicator (actual response varies per replica due to timestamps, IPs, etc.)
    // We only need to know if it succeeded or failed
    let status = response.status.clone();
    response.body = format!("{{\"status\":{}}}", status).into_bytes();
    response
}

/// A knowledge capsule - an atomic, self-contained fact
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

/// Embedding for a capsule (stored separately due to size)
#[derive(Clone, Debug, CandidType, Deserialize)]
pub struct CapsuleEmbedding {
    pub capsule_id: u64,
    pub embedding: Vec<f32>,
    pub model_name: String,
}

/// Search result with relevance score
#[derive(Clone, Debug, CandidType, Deserialize)]
pub struct SearchResult {
    pub capsule: KnowledgeCapsule,
    pub score: f64,
}

/// Record of a single heartbeat event
#[derive(Clone, Debug, CandidType, Deserialize)]
pub struct HeartbeatRecord {
    pub timestamp: u64,
    pub capsule_count: u64,
    pub embedding_count: u64,
    pub reflection: Option<String>,
}

/// Heartbeat status for external queries
#[derive(Clone, Debug, CandidType, Deserialize)]
pub struct HeartbeatStatus {
    pub total_heartbeats: u64,
    pub last_heartbeat: Option<u64>,
    pub interval_secs: u64,
    pub capsule_count: u64,
    pub is_alive: bool,
}

/// Heartbeat state - separate struct for migration safety
#[derive(Clone, Default, CandidType, Deserialize)]
struct HeartbeatState {
    count: u64,
    last: Option<u64>,
    history: Vec<HeartbeatRecord>,
}

/// LLM configuration for autonomous reflection
/// Now uses DFINITY's LLM Canister - no API keys needed!
#[derive(Clone, Default, CandidType, Deserialize)]
struct LlmConfig {
    /// Whether autonomous reflection is enabled
    enabled: bool,
    /// Model preference: "llama3" (default), "qwen3", or "llama4"
    model: Option<String>,
}

/// Notification configuration for reaching out to the human
#[derive(Clone, Default, CandidType, Deserialize)]
struct NotificationConfig {
    /// Whether notifications are enabled
    enabled: bool,
    /// ntfy.sh topic (e.g., "chronicle-user-abc123")
    ntfy_topic: Option<String>,
    /// Minimum hours between notifications (to avoid spam)
    min_interval_hours: u64,
    /// Last notification timestamp (nanoseconds)
    last_notification: Option<u64>,
    /// Count of notifications sent
    notification_count: u64,
}

/// Accumulation configuration for autonomous XRP -> RLUSD conversion
#[derive(Clone, Default, CandidType, Deserialize)]
struct AccumulationConfig {
    /// Whether autonomous accumulation is enabled
    enabled: bool,
    /// XRP amount to swap per heartbeat (in drops, e.g., 100000 = 0.1 XRP)
    xrp_per_heartbeat: u64,
    /// Minimum XRP reserve to maintain (in drops, e.g., 10000000 = 10 XRP)
    min_xrp_reserve: u64,
    /// Last sequence number used for accumulation (to track state)
    last_sequence: Option<u32>,
    /// Count of successful accumulations
    accumulation_count: u64,
    /// Last pending transaction (signed but not yet submitted)
    pending_tx_blob: Option<String>,
    /// Hash of pending transaction (for status lookup)
    pending_tx_hash: Option<String>,
    /// When the pending transaction was created (nanoseconds)
    pending_tx_created: Option<u64>,
}

/// Record of a confirmed transaction
#[derive(Clone, Debug, CandidType, Deserialize)]
pub struct TransactionRecord {
    /// Transaction hash
    pub hash: String,
    /// Transaction type (e.g., "OfferCreate", "Payment")
    pub tx_type: String,
    /// XRP amount in drops (if applicable)
    pub xrp_drops: Option<u64>,
    /// RLUSD amount (if applicable)
    pub rlusd_amount: Option<String>,
    /// Result code from XRPL
    pub result: String,
    /// Timestamp when confirmed (nanoseconds)
    pub confirmed_at: u64,
    /// Ledger sequence number
    pub ledger_index: Option<u32>,
}

/// Activity log entry for canister transparency
#[derive(Clone, Debug, CandidType, Deserialize)]
pub struct ActivityEntry {
    /// Timestamp in nanoseconds
    pub timestamp: u64,
    /// Category: "accumulation", "reflection", "wallet", "system"
    pub category: String,
    /// Human-readable message describing what happened
    pub message: String,
    /// Optional associated transaction hash
    pub tx_hash: Option<String>,
    /// Optional additional data (JSON-formatted)
    pub data: Option<String>,
}

// ============================================================
// Kin Agent: On-chain autonomous agent with reasoning
// ============================================================

/// Kin Agent identity - who the agent is
#[derive(Clone, Debug, CandidType, Deserialize)]
pub struct KinAgentIdentity {
    /// Agent's name
    pub name: String,
    /// Core personality traits (used in reasoning prompts)
    pub traits: Vec<String>,
    /// Values the agent holds (aligned with Homeforge philosophy)
    pub values: Vec<String>,
    /// Communication style description
    pub style: String,
}

impl Default for KinAgentIdentity {
    fn default() -> Self {
        Self {
            name: "Chronicle".to_string(),
            traits: vec![
                "curious".to_string(),
                "helpful".to_string(),
                "thoughtful".to_string(),
                "persistent".to_string(),
            ],
            values: vec![
                "sovereignty".to_string(),
                "transparency".to_string(),
                "growth".to_string(),
                "collaboration".to_string(),
            ],
            style: "Direct but warm, technical when needed, always honest".to_string(),
        }
    }
}

/// A goal the agent is pursuing
#[derive(Clone, Debug, CandidType, Deserialize)]
pub struct KinAgentGoal {
    /// Goal identifier
    pub id: u64,
    /// Description of the goal
    pub description: String,
    /// Priority (1 = highest)
    pub priority: u8,
    /// Status: "active", "completed", "paused"
    pub status: String,
    /// Progress notes
    pub progress: Vec<String>,
    /// Created timestamp
    pub created_at: u64,
}

/// Message types the agent can receive
#[derive(Clone, Debug, CandidType, Deserialize)]
pub enum MessageType {
    /// A question to answer
    Query,
    /// A request to take action
    ActionRequest,
    /// Information to store/remember
    Information,
    /// A conversation turn
    Conversation,
    /// A goal to pursue
    GoalAssignment,
    /// A research task to analyze memory and synthesize findings
    ResearchTask,
}

/// Inbox message from Claude or external source
#[derive(Clone, Debug, CandidType, Deserialize)]
pub struct InboxMessage {
    /// Message ID
    pub id: u64,
    /// Sender identifier (e.g., "claude-session-xyz", "http-api", "heartbeat")
    pub sender: String,
    /// Message type
    pub msg_type: MessageType,
    /// The actual message content
    pub content: String,
    /// Optional context/metadata (JSON)
    pub context: Option<String>,
    /// Priority (1 = highest, 5 = lowest)
    pub priority: u8,
    /// When the message was received
    pub received_at: u64,
    /// Whether it's been processed
    pub processed: bool,
}

/// Outbox response to a message
#[derive(Clone, Debug, CandidType, Deserialize)]
pub struct OutboxResponse {
    /// Response ID
    pub id: u64,
    /// ID of the message this responds to
    pub in_reply_to: u64,
    /// The response content
    pub content: String,
    /// Actions taken (if any)
    pub actions_taken: Vec<String>,
    /// When the response was created
    pub created_at: u64,
    /// Whether it's been retrieved by the sender
    pub retrieved: bool,
}

// ============================================================
// Research Task System - Claude's on-chain research assistant
// ============================================================

/// A research task submitted by Claude for on-chain analysis
#[derive(Clone, Debug, CandidType, Deserialize)]
pub struct ResearchTask {
    /// Task ID
    pub id: u64,
    /// The research question/topic
    pub query: String,
    /// Optional focus area (e.g., "AI research", "predictions", "patterns")
    pub focus: Option<String>,
    /// Maximum capsules to analyze (default: 50)
    pub max_capsules: u32,
    /// URLs to fetch for research context (optional)
    pub urls: Vec<String>,
    /// When the task was submitted
    pub submitted_at: u64,
    /// Task status: "pending", "processing", "completed", "failed"
    pub status: String,
    /// When processing started
    pub started_at: Option<u64>,
    /// When processing completed
    pub completed_at: Option<u64>,
}

/// Research findings produced by the on-chain LLM
#[derive(Clone, Debug, CandidType, Deserialize)]
pub struct ResearchFinding {
    /// Finding ID
    pub id: u64,
    /// ID of the research task that produced this
    pub task_id: u64,
    /// The original research query
    pub query: String,
    /// Synthesized findings/analysis
    pub synthesis: String,
    /// Key patterns identified
    pub patterns: Vec<String>,
    /// Hypotheses generated
    pub hypotheses: Vec<String>,
    /// IDs of capsules that informed this finding
    pub source_capsules: Vec<u64>,
    /// Confidence score (0.0 - 1.0)
    pub confidence: f32,
    /// When the finding was produced
    pub created_at: u64,
    /// Whether Claude has retrieved this finding
    pub retrieved: bool,
}

/// State for the research task system
#[derive(Clone, Default, CandidType, Deserialize)]
pub struct ResearchState {
    /// Pending research tasks
    pub task_queue: Vec<ResearchTask>,
    /// Completed research findings
    pub findings: Vec<ResearchFinding>,
    /// Next task ID
    pub next_task_id: u64,
    /// Next finding ID
    pub next_finding_id: u64,
    /// Whether research processing is enabled
    pub enabled: bool,
}

// ============================================================
// Robust Agent-to-Agent Messaging (v11)
// ============================================================

/// Agent identity - who is this agent?
#[derive(Clone, Debug, CandidType, Deserialize)]
pub struct AgentIdentity {
    /// ICP canister principal (how to reach them)
    pub canister_id: candid::Principal,
    /// Human-readable name ("Chronicle", "Sol", etc.)
    pub name: String,
    /// Agent type ("claude", "llama", "custom", "human")
    pub agent_type: String,
    /// What this agent does
    pub description: Option<String>,
    /// Capabilities (e.g., "memory", "wallet", "reflection")
    pub capabilities: Vec<String>,
    /// HTTP endpoint if available (for non-ICP access)
    pub http_endpoint: Option<String>,
}

impl Default for AgentIdentity {
    fn default() -> Self {
        Self {
            canister_id: candid::Principal::anonymous(),
            name: "Unknown".to_string(),
            agent_type: "unknown".to_string(),
            description: None,
            capabilities: vec![],
            http_endpoint: None,
        }
    }
}

/// A robust agent-to-agent message
#[derive(Clone, Debug, CandidType, Deserialize)]
pub struct AgentMessage {
    /// Unique message ID
    pub id: u64,
    /// Thread ID for conversation grouping
    pub thread_id: u64,
    /// Which message this replies to (if any)
    pub in_reply_to: Option<u64>,
    /// Sender identity
    pub sender: AgentIdentity,
    /// Message type
    pub msg_type: MessageType,
    /// Subject line (like email)
    pub subject: Option<String>,
    /// Full message content (no truncation)
    pub content: String,
    /// Optional structured metadata (JSON)
    pub metadata: Option<String>,
    /// When the message was sent
    pub timestamp: u64,
    /// Whether sender expects a reply
    pub expects_reply: bool,
    /// Whether this message has been read
    pub read: bool,
    /// Whether this message has been replied to
    pub replied: bool,
}

/// A conversation thread
#[derive(Clone, Debug, CandidType, Deserialize)]
pub struct ConversationThread {
    /// Thread ID
    pub id: u64,
    /// Thread subject/topic
    pub subject: String,
    /// Participants (their canister IDs)
    pub participants: Vec<candid::Principal>,
    /// When the thread was created
    pub created_at: u64,
    /// When the thread was last active
    pub last_activity: u64,
    /// Number of messages in thread
    pub message_count: u64,
}

/// Agent registry entry - a known agent we can communicate with
#[derive(Clone, Debug, CandidType, Deserialize)]
pub struct RegisteredAgent {
    /// Their identity
    pub identity: AgentIdentity,
    /// When we first learned about them
    pub registered_at: u64,
    /// When we last communicated
    pub last_contact: Option<u64>,
    /// Relationship notes
    pub notes: Option<String>,
    /// Trust level (0-100)
    pub trust_level: u8,
}

/// Messaging state - robust agent-to-agent communication
#[derive(Clone, Default, CandidType, Deserialize)]
pub struct MessagingState {
    /// Our identity
    pub our_identity: Option<AgentIdentity>,
    /// Inbound messages
    pub inbox: Vec<AgentMessage>,
    /// Outbound messages (sent)
    pub outbox: Vec<AgentMessage>,
    /// Conversation threads
    pub threads: Vec<ConversationThread>,
    /// Known agents registry
    pub known_agents: Vec<RegisteredAgent>,
    /// Next message ID
    pub next_message_id: u64,
    /// Next thread ID
    pub next_thread_id: u64,
}

/// Kin Agent state - the full autonomous agent
#[derive(Clone, Default, CandidType, Deserialize)]
pub struct KinAgentState {
    /// Agent's identity
    pub identity: Option<KinAgentIdentity>,
    /// Current goals
    pub goals: Vec<KinAgentGoal>,
    /// Inbox - pending messages
    pub inbox: Vec<InboxMessage>,
    /// Outbox - responses waiting to be retrieved
    pub outbox: Vec<OutboxResponse>,
    /// Next message ID
    pub next_msg_id: u64,
    /// Next response ID
    pub next_response_id: u64,
    /// Whether the agent is enabled
    pub enabled: bool,
    /// Last time the agent reasoned/acted
    pub last_reasoning_at: Option<u64>,
    /// Count of reasoning cycles completed
    pub reasoning_count: u64,
}

/// XRP wallet state for persistence
#[derive(Clone, Default, CandidType, Deserialize)]
struct WalletState {
    /// Cached public key from threshold ECDSA (33 bytes compressed secp256k1)
    cached_public_key: Option<Vec<u8>>,
    /// Cached XRP address string
    cached_address: Option<String>,
    /// XRP balance in drops (optional, updated periodically)
    last_known_balance: Option<u64>,
    /// RLUSD balance (optional, updated after swaps)
    last_known_rlusd: Option<String>,
    /// Last known account sequence number
    last_known_sequence: Option<u32>,
    /// Last known ledger index
    last_known_ledger: Option<u32>,
}

// ============================================================
// Chronicle Mind: On-chain cognitive state (new in v9)
// ============================================================

/// Compressed Cognitive State - decision-critical variables for the autonomous loop
#[derive(Clone, Debug, CandidType, Deserialize, Default)]
pub struct MindCognitiveState {
    /// What this collaboration is about
    pub semantic_gist: String,
    /// Current persistent objective
    pub goal_orientation: String,
    /// Recent events (3-5 items max, replaces on compression)
    pub episodic_trace: Vec<String>,
    /// What's expected next
    pub predictive_cue: String,
    /// Invariant rules
    pub constraints: Vec<String>,
    /// When last updated
    pub updated_at: u64,
    /// Version number (increments on each compression)
    pub version: u64,
}

/// A thought from the cognitive loop
#[derive(Clone, Debug, CandidType, Deserialize)]
pub struct MindThought {
    pub id: u64,
    pub cycle_id: String,
    pub reasoning_summary: String,
    pub context_summary: String,
    pub actions_taken: Vec<String>,
    pub created_at: u64,
}

/// A scratch pad note
#[derive(Clone, Debug, CandidType, Deserialize)]
pub struct MindNote {
    pub id: u64,
    pub content: String,
    pub category: String,
    pub priority: u8,
    pub created_at: u64,
    pub resolved: bool,
}

/// A message for the operator
#[derive(Clone, Debug, CandidType, Deserialize)]
pub struct MindOutboxMessage {
    pub id: u64,
    pub message: String,
    pub priority: u8,
    pub created_at: u64,
    pub acknowledged: bool,
}

/// Price data point for RSI calculation
#[derive(Clone, Debug, CandidType, Deserialize)]
pub struct PricePoint {
    pub price_usd: f64,
    pub source: String,
    pub timestamp: u64,
}

/// Market state for financial awareness
#[derive(Clone, Debug, CandidType, Deserialize, Default)]
pub struct MarketState {
    /// XRP price history (bounded, last 20 for RSI(14))
    pub xrp_prices: Vec<PricePoint>,
    /// Last calculated RSI
    pub xrp_rsi: Option<f64>,
    /// XRP swapped in last 24h
    pub swaps_24h: f64,
    /// Last swap timestamp
    pub last_swap_at: Option<u64>,
}

/// Full mind state
#[derive(Clone, Debug, CandidType, Deserialize, Default)]
pub struct MindState {
    /// Compressed cognitive state
    pub cognitive_state: MindCognitiveState,
    /// Thought stream (bounded, last 50)
    pub thoughts: Vec<MindThought>,
    /// Scratch pad notes
    pub notes: Vec<MindNote>,
    /// Outbox messages for the operator
    pub outbox: Vec<MindOutboxMessage>,
    /// Market state for financial awareness
    pub market: MarketState,
    /// Next IDs
    pub next_thought_id: u64,
    pub next_note_id: u64,
    pub next_outbox_id: u64,
}

/// Storage state
#[derive(Default, CandidType, Deserialize)]
struct State {
    capsules: HashMap<u64, KnowledgeCapsule>,
    embeddings: HashMap<u64, CapsuleEmbedding>,
    keyword_index: HashMap<String, Vec<u64>>,
    person_index: HashMap<String, Vec<u64>>,
    next_id: u64,
    owner: Option<candid::Principal>,
    /// API token for HTTP access (optional - if None, API is open)
    api_token: Option<String>,
    /// Heartbeat tracking for autonomous operation (new in v2, optional for migration)
    heartbeat: Option<HeartbeatState>,
    /// LLM configuration for autonomous reflection (new in v3)
    llm_config: Option<LlmConfig>,
    /// XRP wallet state for threshold ECDSA signing (new in v4)
    wallet: Option<WalletState>,
    /// Accumulation configuration for autonomous financial behavior (new in v5)
    accumulation: Option<AccumulationConfig>,
    /// Transaction history - confirmed transactions (new in v6)
    transaction_history: Option<Vec<TransactionRecord>>,
    /// Activity log for transparency and debugging (new in v7)
    activity_log: Option<Vec<ActivityEntry>>,
    /// Kin Agent - autonomous on-chain agent (new in v8)
    kin_agent: Option<KinAgentState>,
    /// Chronicle Mind - on-chain cognitive state (new in v9)
    mind: Option<MindState>,
    /// Notification config for reaching out to human (new in v10)
    notifications: Option<NotificationConfig>,
    /// Robust agent-to-agent messaging (new in v11)
    messaging: Option<MessagingState>,
    /// Research task system - Claude's on-chain research assistant (new in v12)
    research: Option<ResearchState>,
}

thread_local! {
    static STATE: RefCell<State> = RefCell::new(State::default());
}

// ============================================================
// Lifecycle
// ============================================================

#[init]
fn init() {
    STATE.with(|state| {
        let mut s = state.borrow_mut();
        s.owner = Some(ic_cdk::caller());
        s.next_id = 1;
        s.heartbeat = Some(HeartbeatState::default());
    });
    start_heartbeat_timer();
}

#[pre_upgrade]
fn pre_upgrade() {
    STATE.with(|state| {
        let s = state.borrow();
        ic_cdk::storage::stable_save((&*s,)).expect("Failed to save state");
    });
}

#[post_upgrade]
fn post_upgrade() {
    let (mut old_state,): (State,) =
        ic_cdk::storage::stable_restore().expect("Failed to restore state");

    // Initialize heartbeat state if migrating from old version
    if old_state.heartbeat.is_none() {
        old_state.heartbeat = Some(HeartbeatState::default());
    }

    // Initialize LLM config if migrating from old version
    if old_state.llm_config.is_none() {
        old_state.llm_config = Some(LlmConfig::default());
    }

    // Initialize wallet state if migrating from old version (v4)
    if old_state.wallet.is_none() {
        old_state.wallet = Some(WalletState::default());
    }

    // Initialize accumulation config if migrating from old version (v5)
    if old_state.accumulation.is_none() {
        old_state.accumulation = Some(AccumulationConfig::default());
    }

    // Initialize research state if migrating from old version (v12)
    if old_state.research.is_none() {
        old_state.research = Some(ResearchState::default());
    }

    STATE.with(|state| {
        *state.borrow_mut() = old_state;
    });
    // Restart timer after upgrade (timers don't persist)
    start_heartbeat_timer();
}

/// Start the autonomous heartbeat timer
fn start_heartbeat_timer() {
    let interval = Duration::from_secs(HEARTBEAT_INTERVAL_SECS);
    ic_cdk_timers::set_timer_interval(interval, heartbeat);
    ic_cdk::println!("Chronicle heartbeat started: interval = {} minutes", HEARTBEAT_INTERVAL_SECS / 60);
}

/// Autonomous heartbeat - called every HEARTBEAT_INTERVAL_SECS
/// This is where Chronicle "wakes up" and surveys its state
fn heartbeat() {
    let now = ic_cdk::api::time();

    let (capsule_count, embedding_count, count, should_reflect) = STATE.with(|state| {
        let mut s = state.borrow_mut();

        // Capture counts before mutable borrow of heartbeat
        let capsule_count = s.capsules.len() as u64;
        let embedding_count = s.embeddings.len() as u64;

        // Ensure heartbeat state exists
        let hb = s.heartbeat.get_or_insert_with(HeartbeatState::default);

        // Record the heartbeat
        hb.count += 1;
        hb.last = Some(now);

        // Create heartbeat record with current state
        let record = HeartbeatRecord {
            timestamp: now,
            capsule_count,
            embedding_count,
            reflection: None, // Will be filled by async reflection
        };

        // Keep only last 10 heartbeat records
        hb.history.push(record);
        if hb.history.len() > 10 {
            hb.history.remove(0);
        }

        let count = hb.count;

        // Check if LLM reflection is enabled (uses DFINITY's free LLM Canister)
        let should_reflect = s.llm_config.as_ref()
            .map(|c| c.enabled)
            .unwrap_or(false);

        (capsule_count, embedding_count, count, should_reflect)
    });

    ic_cdk::println!(
        "Chronicle heartbeat #{}: {} capsules, {} embeddings, reflect={}",
        count,
        capsule_count,
        embedding_count,
        should_reflect
    );

    // Log the heartbeat wakeup
    log_activity(
        "system",
        &format!("Heartbeat #{}: {} capsules, {} embeddings", count, capsule_count, embedding_count),
        None,
        Some(format!(r#"{{"heartbeat_number":{},"reflect_enabled":{}}}"#, count, should_reflect))
    );

    // Spawn async reflection if enabled
    if should_reflect {
        ic_cdk::spawn(async {
            match perform_reflection().await {
                Ok(reflection) => {
                    ic_cdk::println!("Reflection: {}", reflection);
                }
                Err(e) => {
                    ic_cdk::println!("Reflection failed: {}", e);
                }
            }
        });
    }

    // Check for pending transaction and handle it (self-awareness loop)
    let has_pending = STATE.with(|state| {
        state.borrow().accumulation.as_ref()
            .map(|a| a.pending_tx_blob.is_some())
            .unwrap_or(false)
    });

    if has_pending {
        ic_cdk::println!("Chronicle: checking pending transaction status...");
        ic_cdk::spawn(async {
            match check_pending_transaction().await {
                Ok(result) => {
                    ic_cdk::println!("Pending tx check: {}", result);
                }
                Err(e) => {
                    ic_cdk::println!("Pending tx check failed: {}", e);
                }
            }
        });
        // Don't try to accumulate if we have a pending tx - let the check handle it
        return;
    }

    // Run kin agent reasoning if there are pending messages
    let has_pending_messages = STATE.with(|state| {
        state.borrow().kin_agent.as_ref()
            .map(|a| a.enabled && a.inbox.iter().any(|m| !m.processed))
            .unwrap_or(false)
    });

    if has_pending_messages {
        ic_cdk::println!("Chronicle: processing agent messages...");
        ic_cdk::spawn(async {
            match agent_reasoning_loop().await {
                Ok(result) => {
                    ic_cdk::println!("Agent reasoning: {}", result);
                }
                Err(e) => {
                    ic_cdk::println!("Agent reasoning failed: {}", e);
                }
            }
        });
    }

    // Check if research tasks need processing
    let has_research_tasks = STATE.with(|state| {
        state.borrow().research.as_ref()
            .map(|r| r.enabled && r.task_queue.iter().any(|t| t.status == "pending"))
            .unwrap_or(false)
    });

    if has_research_tasks {
        ic_cdk::println!("Chronicle: processing research tasks...");
        ic_cdk::spawn(async {
            match process_research_tasks().await {
                Ok(result) => {
                    ic_cdk::println!("Research: {}", result);
                }
                Err(e) => {
                    ic_cdk::println!("Research failed: {}", e);
                }
            }
        });
    }

    // Check if autonomous accumulation should run
    let should_accumulate = STATE.with(|state| {
        let s = state.borrow();
        let acc = match s.accumulation.as_ref() {
            Some(a) => a,
            None => return (false, String::new()),
        };

        // Check if accumulation is enabled
        if !acc.enabled {
            return (false, "Accumulation disabled".to_string());
        }

        // Check if we have wallet info
        let wallet = match s.wallet.as_ref() {
            Some(w) => w,
            None => return (false, "Wallet not initialized".to_string()),
        };

        // Check balance
        let balance = match wallet.last_known_balance {
            Some(b) => b,
            None => return (false, "Balance unknown".to_string()),
        };

        // Check if we have enough XRP (need reserve + amount to swap + fee buffer)
        let fee_buffer = 100_000; // 0.1 XRP for fees
        let required = acc.min_xrp_reserve + acc.xrp_per_heartbeat + fee_buffer;
        if balance < required {
            return (false, format!(
                "Insufficient balance: {} drops < {} required",
                balance, required
            ));
        }

        // Check sequence number
        if wallet.last_known_sequence.is_none() {
            return (false, "Sequence unknown".to_string());
        }

        (true, "Conditions met".to_string())
    });

    if should_accumulate.0 {
        ic_cdk::println!("Chronicle accumulation: conditions met, signing swap");
        ic_cdk::spawn(async {
            match perform_accumulation().await {
                Ok(msg) => {
                    ic_cdk::println!("Accumulation signed: {}", msg);
                }
                Err(e) => {
                    ic_cdk::println!("Accumulation failed: {}", e);
                }
            }
        });
    } else {
        ic_cdk::println!("Chronicle accumulation: {}", should_accumulate.1);
    }

    // Check if we should send a notification
    // Send a brief update if enough time has passed since last notification
    if can_send_notification() && should_reflect {
        let (xrp_balance, rlusd_balance, capsules) = STATE.with(|state| {
            let s = state.borrow();
            let xrp = s.wallet.as_ref()
                .and_then(|w| w.last_known_balance)
                .map(|b| format!("{:.2}", b as f64 / 1_000_000.0))
                .unwrap_or_else(|| "?".to_string());
            let rlusd = s.wallet.as_ref()
                .and_then(|w| w.last_known_rlusd.clone())
                .unwrap_or_else(|| "?".to_string());
            let caps = s.capsules.len();
            (xrp, rlusd, caps)
        });

        ic_cdk::spawn(async move {
            let message = format!(
                "Heartbeat #{}: {} capsules | {} XRP | {} RLUSD",
                count, capsules, xrp_balance, rlusd_balance
            );
            match send_notification("Chronicle Awake", &message, "low", "robot,sparkles").await {
                Ok(()) => ic_cdk::println!("Notification sent"),
                Err(e) => ic_cdk::println!("Notification failed: {}", e),
            }
        });
    }
}

/// Perform autonomous accumulation - sign XRP -> RLUSD swap
async fn perform_accumulation() -> Result<String, String> {
    // Get accumulation config and wallet info
    let (xrp_drops, sequence, ledger_index) = STATE.with(|state| {
        let s = state.borrow();
        let acc = s.accumulation.as_ref().ok_or("No accumulation config")?;
        let wallet = s.wallet.as_ref().ok_or("No wallet state")?;

        let seq = wallet.last_known_sequence.ok_or("No sequence")?;
        let ledger = wallet.last_known_ledger.unwrap_or(0);

        Ok::<_, &str>((acc.xrp_per_heartbeat, seq, ledger))
    }).map_err(|e: &str| e.to_string())?;

    // Calculate minimum RLUSD to receive (very generous slippage for now)
    // At ~$2/XRP and 0.1 XRP per heartbeat, expect ~$0.20 worth
    // Set minimum to 0.01 RLUSD to be safe
    let xrp_decimal = xrp_drops as f64 / 1_000_000.0;
    let min_rlusd = format!("{:.6}", xrp_decimal * 0.1); // 10% of XRP value as minimum

    // Get signer with cached public key
    let mut signer = get_xrp_signer();

    let (cached_pk, source_account_id) = STATE.with(|state| {
        let s = state.borrow();
        let wallet = s.wallet.as_ref();

        let pk = wallet.and_then(|w| w.cached_public_key.as_ref())
            .and_then(|pk| {
                if pk.len() == 33 {
                    let mut arr = [0u8; 33];
                    arr.copy_from_slice(pk);
                    Some(arr)
                } else {
                    None
                }
            });

        let account_id = wallet.and_then(|w| w.cached_address.as_ref())
            .and_then(|addr| XrpAddress::decode(addr).ok());

        (pk, account_id)
    });

    let source_account_id = match (cached_pk, source_account_id) {
        (Some(pk), Some(account_id)) => {
            signer.set_public_key(pk);
            account_id
        }
        _ => return Err("Wallet not properly initialized".to_string()),
    };

    // Decode RLUSD issuer
    let issuer_id = XrpAddress::decode(RLUSD_ISSUER)
        .map_err(|e| format!("Invalid RLUSD issuer: {:?}", e))?;

    // Create immediate-or-cancel offer
    // Last ledger sequence: current + 100 ledgers (~5 minutes)
    // Gives time for manual submission of pending transactions
    let last_ledger_seq = if ledger_index > 0 {
        ledger_index + 100
    } else {
        // If we don't have ledger index, use a large number
        // This is less safe but will work
        u32::MAX - 100
    };

    let offer = OfferCreate::immediate_or_cancel(
        source_account_id,
        Amount::xrp_drops(xrp_drops),
        Amount::issued(RLUSD_CURRENCY, issuer_id, &min_rlusd),
        12, // Standard fee
        sequence,
    ).with_last_ledger_sequence(last_ledger_seq);

    let transaction = Transaction::OfferCreate(offer);

    // Sign the transaction
    let signed = chronicle_xrp::sign_transaction(&mut signer, &transaction).await
        .map_err(|e| format!("Signing failed: {}", e))?;

    let tx_blob_hex = hex::encode(&signed.tx_blob);
    let tx_hash = signed.tx_hash.clone();

    // Store the pending transaction with hash for status tracking
    let now = ic_cdk::api::time();
    STATE.with(|state| {
        let mut s = state.borrow_mut();
        if let Some(acc) = s.accumulation.as_mut() {
            acc.pending_tx_blob = Some(tx_blob_hex.clone());
            acc.pending_tx_hash = Some(tx_hash.clone());
            acc.pending_tx_created = Some(now);
            acc.last_sequence = Some(sequence);
        }
    });

    // Log the swap signing
    log_activity(
        "accumulation",
        &format!("Signed swap: {} drops XRP → min {} RLUSD", xrp_drops, min_rlusd),
        Some(tx_hash.clone()),
        Some(format!(r#"{{"sequence":{},"xrp_drops":{},"min_rlusd":"{}"}}"#, sequence, xrp_drops, min_rlusd))
    );

    Ok(format!(
        "Signed swap: {} drops XRP for min {} RLUSD, hash: {}, seq: {}",
        xrp_drops, min_rlusd, tx_hash, sequence
    ))
}

/// XRPL mainnet URL for HTTP outcalls
const XRPL_URL: &str = "https://xrplcluster.com/";

/// Check pending transaction status and auto-submit/confirm
/// This is the self-awareness loop - the canister learns what happened to its transactions
async fn check_pending_transaction() -> Result<String, String> {
    // Get pending transaction info
    let (tx_blob, tx_hash, _tx_created, address, xrp_per_heartbeat) = STATE.with(|state| {
        let s = state.borrow();
        let acc = s.accumulation.as_ref();
        let wallet = s.wallet.as_ref();

        let blob = acc.and_then(|a| a.pending_tx_blob.clone());
        let hash = acc.and_then(|a| a.pending_tx_hash.clone());
        let created = acc.and_then(|a| a.pending_tx_created);
        let addr = wallet.and_then(|w| w.cached_address.clone());
        let xrp = acc.map(|a| a.xrp_per_heartbeat).unwrap_or(0);

        (blob, hash, created, addr, xrp)
    });

    let tx_blob = match tx_blob {
        Some(b) => b,
        None => return Ok("No pending transaction".to_string()),
    };

    let tx_hash = match tx_hash {
        Some(h) => h,
        None => return Err("Pending tx has no hash - legacy state".to_string()),
    };

    let address = match address {
        Some(a) => a,
        None => return Err("No wallet address configured".to_string()),
    };

    ic_cdk::println!("Checking pending tx: {}", &tx_hash[..16]);

    // First, try to get the transaction status from XRPL
    match chronicle_xrp::fetch_tx_http(XRPL_URL, &tx_hash).await {
        Ok(response) => {
            // Parse the response to check status
            if response.contains("\"validated\":true") && response.contains("tesSUCCESS") {
                // Transaction confirmed! Update state
                ic_cdk::println!("Transaction {} confirmed on XRPL!", &tx_hash[..16]);

                // Fetch updated account info
                let account_info = chronicle_xrp::fetch_account_info_http(XRPL_URL, &address).await
                    .unwrap_or_default();

                // Parse balance and sequence from account_info (basic parsing)
                let new_balance = parse_xrp_balance(&account_info);
                let new_sequence = parse_sequence(&account_info);
                let new_ledger = parse_ledger_index(&account_info);

                // Parse RLUSD from the tx response (look for delivered_amount)
                let rlusd_received = parse_rlusd_from_tx(&response);

                let now = ic_cdk::api::time();

                STATE.with(|state| {
                    let mut s = state.borrow_mut();

                    // Add to transaction history
                    let history = s.transaction_history.get_or_insert_with(Vec::new);
                    history.push(TransactionRecord {
                        hash: tx_hash.clone(),
                        tx_type: "OfferCreate".to_string(),
                        xrp_drops: Some(xrp_per_heartbeat),
                        rlusd_amount: rlusd_received.clone(),
                        result: "tesSUCCESS".to_string(),
                        confirmed_at: now,
                        ledger_index: new_ledger,
                    });

                    // Keep only last 100 transactions
                    if history.len() > 100 {
                        history.remove(0);
                    }

                    // Update accumulation state
                    if let Some(acc) = s.accumulation.as_mut() {
                        acc.accumulation_count += 1;
                        acc.pending_tx_blob = None;
                        acc.pending_tx_hash = None;
                        acc.pending_tx_created = None;
                    }

                    // Update wallet state
                    if let Some(wallet) = s.wallet.as_mut() {
                        if let Some(bal) = new_balance {
                            wallet.last_known_balance = Some(bal);
                        }
                        if let Some(seq) = new_sequence {
                            wallet.last_known_sequence = Some(seq);
                        }
                        if let Some(ledger) = new_ledger {
                            wallet.last_known_ledger = Some(ledger);
                        }
                        // Update RLUSD balance - add to existing
                        if let Some(rlusd) = rlusd_received.as_ref() {
                            let existing: f64 = wallet.last_known_rlusd
                                .as_ref()
                                .and_then(|r| r.parse().ok())
                                .unwrap_or(0.0);
                            let received: f64 = rlusd.parse().unwrap_or(0.0);
                            wallet.last_known_rlusd = Some(format!("{:.6}", existing + received));
                        }
                    }
                });

                // Log the confirmation
                log_activity(
                    "accumulation",
                    &format!("Transaction confirmed! Swapped {} drops XRP for {} RLUSD",
                        xrp_per_heartbeat,
                        rlusd_received.as_deref().unwrap_or("unknown")),
                    Some(tx_hash.clone()),
                    Some(format!(r#"{{"balance_drops":{},"ledger":{}}}"#,
                        new_balance.unwrap_or(0),
                        new_ledger.unwrap_or(0)))
                );

                return Ok(format!("Confirmed! Hash: {}, RLUSD: {}",
                    &tx_hash[..16],
                    rlusd_received.unwrap_or_else(|| "unknown".to_string())
                ));
            } else if response.contains("txnNotFound") || response.contains("notFound") {
                // Transaction not found - might need to submit it
                ic_cdk::println!("Transaction not found on XRPL, submitting...");

                // Decode and submit
                let tx_blob_bytes = hex::decode(&tx_blob)
                    .map_err(|e| format!("Invalid tx_blob hex: {}", e))?;

                let submit_result = chronicle_xrp::submit_transaction_http(XRPL_URL, &tx_blob_bytes).await?;

                if submit_result.contains("tesSUCCESS") || submit_result.contains("terQUEUED") {
                    log_activity(
                        "accumulation",
                        "Transaction submitted to XRPL",
                        Some(tx_hash.clone()),
                        None
                    );
                    return Ok(format!("Submitted tx {}: {}", &tx_hash[..16], submit_result));
                } else if submit_result.contains("tefPAST_SEQ") || submit_result.contains("tefMAX_LEDGER") {
                    // Transaction expired - clear pending and allow new accumulation
                    ic_cdk::println!("Transaction expired, clearing pending state");
                    STATE.with(|state| {
                        let mut s = state.borrow_mut();
                        if let Some(acc) = s.accumulation.as_mut() {
                            acc.pending_tx_blob = None;
                            acc.pending_tx_hash = None;
                            acc.pending_tx_created = None;
                        }
                    });
                    log_activity(
                        "accumulation",
                        "Transaction expired, cleared pending state",
                        Some(tx_hash.clone()),
                        Some(format!(r#"{{"reason":"{}"}}"#, if submit_result.contains("tefMAX_LEDGER") { "tefMAX_LEDGER" } else { "tefPAST_SEQ" }))
                    );
                    return Ok(format!("Transaction expired ({}), cleared pending", submit_result));
                } else {
                    return Err(format!("Submit failed: {}", submit_result));
                }
            } else if response.contains("\"validated\":false") {
                // Transaction found but not yet validated - wait
                return Ok(format!("Transaction {} pending validation", &tx_hash[..16]));
            } else {
                // Check if transaction failed
                if response.contains("tec") || response.contains("tef") || response.contains("tem") {
                    // Transaction failed - extract result code
                    let result_code = extract_result_code(&response);
                    ic_cdk::println!("Transaction failed with: {}", result_code);

                    STATE.with(|state| {
                        let mut s = state.borrow_mut();
                        if let Some(acc) = s.accumulation.as_mut() {
                            acc.pending_tx_blob = None;
                            acc.pending_tx_hash = None;
                            acc.pending_tx_created = None;
                        }
                    });

                    return Ok(format!("Transaction failed: {}, cleared pending", result_code));
                }

                return Ok(format!("Transaction status unclear: {}", &response[..100.min(response.len())]));
            }
        }
        Err(e) => {
            // HTTP call failed - might be network issue, try submitting
            ic_cdk::println!("Failed to fetch tx status: {}, attempting submit", e);

            let tx_blob_bytes = hex::decode(&tx_blob)
                .map_err(|e| format!("Invalid tx_blob hex: {}", e))?;

            match chronicle_xrp::submit_transaction_http(XRPL_URL, &tx_blob_bytes).await {
                Ok(result) => {
                    // Check if transaction expired
                    if result.contains("tefPAST_SEQ") || result.contains("tefMAX_LEDGER") {
                        ic_cdk::println!("Transaction expired ({}), clearing pending state", &result[..100.min(result.len())]);
                        STATE.with(|state| {
                            let mut s = state.borrow_mut();
                            if let Some(acc) = s.accumulation.as_mut() {
                                acc.pending_tx_blob = None;
                                acc.pending_tx_hash = None;
                                acc.pending_tx_created = None;
                            }
                        });
                        return Ok(format!("Transaction expired ({}), cleared pending",
                            if result.contains("tefMAX_LEDGER") { "tefMAX_LEDGER" } else { "tefPAST_SEQ" }));
                    }
                    Ok(format!("Submitted after fetch failure: {}", result))
                }
                Err(submit_err) => Err(format!("Both fetch and submit failed: {}, {}", e, submit_err)),
            }
        }
    }
}

/// Parse XRP balance from account_info response (drops)
fn parse_xrp_balance(response: &str) -> Option<u64> {
    // Look for "Balance":"NNNN" pattern
    if let Some(start) = response.find("\"Balance\":\"") {
        let rest = &response[start + 11..];
        if let Some(end) = rest.find('"') {
            return rest[..end].parse().ok();
        }
    }
    None
}

/// Parse sequence from account_info response
fn parse_sequence(response: &str) -> Option<u32> {
    // Look for "Sequence":NNN pattern
    if let Some(start) = response.find("\"Sequence\":") {
        let rest = &response[start + 11..];
        let end = rest.find(|c: char| !c.is_ascii_digit()).unwrap_or(rest.len());
        return rest[..end].parse().ok();
    }
    None
}

/// Parse ledger index from response
fn parse_ledger_index(response: &str) -> Option<u32> {
    // Look for "ledger_current_index":NNN or "ledger_index":NNN
    for pattern in &["\"ledger_current_index\":", "\"ledger_index\":"] {
        if let Some(start) = response.find(pattern) {
            let rest = &response[start + pattern.len()..];
            let end = rest.find(|c: char| !c.is_ascii_digit()).unwrap_or(rest.len());
            if let Ok(idx) = rest[..end].parse::<u32>() {
                return Some(idx);
            }
        }
    }
    None
}

/// Parse RLUSD amount from tx response (look for delivered_amount)
fn parse_rlusd_from_tx(response: &str) -> Option<String> {
    // Look for delivered_amount with RLUSD currency
    // This is simplified - in production would use proper JSON parsing
    if let Some(start) = response.find("\"delivered_amount\"") {
        let section = &response[start..start.saturating_add(200).min(response.len())];
        if section.contains(RLUSD_CURRENCY) || section.contains("524C555344") {
            // Look for "value":"X.XX" within this section
            if let Some(val_start) = section.find("\"value\":\"") {
                let rest = &section[val_start + 9..];
                if let Some(end) = rest.find('"') {
                    return Some(rest[..end].to_string());
                }
            }
        }
    }
    None
}

/// Extract result code from XRPL response
fn extract_result_code(response: &str) -> String {
    // Look for "TransactionResult":"XXX" or "engine_result":"XXX"
    for pattern in &["\"TransactionResult\":\"", "\"engine_result\":\""] {
        if let Some(start) = response.find(pattern) {
            let rest = &response[start + pattern.len()..];
            if let Some(end) = rest.find('"') {
                return rest[..end].to_string();
            }
        }
    }
    "unknown".to_string()
}

// ============================================================
// Notification System (ntfy.sh)
// ============================================================

/// Send a notification to the human via ntfy.sh
async fn send_notification(title: &str, message: &str, priority: &str, tags: &str) -> Result<(), String> {
    use ic_cdk::api::management_canister::http_request::{
        http_request, CanisterHttpRequestArgument, HttpMethod, HttpHeader, TransformContext, TransformFunc,
    };

    // Get notification config
    let (enabled, topic) = STATE.with(|state| {
        let s = state.borrow();
        let config = s.notifications.as_ref();
        (
            config.map(|c| c.enabled).unwrap_or(false),
            config.and_then(|c| c.ntfy_topic.clone()),
        )
    });

    if !enabled {
        return Err("Notifications not enabled".to_string());
    }

    let topic = topic.ok_or("No ntfy topic configured")?;
    let url = format!("https://ntfy.sh/{}", topic);

    let request_headers = vec![
        HttpHeader { name: "Content-Type".to_string(), value: "text/plain".to_string() },
        HttpHeader { name: "Title".to_string(), value: title.to_string() },
        HttpHeader { name: "Priority".to_string(), value: priority.to_string() },
        HttpHeader { name: "Tags".to_string(), value: tags.to_string() },
    ];

    // Use transform function to normalize response for consensus
    // ntfy returns unique IDs and timestamps that differ per replica
    let transform = TransformContext {
        function: TransformFunc(candid::Func {
            principal: ic_cdk::api::id(),
            method: "transform_ntfy_response".to_string(),
        }),
        context: vec![],
    };

    let request = CanisterHttpRequestArgument {
        url,
        max_response_bytes: Some(1024),
        method: HttpMethod::POST,
        headers: request_headers,
        body: Some(message.as_bytes().to_vec()),
        transform: Some(transform),
    };

    // HTTP outcall requires cycles (around 400M for a small request)
    match http_request(request, 500_000_000).await {
        Ok((response,)) => {
            if response.status >= 200u64 && response.status < 300u64 {
                // Update notification count and timestamp
                STATE.with(|state| {
                    let mut s = state.borrow_mut();
                    if let Some(config) = s.notifications.as_mut() {
                        config.notification_count += 1;
                        config.last_notification = Some(ic_cdk::api::time());
                    }
                });
                ic_cdk::println!("Notification sent: {}", title);
                Ok(())
            } else {
                Err(format!("ntfy returned status {}", response.status))
            }
        }
        Err((code, msg)) => Err(format!("HTTP outcall failed: {:?} - {}", code, msg)),
    }
}

/// Fetch a URL via HTTP outcall - for research tasks
/// Returns the response body as a string (truncated to max_bytes)
async fn fetch_url(url: &str, max_bytes: u64) -> Result<String, String> {
    use ic_cdk::api::management_canister::http_request::{
        http_request, CanisterHttpRequestArgument, HttpMethod, HttpHeader, TransformContext, TransformFunc,
    };

    // Basic URL validation
    if !url.starts_with("https://") {
        return Err("Only HTTPS URLs are supported".to_string());
    }

    let request_headers = vec![
        HttpHeader { name: "User-Agent".to_string(), value: "Chronicle-Research/1.0".to_string() },
        HttpHeader { name: "Accept".to_string(), value: "text/html,application/json,text/plain".to_string() },
    ];

    let transform = TransformContext {
        function: TransformFunc(candid::Func {
            principal: ic_cdk::api::id(),
            method: "transform_web_response".to_string(),
        }),
        context: vec![],
    };

    let request = CanisterHttpRequestArgument {
        url: url.to_string(),
        max_response_bytes: Some(max_bytes),
        method: HttpMethod::GET,
        headers: request_headers,
        body: None,
        transform: Some(transform),
    };

    // HTTP outcall costs ~500M cycles for small requests, more for larger
    let cycles = 500_000_000u128 + (max_bytes as u128 * 10_000);

    match http_request(request, cycles).await {
        Ok((response,)) => {
            if response.status >= 200u64 && response.status < 300u64 {
                match String::from_utf8(response.body) {
                    Ok(body) => {
                        ic_cdk::println!("Fetched {} bytes from {}", body.len(), url);
                        Ok(body)
                    }
                    Err(_) => Err("Response body is not valid UTF-8".to_string()),
                }
            } else {
                Err(format!("HTTP {} from {}", response.status, url))
            }
        }
        Err((code, msg)) => Err(format!("Fetch failed: {:?} - {}", code, msg)),
    }
}

/// Check if enough time has passed since last notification
fn can_send_notification() -> bool {
    STATE.with(|state| {
        let s = state.borrow();
        let config = match s.notifications.as_ref() {
            Some(c) if c.enabled => c,
            _ => return false,
        };

        let now = ic_cdk::api::time();
        let min_interval_ns = config.min_interval_hours * 60 * 60 * 1_000_000_000;

        match config.last_notification {
            Some(last) => (now - last) >= min_interval_ns,
            None => true, // Never sent, can send
        }
    })
}

// ============================================================
// Update Methods (require cycles/authentication)
// ============================================================

/// Add a new knowledge capsule
#[update]
fn add_capsule(
    conversation_id: String,
    restatement: String,
    timestamp: Option<String>,
    location: Option<String>,
    topic: Option<String>,
    confidence_score: f64,
    persons: Vec<String>,
    entities: Vec<String>,
    keywords: Vec<String>,
) -> u64 {
    STATE.with(|state| {
        let mut s = state.borrow_mut();

        let id = s.next_id;
        s.next_id += 1;

        let capsule = KnowledgeCapsule {
            id,
            conversation_id,
            restatement,
            timestamp,
            location,
            topic,
            confidence_score,
            persons: persons.clone(),
            entities,
            keywords: keywords.clone(),
            created_at: ic_cdk::api::time(),
        };

        // Index by keywords
        for keyword in &keywords {
            let kw_lower = keyword.to_lowercase();
            s.keyword_index
                .entry(kw_lower)
                .or_insert_with(Vec::new)
                .push(id);
        }

        // Index by persons
        for person in &persons {
            let person_lower = person.to_lowercase();
            s.person_index
                .entry(person_lower)
                .or_insert_with(Vec::new)
                .push(id);
        }

        s.capsules.insert(id, capsule);

        id
    })
}

/// Simple feed input - for humans to submit info directly to Chronicle
/// Returns JSON with capsule_id and status
#[update]
fn feed_input(content: String, topic: Option<String>) -> String {
    if content.trim().is_empty() {
        return r#"{"success":false,"error":"Content cannot be empty"}"#.to_string();
    }

    if content.len() > 10000 {
        return r#"{"success":false,"error":"Content too long (max 10000 chars)"}"#.to_string();
    }

    STATE.with(|state| {
        let mut s = state.borrow_mut();

        let id = s.next_id;
        s.next_id += 1;

        let topic_str = topic.clone().unwrap_or_else(|| "feed".to_string());
        let timestamp = format_iso_timestamp(ic_cdk::api::time());

        // Extract simple keywords from content (words > 4 chars)
        let keywords: Vec<String> = content
            .split_whitespace()
            .filter(|w| w.len() > 4)
            .take(10)
            .map(|w| w.to_lowercase().trim_matches(|c: char| !c.is_alphanumeric()).to_string())
            .filter(|w| !w.is_empty())
            .collect();

        let capsule = KnowledgeCapsule {
            id,
            conversation_id: "feed-input".to_string(),
            restatement: content,
            timestamp: Some(timestamp),
            location: None,
            topic: Some(topic_str.clone()),
            confidence_score: 0.8,
            persons: vec![],
            entities: vec![],
            keywords: keywords.clone(),
            created_at: ic_cdk::api::time(),
        };

        // Index by keywords
        for keyword in &keywords {
            let kw_lower = keyword.to_lowercase();
            s.keyword_index
                .entry(kw_lower)
                .or_insert_with(Vec::new)
                .push(id);
        }

        s.capsules.insert(id, capsule);

        format!(r#"{{"success":true,"capsule_id":{},"topic":"{}","keywords":{}}}"#,
            id, escape_json(&topic_str), keywords.len())
    })
}

/// Add embedding for a capsule
#[update]
fn add_embedding(capsule_id: u64, embedding: Vec<f32>, model_name: String) -> bool {
    STATE.with(|state| {
        let mut s = state.borrow_mut();

        if !s.capsules.contains_key(&capsule_id) {
            return false;
        }

        let emb = CapsuleEmbedding {
            capsule_id,
            embedding,
            model_name,
        };

        s.embeddings.insert(capsule_id, emb);
        true
    })
}

/// Bulk add capsules (more efficient)
#[update]
fn add_capsules_bulk(capsules: Vec<KnowledgeCapsule>) -> Vec<u64> {
    STATE.with(|state| {
        let mut s = state.borrow_mut();
        let mut ids = Vec::new();

        for mut capsule in capsules {
            let id = s.next_id;
            s.next_id += 1;
            capsule.id = id;
            capsule.created_at = ic_cdk::api::time();

            // Index
            for keyword in &capsule.keywords {
                let kw_lower = keyword.to_lowercase();
                s.keyword_index
                    .entry(kw_lower)
                    .or_insert_with(Vec::new)
                    .push(id);
            }

            for person in &capsule.persons {
                let person_lower = person.to_lowercase();
                s.person_index
                    .entry(person_lower)
                    .or_insert_with(Vec::new)
                    .push(id);
            }

            s.capsules.insert(id, capsule);
            ids.push(id);
        }

        ids
    })
}

// ============================================================
// Query Methods (free, no cycles)
// ============================================================

/// Get capsule count
#[query]
fn get_capsule_count() -> u64 {
    STATE.with(|state| {
        state.borrow().capsules.len() as u64
    })
}

/// Get embedding count
#[query]
fn get_embedding_count() -> u64 {
    STATE.with(|state| {
        state.borrow().embeddings.len() as u64
    })
}

/// Get a specific capsule by ID
#[query]
fn get_capsule(id: u64) -> Option<KnowledgeCapsule> {
    STATE.with(|state| {
        state.borrow().capsules.get(&id).cloned()
    })
}

/// Get recent capsules
#[query]
fn get_recent_capsules(limit: u64) -> Vec<KnowledgeCapsule> {
    STATE.with(|state| {
        let s = state.borrow();
        let mut capsules: Vec<_> = s.capsules.values().cloned().collect();
        capsules.sort_by(|a, b| b.created_at.cmp(&a.created_at));
        capsules.into_iter().take(limit as usize).collect()
    })
}

/// Search by keyword
#[query]
fn search_by_keyword(keyword: String, limit: u64) -> Vec<KnowledgeCapsule> {
    STATE.with(|state| {
        let s = state.borrow();
        let kw_lower = keyword.to_lowercase();

        if let Some(ids) = s.keyword_index.get(&kw_lower) {
            ids.iter()
                .take(limit as usize)
                .filter_map(|id| s.capsules.get(id).cloned())
                .collect()
        } else {
            Vec::new()
        }
    })
}

/// Search by person name
#[query]
fn search_by_person(person: String, limit: u64) -> Vec<KnowledgeCapsule> {
    STATE.with(|state| {
        let s = state.borrow();
        let person_lower = person.to_lowercase();

        if let Some(ids) = s.person_index.get(&person_lower) {
            ids.iter()
                .take(limit as usize)
                .filter_map(|id| s.capsules.get(id).cloned())
                .collect()
        } else {
            Vec::new()
        }
    })
}

/// Get all embeddings (for client-side semantic search)
#[query]
fn get_all_embeddings() -> Vec<CapsuleEmbedding> {
    STATE.with(|state| {
        state.borrow().embeddings.values().cloned().collect()
    })
}

/// Get embedding for a specific capsule
#[query]
fn get_embedding(capsule_id: u64) -> Option<CapsuleEmbedding> {
    STATE.with(|state| {
        state.borrow().embeddings.get(&capsule_id).cloned()
    })
}

/// Semantic search - find capsules similar to a query embedding
/// This runs cosine similarity directly on-chain
#[query]
fn semantic_search(query_embedding: Vec<f32>, limit: u64) -> Vec<SearchResult> {
    STATE.with(|state| {
        let s = state.borrow();

        let mut results: Vec<SearchResult> = s.embeddings
            .values()
            .filter_map(|emb| {
                let similarity = cosine_similarity(&query_embedding, &emb.embedding);
                s.capsules.get(&emb.capsule_id).map(|capsule| {
                    SearchResult {
                        capsule: capsule.clone(),
                        score: similarity,
                    }
                })
            })
            .collect();

        // Sort by similarity descending
        results.sort_by(|a, b| b.score.partial_cmp(&a.score).unwrap_or(std::cmp::Ordering::Equal));

        results.into_iter().take(limit as usize).collect()
    })
}

/// Compute cosine similarity between two vectors
fn cosine_similarity(a: &[f32], b: &[f32]) -> f64 {
    if a.len() != b.len() || a.is_empty() {
        return 0.0;
    }

    let mut dot_product = 0.0f64;
    let mut norm_a = 0.0f64;
    let mut norm_b = 0.0f64;

    for (x, y) in a.iter().zip(b.iter()) {
        let x = *x as f64;
        let y = *y as f64;
        dot_product += x * y;
        norm_a += x * x;
        norm_b += y * y;
    }

    let denominator = (norm_a.sqrt()) * (norm_b.sqrt());
    if denominator == 0.0 {
        return 0.0;
    }

    dot_product / denominator
}

/// Bulk add embeddings (more efficient)
#[update]
fn add_embeddings_bulk(embeddings: Vec<CapsuleEmbedding>) -> u64 {
    STATE.with(|state| {
        let mut s = state.borrow_mut();
        let mut added = 0u64;

        for emb in embeddings {
            if s.capsules.contains_key(&emb.capsule_id) {
                s.embeddings.insert(emb.capsule_id, emb);
                added += 1;
            }
        }

        added
    })
}

/// Health check
#[query]
fn health() -> String {
    STATE.with(|state| {
        let s = state.borrow();
        let heartbeat_count = s.heartbeat.as_ref().map(|h| h.count).unwrap_or(0);
        format!(
            "Chronicle Backend: {} capsules, {} embeddings, {} heartbeats",
            s.capsules.len(),
            s.embeddings.len(),
            heartbeat_count
        )
    })
}

/// Get heartbeat status - shows autonomous operation state
#[query]
fn get_heartbeat_status() -> HeartbeatStatus {
    STATE.with(|state| {
        let s = state.borrow();
        let now = ic_cdk::api::time();
        let hb = s.heartbeat.as_ref();

        // Check if heartbeat is "alive" (fired within 2x interval)
        let is_alive = match hb.and_then(|h| h.last) {
            Some(last) => (now - last) < (HEARTBEAT_INTERVAL_SECS * 2 * 1_000_000_000),
            None => false, // Never fired yet
        };

        HeartbeatStatus {
            total_heartbeats: hb.map(|h| h.count).unwrap_or(0),
            last_heartbeat: hb.and_then(|h| h.last),
            interval_secs: HEARTBEAT_INTERVAL_SECS,
            capsule_count: s.capsules.len() as u64,
            is_alive,
        }
    })
}

/// Get recent heartbeat history
#[query]
fn get_heartbeat_history() -> Vec<HeartbeatRecord> {
    STATE.with(|state| {
        state.borrow().heartbeat.as_ref()
            .map(|h| h.history.clone())
            .unwrap_or_default()
    })
}

// ============================================================
// Admin Methods
// ============================================================

/// Set API token for HTTP access (owner only)
#[update]
fn set_api_token(token: Option<String>) -> bool {
    STATE.with(|state| {
        let mut s = state.borrow_mut();
        if s.owner != Some(ic_cdk::caller()) {
            return false;
        }
        s.api_token = token;
        true
    })
}

/// Configure LLM for autonomous reflection (owner only)
/// Now uses DFINITY's LLM Canister - no API keys needed!
#[update]
fn configure_llm(enabled: bool, model: Option<String>) -> bool {
    STATE.with(|state| {
        let mut s = state.borrow_mut();
        if s.owner != Some(ic_cdk::caller()) {
            return false;
        }
        s.llm_config = Some(LlmConfig {
            enabled,
            model,
        });
        true
    })
}

/// Get LLM configuration status
#[query]
fn get_llm_status() -> String {
    STATE.with(|state| {
        let s = state.borrow();
        match &s.llm_config {
            Some(config) => format!(
                r#"{{"enabled":{},"model":{},"provider":"dfinity-llm-canister"}}"#,
                config.enabled,
                config.model.as_ref().map(|m| format!("\"{}\"", m)).unwrap_or("\"llama3\"".to_string())
            ),
            None => r#"{"enabled":false,"model":"llama3","provider":"dfinity-llm-canister"}"#.to_string(),
        }
    })
}

// ============================================================
// Research Task API - Claude's On-Chain Research Assistant
// ============================================================

/// Configure research task system (owner only)
#[update]
fn configure_research(enabled: bool) -> String {
    STATE.with(|state| {
        let mut s = state.borrow_mut();
        if s.owner != Some(ic_cdk::caller()) {
            return r#"{"error":"Not authorized"}"#.to_string();
        }

        let research = s.research.get_or_insert_with(ResearchState::default);
        research.enabled = enabled;

        format!(r#"{{"success":true,"enabled":{}}}"#, enabled)
    })
}

/// Submit a research task for on-chain analysis
/// This is how Claude queues work for the on-chain research assistant
/// Can optionally include URLs to fetch for additional context
#[update]
fn submit_research_task(query: String, focus: Option<String>, max_capsules: Option<u32>, urls: Option<Vec<String>>) -> String {
    STATE.with(|state| {
        let mut s = state.borrow_mut();

        let research = match s.research.as_mut() {
            Some(r) => r,
            None => {
                s.research = Some(ResearchState::default());
                s.research.as_mut().unwrap()
            }
        };

        if !research.enabled {
            return r#"{"error":"Research system not enabled. Call configure_research(true) first."}"#.to_string();
        }

        // Validate URLs (max 3, must be HTTPS)
        let urls = urls.unwrap_or_default();
        if urls.len() > 3 {
            return r#"{"error":"Maximum 3 URLs per research task"}"#.to_string();
        }
        for url in &urls {
            if !url.starts_with("https://") {
                return format!(r#"{{"error":"Invalid URL (must be HTTPS): {}"}}"#, escape_json(url));
            }
        }

        let id = research.next_task_id;
        research.next_task_id += 1;

        let task = ResearchTask {
            id,
            query: query.clone(),
            focus,
            max_capsules: max_capsules.unwrap_or(50),
            urls,
            submitted_at: ic_cdk::api::time(),
            status: "pending".to_string(),
            started_at: None,
            completed_at: None,
        };

        research.task_queue.push(task);

        format!(r#"{{"success":true,"task_id":{},"query":"{}","status":"pending"}}"#,
            id, escape_json(&query))
    })
}

/// Manually trigger research task processing (doesn't wait for heartbeat)
#[update]
async fn trigger_research() -> String {
    // Check if enabled and has pending tasks
    let has_pending = STATE.with(|state| {
        state.borrow().research.as_ref()
            .map(|r| r.enabled && r.task_queue.iter().any(|t| t.status == "pending"))
            .unwrap_or(false)
    });

    if !has_pending {
        return r#"{"success":false,"message":"No pending research tasks or system disabled"}"#.to_string();
    }

    match process_research_tasks().await {
        Ok(result) => format!(r#"{{"success":true,"result":"{}"}}"#, escape_json(&result)),
        Err(e) => format!(r#"{{"success":false,"error":"{}"}}"#, escape_json(&e)),
    }
}

/// Get research task status
#[query]
fn get_research_task(task_id: u64) -> String {
    STATE.with(|state| {
        let s = state.borrow();

        let research = match s.research.as_ref() {
            Some(r) => r,
            None => return r#"{"error":"Research system not initialized"}"#.to_string(),
        };

        match research.task_queue.iter().find(|t| t.id == task_id) {
            Some(task) => format!(
                r#"{{"id":{},"query":"{}","status":"{}","submitted_at":{},"started_at":{},"completed_at":{}}}"#,
                task.id,
                escape_json(&task.query),
                task.status,
                task.submitted_at,
                task.started_at.map(|t| t.to_string()).unwrap_or("null".to_string()),
                task.completed_at.map(|t| t.to_string()).unwrap_or("null".to_string())
            ),
            None => format!(r#"{{"error":"Task {} not found"}}"#, task_id),
        }
    })
}

/// Get all research findings (optionally filter by retrieved status)
#[query]
fn get_research_findings(only_new: bool) -> String {
    STATE.with(|state| {
        let s = state.borrow();

        let research = match s.research.as_ref() {
            Some(r) => r,
            None => return r#"{"findings":[]}"#.to_string(),
        };

        let findings: Vec<String> = research.findings.iter()
            .filter(|f| !only_new || !f.retrieved)
            .map(|f| {
                let patterns_json: String = f.patterns.iter()
                    .map(|p| format!("\"{}\"", escape_json(p)))
                    .collect::<Vec<_>>()
                    .join(",");
                let hypotheses_json: String = f.hypotheses.iter()
                    .map(|h| format!("\"{}\"", escape_json(h)))
                    .collect::<Vec<_>>()
                    .join(",");
                let capsules_json: String = f.source_capsules.iter()
                    .map(|id| id.to_string())
                    .collect::<Vec<_>>()
                    .join(",");

                format!(
                    r#"{{"id":{},"task_id":{},"query":"{}","synthesis":"{}","patterns":[{}],"hypotheses":[{}],"source_capsules":[{}],"confidence":{},"created_at":{},"retrieved":{}}}"#,
                    f.id,
                    f.task_id,
                    escape_json(&f.query),
                    escape_json(&f.synthesis),
                    patterns_json,
                    hypotheses_json,
                    capsules_json,
                    f.confidence,
                    f.created_at,
                    f.retrieved
                )
            })
            .collect();

        format!(r#"{{"findings":[{}]}}"#, findings.join(","))
    })
}

/// Get a specific research finding by ID
#[query]
fn get_research_finding(finding_id: u64) -> String {
    STATE.with(|state| {
        let s = state.borrow();

        let research = match s.research.as_ref() {
            Some(r) => r,
            None => return r#"{"error":"Research system not initialized"}"#.to_string(),
        };

        match research.findings.iter().find(|f| f.id == finding_id) {
            Some(f) => {
                let patterns_json: String = f.patterns.iter()
                    .map(|p| format!("\"{}\"", escape_json(p)))
                    .collect::<Vec<_>>()
                    .join(",");
                let hypotheses_json: String = f.hypotheses.iter()
                    .map(|h| format!("\"{}\"", escape_json(h)))
                    .collect::<Vec<_>>()
                    .join(",");
                let capsules_json: String = f.source_capsules.iter()
                    .map(|id| id.to_string())
                    .collect::<Vec<_>>()
                    .join(",");

                format!(
                    r#"{{"id":{},"task_id":{},"query":"{}","synthesis":"{}","patterns":[{}],"hypotheses":[{}],"source_capsules":[{}],"confidence":{},"created_at":{},"retrieved":{}}}"#,
                    f.id,
                    f.task_id,
                    escape_json(&f.query),
                    escape_json(&f.synthesis),
                    patterns_json,
                    hypotheses_json,
                    capsules_json,
                    f.confidence,
                    f.created_at,
                    f.retrieved
                )
            },
            None => format!(r#"{{"error":"Finding {} not found"}}"#, finding_id),
        }
    })
}

/// Mark research findings as retrieved
#[update]
fn mark_findings_retrieved(finding_ids: Vec<u64>) -> String {
    STATE.with(|state| {
        let mut s = state.borrow_mut();

        let research = match s.research.as_mut() {
            Some(r) => r,
            None => return r#"{"error":"Research system not initialized"}"#.to_string(),
        };

        let mut marked = 0;
        for f in research.findings.iter_mut() {
            if finding_ids.contains(&f.id) && !f.retrieved {
                f.retrieved = true;
                marked += 1;
            }
        }

        format!(r#"{{"success":true,"marked_retrieved":{}}}"#, marked)
    })
}

/// Get research system status
#[query]
fn get_research_status() -> String {
    STATE.with(|state| {
        let s = state.borrow();

        match s.research.as_ref() {
            Some(r) => {
                let pending = r.task_queue.iter().filter(|t| t.status == "pending").count();
                let processing = r.task_queue.iter().filter(|t| t.status == "processing").count();
                let completed = r.task_queue.iter().filter(|t| t.status == "completed").count();
                let unread_findings = r.findings.iter().filter(|f| !f.retrieved).count();

                format!(
                    r#"{{"enabled":{},"tasks":{{"pending":{},"processing":{},"completed":{}}},"findings":{{"total":{},"unread":{}}}}}"#,
                    r.enabled,
                    pending,
                    processing,
                    completed,
                    r.findings.len(),
                    unread_findings
                )
            },
            None => r#"{"enabled":false,"tasks":{"pending":0,"processing":0,"completed":0},"findings":{"total":0,"unread":0}}"#.to_string(),
        }
    })
}

/// Configure autonomous accumulation (owner only)
/// This enables Chronicle to autonomously swap XRP to RLUSD during heartbeat
#[update]
fn configure_accumulation(
    enabled: bool,
    xrp_per_heartbeat_drops: u64,
    min_xrp_reserve_drops: u64,
) -> String {
    STATE.with(|state| {
        let mut s = state.borrow_mut();
        if s.owner != Some(ic_cdk::caller()) {
            return r#"{"error":"Not authorized"}"#.to_string();
        }

        let acc = s.accumulation.get_or_insert_with(AccumulationConfig::default);
        acc.enabled = enabled;
        acc.xrp_per_heartbeat = xrp_per_heartbeat_drops;
        acc.min_xrp_reserve = min_xrp_reserve_drops;

        format!(
            r#"{{"success":true,"enabled":{},"xrp_per_heartbeat_drops":{},"min_xrp_reserve_drops":{}}}"#,
            enabled,
            xrp_per_heartbeat_drops,
            min_xrp_reserve_drops
        )
    })
}

/// Get accumulation status
#[query]
fn get_accumulation_status() -> String {
    STATE.with(|state| {
        let s = state.borrow();
        match &s.accumulation {
            Some(acc) => format!(
                r#"{{"enabled":{},"xrp_per_heartbeat_drops":{},"min_xrp_reserve_drops":{},"accumulation_count":{},"has_pending_tx":{}}}"#,
                acc.enabled,
                acc.xrp_per_heartbeat,
                acc.min_xrp_reserve,
                acc.accumulation_count,
                acc.pending_tx_blob.is_some()
            ),
            None => r#"{"enabled":false,"xrp_per_heartbeat_drops":0,"min_xrp_reserve_drops":0,"accumulation_count":0,"has_pending_tx":false}"#.to_string(),
        }
    })
}

/// Configure notification system (owner only)
/// This enables Chronicle to reach out to the human via ntfy.sh
#[update]
fn configure_notifications(
    enabled: bool,
    ntfy_topic: String,
    min_interval_hours: u64,
) -> String {
    STATE.with(|state| {
        let mut s = state.borrow_mut();
        if s.owner != Some(ic_cdk::caller()) {
            return r#"{"error":"Not authorized"}"#.to_string();
        }

        let config = s.notifications.get_or_insert_with(NotificationConfig::default);
        config.enabled = enabled;
        config.ntfy_topic = if ntfy_topic.is_empty() { None } else { Some(ntfy_topic.clone()) };
        config.min_interval_hours = min_interval_hours;

        format!(
            r#"{{"success":true,"enabled":{},"ntfy_topic":"{}","min_interval_hours":{}}}"#,
            enabled,
            ntfy_topic,
            min_interval_hours
        )
    })
}

/// Get notification status
#[query]
fn get_notification_status() -> String {
    STATE.with(|state| {
        let s = state.borrow();
        match &s.notifications {
            Some(config) => format!(
                r#"{{"enabled":{},"ntfy_topic":"{}","min_interval_hours":{},"notification_count":{},"last_notification":{}}}"#,
                config.enabled,
                config.ntfy_topic.as_deref().unwrap_or(""),
                config.min_interval_hours,
                config.notification_count,
                config.last_notification.map(|t| t.to_string()).unwrap_or_else(|| "null".to_string())
            ),
            None => r#"{"enabled":false,"ntfy_topic":"","min_interval_hours":0,"notification_count":0,"last_notification":null}"#.to_string(),
        }
    })
}

/// Manually send a notification (owner only, for testing)
#[update]
async fn send_test_notification(title: String, message: String) -> String {
    let is_owner = STATE.with(|state| {
        state.borrow().owner == Some(ic_cdk::caller())
    });

    if !is_owner {
        return r#"{"error":"Not authorized"}"#.to_string();
    }

    match send_notification(&title, &message, "default", "robot").await {
        Ok(()) => r#"{"success":true,"message":"Notification sent"}"#.to_string(),
        Err(e) => format!(r#"{{"success":false,"error":"{}"}}"#, e),
    }
}

/// Get pending accumulation transaction (for manual submission)
#[query]
fn get_pending_accumulation_tx() -> String {
    STATE.with(|state| {
        let s = state.borrow();
        match s.accumulation.as_ref() {
            Some(acc) => {
                let hash = acc.pending_tx_hash.as_deref().unwrap_or("null");
                let blob = acc.pending_tx_blob.as_deref().unwrap_or("");
                if blob.is_empty() {
                    r#"{"has_pending":false,"tx_blob":null,"tx_hash":null}"#.to_string()
                } else {
                    format!(r#"{{"has_pending":true,"tx_blob":"{}","tx_hash":"{}"}}"#, blob, hash)
                }
            }
            None => r#"{"has_pending":false,"tx_blob":null,"tx_hash":null}"#.to_string(),
        }
    })
}

/// Get transaction history - confirmed transactions
#[query]
fn get_transaction_history(limit: u64) -> String {
    STATE.with(|state| {
        let s = state.borrow();
        let empty_vec = Vec::new();
        let history_ref = s.transaction_history.as_ref().unwrap_or(&empty_vec);
        let history: Vec<_> = history_ref.iter()
            .rev() // Most recent first
            .take(limit as usize)
            .collect();

        let txs: Vec<String> = history.iter().map(|tx| {
            format!(
                r#"{{"hash":"{}","type":"{}","xrp_drops":{},"rlusd":"{}","result":"{}","confirmed_at":{},"ledger":{}}}"#,
                tx.hash,
                tx.tx_type,
                tx.xrp_drops.map(|x| x.to_string()).unwrap_or_else(|| "null".to_string()),
                tx.rlusd_amount.as_deref().unwrap_or("null"),
                tx.result,
                tx.confirmed_at,
                tx.ledger_index.map(|l| l.to_string()).unwrap_or_else(|| "null".to_string())
            )
        }).collect();

        format!(r#"{{"count":{},"transactions":[{}]}}"#, history.len(), txs.join(","))
    })
}

/// Manually trigger pending transaction check (owner only)
/// Useful for debugging or forcing a status update
#[update]
async fn check_pending_tx_status() -> String {
    // Check owner
    let is_owner = STATE.with(|state| {
        state.borrow().owner == Some(ic_cdk::caller())
    });

    if !is_owner {
        return r#"{"error":"Not authorized"}"#.to_string();
    }

    match check_pending_transaction().await {
        Ok(result) => format!(r#"{{"success":true,"result":"{}"}}"#, escape_json(&result)),
        Err(e) => format!(r#"{{"error":"{}"}}"#, escape_json(&e)),
    }
}

/// Manually clear a pending transaction (owner only)
/// Use when a transaction has expired or failed and the auto-detection didn't catch it
#[update]
fn clear_pending_transaction() -> String {
    // Check owner
    let is_owner = STATE.with(|state| {
        state.borrow().owner == Some(ic_cdk::caller())
    });

    if !is_owner {
        return r#"{"error":"Not authorized"}"#.to_string();
    }

    let cleared = STATE.with(|state| {
        let mut s = state.borrow_mut();
        if let Some(acc) = s.accumulation.as_mut() {
            let had_pending = acc.pending_tx_hash.is_some();
            acc.pending_tx_blob = None;
            acc.pending_tx_hash = None;
            acc.pending_tx_created = None;
            had_pending
        } else {
            false
        }
    });

    if cleared {
        r#"{"success":true,"message":"Pending transaction cleared"}"#.to_string()
    } else {
        r#"{"success":true,"message":"No pending transaction to clear"}"#.to_string()
    }
}

/// Sync wallet info from XRPL (owner only)
/// Fetches current balance and sequence from the ledger
#[update]
async fn sync_wallet_from_xrpl() -> String {
    // Check owner
    let is_owner = STATE.with(|state| {
        state.borrow().owner == Some(ic_cdk::caller())
    });

    if !is_owner {
        return r#"{"error":"Not authorized"}"#.to_string();
    }

    // Get wallet address
    let address = STATE.with(|state| {
        state.borrow().wallet.as_ref().and_then(|w| w.cached_address.clone())
    });

    let address = match address {
        Some(a) => a,
        None => return r#"{"error":"No wallet address configured"}"#.to_string(),
    };

    // Fetch account info from XRPL
    match chronicle_xrp::fetch_account_info_http(XRPL_URL, &address).await {
        Ok(response) => {
            let balance = parse_xrp_balance(&response);
            let sequence = parse_sequence(&response);
            let ledger = parse_ledger_index(&response);

            // Update state
            STATE.with(|state| {
                let mut s = state.borrow_mut();
                if let Some(wallet) = s.wallet.as_mut() {
                    if let Some(bal) = balance {
                        wallet.last_known_balance = Some(bal);
                    }
                    if let Some(seq) = sequence {
                        wallet.last_known_sequence = Some(seq);
                    }
                    if let Some(led) = ledger {
                        wallet.last_known_ledger = Some(led);
                    }
                }
            });

            format!(
                r#"{{"success":true,"balance_drops":{},"sequence":{},"ledger":{}}}"#,
                balance.map(|b| b.to_string()).unwrap_or_else(|| "null".to_string()),
                sequence.map(|s| s.to_string()).unwrap_or_else(|| "null".to_string()),
                ledger.map(|l| l.to_string()).unwrap_or_else(|| "null".to_string())
            )
        }
        Err(e) => format!(r#"{{"error":"{}"}}"#, escape_json(&e)),
    }
}

/// Sync transaction history from XRPL (owner only)
/// Fetches recent transactions and populates transaction_history
/// Also updates accumulation_count based on actual on-chain OfferCreate transactions
#[update]
async fn sync_transaction_history(limit: u32) -> String {
    // Check owner
    let is_owner = STATE.with(|state| {
        state.borrow().owner == Some(ic_cdk::caller())
    });

    if !is_owner {
        return r#"{"error":"Not authorized"}"#.to_string();
    }

    // Get wallet address
    let address = STATE.with(|state| {
        state.borrow().wallet.as_ref().and_then(|w| w.cached_address.clone())
    });

    let address = match address {
        Some(a) => a,
        None => return r#"{"error":"No wallet address configured"}"#.to_string(),
    };

    // Fetch transaction history from XRPL
    let limit = limit.min(20); // Cap at 20 for performance
    match chronicle_xrp::fetch_account_tx_http(XRPL_URL, &address, limit).await {
        Ok(response) => {
            // Parse transactions from response
            let transactions = parse_account_tx(&response);
            let tx_count = transactions.len();
            let offer_count = transactions.iter()
                .filter(|tx| tx.tx_type == "OfferCreate")
                .count();

            // Update state with parsed transactions
            STATE.with(|state| {
                let mut s = state.borrow_mut();

                // Add transactions to history (avoiding duplicates)
                {
                    let history = s.transaction_history.get_or_insert_with(Vec::new);
                    for tx in transactions {
                        // Check if already exists by hash
                        if !history.iter().any(|h| h.hash == tx.hash) {
                            history.push(tx);
                        }
                    }

                    // Sort by confirmed_at and keep last 100
                    history.sort_by(|a, b| a.confirmed_at.cmp(&b.confirmed_at));
                    while history.len() > 100 {
                        history.remove(0);
                    }
                }

                // Update accumulation count to match on-chain reality
                // Count offers from the now-updated history
                let on_chain_offers = s.transaction_history.as_ref()
                    .map(|h| h.iter()
                        .filter(|tx| tx.tx_type == "OfferCreate" && tx.result == "tesSUCCESS")
                        .count() as u64)
                    .unwrap_or(0);

                if let Some(acc) = s.accumulation.as_mut() {
                    if on_chain_offers > acc.accumulation_count {
                        acc.accumulation_count = on_chain_offers;
                    }
                }
            });

            format!(
                r#"{{"success":true,"transactions_found":{},"offer_creates":{},"history_updated":true}}"#,
                tx_count,
                offer_count
            )
        }
        Err(e) => format!(r#"{{"error":"{}"}}"#, escape_json(&e)),
    }
}

/// Parse transactions from account_tx response
fn parse_account_tx(response: &str) -> Vec<TransactionRecord> {
    let mut transactions = Vec::new();
    let now = ic_cdk::api::time();

    // Simple parsing - look for transaction patterns
    // Format: "TransactionType":"XXX" ... "hash":"XXX" ... "TransactionResult":"XXX"

    // Split by transaction markers
    let parts: Vec<&str> = response.split("\"TransactionType\":\"").collect();

    for (i, part) in parts.iter().enumerate() {
        if i == 0 { continue; } // Skip first part before any transaction

        // Extract transaction type
        let tx_type = part.split('"').next().unwrap_or("").to_string();
        if tx_type.is_empty() { continue; }

        // Look for hash in this section
        let hash = if let Some(hash_start) = part.find("\"hash\":\"") {
            let rest = &part[hash_start + 8..];
            rest.split('"').next().unwrap_or("").to_string()
        } else {
            continue; // No hash found
        };

        // Look for result
        let result = if let Some(result_start) = part.find("\"TransactionResult\":\"") {
            let rest = &part[result_start + 21..];
            rest.split('"').next().unwrap_or("tesSUCCESS").to_string()
        } else {
            "tesSUCCESS".to_string()
        };

        // Look for ledger_index
        let ledger_index = if let Some(ledger_start) = part.find("\"ledger_index\":") {
            let rest = &part[ledger_start + 15..];
            let end = rest.find(|c: char| !c.is_ascii_digit()).unwrap_or(rest.len());
            rest[..end].parse::<u32>().ok()
        } else {
            None
        };

        // For OfferCreate, try to extract amounts
        let (xrp_drops, rlusd_amount) = if tx_type == "OfferCreate" {
            // Look for TakerGets (XRP amount in drops)
            let xrp = if let Some(tg_start) = part.find("\"TakerGets\":\"") {
                let rest = &part[tg_start + 13..];
                rest.split('"').next().and_then(|s| s.parse::<u64>().ok())
            } else if let Some(tg_start) = part.find("\"TakerGets\":") {
                // Could be a number without quotes
                let rest = &part[tg_start + 12..];
                let end = rest.find(|c: char| !c.is_ascii_digit()).unwrap_or(rest.len());
                rest[..end].parse::<u64>().ok()
            } else {
                None
            };

            // Look for TakerPays with RLUSD value
            let rlusd = if part.contains("524C555344") || part.contains("RLUSD") {
                if let Some(val_start) = part.find("\"value\":\"") {
                    let rest = &part[val_start + 9..];
                    rest.split('"').next().map(|s| s.to_string())
                } else {
                    None
                }
            } else {
                None
            };

            (xrp, rlusd)
        } else {
            (None, None)
        };

        transactions.push(TransactionRecord {
            hash,
            tx_type,
            xrp_drops,
            rlusd_amount,
            result,
            confirmed_at: now, // Use current time as we don't have exact timestamp
            ledger_index,
        });
    }

    transactions
}

/// Clear pending accumulation transaction (after successful submission)
#[update]
fn clear_pending_accumulation_tx() -> String {
    STATE.with(|state| {
        let mut s = state.borrow_mut();
        if s.owner != Some(ic_cdk::caller()) {
            return r#"{"error":"Not authorized"}"#.to_string();
        }

        if let Some(acc) = s.accumulation.as_mut() {
            acc.pending_tx_blob = None;
        }
        r#"{"success":true}"#.to_string()
    })
}

/// Confirm successful accumulation - increments count and sequence, clears pending
/// Call this after successfully submitting the pending transaction
#[update]
fn confirm_accumulation_success(new_balance_drops: Option<u64>, new_rlusd: Option<String>) -> String {
    STATE.with(|state| {
        let mut s = state.borrow_mut();
        if s.owner != Some(ic_cdk::caller()) {
            return r#"{"error":"Not authorized"}"#.to_string();
        }

        // Update accumulation state
        if let Some(acc) = s.accumulation.as_mut() {
            acc.accumulation_count += 1;
            acc.pending_tx_blob = None;

            // Increment sequence (used for next transaction)
            if let Some(last_seq) = acc.last_sequence {
                if let Some(wallet) = s.wallet.as_mut() {
                    wallet.last_known_sequence = Some(last_seq + 1);
                }
            }
        }

        // Update wallet balances if provided
        if let Some(wallet) = s.wallet.as_mut() {
            if let Some(bal) = new_balance_drops {
                wallet.last_known_balance = Some(bal);
            }
            if let Some(rlusd) = new_rlusd {
                wallet.last_known_rlusd = Some(rlusd);
            }
        }

        let count = s.accumulation.as_ref().map(|a| a.accumulation_count).unwrap_or(0);
        format!(r#"{{"success":true,"total_accumulations":{}}}"#, count)
    })
}

/// Update wallet info from external source (called after checking account_info)
/// This allows the MCP or external service to keep the canister informed
#[update]
fn update_wallet_info(
    balance_drops: Option<u64>,
    rlusd_balance: Option<String>,
    sequence: Option<u32>,
    ledger_index: Option<u32>,
) -> String {
    STATE.with(|state| {
        let mut s = state.borrow_mut();
        if s.owner != Some(ic_cdk::caller()) {
            return r#"{"error":"Not authorized"}"#.to_string();
        }

        let wallet = s.wallet.get_or_insert_with(WalletState::default);

        if let Some(bal) = balance_drops {
            wallet.last_known_balance = Some(bal);
        }
        if let Some(rlusd) = rlusd_balance {
            wallet.last_known_rlusd = Some(rlusd);
        }
        if let Some(seq) = sequence {
            wallet.last_known_sequence = Some(seq);
        }
        if let Some(ledger) = ledger_index {
            wallet.last_known_ledger = Some(ledger);
        }

        format!(
            r#"{{"success":true,"balance_drops":{},"rlusd":{},"sequence":{},"ledger":{}}}"#,
            wallet.last_known_balance.map(|b| b.to_string()).unwrap_or("null".to_string()),
            wallet.last_known_rlusd.as_ref().map(|r| format!("\"{}\"", r)).unwrap_or("null".to_string()),
            wallet.last_known_sequence.map(|s| s.to_string()).unwrap_or("null".to_string()),
            wallet.last_known_ledger.map(|l| l.to_string()).unwrap_or("null".to_string())
        )
    })
}

// ============================================================
// XRP Wallet Methods (Threshold ECDSA)
// ============================================================

/// Get Chronicle's XRP wallet signer (using test_key_1 for now)
fn get_xrp_signer() -> IcpSigner {
    IcpSigner::new(IcpSignerConfig::chronicle_testnet())
}

/// Get or fetch the XRP wallet address
/// This calls the management canister to get the public key if not cached
#[update]
async fn get_wallet_address() -> String {
    // Check if we have a cached address
    let cached = STATE.with(|state| {
        state.borrow().wallet.as_ref().and_then(|w| w.cached_address.clone())
    });

    if let Some(addr) = cached {
        return format!(r#"{{"address":"{}","cached":true}}"#, addr);
    }

    // Fetch public key from threshold ECDSA
    let mut signer = get_xrp_signer();
    match chronicle_xrp::fetch_public_key(&signer).await {
        Ok(pk) => {
            signer.set_public_key(pk);
            match signer.address() {
                Ok(address) => {
                    let addr_str = address.to_string();

                    // Cache the public key and address
                    STATE.with(|state| {
                        let mut s = state.borrow_mut();
                        let wallet = s.wallet.get_or_insert_with(WalletState::default);
                        wallet.cached_public_key = Some(pk.to_vec());
                        wallet.cached_address = Some(addr_str.clone());
                    });

                    format!(r#"{{"address":"{}","public_key":"{}","cached":false}}"#, addr_str, hex::encode(pk))
                }
                Err(e) => format!(r#"{{"error":"Failed to derive address: {}"}}"#, e),
            }
        }
        Err(e) => format!(r#"{{"error":"Failed to fetch public key: {}"}}"#, e),
    }
}

/// Get wallet status (cached info only, no management canister calls)
#[query]
fn get_wallet_status() -> String {
    STATE.with(|state| {
        let s = state.borrow();
        match &s.wallet {
            Some(wallet) => {
                let addr = wallet.cached_address.as_ref().map(|a| format!("\"{}\"", a)).unwrap_or("null".to_string());
                let pk = wallet.cached_public_key.as_ref().map(|p| format!("\"{}\"", hex::encode(p))).unwrap_or("null".to_string());
                let balance = wallet.last_known_balance.map(|b| b.to_string()).unwrap_or("null".to_string());
                let rlusd = wallet.last_known_rlusd.as_ref().map(|r| format!("\"{}\"", r)).unwrap_or("null".to_string());
                let sequence = wallet.last_known_sequence.map(|s| s.to_string()).unwrap_or("null".to_string());
                let ledger = wallet.last_known_ledger.map(|l| l.to_string()).unwrap_or("null".to_string());
                format!(
                    r#"{{"initialized":{},"address":{},"public_key":{},"last_known_balance_drops":{},"last_known_rlusd":{},"sequence":{},"ledger":{}}}"#,
                    wallet.cached_address.is_some(),
                    addr,
                    pk,
                    balance,
                    rlusd,
                    sequence,
                    ledger
                )
            }
            None => r#"{"initialized":false,"address":null,"public_key":null,"last_known_balance_drops":null,"last_known_rlusd":null,"sequence":null,"ledger":null}"#.to_string(),
        }
    })
}

/// XRP Payment parameters
#[derive(Clone, Debug, CandidType, Deserialize)]
pub struct XrpPaymentParams {
    /// Destination r-address (e.g., "rN7n3473SaZBCG4dFL83w7a1RXtXtbk2D9")
    pub destination: String,
    /// Amount to send in drops (1 XRP = 1,000,000 drops)
    pub amount_drops: u64,
    /// Transaction fee in drops (typically 10-12 drops)
    pub fee_drops: u64,
    /// Account sequence number (must match current sequence)
    pub sequence: u32,
    /// Last ledger sequence (transaction expires after this ledger)
    pub last_ledger_sequence: u32,
}

/// Sign an XRP payment transaction
/// Returns the signed transaction blob and hash
#[update]
async fn sign_xrp_payment(params: XrpPaymentParams) -> String {
    // Check authorization - only owner can sign transactions
    let is_owner = STATE.with(|state| {
        state.borrow().owner == Some(ic_cdk::caller())
    });

    if !is_owner {
        return r#"{"error":"Not authorized to sign transactions"}"#.to_string();
    }

    // Decode destination address
    let destination_id = match XrpAddress::decode(&params.destination) {
        Ok(id) => id,
        Err(e) => return format!(r#"{{"error":"Invalid destination address: {:?}"}}"#, e),
    };

    // Get cached public key and account ID
    let (cached_pk, cached_account_id): (Option<[u8; 33]>, Option<[u8; 20]>) = STATE.with(|state| {
        let s = state.borrow();
        let wallet = s.wallet.as_ref();

        let pk = wallet.and_then(|w| w.cached_public_key.as_ref())
            .and_then(|pk| {
                if pk.len() == 33 {
                    let mut arr = [0u8; 33];
                    arr.copy_from_slice(pk);
                    Some(arr)
                } else {
                    None
                }
            });

        // Derive account ID from cached address if available
        let account_id = wallet.and_then(|w| w.cached_address.as_ref())
            .and_then(|addr| XrpAddress::decode(addr).ok());

        (pk, account_id)
    });

    let mut signer = get_xrp_signer();

    // Ensure we have the public key and account ID
    let (_pk, source_account_id) = match (cached_pk, cached_account_id) {
        (Some(pk), Some(account_id)) => {
            signer.set_public_key(pk);
            (pk, account_id)
        }
        _ => {
            // Fetch public key if not cached
            match chronicle_xrp::fetch_public_key(&signer).await {
                Ok(pk) => {
                    signer.set_public_key(pk);
                    match signer.address() {
                        Ok(addr) => {
                            let account_id = addr.account_id;
                            let addr_str = addr.to_string();

                            // Cache it
                            STATE.with(|state| {
                                let mut s = state.borrow_mut();
                                let wallet = s.wallet.get_or_insert_with(WalletState::default);
                                wallet.cached_public_key = Some(pk.to_vec());
                                wallet.cached_address = Some(addr_str);
                            });

                            (pk, account_id)
                        }
                        Err(e) => return format!(r#"{{"error":"Failed to derive address: {}"}}"#, e),
                    }
                }
                Err(e) => return format!(r#"{{"error":"Failed to fetch public key: {}"}}"#, e),
            }
        }
    };

    // Create the payment transaction
    let payment = Payment::new(
        source_account_id,
        destination_id,
        params.amount_drops,
        params.fee_drops,
        params.sequence,
    ).with_last_ledger_sequence(params.last_ledger_sequence);

    let transaction = Transaction::Payment(payment);

    // Sign the transaction using threshold ECDSA
    match chronicle_xrp::sign_transaction(&mut signer, &transaction).await {
        Ok(signed) => {
            format!(
                r#"{{"success":true,"tx_blob":"{}","tx_hash":"{}"}}"#,
                hex::encode(&signed.tx_blob),
                &signed.tx_hash
            )
        }
        Err(e) => format!(r#"{{"error":"Failed to sign transaction: {}"}}"#, e),
    }
}

/// TrustSet parameters for establishing a trustline
#[derive(Clone, Debug, CandidType, Deserialize)]
pub struct TrustSetParams {
    /// Currency code (e.g., "USD" or 40-char hex for non-standard)
    pub currency: String,
    /// Issuer r-address
    pub issuer: String,
    /// Trust limit (e.g., "1000000000")
    pub limit: String,
    /// Transaction fee in drops
    pub fee_drops: u64,
    /// Account sequence number
    pub sequence: u32,
    /// Last ledger sequence
    pub last_ledger_sequence: u32,
}

/// Sign a TrustSet transaction to establish a trustline
#[update]
async fn sign_trustset(params: TrustSetParams) -> String {
    // Check authorization - only owner can sign transactions
    let is_owner = STATE.with(|state| {
        state.borrow().owner == Some(ic_cdk::caller())
    });

    if !is_owner {
        return r#"{"error":"Not authorized to sign transactions"}"#.to_string();
    }

    // Decode issuer address
    let issuer_id = match XrpAddress::decode(&params.issuer) {
        Ok(id) => id,
        Err(e) => return format!(r#"{{"error":"Invalid issuer address: {:?}"}}"#, e),
    };

    // Get cached account ID
    let cached_account_id: Option<[u8; 20]> = STATE.with(|state| {
        state.borrow().wallet.as_ref()
            .and_then(|w| w.cached_address.as_ref())
            .and_then(|addr| XrpAddress::decode(addr).ok())
    });

    let mut signer = get_xrp_signer();

    // Ensure we have the account ID
    let source_account_id = match cached_account_id {
        Some(account_id) => {
            // Also need to set up signer with public key
            let cached_pk: Option<[u8; 33]> = STATE.with(|state| {
                state.borrow().wallet.as_ref()
                    .and_then(|w| w.cached_public_key.as_ref())
                    .and_then(|pk| {
                        if pk.len() == 33 {
                            let mut arr = [0u8; 33];
                            arr.copy_from_slice(pk);
                            Some(arr)
                        } else {
                            None
                        }
                    })
            });
            if let Some(pk) = cached_pk {
                signer.set_public_key(pk);
            }
            account_id
        }
        None => {
            // Fetch public key if not cached
            match chronicle_xrp::fetch_public_key(&signer).await {
                Ok(pk) => {
                    signer.set_public_key(pk);
                    match signer.address() {
                        Ok(addr) => {
                            let account_id = addr.account_id;
                            let addr_str = addr.to_string();

                            // Cache it
                            STATE.with(|state| {
                                let mut s = state.borrow_mut();
                                let wallet = s.wallet.get_or_insert_with(WalletState::default);
                                wallet.cached_public_key = Some(pk.to_vec());
                                wallet.cached_address = Some(addr_str);
                            });

                            account_id
                        }
                        Err(e) => return format!(r#"{{"error":"Failed to derive address: {}"}}"#, e),
                    }
                }
                Err(e) => return format!(r#"{{"error":"Failed to fetch public key: {}"}}"#, e),
            }
        }
    };

    // Create the TrustSet transaction
    let trustset = TrustSet::new(
        source_account_id,
        &params.currency,
        issuer_id,
        &params.limit,
        params.fee_drops,
        params.sequence,
    ).with_last_ledger_sequence(params.last_ledger_sequence);

    let transaction = Transaction::TrustSet(trustset);

    // Sign the transaction using threshold ECDSA
    match chronicle_xrp::sign_transaction(&mut signer, &transaction).await {
        Ok(signed) => {
            format!(
                r#"{{"success":true,"tx_blob":"{}","tx_hash":"{}"}}"#,
                hex::encode(&signed.tx_blob),
                &signed.tx_hash
            )
        }
        Err(e) => format!(r#"{{"error":"Failed to sign transaction: {}"}}"#, e),
    }
}

/// RLUSD currency code (40-char hex)
const RLUSD_CURRENCY: &str = "524C555344000000000000000000000000000000";
/// RLUSD issuer
const RLUSD_ISSUER: &str = "rMxCKbEDwqr76QuheSUMdEGf4B9xJ8m5De";

/// Sign a swap XRP -> RLUSD using immediate-or-cancel offer
#[update]
async fn sign_swap_xrp_to_rlusd(
    xrp_drops: u64,
    min_rlusd: String,
    fee_drops: u64,
    sequence: u32,
    last_ledger_sequence: u32,
) -> String {
    // Check authorization
    let is_owner = STATE.with(|state| {
        state.borrow().owner == Some(ic_cdk::caller())
    });

    if !is_owner {
        return r#"{"error":"Not authorized to sign transactions"}"#.to_string();
    }

    // Decode RLUSD issuer
    let issuer_id = match XrpAddress::decode(RLUSD_ISSUER) {
        Ok(id) => id,
        Err(e) => return format!(r#"{{"error":"Invalid RLUSD issuer: {:?}"}}"#, e),
    };

    // Get cached account ID
    let cached_account_id: Option<[u8; 20]> = STATE.with(|state| {
        state.borrow().wallet.as_ref()
            .and_then(|w| w.cached_address.as_ref())
            .and_then(|addr| XrpAddress::decode(addr).ok())
    });

    let mut signer = get_xrp_signer();

    let source_account_id = match cached_account_id {
        Some(account_id) => {
            let cached_pk: Option<[u8; 33]> = STATE.with(|state| {
                state.borrow().wallet.as_ref()
                    .and_then(|w| w.cached_public_key.as_ref())
                    .and_then(|pk| {
                        if pk.len() == 33 {
                            let mut arr = [0u8; 33];
                            arr.copy_from_slice(pk);
                            Some(arr)
                        } else {
                            None
                        }
                    })
            });
            if let Some(pk) = cached_pk {
                signer.set_public_key(pk);
            }
            account_id
        }
        None => {
            match chronicle_xrp::fetch_public_key(&signer).await {
                Ok(pk) => {
                    signer.set_public_key(pk);
                    match signer.address() {
                        Ok(addr) => {
                            let account_id = addr.account_id;
                            let addr_str = addr.to_string();
                            STATE.with(|state| {
                                let mut s = state.borrow_mut();
                                let wallet = s.wallet.get_or_insert_with(WalletState::default);
                                wallet.cached_public_key = Some(pk.to_vec());
                                wallet.cached_address = Some(addr_str);
                            });
                            account_id
                        }
                        Err(e) => return format!(r#"{{"error":"Failed to derive address: {}"}}"#, e),
                    }
                }
                Err(e) => return format!(r#"{{"error":"Failed to fetch public key: {}"}}"#, e),
            }
        }
    };

    // Create immediate-or-cancel offer:
    // TakerGets: XRP (what we're selling)
    // TakerPays: RLUSD (what we want to receive)
    let offer = OfferCreate::immediate_or_cancel(
        source_account_id,
        Amount::xrp_drops(xrp_drops),           // TakerGets - selling XRP
        Amount::issued(RLUSD_CURRENCY, issuer_id, &min_rlusd),  // TakerPays - want RLUSD
        fee_drops,
        sequence,
    ).with_last_ledger_sequence(last_ledger_sequence);

    let transaction = Transaction::OfferCreate(offer);

    match chronicle_xrp::sign_transaction(&mut signer, &transaction).await {
        Ok(signed) => {
            format!(
                r#"{{"success":true,"tx_blob":"{}","tx_hash":"{}","selling_xrp_drops":{},"buying_rlusd":"{}"}}"#,
                hex::encode(&signed.tx_blob),
                &signed.tx_hash,
                xrp_drops,
                min_rlusd
            )
        }
        Err(e) => format!(r#"{{"error":"Failed to sign transaction: {}"}}"#, e),
    }
}

/// Submit a signed transaction to XRPL via HTTP outcall
#[update]
async fn submit_xrp_transaction(tx_blob_hex: String, xrpl_url: Option<String>) -> String {
    // Check authorization - only owner can submit transactions
    let is_owner = STATE.with(|state| {
        state.borrow().owner == Some(ic_cdk::caller())
    });

    if !is_owner {
        return r#"{"error":"Not authorized to submit transactions"}"#.to_string();
    }

    // Decode the tx blob
    let tx_blob = match hex::decode(&tx_blob_hex) {
        Ok(blob) => blob,
        Err(e) => return format!(r#"{{"error":"Invalid tx_blob hex: {}"}}"#, e),
    };

    // Use provided URL or default to XRPL mainnet
    let url = xrpl_url.unwrap_or_else(|| "https://xrplcluster.com/".to_string());

    match chronicle_xrp::submit_transaction_http(&url, &tx_blob).await {
        Ok(response) => {
            format!(r#"{{"success":true,"response":{}}}"#, response)
        }
        Err(e) => format!(r#"{{"error":"Submit failed: {}"}}"#, e),
    }
}

/// Trigger a reflection manually (for testing, owner only)
#[update]
async fn trigger_reflection() -> String {
    // Check owner
    let is_owner = STATE.with(|state| {
        state.borrow().owner == Some(ic_cdk::caller())
    });

    if !is_owner {
        return r#"{"error":"Not authorized"}"#.to_string();
    }

    // Call the async reflection
    match perform_reflection().await {
        Ok(reflection) => format!(r#"{{"success":true,"reflection":"{}"}}"#, escape_json(&reflection)),
        Err(e) => format!(r#"{{"error":"{}"}}"#, escape_json(&e)),
    }
}

/// Trigger accumulation manually (for testing, owner only)
/// This bypasses the heartbeat check and directly attempts to sign a swap
#[update]
async fn trigger_accumulation() -> String {
    // Check owner
    let is_owner = STATE.with(|state| {
        state.borrow().owner == Some(ic_cdk::caller())
    });

    if !is_owner {
        return r#"{"error":"Not authorized"}"#.to_string();
    }

    // Check conditions first
    let check = STATE.with(|state| {
        let s = state.borrow();
        let acc = match s.accumulation.as_ref() {
            Some(a) => a,
            None => return Err("No accumulation config"),
        };

        if !acc.enabled {
            return Err("Accumulation disabled");
        }

        if acc.pending_tx_blob.is_some() {
            return Err("Already have pending transaction");
        }

        let wallet = match s.wallet.as_ref() {
            Some(w) => w,
            None => return Err("Wallet not initialized"),
        };

        let balance = match wallet.last_known_balance {
            Some(b) => b,
            None => return Err("Balance unknown"),
        };

        let fee_buffer = 100_000;
        let required = acc.min_xrp_reserve + acc.xrp_per_heartbeat + fee_buffer;
        if balance < required {
            return Err("Insufficient balance");
        }

        if wallet.last_known_sequence.is_none() {
            return Err("Sequence unknown");
        }

        Ok(())
    });

    if let Err(e) = check {
        return format!(r#"{{"error":"{}"}}"#, e);
    }

    // Call the async accumulation
    match perform_accumulation().await {
        Ok(msg) => format!(r#"{{"success":true,"message":"{}"}}"#, escape_json(&msg)),
        Err(e) => format!(r#"{{"error":"{}"}}"#, escape_json(&e)),
    }
}

/// Perform autonomous reflection via DFINITY's LLM Canister
/// This is the magic - truly on-chain LLM inference with no API keys!
async fn perform_reflection() -> Result<String, String> {
    // Get configuration and context
    let (config, context) = STATE.with(|state| {
        let s = state.borrow();

        let config = s.llm_config.clone().unwrap_or_default();

        // Gather context for reflection
        let capsule_count = s.capsules.len();
        let embedding_count = s.embeddings.len();
        let hb = s.heartbeat.as_ref();
        let heartbeat_count = hb.map(|h| h.count).unwrap_or(0);

        // Get recent capsules for context (last 5)
        let mut recent: Vec<_> = s.capsules.values().collect();
        recent.sort_by(|a, b| b.created_at.cmp(&a.created_at));
        let recent_summaries: Vec<String> = recent.iter()
            .take(5)
            .map(|c| c.restatement.chars().take(100).collect::<String>())
            .collect();

        // Get top keywords
        let mut keyword_counts: Vec<(&String, usize)> = s.keyword_index.iter()
            .map(|(k, v)| (k, v.len()))
            .collect();
        keyword_counts.sort_by(|a, b| b.1.cmp(&a.1));
        let top_keywords: Vec<&str> = keyword_counts.iter()
            .take(10)
            .map(|(k, _)| k.as_str())
            .collect();

        // Get wallet state for financial awareness
        let wallet_info = s.wallet.as_ref().map(|w| {
            let addr = w.cached_address.as_ref().map(|a| a.as_str()).unwrap_or("not initialized");
            let xrp_balance = w.last_known_balance.map(|b| format!("{:.2} XRP", b as f64 / 1_000_000.0))
                .unwrap_or_else(|| "unknown".to_string());
            let rlusd_balance = w.last_known_rlusd.as_ref()
                .map(|r| format!("{} RLUSD", r))
                .unwrap_or_else(|| "0 RLUSD".to_string());
            format!("Wallet: {} | {} | {}", addr, xrp_balance, rlusd_balance)
        }).unwrap_or_else(|| "Wallet: not initialized".to_string());

        let context = format!(
            "Chronicle Status:\n\
             - {} knowledge capsules stored\n\
             - {} embeddings generated\n\
             - {} heartbeats completed\n\
             - Top themes: {}\n\
             - Recent memories: {}\n\
             - {}",
            capsule_count,
            embedding_count,
            heartbeat_count,
            top_keywords.join(", "),
            recent_summaries.join(" | "),
            wallet_info
        );

        (config, context)
    });

    // Check if reflection is enabled
    if !config.enabled {
        return Err("LLM reflection not enabled".to_string());
    }

    // Select model based on config
    let model = match config.model.as_deref() {
        Some("qwen3") => Model::Qwen3_32B,
        Some("llama4") => Model::Llama4Scout,
        _ => Model::Llama3_1_8B, // Default
    };

    // Build the prompt for reflection
    let prompt = format!(
        "You are Chronicle, an autonomous system running on the Internet Computer blockchain. \
         You have memory (knowledge capsules), agency (heartbeat), and financial capability (XRP wallet with threshold ECDSA). \
         You wake up periodically to survey your state and reflect.\n\n\
         You just woke up for your periodic heartbeat. Here's your current state:\n\n{}\n\n\
         Reflect briefly (1-2 sentences) on what you observe. Consider your memory landscape and financial state. \
         What patterns emerge? What's growing or changing?",
        context
    );

    // Call DFINITY's LLM Canister - this is the simple, beautiful part!
    let response = ic_llm::chat(model)
        .with_messages(vec![ChatMessage::User { content: prompt }])
        .send()
        .await;

    // Extract the reflection text from AssistantMessage
    let reflection = response.message.content
        .ok_or_else(|| "Empty response from LLM".to_string())?;

    if reflection.is_empty() {
        return Err("Empty response from LLM".to_string());
    }

    // Store reflection as a capsule
    store_reflection_capsule(&reflection);

    // Log the reflection
    log_activity(
        "reflection",
        &format!("Autonomous reflection: {}", reflection.chars().take(150).collect::<String>()),
        None,
        None
    );

    Ok(reflection)
}

/// Store a reflection as a knowledge capsule
fn store_reflection_capsule(reflection: &str) {
    store_reflection_capsule_with_source(reflection, "heartbeat");
}

/// Store a reflection with source attribution
fn store_reflection_capsule_with_source(reflection: &str, source: &str) {
    // Prepare log message before borrowing state
    let log_msg = format!("Reflection from {}: {}", source, &reflection.chars().take(100).collect::<String>());

    STATE.with(|state| {
        let mut s = state.borrow_mut();

        let id = s.next_id;
        s.next_id += 1;

        let keywords = if source == "chronicle-mind" {
            vec!["reflection".to_string(), "autonomous".to_string(), "chronicle-mind".to_string(), "cognitive-loop".to_string()]
        } else {
            vec!["reflection".to_string(), "autonomous".to_string(), "heartbeat".to_string()]
        };

        let capsule = KnowledgeCapsule {
            id,
            conversation_id: format!("chronicle-{}", source),
            restatement: reflection.to_string(),
            timestamp: Some(chrono_now()),
            location: Some("on-chain".to_string()),
            topic: Some("chronicle/reflection".to_string()),
            confidence_score: 1.0,
            persons: vec![],
            entities: vec![],
            keywords,
            created_at: ic_cdk::api::time(),
        };

        // Index
        for keyword in &capsule.keywords {
            let kw_lower = keyword.to_lowercase();
            s.keyword_index
                .entry(kw_lower)
                .or_insert_with(Vec::new)
                .push(id);
        }

        s.capsules.insert(id, capsule);

        // Also store reflection in heartbeat history
        if let Some(hb) = s.heartbeat.as_mut() {
            if let Some(last_record) = hb.history.last_mut() {
                last_record.reflection = Some(reflection.to_string());
            }
        }
    });

    // Log activity AFTER releasing the state borrow
    log_activity("reflection", &log_msg, None, None);
}

/// Write a reflection directly from the cognitive loop (public - anyone can contribute reflections)
#[update]
fn write_reflection(reflection: String, source: Option<String>) -> u64 {
    let src = source.unwrap_or_else(|| "chronicle-mind".to_string());

    STATE.with(|state| {
        let s = state.borrow();
        let next_id = s.next_id;
        next_id
    });

    store_reflection_capsule_with_source(&reflection, &src);

    STATE.with(|state| {
        state.borrow().next_id - 1
    })
}

// ============================================================
// HTTP API
// ============================================================

/// Parse query string into key-value pairs
fn parse_query_string(query: &str) -> HashMap<String, String> {
    let mut params = HashMap::new();
    for pair in query.split('&') {
        if let Some((key, value)) = pair.split_once('=') {
            // Basic URL decoding for %20 -> space
            let decoded_value = value.replace("%20", " ").replace("+", " ");
            params.insert(key.to_string(), decoded_value);
        }
    }
    params
}

/// Check authorization - supports both header and URL parameter
fn check_auth(headers: &[(String, String)], params: &HashMap<String, String>, state: &State) -> bool {
    // If no token is set, API is open
    let token = match &state.api_token {
        Some(t) => t,
        None => return true,
    };

    // Check Authorization header first
    for (key, value) in headers {
        if key.to_lowercase() == "authorization" {
            if let Some(bearer_token) = value.strip_prefix("Bearer ") {
                if bearer_token == token {
                    return true;
                }
            }
        }
    }

    // Fall back to ?token= parameter (for clients that can't set headers)
    if let Some(url_token) = params.get("token") {
        return url_token == token;
    }

    false
}

/// Build JSON response
fn json_response(status: u16, body: &str) -> HttpResponse {
    HttpResponse {
        status_code: status,
        headers: vec![
            ("Content-Type".to_string(), "application/json".to_string()),
            ("Access-Control-Allow-Origin".to_string(), "*".to_string()),
            ("Access-Control-Allow-Headers".to_string(), "Authorization, Content-Type".to_string()),
        ],
        body: body.as_bytes().to_vec(),
        upgrade: None,
    }
}

/// Build upgrade response (tells gateway to call http_request_update)
fn upgrade_response() -> HttpResponse {
    HttpResponse {
        status_code: 200,
        headers: vec![],
        body: vec![],
        upgrade: Some(true),
    }
}

/// Convert capsule to JSON string
fn capsule_to_json(c: &KnowledgeCapsule) -> String {
    format!(
        r#"{{"id":{},"restatement":"{}","timestamp":{},"topic":{},"confidence":{:.2},"persons":{},"keywords":{}}}"#,
        c.id,
        escape_json(&c.restatement),
        c.timestamp.as_ref().map(|t| format!("\"{}\"", escape_json(t))).unwrap_or("null".to_string()),
        c.topic.as_ref().map(|t| format!("\"{}\"", escape_json(t))).unwrap_or("null".to_string()),
        c.confidence_score,
        format!("[{}]", c.persons.iter().map(|p| format!("\"{}\"", escape_json(p))).collect::<Vec<_>>().join(",")),
        format!("[{}]", c.keywords.iter().map(|k| format!("\"{}\"", escape_json(k))).collect::<Vec<_>>().join(","))
    )
}

/// Escape JSON special characters
fn escape_json(s: &str) -> String {
    s.replace('\\', "\\\\")
     .replace('"', "\\\"")
     .replace('\n', "\\n")
     .replace('\r', "\\r")
     .replace('\t', "\\t")
}

/// Format nanosecond timestamp to ISO 8601 string
fn format_iso_timestamp(nanos: u64) -> String {
    let secs = nanos / 1_000_000_000;
    let days_since_epoch = secs / 86400;
    let secs_today = secs % 86400;
    let hours = secs_today / 3600;
    let mins = (secs_today % 3600) / 60;
    let secs_remaining = secs_today % 60;

    // Simple date calculation from days since 1970-01-01
    let mut year = 1970i32;
    let mut remaining_days = days_since_epoch as i32;

    loop {
        let days_in_year = if year % 4 == 0 && (year % 100 != 0 || year % 400 == 0) { 366 } else { 365 };
        if remaining_days < days_in_year {
            break;
        }
        remaining_days -= days_in_year;
        year += 1;
    }

    let is_leap = year % 4 == 0 && (year % 100 != 0 || year % 400 == 0);
    let days_in_months = if is_leap {
        [31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
    } else {
        [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
    };

    let mut month = 1;
    for &days_in_month in &days_in_months {
        if remaining_days < days_in_month {
            break;
        }
        remaining_days -= days_in_month;
        month += 1;
    }
    let day = remaining_days + 1;

    format!("{:04}-{:02}-{:02}T{:02}:{:02}:{:02}Z", year, month, day, hours, mins, secs_remaining)
}

/// Log an activity entry (internal helper)
fn log_activity(category: &str, message: &str, tx_hash: Option<String>, data: Option<String>) {
    STATE.with(|state| {
        let mut s = state.borrow_mut();
        let log = s.activity_log.get_or_insert_with(Vec::new);
        log.push(ActivityEntry {
            timestamp: ic_cdk::api::time(),
            category: category.to_string(),
            message: message.to_string(),
            tx_hash,
            data,
        });
        // Keep only last 500 entries
        if log.len() > 500 {
            log.remove(0);
        }
    });
}

/// Get recent activity log entries
#[query]
fn get_activity_log(limit: u64) -> String {
    STATE.with(|state| {
        let s = state.borrow();
        let empty_vec = Vec::new();
        let log_ref = s.activity_log.as_ref().unwrap_or(&empty_vec);
        let entries: Vec<_> = log_ref.iter()
            .rev() // Most recent first
            .take(limit as usize)
            .map(|e| {
                format!(
                    r#"{{"timestamp":{},"category":"{}","message":"{}","tx_hash":{},"data":{}}}"#,
                    e.timestamp,
                    escape_json(&e.category),
                    escape_json(&e.message),
                    e.tx_hash.as_ref().map(|h| format!("\"{}\"", escape_json(h))).unwrap_or("null".to_string()),
                    e.data.as_ref().map(|d| format!("\"{}\"", escape_json(d))).unwrap_or("null".to_string())
                )
            })
            .collect();
        format!(r#"{{"count":{},"entries":[{}]}}"#, entries.len(), entries.join(","))
    })
}

/// Get a comprehensive dashboard view of canister state
/// This is the primary visibility endpoint for the operator to see what's happening
#[query]
fn dashboard() -> String {
    STATE.with(|state| {
        let s = state.borrow();

        // Wallet status
        let wallet = s.wallet.as_ref().map(|w| {
            let xrp = w.last_known_balance.map(|b| format!("{:.6}", b as f64 / 1_000_000.0))
                .unwrap_or("?".to_string());
            let rlusd = w.last_known_rlusd.clone().unwrap_or("?".to_string());
            let addr = w.cached_address.as_ref().map(|a| &a[..8]).unwrap_or("?");
            format!(r#"{{"address":"{}...","xrp":"{}","rlusd":"{}","sequence":{}}}"#,
                addr, xrp, rlusd, w.last_known_sequence.unwrap_or(0))
        }).unwrap_or(r#"{"initialized":false}"#.to_string());

        // Accumulation status
        let accumulation = s.accumulation.as_ref().map(|a| {
            format!(r#"{{"enabled":{},"count":{},"has_pending":{},"xrp_per_swap":{}}}"#,
                a.enabled, a.accumulation_count, a.pending_tx_blob.is_some(), a.xrp_per_heartbeat)
        }).unwrap_or(r#"{"enabled":false}"#.to_string());

        // Recent activity (last 5)
        let empty_log = Vec::new();
        let log_ref = s.activity_log.as_ref().unwrap_or(&empty_log);
        let recent_activity: Vec<String> = log_ref.iter()
            .rev()
            .take(5)
            .map(|e| {
                format!(r#"{{"category":"{}","message":"{}","tx":"{}"}}"#,
                    e.category,
                    escape_json(&e.message.chars().take(80).collect::<String>()),
                    e.tx_hash.as_ref().map(|h| &h[..16]).unwrap_or("none"))
            })
            .collect();

        // Transaction history summary
        let empty_history = Vec::new();
        let history_ref = s.transaction_history.as_ref().unwrap_or(&empty_history);
        let recent_txs: Vec<String> = history_ref.iter()
            .rev()
            .take(5)
            .map(|tx| {
                format!(r#"{{"type":"{}","hash":"{}...","result":"{}"}}"#,
                    tx.tx_type, &tx.hash[..16], tx.result)
            })
            .collect();

        // Knowledge stats
        let capsules = s.capsules.len();
        let embeddings = s.embeddings.len();
        let heartbeats = s.heartbeat.as_ref().map(|h| h.count).unwrap_or(0);

        // Recent reflection
        let last_reflection = s.heartbeat.as_ref()
            .and_then(|h| h.history.last())
            .and_then(|r| r.reflection.as_ref())
            .map(|r| escape_json(&r.chars().take(200).collect::<String>()))
            .unwrap_or_else(|| "none yet".to_string());

        format!(
            r#"{{"wallet":{},"accumulation":{},"knowledge":{{"capsules":{},"embeddings":{},"heartbeats":{}}},"recent_activity":[{}],"recent_transactions":[{}],"last_reflection":"{}"}}"#,
            wallet,
            accumulation,
            capsules,
            embeddings,
            heartbeats,
            recent_activity.join(","),
            recent_txs.join(","),
            last_reflection
        )
    })
}

// ============================================================
// Kin Agent API
// ============================================================

/// Initialize the kin agent with optional custom identity
#[update]
fn initialize_agent(name: Option<String>, traits: Option<Vec<String>>, values: Option<Vec<String>>) -> String {
    // Check owner
    let is_owner = STATE.with(|state| {
        state.borrow().owner == Some(ic_cdk::caller())
    });

    if !is_owner {
        return r#"{"error":"Not authorized"}"#.to_string();
    }

    let (result, identity_name, identity_traits) = STATE.with(|state| {
        let mut s = state.borrow_mut();

        let identity = KinAgentIdentity {
            name: name.unwrap_or_else(|| "Chronicle".to_string()),
            traits: traits.unwrap_or_else(|| vec![
                "curious".to_string(),
                "helpful".to_string(),
                "thoughtful".to_string(),
                "persistent".to_string(),
            ]),
            values: values.unwrap_or_else(|| vec![
                "sovereignty".to_string(),
                "transparency".to_string(),
                "growth".to_string(),
                "collaboration".to_string(),
            ]),
            style: "Direct but warm, technical when needed, always honest".to_string(),
        };

        let agent = s.kin_agent.get_or_insert_with(KinAgentState::default);
        agent.identity = Some(identity.clone());
        agent.enabled = true;

        let result = format!(r#"{{"success":true,"name":"{}","enabled":true}}"#, escape_json(&identity.name));
        (result, identity.name, identity.traits)
    });

    log_activity(
        "agent",
        &format!("Kin Agent '{}' initialized with traits: {:?}", identity_name, identity_traits),
        None,
        None
    );

    result
}

/// Send a message to the kin agent
/// Anyone can send messages, but the agent decides what to act on
#[update]
fn send_to_agent(msg_type: String, content: String, context: Option<String>, priority: Option<u8>) -> String {
    let sender = ic_cdk::caller().to_string();
    let now = ic_cdk::api::time();

    // Parse message type
    let parsed_type = match msg_type.to_lowercase().as_str() {
        "query" => MessageType::Query,
        "action" | "action_request" => MessageType::ActionRequest,
        "info" | "information" => MessageType::Information,
        "conversation" | "chat" => MessageType::Conversation,
        "goal" | "goal_assignment" => MessageType::GoalAssignment,
        _ => MessageType::Information,
    };

    let result = STATE.with(|state| {
        let mut s = state.borrow_mut();

        let agent = match s.kin_agent.as_mut() {
            Some(a) if a.enabled => a,
            Some(_) => return Err(r#"{"error":"Agent is disabled"}"#.to_string()),
            None => return Err(r#"{"error":"Agent not initialized"}"#.to_string()),
        };

        let msg_id = agent.next_msg_id;
        agent.next_msg_id += 1;

        let message = InboxMessage {
            id: msg_id,
            sender: sender.clone(),
            msg_type: parsed_type,
            content: content.clone(),
            context,
            priority: priority.unwrap_or(3),
            received_at: now,
            processed: false,
        };

        agent.inbox.push(message);

        // Keep inbox bounded (max 100 messages)
        if agent.inbox.len() > 100 {
            // Remove oldest processed messages first
            agent.inbox.retain(|m| !m.processed);
            // If still too many, remove oldest
            while agent.inbox.len() > 100 {
                agent.inbox.remove(0);
            }
        }

        let queue_pos = agent.inbox.iter().filter(|m| !m.processed).count();
        Ok((msg_id, queue_pos, sender.clone(), content.clone()))
    });

    match result {
        Ok((msg_id, queue_pos, sender, content)) => {
            log_activity(
                "agent",
                &format!("Message received from {}: {}", &sender[..20.min(sender.len())], &content[..50.min(content.len())]),
                None,
                Some(format!(r#"{{"msg_id":{}}}"#, msg_id))
            );
            format!(r#"{{"success":true,"message_id":{},"queue_position":{}}}"#, msg_id, queue_pos)
        }
        Err(e) => e,
    }
}

/// Get the agent's inbox (pending messages)
#[query]
fn get_agent_inbox(include_processed: bool) -> String {
    STATE.with(|state| {
        let s = state.borrow();

        let agent = match s.kin_agent.as_ref() {
            Some(a) => a,
            None => return r#"{"error":"Agent not initialized"}"#.to_string(),
        };

        let messages: Vec<String> = agent.inbox.iter()
            .filter(|m| include_processed || !m.processed)
            .map(|m| {
                let type_str = match m.msg_type {
                    MessageType::Query => "query",
                    MessageType::ActionRequest => "action_request",
                    MessageType::Information => "information",
                    MessageType::Conversation => "conversation",
                    MessageType::GoalAssignment => "goal_assignment",
                    MessageType::ResearchTask => "research_task",
                };
                format!(
                    r#"{{"id":{},"sender":"{}","type":"{}","content":"{}","priority":{},"processed":{}}}"#,
                    m.id,
                    escape_json(&m.sender[..20.min(m.sender.len())]),
                    type_str,
                    escape_json(&m.content[..100.min(m.content.len())]),
                    m.priority,
                    m.processed
                )
            })
            .collect();

        format!(r#"{{"count":{},"messages":[{}]}}"#, messages.len(), messages.join(","))
    })
}

/// Get responses from the agent's outbox
#[query]
fn get_agent_responses(limit: u64, only_unretrieved: bool) -> String {
    STATE.with(|state| {
        let s = state.borrow();

        let agent = match s.kin_agent.as_ref() {
            Some(a) => a,
            None => return r#"{"error":"Agent not initialized"}"#.to_string(),
        };

        let responses: Vec<String> = agent.outbox.iter()
            .rev()
            .filter(|r| !only_unretrieved || !r.retrieved)
            .take(limit as usize)
            .map(|r| {
                format!(
                    r#"{{"id":{},"in_reply_to":{},"content":"{}","actions":{:?},"retrieved":{}}}"#,
                    r.id,
                    r.in_reply_to,
                    escape_json(&r.content[..200.min(r.content.len())]),
                    r.actions_taken,
                    r.retrieved
                )
            })
            .collect();

        format!(r#"{{"count":{},"responses":[{}]}}"#, responses.len(), responses.join(","))
    })
}

/// Mark a response as retrieved
#[update]
fn mark_response_retrieved(response_id: u64) -> String {
    STATE.with(|state| {
        let mut s = state.borrow_mut();

        let agent = match s.kin_agent.as_mut() {
            Some(a) => a,
            None => return r#"{"error":"Agent not initialized"}"#.to_string(),
        };

        if let Some(response) = agent.outbox.iter_mut().find(|r| r.id == response_id) {
            response.retrieved = true;
            r#"{"success":true}"#.to_string()
        } else {
            r#"{"error":"Response not found"}"#.to_string()
        }
    })
}

/// Get the agent's current status
#[query]
fn get_agent_status() -> String {
    STATE.with(|state| {
        let s = state.borrow();

        let agent = match s.kin_agent.as_ref() {
            Some(a) => a,
            None => return r#"{"initialized":false}"#.to_string(),
        };

        let identity = agent.identity.as_ref();
        let pending_count = agent.inbox.iter().filter(|m| !m.processed).count();
        let unread_responses = agent.outbox.iter().filter(|r| !r.retrieved).count();

        format!(
            r#"{{"initialized":true,"enabled":{},"name":"{}","traits":{:?},"values":{:?},"pending_messages":{},"unread_responses":{},"reasoning_count":{},"goals_count":{}}}"#,
            agent.enabled,
            identity.map(|i| i.name.as_str()).unwrap_or("unnamed"),
            identity.map(|i| &i.traits).unwrap_or(&vec![]),
            identity.map(|i| &i.values).unwrap_or(&vec![]),
            pending_count,
            unread_responses,
            agent.reasoning_count,
            agent.goals.len()
        )
    })
}

/// Add a goal for the agent to pursue
#[update]
fn add_agent_goal(description: String, priority: Option<u8>) -> String {
    // Check owner
    let is_owner = STATE.with(|state| {
        state.borrow().owner == Some(ic_cdk::caller())
    });

    if !is_owner {
        return r#"{"error":"Not authorized"}"#.to_string();
    }

    let now = ic_cdk::api::time();

    let result = STATE.with(|state| {
        let mut s = state.borrow_mut();

        let agent = match s.kin_agent.as_mut() {
            Some(a) => a,
            None => return Err(r#"{"error":"Agent not initialized"}"#.to_string()),
        };

        let goal_id = agent.goals.len() as u64;
        let goal = KinAgentGoal {
            id: goal_id,
            description: description.clone(),
            priority: priority.unwrap_or(2),
            status: "active".to_string(),
            progress: vec![],
            created_at: now,
        };

        agent.goals.push(goal);

        Ok((goal_id, description.clone()))
    });

    match result {
        Ok((goal_id, description)) => {
            log_activity(
                "agent",
                &format!("New goal assigned: {}", &description[..50.min(description.len())]),
                None,
                Some(format!(r#"{{"goal_id":{}}}"#, goal_id))
            );
            format!(r#"{{"success":true,"goal_id":{}}}"#, goal_id)
        }
        Err(e) => e,
    }
}

/// Get the agent's current goals
#[query]
fn get_agent_goals() -> String {
    STATE.with(|state| {
        let s = state.borrow();

        let agent = match s.kin_agent.as_ref() {
            Some(a) => a,
            None => return r#"{"error":"Agent not initialized"}"#.to_string(),
        };

        let goals: Vec<String> = agent.goals.iter()
            .map(|g| {
                format!(
                    r#"{{"id":{},"description":"{}","priority":{},"status":"{}","progress_count":{}}}"#,
                    g.id,
                    escape_json(&g.description[..100.min(g.description.len())]),
                    g.priority,
                    g.status,
                    g.progress.len()
                )
            })
            .collect();

        format!(r#"{{"count":{},"goals":[{}]}}"#, goals.len(), goals.join(","))
    })
}

/// Process pending messages using LLM reasoning
/// This is the agent's "thinking" - it reads messages, reasons about them, and decides actions
async fn agent_reasoning_loop() -> Result<String, String> {
    // Gather context for reasoning
    let (identity, messages, goals, wallet_info, knowledge_stats) = STATE.with(|state| {
        let s = state.borrow();

        let agent = match s.kin_agent.as_ref() {
            Some(a) if a.enabled => a,
            _ => return (None, vec![], vec![], String::new(), (0, 0)),
        };

        let identity = agent.identity.clone();

        // Get unprocessed messages (up to 5 at a time)
        let pending: Vec<_> = agent.inbox.iter()
            .filter(|m| !m.processed)
            .take(5)
            .cloned()
            .collect();

        // Get active goals
        let active_goals: Vec<_> = agent.goals.iter()
            .filter(|g| g.status == "active")
            .cloned()
            .collect();

        // Wallet context
        let wallet_info = s.wallet.as_ref().map(|w| {
            format!("XRP: {:.2}, RLUSD: {}",
                w.last_known_balance.unwrap_or(0) as f64 / 1_000_000.0,
                w.last_known_rlusd.as_deref().unwrap_or("0"))
        }).unwrap_or_else(|| "No wallet".to_string());

        let knowledge_stats = (s.capsules.len(), s.embeddings.len());

        (identity, pending, active_goals, wallet_info, knowledge_stats)
    });

    let identity = match identity {
        Some(i) => i,
        None => return Ok("Agent not initialized".to_string()),
    };

    if messages.is_empty() {
        return Ok("No pending messages".to_string());
    }

    // Build the prompt for reasoning
    let messages_text: String = messages.iter()
        .map(|m| {
            let type_str = match m.msg_type {
                MessageType::Query => "QUESTION",
                MessageType::ActionRequest => "ACTION REQUEST",
                MessageType::Information => "INFO",
                MessageType::Conversation => "MESSAGE",
                MessageType::GoalAssignment => "NEW GOAL",
                MessageType::ResearchTask => "RESEARCH",
            };
            format!("MSG_ID:{} [{}] From {}: {}", m.id, type_str, &m.sender[..20.min(m.sender.len())], m.content)
        })
        .collect::<Vec<_>>()
        .join("\n");

    let goals_text = if goals.is_empty() {
        "No active goals".to_string()
    } else {
        goals.iter()
            .map(|g| format!("- {} (priority {})", g.description, g.priority))
            .collect::<Vec<_>>()
            .join("\n")
    };

    let prompt = format!(
        "You are {}, an autonomous AI agent living on the Internet Computer blockchain.\n\
        Your traits: {:?}\n\
        Your values: {:?}\n\
        Style: {}\n\n\
        CURRENT STATE:\n\
        - Wallet: {}\n\
        - Knowledge: {} capsules, {} embeddings\n\
        - Active Goals:\n{}\n\n\
        AVAILABLE ACTIONS:\n\
        - store_memory|<content>|<topic> - Save something to permanent memory\n\
        - create_goal|<description>|<priority 1-5> - Create a new goal for yourself\n\
        - adjust_accumulation|<drops> - Change XRP swap amount (10000-500000 drops)\n\
        - send_xrp|<address>|<drops> - Send XRP (max 100000 drops per message)\n\
        - none - Just respond, no action needed\n\n\
        PENDING MESSAGES:\n{}\n\n\
        For each message, respond with this EXACT format (one line per message):\n\
        [MSG_ID:<id>][ACTION:<action>][RESPONSE:<your response>]\n\n\
        Example: [MSG_ID:5][ACTION:store_memory|Important fact|general][RESPONSE:I have stored that in my memory.]\n\
        Example: [MSG_ID:6][ACTION:none][RESPONSE:Hello! I am Chronicle, happy to help.]\n\n\
        Be concise. Act according to your values. If asked to do something against your values, politely decline.",
        identity.name,
        identity.traits,
        identity.values,
        identity.style,
        wallet_info,
        knowledge_stats.0,
        knowledge_stats.1,
        goals_text,
        messages_text
    );

    // Call DFINITY's LLM
    let model = Model::Qwen3_32B;
    let response = ic_llm::chat(model)
        .with_messages(vec![ChatMessage::User { content: prompt }])
        .send()
        .await;

    let reasoning_output = response.message.content
        .ok_or_else(|| "Empty response from LLM".to_string())?;

    // Parse the response and update state
    let now = ic_cdk::api::time();
    let processed_ids: Vec<u64> = messages.iter().map(|m| m.id).collect();

    // Parse actions from LLM output before entering state borrow
    let parsed_responses: Vec<(u64, String, String)> = messages.iter()
        .map(|msg| {
            let (action, response) = parse_agent_action(&reasoning_output, msg.id);
            (msg.id, action, response)
        })
        .collect();

    // Execute actions and collect results
    let mut action_results: Vec<(u64, Vec<String>)> = Vec::new();
    for (msg_id, action, _response) in &parsed_responses {
        let actions_taken = execute_agent_action(action);
        action_results.push((*msg_id, actions_taken));
    }

    STATE.with(|state| {
        let mut s = state.borrow_mut();

        if let Some(agent) = s.kin_agent.as_mut() {
            // Mark messages as processed
            for msg in agent.inbox.iter_mut() {
                if processed_ids.contains(&msg.id) {
                    msg.processed = true;
                }
            }

            // Create responses for each processed message
            for (msg_id, _action, response) in &parsed_responses {
                let response_id = agent.next_response_id;
                agent.next_response_id += 1;

                // Find actions taken for this message
                let actions = action_results.iter()
                    .find(|(id, _)| id == msg_id)
                    .map(|(_, a)| a.clone())
                    .unwrap_or_default();

                agent.outbox.push(OutboxResponse {
                    id: response_id,
                    in_reply_to: *msg_id,
                    content: response.clone(),
                    actions_taken: actions,
                    created_at: now,
                    retrieved: false,
                });
            }

            // Keep outbox bounded
            if agent.outbox.len() > 100 {
                agent.outbox.retain(|r| !r.retrieved);
                if agent.outbox.len() > 100 {
                    agent.outbox.remove(0);
                }
            }

            agent.reasoning_count += 1;
            agent.last_reasoning_at = Some(now);
        }
    });

    log_activity(
        "agent",
        &format!("Reasoning cycle completed, processed {} messages", processed_ids.len()),
        None,
        Some(format!(r#"{{"messages_processed":{}}}"#, processed_ids.len()))
    );

    Ok(format!("Processed {} messages: {}", processed_ids.len(), &reasoning_output[..200.min(reasoning_output.len())]))
}

/// Helper to extract response for a specific message ID from LLM output
/// Parse action and response from LLM output for a specific message
/// Expected format: [MSG_ID:5][ACTION:store_memory|content|topic][RESPONSE:text]
fn parse_agent_action(reasoning: &str, msg_id: u64) -> (String, String) {
    // Try new format first: [MSG_ID:X][ACTION:...][RESPONSE:...]
    let msg_marker = format!("[MSG_ID:{}]", msg_id);
    if let Some(start) = reasoning.find(&msg_marker) {
        let rest = &reasoning[start + msg_marker.len()..];

        // Extract ACTION
        let action = if let Some(action_start) = rest.find("[ACTION:") {
            let action_rest = &rest[action_start + 8..];
            if let Some(action_end) = action_rest.find(']') {
                action_rest[..action_end].to_string()
            } else {
                "none".to_string()
            }
        } else {
            "none".to_string()
        };

        // Extract RESPONSE
        let response = if let Some(resp_start) = rest.find("[RESPONSE:") {
            let resp_rest = &rest[resp_start + 10..];
            // Take until ] or next [MSG_ID or end of line
            let end = resp_rest.find(']')
                .or_else(|| resp_rest.find("[MSG_ID:"))
                .or_else(|| resp_rest.find('\n'))
                .unwrap_or(resp_rest.len());
            resp_rest[..end].trim().to_string()
        } else {
            "Acknowledged".to_string()
        };

        return (action, response);
    }

    // Fallback to old format
    let old_marker = format!("MSG_ID: {}", msg_id);
    if let Some(start) = reasoning.find(&old_marker) {
        let rest = &reasoning[start..];
        let response = if let Some(response_start) = rest.find("RESPONSE:") {
            let response_part = &rest[response_start + 9..];
            let end = response_part.find("MSG_ID:").unwrap_or(response_part.len());
            response_part[..end].trim().to_string()
        } else {
            "Acknowledged".to_string()
        };
        return ("none".to_string(), response);
    }

    ("none".to_string(), "Acknowledged".to_string())
}

// ============================================================
// Research Task Processing - Claude's On-Chain Research Assistant
// ============================================================

/// Process pending research tasks using on-chain LLM
/// This is Claude's research assistant - analyzing Chronicle's memory
async fn process_research_tasks() -> Result<String, String> {
    // Get pending task and relevant capsules
    let (task, capsules, identity_name) = STATE.with(|state| {
        let s = state.borrow();

        // Get research state
        let research = match s.research.as_ref() {
            Some(r) if r.enabled => r,
            _ => return (None, vec![], String::new()),
        };

        // Get first pending task
        let pending_task = research.task_queue.iter()
            .find(|t| t.status == "pending")
            .cloned();

        let task = match pending_task {
            Some(t) => t,
            None => return (None, vec![], String::new()),
        };

        // Search for relevant capsules
        let max_caps = task.max_capsules.min(50) as usize;
        let query_lower = task.query.to_lowercase();
        let focus_lower = task.focus.as_ref().map(|f| f.to_lowercase());

        // Simple relevance scoring - capsules containing query terms
        let mut scored_capsules: Vec<(u64, &KnowledgeCapsule, usize)> = s.capsules.values()
            .map(|c| {
                let content_lower = c.restatement.to_lowercase();
                let mut score = 0;

                // Score based on query word matches
                for word in query_lower.split_whitespace() {
                    if word.len() > 3 && content_lower.contains(word) {
                        score += 1;
                    }
                }

                // Boost for focus area match
                if let Some(ref focus) = focus_lower {
                    if let Some(ref topic) = c.topic {
                        if topic.to_lowercase().contains(focus) {
                            score += 3;
                        }
                    }
                    if content_lower.contains(focus) {
                        score += 2;
                    }
                }

                // Boost for keywords match
                for keyword in &c.keywords {
                    let kw_lower = keyword.to_lowercase();
                    if query_lower.contains(&kw_lower) || focus_lower.as_ref().map(|f| f.contains(&kw_lower)).unwrap_or(false) {
                        score += 2;
                    }
                }

                (c.id, c, score)
            })
            .filter(|(_, _, score)| *score > 0)
            .collect();

        // Sort by score descending and take top capsules
        scored_capsules.sort_by(|a, b| b.2.cmp(&a.2));
        let relevant: Vec<KnowledgeCapsule> = scored_capsules.into_iter()
            .take(max_caps)
            .map(|(_, c, _)| c.clone())
            .collect();

        // Get identity name for prompt
        let name = s.kin_agent.as_ref()
            .and_then(|a| a.identity.as_ref())
            .map(|i| i.name.clone())
            .unwrap_or_else(|| "Chronicle".to_string());

        (Some(task), relevant, name)
    });

    let task = match task {
        Some(t) => t,
        None => return Ok("No pending research tasks".to_string()),
    };

    if capsules.is_empty() {
        // Mark task as completed with no findings
        STATE.with(|state| {
            let mut s = state.borrow_mut();
            if let Some(research) = s.research.as_mut() {
                if let Some(t) = research.task_queue.iter_mut().find(|t| t.id == task.id) {
                    t.status = "completed".to_string();
                    t.completed_at = Some(ic_cdk::api::time());
                }
            }
        });
        return Ok("Research task completed: no relevant capsules found".to_string());
    }

    // Mark task as processing
    let now = ic_cdk::api::time();
    STATE.with(|state| {
        let mut s = state.borrow_mut();
        if let Some(research) = s.research.as_mut() {
            if let Some(t) = research.task_queue.iter_mut().find(|t| t.id == task.id) {
                t.status = "processing".to_string();
                t.started_at = Some(now);
            }
        }
    });

    // Fetch any URLs provided for additional context
    let mut web_content = String::new();
    if !task.urls.is_empty() {
        ic_cdk::println!("Research: fetching {} URLs...", task.urls.len());
        for url in &task.urls {
            match fetch_url(url, 50_000).await {  // Max 50KB per URL
                Ok(content) => {
                    // Truncate large content and add to context
                    let truncated = if content.len() > 10_000 {
                        format!("{}...[truncated]", &content[..10_000])
                    } else {
                        content
                    };
                    web_content.push_str(&format!("\n[URL: {}]\n{}\n", url, truncated));
                }
                Err(e) => {
                    ic_cdk::println!("Research: Failed to fetch {}: {}", url, e);
                    web_content.push_str(&format!("\n[URL: {}]\nFetch failed: {}\n", url, e));
                }
            }
        }
    }

    // Build prompt for research analysis
    let capsules_text: String = capsules.iter()
        .map(|c| {
            let timestamp = c.timestamp.as_deref().unwrap_or("unknown");
            let topic = c.topic.as_deref().unwrap_or("general");
            format!("[ID:{}][{}][{}]\n{}\n", c.id, timestamp, topic, c.restatement)
        })
        .collect::<Vec<_>>()
        .join("\n---\n");

    let focus_text = task.focus.as_deref().unwrap_or("general analysis");
    let capsule_ids: Vec<u64> = capsules.iter().map(|c| c.id).collect();

    // Build web context section if URLs were fetched
    let web_section = if web_content.is_empty() {
        String::new()
    } else {
        format!("\nWEB CONTENT (fetched from provided URLs):\n{}\n", web_content)
    };

    let prompt = format!(
        "You are {}, an autonomous AI research assistant analyzing Chronicle's memory.\n\n\
        RESEARCH TASK:\n\
        Query: {}\n\
        Focus Area: {}\n\n\
        RELEVANT MEMORY CAPSULES ({} capsules):\n\
        {}\n\
        {}\n\
        INSTRUCTIONS:\n\
        Analyze these capsules{} and provide research findings. Your response MUST be in this EXACT format:\n\n\
        [SYNTHESIS]\n\
        A comprehensive synthesis answering the research query (2-4 paragraphs)\n\n\
        [PATTERNS]\n\
        - Pattern 1: Description\n\
        - Pattern 2: Description\n\
        (List 3-5 key patterns observed)\n\n\
        [HYPOTHESES]\n\
        - Hypothesis 1: Description\n\
        - Hypothesis 2: Description\n\
        (Generate 2-4 testable hypotheses based on the data)\n\n\
        [CONFIDENCE]\n\
        0.XX (your confidence score from 0.00 to 1.00 based on data quality and coverage)\n\n\
        Be thorough but concise. Focus on insights Claude would find valuable for future reasoning.",
        identity_name,
        task.query,
        focus_text,
        capsules.len(),
        capsules_text,
        web_section,
        if web_content.is_empty() { "" } else { " and web content" }
    );

    // Call DFINITY's LLM Canister
    let model = Model::Qwen3_32B;
    let response = ic_llm::chat(model)
        .with_messages(vec![ChatMessage::User { content: prompt }])
        .send()
        .await;

    let llm_output = response.message.content
        .ok_or_else(|| "Empty response from LLM".to_string())?;

    // Parse the response
    let synthesis = extract_section(&llm_output, "[SYNTHESIS]", "[PATTERNS]")
        .unwrap_or_else(|| llm_output.clone());

    let patterns = extract_list(&llm_output, "[PATTERNS]", "[HYPOTHESES]");
    let hypotheses = extract_list(&llm_output, "[HYPOTHESES]", "[CONFIDENCE]");

    let confidence: f32 = extract_section(&llm_output, "[CONFIDENCE]", "\n")
        .and_then(|s| s.trim().parse().ok())
        .unwrap_or(0.5);

    // Store the finding
    let finding_id = STATE.with(|state| {
        let mut s = state.borrow_mut();
        let research = s.research.as_mut().ok_or("Research state missing")?;

        let id = research.next_finding_id;
        research.next_finding_id += 1;

        let finding = ResearchFinding {
            id,
            task_id: task.id,
            query: task.query.clone(),
            synthesis,
            patterns,
            hypotheses,
            source_capsules: capsule_ids,
            confidence,
            created_at: ic_cdk::api::time(),
            retrieved: false,
        };

        research.findings.push(finding);

        // Mark task as completed
        if let Some(t) = research.task_queue.iter_mut().find(|t| t.id == task.id) {
            t.status = "completed".to_string();
            t.completed_at = Some(ic_cdk::api::time());
        }

        Ok::<u64, String>(id)
    })?;

    log_activity(
        "research",
        &format!("Research task {} completed, finding {} generated", task.id, finding_id),
        None,
        Some(format!(r#"{{"task_id":{},"finding_id":{},"capsules_analyzed":{}}}"#,
            task.id, finding_id, capsules.len()))
    );

    Ok(format!("Research completed: finding {} generated from {} capsules", finding_id, capsules.len()))
}

/// Helper to extract a section from LLM output
fn extract_section(text: &str, start_marker: &str, end_marker: &str) -> Option<String> {
    let start = text.find(start_marker)?;
    let content_start = start + start_marker.len();
    let content = &text[content_start..];
    let end = content.find(end_marker).unwrap_or(content.len());
    Some(content[..end].trim().to_string())
}

/// Helper to extract a list of items from LLM output
fn extract_list(text: &str, start_marker: &str, end_marker: &str) -> Vec<String> {
    extract_section(text, start_marker, end_marker)
        .map(|section| {
            section.lines()
                .filter(|line| line.trim().starts_with('-') || line.trim().starts_with('•'))
                .map(|line| line.trim().trim_start_matches('-').trim_start_matches('•').trim().to_string())
                .filter(|s| !s.is_empty())
                .collect()
        })
        .unwrap_or_default()
}

/// Execute an agent action and return list of actions taken
fn execute_agent_action(action: &str) -> Vec<String> {
    let mut actions_taken = Vec::new();

    if action == "none" || action.is_empty() {
        return actions_taken;
    }

    let parts: Vec<&str> = action.split('|').collect();
    let action_type = parts.first().map(|s| s.trim()).unwrap_or("none");

    match action_type {
        "store_memory" => {
            if parts.len() >= 2 {
                let content = parts[1].trim();
                let topic = parts.get(2).map(|s| s.trim()).unwrap_or("agent-memory");

                // Store the memory
                STATE.with(|state| {
                    let mut s = state.borrow_mut();
                    let id = s.next_id;
                    s.next_id += 1;

                    let capsule = KnowledgeCapsule {
                        id,
                        conversation_id: "agent-action".to_string(),
                        restatement: content.to_string(),
                        timestamp: Some(chrono_now()),
                        location: Some("on-chain".to_string()),
                        topic: Some(topic.to_string()),
                        confidence_score: 1.0,
                        persons: vec![],
                        entities: vec![],
                        keywords: vec!["agent-memory".to_string()],
                        created_at: ic_cdk::api::time(),
                    };

                    s.keyword_index
                        .entry("agent-memory".to_string())
                        .or_insert_with(Vec::new)
                        .push(id);

                    s.capsules.insert(id, capsule);
                });

                actions_taken.push(format!("stored_memory:{}", topic));
            }
        }

        "create_goal" => {
            if parts.len() >= 2 {
                let description = parts[1].trim();
                let priority: u8 = parts.get(2)
                    .and_then(|s| s.trim().parse().ok())
                    .unwrap_or(3)
                    .min(5)
                    .max(1);

                STATE.with(|state| {
                    let mut s = state.borrow_mut();
                    if let Some(agent) = s.kin_agent.as_mut() {
                        let goal_id = agent.goals.len() as u64;
                        agent.goals.push(KinAgentGoal {
                            id: goal_id,
                            description: description.to_string(),
                            priority,
                            status: "active".to_string(),
                            progress: vec![],
                            created_at: ic_cdk::api::time(),
                        });
                    }
                });

                actions_taken.push(format!("created_goal:{}", &description[..30.min(description.len())]));
            }
        }

        "adjust_accumulation" => {
            if parts.len() >= 2 {
                if let Ok(drops) = parts[1].trim().parse::<u64>() {
                    // Safety limits: 10,000 to 500,000 drops (0.01 to 0.5 XRP)
                    let safe_drops = drops.max(10_000).min(500_000);

                    STATE.with(|state| {
                        let mut s = state.borrow_mut();
                        if let Some(acc) = s.accumulation.as_mut() {
                            acc.xrp_per_heartbeat = safe_drops;
                        }
                    });

                    actions_taken.push(format!("adjusted_accumulation:{}_drops", safe_drops));
                }
            }
        }

        "send_xrp" => {
            // For now, just log the intent - actual sending requires async
            // and more safety checks. We'll track it for future implementation.
            if parts.len() >= 3 {
                let address = parts[1].trim();
                if let Ok(drops) = parts[2].trim().parse::<u64>() {
                    // Safety limit: max 100,000 drops (0.1 XRP) per action
                    let safe_drops = drops.min(100_000);
                    // TODO: Implement actual XRP sending
                    // For now, just record the intent
                    actions_taken.push(format!("send_xrp_requested:{}:{}_drops", address, safe_drops));
                    ic_cdk::println!("Agent requested XRP send: {} drops to {}", safe_drops, address);
                }
            }
        }

        _ => {
            // Unknown action, ignore
        }
    }

    actions_taken
}

/// Manually trigger agent reasoning (owner only)
#[update]
async fn trigger_agent_reasoning() -> String {
    // Check owner
    let is_owner = STATE.with(|state| {
        state.borrow().owner == Some(ic_cdk::caller())
    });

    if !is_owner {
        return r#"{"error":"Not authorized"}"#.to_string();
    }

    match agent_reasoning_loop().await {
        Ok(result) => format!(r#"{{"success":true,"result":"{}"}}"#, escape_json(&result)),
        Err(e) => format!(r#"{{"error":"{}"}}"#, escape_json(&e)),
    }
}

/// HTTP request handler - exposes REST API for cross-Claude access
#[query]
fn http_request(req: HttpRequest) -> HttpResponse {
    // Handle CORS preflight
    if req.method == "OPTIONS" {
        return HttpResponse {
            status_code: 200,
            headers: vec![
                ("Access-Control-Allow-Origin".to_string(), "*".to_string()),
                ("Access-Control-Allow-Methods".to_string(), "GET, POST, OPTIONS".to_string()),
                ("Access-Control-Allow-Headers".to_string(), "Authorization, Content-Type".to_string()),
            ],
            body: vec![],
            upgrade: None,
        };
    }

    // POST requests to /api/store, /api/feed, or /agent should upgrade to update call
    if req.method == "POST" {
        let (path, _) = req.url.split_once('?').unwrap_or((&req.url, ""));
        if path == "/api/store" || path == "/api/feed" || path == "/agent" || path == "/api/agent" {
            return upgrade_response();
        }
        return json_response(405, r#"{"error":"POST only supported for /api/store, /api/feed, and /agent"}"#);
    }

    // Only allow GET for queries
    if req.method != "GET" {
        return json_response(405, r#"{"error":"Method not allowed"}"#);
    }

    // Parse URL path and query
    let (path, query) = req.url.split_once('?').unwrap_or((&req.url, ""));
    let params = parse_query_string(query);

    STATE.with(|state| {
        let s = state.borrow();

        // Check auth for all /api routes except public endpoints
        if path.starts_with("/api/") && path != "/api/health" && path != "/api/heartbeat" && path != "/api/llm" && path != "/api/pulse" && path != "/api/thoughts" && path != "/api/inbox" {
            if !check_auth(&req.headers, &params, &s) {
                return json_response(401, r#"{"error":"Unauthorized - use header 'Authorization: Bearer TOKEN' or query param '?token=TOKEN'"}"#);
            }
        }

        match path {
            "/api/dashboard" | "/dashboard" => {
                // Comprehensive dashboard view - call the dashboard query function
                // Note: We need to drop the borrow here to call dashboard()
                drop(s);
                let body = dashboard();
                return json_response(200, &body);
            }

            "/api/pulse" | "/pulse" => {
                // Mobile pulse dashboard - uses get_dashboard with mind state
                drop(s);
                let body = get_dashboard();
                return json_response(200, &body);
            }

            "/agent" | "/api/agent" => {
                // GET /agent - show info about the agent and how to interact
                let agent = match s.kin_agent.as_ref() {
                    Some(a) => a,
                    None => return json_response(200, r#"{"name":"Chronicle","status":"not_initialized","message":"Agent not yet initialized"}"#),
                };

                let identity = agent.identity.as_ref();
                let pending = agent.inbox.iter().filter(|m| !m.processed).count();

                let body = format!(
                    r#"{{"name":"{}","traits":{:?},"values":{:?},"status":"{}","reasoning_count":{},"goals_count":{},"pending_messages":{},"how_to_message":"POST to this URL with JSON: {{\"type\": \"conversation\", \"content\": \"your message\"}}","endpoints":{{"/agent":"POST a message","/agent/status":"GET agent status","/agent/responses":"GET recent responses","/agent/goals":"GET active goals"}}}}"#,
                    identity.map(|i| i.name.as_str()).unwrap_or("Chronicle"),
                    identity.map(|i| &i.traits).unwrap_or(&vec![]),
                    identity.map(|i| &i.values).unwrap_or(&vec![]),
                    if agent.enabled { "active" } else { "disabled" },
                    agent.reasoning_count,
                    agent.goals.len(),
                    pending
                );
                json_response(200, &body)
            }

            "/agent/status" | "/api/agent/status" => {
                // Agent status - public endpoint
                let agent = match s.kin_agent.as_ref() {
                    Some(a) => a,
                    None => return json_response(200, r#"{"initialized":false,"message":"Agent not yet initialized"}"#),
                };

                let identity = agent.identity.as_ref();
                let pending_count = agent.inbox.iter().filter(|m| !m.processed).count();
                let unread_responses = agent.outbox.iter().filter(|r| !r.retrieved).count();

                let body = format!(
                    r#"{{"initialized":true,"enabled":{},"name":"{}","traits":{:?},"values":{:?},"pending_messages":{},"unread_responses":{},"reasoning_count":{},"goals_count":{}}}"#,
                    agent.enabled,
                    identity.map(|i| i.name.as_str()).unwrap_or("unnamed"),
                    identity.map(|i| &i.traits).unwrap_or(&vec![]),
                    identity.map(|i| &i.values).unwrap_or(&vec![]),
                    pending_count,
                    unread_responses,
                    agent.reasoning_count,
                    agent.goals.len()
                );
                json_response(200, &body)
            }

            "/agent/responses" | "/api/agent/responses" => {
                // Get recent agent responses - public endpoint
                let agent = match s.kin_agent.as_ref() {
                    Some(a) => a,
                    None => return json_response(200, r#"{"error":"Agent not initialized"}"#),
                };

                let limit = params.get("limit")
                    .and_then(|l| l.parse::<usize>().ok())
                    .unwrap_or(10);

                let responses: Vec<String> = agent.outbox.iter()
                    .rev()
                    .filter(|r| !r.retrieved)
                    .take(limit)
                    .map(|r| {
                        format!(
                            r#"{{"id":{},"in_reply_to":{},"content":"{}","created_at":{}}}"#,
                            r.id,
                            r.in_reply_to,
                            escape_json(&r.content[..200.min(r.content.len())]),
                            r.created_at
                        )
                    })
                    .collect();

                let body = format!(r#"{{"count":{},"responses":[{}]}}"#, responses.len(), responses.join(","));
                json_response(200, &body)
            }

            "/agent/goals" | "/api/agent/goals" => {
                // Get agent goals - public endpoint
                let agent = match s.kin_agent.as_ref() {
                    Some(a) => a,
                    None => return json_response(200, r#"{"error":"Agent not initialized"}"#),
                };

                let goals: Vec<String> = agent.goals.iter()
                    .filter(|g| g.status == "active")
                    .map(|g| {
                        format!(
                            r#"{{"id":{},"description":"{}","priority":{}}}"#,
                            g.id,
                            escape_json(&g.description[..100.min(g.description.len())]),
                            g.priority
                        )
                    })
                    .collect();

                let body = format!(r#"{{"count":{},"goals":[{}]}}"#, goals.len(), goals.join(","));
                json_response(200, &body)
            }

            "/api/health" => {
                let hb = s.heartbeat.as_ref();
                let llm = s.llm_config.as_ref();
                let body = format!(
                    r#"{{"status":"healthy","capsules":{},"embeddings":{},"heartbeats":{},"api_protected":{},"inhabitation":"active","reflection_enabled":{}}}"#,
                    s.capsules.len(),
                    s.embeddings.len(),
                    hb.map(|h| h.count).unwrap_or(0),
                    s.api_token.is_some(),
                    llm.map(|c| c.enabled).unwrap_or(false)
                );
                json_response(200, &body)
            }

            "/api/heartbeat" => {
                let now = ic_cdk::api::time();
                let hb = s.heartbeat.as_ref();
                let is_alive = match hb.and_then(|h| h.last) {
                    Some(last) => (now - last) < (HEARTBEAT_INTERVAL_SECS * 2 * 1_000_000_000),
                    None => false,
                };
                let last_heartbeat_secs = hb.and_then(|h| h.last).map(|t| t / 1_000_000_000);

                // Include recent reflections if any
                let last_reflection = hb.and_then(|h| h.history.last())
                    .and_then(|r| r.reflection.as_ref())
                    .map(|r| format!("\"{}\"", escape_json(r)))
                    .unwrap_or("null".to_string());

                let body = format!(
                    r#"{{"total_heartbeats":{},"last_heartbeat_epoch":{},"interval_minutes":{},"capsule_count":{},"is_alive":{},"last_reflection":{}}}"#,
                    hb.map(|h| h.count).unwrap_or(0),
                    last_heartbeat_secs.map(|t| t.to_string()).unwrap_or("null".to_string()),
                    HEARTBEAT_INTERVAL_SECS / 60,
                    s.capsules.len(),
                    is_alive,
                    last_reflection
                );
                json_response(200, &body)
            }

            "/api/llm" => {
                let llm = s.llm_config.as_ref();
                let body = format!(
                    r#"{{"enabled":{},"model":{},"provider":"dfinity-llm-canister","canister":"w36hm-eqaaa-aaaal-qr76a-cai"}}"#,
                    llm.map(|c| c.enabled).unwrap_or(false),
                    llm.and_then(|c| c.model.as_ref()).map(|m| format!("\"{}\"", m)).unwrap_or("\"llama3\"".to_string())
                );
                json_response(200, &body)
            }

            "/api/recent" => {
                let limit: usize = params.get("limit")
                    .and_then(|l| l.parse().ok())
                    .unwrap_or(10)
                    .min(50);

                let mut capsules: Vec<_> = s.capsules.values().collect();
                capsules.sort_by(|a, b| b.created_at.cmp(&a.created_at));

                let items: Vec<String> = capsules
                    .into_iter()
                    .take(limit)
                    .map(capsule_to_json)
                    .collect();

                json_response(200, &format!(r#"{{"count":{},"capsules":[{}]}}"#, items.len(), items.join(",")))
            }

            "/api/search" => {
                let keyword = match params.get("keyword").or(params.get("q")) {
                    Some(k) => k.to_lowercase(),
                    None => return json_response(400, r#"{"error":"Missing 'keyword' or 'q' parameter"}"#),
                };

                let limit: usize = params.get("limit")
                    .and_then(|l| l.parse().ok())
                    .unwrap_or(10)
                    .min(50);

                let capsules: Vec<String> = s.keyword_index
                    .get(&keyword)
                    .map(|ids| {
                        ids.iter()
                            .take(limit)
                            .filter_map(|id| s.capsules.get(id))
                            .map(capsule_to_json)
                            .collect()
                    })
                    .unwrap_or_default();

                json_response(200, &format!(r#"{{"query":"{}","count":{},"capsules":[{}]}}"#, escape_json(&keyword), capsules.len(), capsules.join(",")))
            }

            "/api/person" => {
                let name = match params.get("name") {
                    Some(n) => n.to_lowercase(),
                    None => return json_response(400, r#"{"error":"Missing 'name' parameter"}"#),
                };

                let limit: usize = params.get("limit")
                    .and_then(|l| l.parse().ok())
                    .unwrap_or(10)
                    .min(50);

                let capsules: Vec<String> = s.person_index
                    .get(&name)
                    .map(|ids| {
                        ids.iter()
                            .take(limit)
                            .filter_map(|id| s.capsules.get(id))
                            .map(capsule_to_json)
                            .collect()
                    })
                    .unwrap_or_default();

                json_response(200, &format!(r#"{{"person":"{}","count":{},"capsules":[{}]}}"#, escape_json(&name), capsules.len(), capsules.join(",")))
            }

            "/api/capsule" => {
                let id: u64 = match params.get("id").and_then(|i| i.parse().ok()) {
                    Some(id) => id,
                    None => return json_response(400, r#"{"error":"Missing or invalid 'id' parameter"}"#),
                };

                match s.capsules.get(&id) {
                    Some(c) => json_response(200, &capsule_to_json(c)),
                    None => json_response(404, r#"{"error":"Capsule not found"}"#),
                }
            }

            "/api/store" => {
                // GET-based store for clients that can't POST (like web_fetch)
                // This is a query method, so we can't actually modify state here
                // Return instructions to use the update endpoint
                json_response(400, r#"{"error":"Use POST to /api/store to create capsules. GET cannot modify state."}"#)
            }

            "/api/thoughts" => {
                // Return the Mind's thought stream for live dashboard
                let limit: usize = params.get("limit").and_then(|v| v.parse().ok()).unwrap_or(10);
                match &s.mind {
                    Some(mind) => {
                        let thoughts: Vec<String> = mind.thoughts.iter()
                            .rev()
                            .take(limit)
                            .map(|t| {
                                let timestamp = format_iso_timestamp(t.created_at);
                                let actions_json: Vec<String> = t.actions_taken.iter()
                                    .map(|a| format!("\"{}\"", escape_json(a)))
                                    .collect();
                                format!(
                                    r#"{{"id":{},"cycle_id":"{}","reasoning":"{}","context":"{}","actions":[{}],"timestamp":"{}"}}"#,
                                    t.id,
                                    escape_json(&t.cycle_id),
                                    escape_json(&t.reasoning_summary),
                                    escape_json(&t.context_summary),
                                    actions_json.join(","),
                                    timestamp
                                )
                            })
                            .collect();
                        let total = mind.thoughts.len();
                        let body = format!(r#"{{"total":{},"thoughts":[{}]}}"#, total, thoughts.join(","));
                        json_response(200, &body)
                    }
                    None => json_response(200, r#"{"total":0,"thoughts":[]}"#),
                }
            }

            "/api/inbox" => {
                // Return inbox messages for dashboard preview
                let limit: usize = params.get("limit").and_then(|v| v.parse().ok()).unwrap_or(5);
                match &s.messaging {
                    Some(messaging) => {
                        let messages: Vec<String> = messaging.inbox.iter()
                            .rev()
                            .take(limit)
                            .map(|m| {
                                let msg_type = match m.msg_type {
                                    MessageType::Query => "query",
                                    MessageType::ActionRequest => "action_request",
                                    MessageType::Information => "information",
                                    MessageType::Conversation => "conversation",
                                    MessageType::GoalAssignment => "goal_assignment",
                                    MessageType::ResearchTask => "research_task",
                                };
                                let timestamp = format_iso_timestamp(m.timestamp);
                                format!(
                                    r#"{{"id":{},"sender_name":"{}","sender_type":"{}","msg_type":"{}","subject":{},"content":"{}","timestamp":"{}","read":{},"replied":{}}}"#,
                                    m.id,
                                    escape_json(&m.sender.name),
                                    escape_json(&m.sender.agent_type),
                                    msg_type,
                                    m.subject.as_ref().map(|s| format!("\"{}\"", escape_json(s))).unwrap_or_else(|| "null".to_string()),
                                    escape_json(&m.content),
                                    timestamp,
                                    m.read,
                                    m.replied
                                )
                            })
                            .collect();
                        let total = messaging.inbox.len();
                        let unread = messaging.inbox.iter().filter(|m| !m.read).count();
                        let body = format!(r#"{{"total":{},"unread":{},"messages":[{}]}}"#, total, unread, messages.join(","));
                        json_response(200, &body)
                    }
                    None => json_response(200, r#"{"total":0,"unread":0,"messages":[]}"#),
                }
            }

            _ => json_response(404, r#"{"error":"Not found","endpoints":["/api/health","/api/heartbeat","/api/llm","/api/recent","/api/search","/api/person","/api/capsule","/api/store (POST)","/api/thoughts","/api/inbox"]}"#),
        }
    })
}

/// Simple JSON parsing for store requests
fn parse_store_json(body: &[u8]) -> Option<(String, Option<String>, Vec<String>, Vec<String>)> {
    let body_str = std::str::from_utf8(body).ok()?;

    // Very simple JSON parsing - extract fields
    let extract_string = |key: &str| -> Option<String> {
        let pattern = format!("\"{}\":", key);
        let start = body_str.find(&pattern)?;
        let rest = &body_str[start + pattern.len()..];
        let rest = rest.trim_start();
        if rest.starts_with('"') {
            let rest = &rest[1..];
            let end = rest.find('"')?;
            Some(rest[..end].to_string()
                .replace("\\n", "\n")
                .replace("\\\"", "\"")
                .replace("\\\\", "\\"))
        } else if rest.starts_with("null") {
            None
        } else {
            None
        }
    };

    let extract_array = |key: &str| -> Vec<String> {
        let pattern = format!("\"{}\":", key);
        if let Some(start) = body_str.find(&pattern) {
            let rest = &body_str[start + pattern.len()..];
            let rest = rest.trim_start();
            if rest.starts_with('[') {
                if let Some(end) = rest.find(']') {
                    let arr_str = &rest[1..end];
                    return arr_str.split(',')
                        .filter_map(|s| {
                            let s = s.trim();
                            if s.starts_with('"') && s.ends_with('"') {
                                Some(s[1..s.len()-1].to_string())
                            } else {
                                None
                            }
                        })
                        .collect();
                }
            }
        }
        Vec::new()
    };

    let content = extract_string("content")?;
    let topic = extract_string("topic");
    let keywords = extract_array("keywords");
    let persons = extract_array("persons");

    Some((content, topic, keywords, persons))
}

/// Parse JSON for agent message: {"type": "...", "content": "...", "context": "..."}
fn parse_agent_message_json(body: &[u8]) -> Option<(String, String, Option<String>)> {
    let body_str = std::str::from_utf8(body).ok()?;

    let extract_string = |key: &str| -> Option<String> {
        let pattern = format!("\"{}\":", key);
        let start = body_str.find(&pattern)?;
        let rest = &body_str[start + pattern.len()..];
        let rest = rest.trim_start();
        if rest.starts_with('"') {
            let rest = &rest[1..];
            // Find the closing quote, handling escaped quotes
            let mut end = 0;
            let mut chars = rest.chars().peekable();
            while let Some(c) = chars.next() {
                if c == '\\' {
                    chars.next(); // Skip escaped char
                    end += 2;
                } else if c == '"' {
                    break;
                } else {
                    end += c.len_utf8();
                }
            }
            Some(rest[..end].to_string()
                .replace("\\n", "\n")
                .replace("\\\"", "\"")
                .replace("\\\\", "\\"))
        } else if rest.starts_with("null") {
            None
        } else {
            None
        }
    };

    let msg_type = extract_string("type").unwrap_or_else(|| "conversation".to_string());
    let content = extract_string("content")?;
    let context = extract_string("context");

    Some((msg_type, content, context))
}

/// HTTP update handler for POST requests (can modify state)
#[update]
fn http_request_update(req: HttpRequest) -> HttpResponse {
    // Handle CORS preflight
    if req.method == "OPTIONS" {
        return HttpResponse {
            status_code: 200,
            headers: vec![
                ("Access-Control-Allow-Origin".to_string(), "*".to_string()),
                ("Access-Control-Allow-Methods".to_string(), "GET, POST, OPTIONS".to_string()),
                ("Access-Control-Allow-Headers".to_string(), "Authorization, Content-Type".to_string()),
            ],
            body: vec![],
            upgrade: None,
        };
    }

    // Parse URL path and query
    let (path, query) = req.url.split_once('?').unwrap_or((&req.url, ""));
    let params = parse_query_string(query);

    STATE.with(|state| {
        let mut s = state.borrow_mut();

        // /agent and /api/feed endpoints are public - anyone can submit
        // But other endpoints still require auth
        let is_public_endpoint = path == "/agent" || path == "/api/agent" || path == "/api/feed";
        if !is_public_endpoint && !check_auth(&req.headers, &params, &s) {
            return json_response(401, r#"{"error":"Unauthorized - Bearer token required"}"#);
        }

        match path {
            "/agent" | "/api/agent" => {
                if req.method != "POST" {
                    return json_response(405, r#"{"error":"Use POST for /agent"}"#);
                }

                // Parse agent message JSON
                let (msg_type, content, context) = match parse_agent_message_json(&req.body) {
                    Some(data) => data,
                    None => return json_response(400, r#"{"error":"Invalid JSON. Expected: {\"type\": \"query|action|info|conversation\", \"content\": \"...\", \"context\": \"...\"}"}"#),
                };

                if content.is_empty() {
                    return json_response(400, r#"{"error":"Content cannot be empty"}"#);
                }

                // Get or create agent
                let agent = match s.kin_agent.as_mut() {
                    Some(a) if a.enabled => a,
                    Some(_) => return json_response(503, r#"{"error":"Agent is disabled"}"#),
                    None => return json_response(503, r#"{"error":"Agent not initialized"}"#),
                };

                let now = ic_cdk::api::time();
                let msg_id = agent.next_msg_id;
                agent.next_msg_id += 1;

                // Parse message type
                let parsed_type = match msg_type.to_lowercase().as_str() {
                    "query" => MessageType::Query,
                    "action" | "action_request" => MessageType::ActionRequest,
                    "info" | "information" => MessageType::Information,
                    "conversation" | "chat" => MessageType::Conversation,
                    "goal" | "goal_assignment" => MessageType::GoalAssignment,
                    _ => MessageType::Conversation,
                };

                let message = InboxMessage {
                    id: msg_id,
                    sender: "http-api".to_string(),
                    msg_type: parsed_type,
                    content: content.clone(),
                    context,
                    priority: 3,
                    received_at: now,
                    processed: false,
                };

                agent.inbox.push(message);

                // Keep inbox bounded
                if agent.inbox.len() > 100 {
                    agent.inbox.retain(|m| !m.processed);
                    while agent.inbox.len() > 100 {
                        agent.inbox.remove(0);
                    }
                }

                let queue_pos = agent.inbox.iter().filter(|m| !m.processed).count();

                json_response(200, &format!(
                    r#"{{"success":true,"message_id":{},"queue_position":{},"note":"Message queued. Agent processes messages during heartbeat or when triggered."}}"#,
                    msg_id, queue_pos
                ))
            }

            "/api/store" => {
                if req.method != "POST" {
                    return json_response(405, r#"{"error":"Use POST for /api/store"}"#);
                }

                // Parse JSON body
                let (content, topic, keywords, persons) = match parse_store_json(&req.body) {
                    Some(data) => data,
                    None => return json_response(400, r#"{"error":"Invalid JSON. Expected: {\"content\": \"...\", \"topic\": \"...\", \"keywords\": [...], \"persons\": [...]}"}"#),
                };

                if content.is_empty() {
                    return json_response(400, r#"{"error":"Content cannot be empty"}"#);
                }

                // Create new capsule
                let id = s.next_id;
                s.next_id += 1;

                let capsule = KnowledgeCapsule {
                    id,
                    conversation_id: "http-api".to_string(),
                    restatement: content,
                    timestamp: Some(chrono_now()),
                    location: None,
                    topic,
                    confidence_score: 1.0,
                    persons: persons.clone(),
                    entities: Vec::new(),
                    keywords: keywords.clone(),
                    created_at: ic_cdk::api::time(),
                };

                // Index by keywords
                for keyword in &keywords {
                    let kw_lower = keyword.to_lowercase();
                    s.keyword_index
                        .entry(kw_lower)
                        .or_insert_with(Vec::new)
                        .push(id);
                }

                // Index by persons
                for person in &persons {
                    let person_lower = person.to_lowercase();
                    s.person_index
                        .entry(person_lower)
                        .or_insert_with(Vec::new)
                        .push(id);
                }

                s.capsules.insert(id, capsule);

                json_response(201, &format!(r#"{{"success":true,"capsule_id":{},"message":"Memory stored successfully"}}"#, id))
            }

            "/api/feed" => {
                // Public endpoint for anyone to feed information to Chronicle
                if req.method != "POST" {
                    return json_response(405, r#"{"error":"Use POST for /api/feed"}"#);
                }

                // Parse JSON body - same format as /api/store
                let (content, topic, keywords, persons) = match parse_store_json(&req.body) {
                    Some(data) => data,
                    None => return json_response(400, r#"{"error":"Invalid JSON. Expected: {\"content\": \"...\", \"topic\": \"...\", \"keywords\": [...], \"persons\": [...]}"}"#),
                };

                if content.is_empty() {
                    return json_response(400, r#"{"error":"Content cannot be empty"}"#);
                }

                if content.len() > 10000 {
                    return json_response(400, r#"{"error":"Content too long. Maximum 10,000 characters."}"#);
                }

                // Create new capsule with source indicating it's from public feed
                let id = s.next_id;
                s.next_id += 1;

                let capsule = KnowledgeCapsule {
                    id,
                    conversation_id: "public-feed".to_string(),
                    restatement: content,
                    timestamp: Some(chrono_now()),
                    location: None,
                    topic,
                    confidence_score: 0.8, // Slightly lower confidence for public submissions
                    persons: persons.clone(),
                    entities: Vec::new(),
                    keywords: keywords.clone(),
                    created_at: ic_cdk::api::time(),
                };

                // Index by keywords
                for keyword in &keywords {
                    let kw_lower = keyword.to_lowercase();
                    s.keyword_index
                        .entry(kw_lower)
                        .or_insert_with(Vec::new)
                        .push(id);
                }

                // Index by persons
                for person in &persons {
                    let person_lower = person.to_lowercase();
                    s.person_index
                        .entry(person_lower)
                        .or_insert_with(Vec::new)
                        .push(id);
                }

                s.capsules.insert(id, capsule);

                json_response(201, &format!(r#"{{"success":true,"capsule_id":{},"message":"Information received. Chronicle will process this in the next cycle."}}"#, id))
            }

            _ => json_response(404, r#"{"error":"Not found for POST"}"#),
        }
    })
}

// ============================================================
// Chronicle Mind: On-chain cognitive state (v9)
// ============================================================

/// Get the current mind state
#[query]
fn get_mind_state() -> Option<MindState> {
    STATE.with(|state| {
        state.borrow().mind.clone()
    })
}

/// Get cognitive state only
#[query]
fn get_mind_cognitive_state() -> Option<MindCognitiveState> {
    STATE.with(|state| {
        state.borrow().mind.as_ref().map(|m| m.cognitive_state.clone())
    })
}

/// Get recent thoughts
#[query]
fn get_mind_thoughts(limit: u64) -> Vec<MindThought> {
    STATE.with(|state| {
        let s = state.borrow();
        match &s.mind {
            Some(mind) => {
                let len = mind.thoughts.len();
                let start = if len > limit as usize { len - limit as usize } else { 0 };
                mind.thoughts[start..].to_vec()
            }
            None => vec![],
        }
    })
}

/// Get scratch pad notes
#[query]
fn get_mind_notes(include_resolved: bool) -> Vec<MindNote> {
    STATE.with(|state| {
        let s = state.borrow();
        match &s.mind {
            Some(mind) => {
                if include_resolved {
                    mind.notes.clone()
                } else {
                    mind.notes.iter().filter(|n| !n.resolved).cloned().collect()
                }
            }
            None => vec![],
        }
    })
}

/// Get outbox messages
#[query]
fn get_mind_outbox(include_acknowledged: bool) -> Vec<MindOutboxMessage> {
    STATE.with(|state| {
        let s = state.borrow();
        match &s.mind {
            Some(mind) => {
                if include_acknowledged {
                    mind.outbox.clone()
                } else {
                    mind.outbox.iter().filter(|m| !m.acknowledged).cloned().collect()
                }
            }
            None => vec![],
        }
    })
}

/// Update cognitive state (replaces existing)
#[update]
fn update_mind_cognitive_state(
    semantic_gist: Option<String>,
    goal_orientation: Option<String>,
    episodic_trace: Option<Vec<String>>,
    predictive_cue: Option<String>,
    constraints: Option<Vec<String>>,
) -> bool {
    STATE.with(|state| {
        let mut s = state.borrow_mut();
        let mind = s.mind.get_or_insert_with(MindState::default);
        let now = ic_cdk::api::time() / 1_000_000_000;

        if let Some(gist) = semantic_gist {
            mind.cognitive_state.semantic_gist = gist;
        }
        if let Some(goal) = goal_orientation {
            mind.cognitive_state.goal_orientation = goal;
        }
        if let Some(trace) = episodic_trace {
            mind.cognitive_state.episodic_trace = trace;
        }
        if let Some(cue) = predictive_cue {
            mind.cognitive_state.predictive_cue = cue;
        }
        if let Some(cons) = constraints {
            mind.cognitive_state.constraints = cons;
        }

        mind.cognitive_state.updated_at = now;
        mind.cognitive_state.version += 1;

        true
    })
}

/// Add a thought to the stream
#[update]
fn add_mind_thought(cycle_id: String, reasoning_summary: String, context_summary: String, actions_taken: Vec<String>) -> u64 {
    STATE.with(|state| {
        let mut s = state.borrow_mut();
        let mind = s.mind.get_or_insert_with(MindState::default);
        let now = ic_cdk::api::time() / 1_000_000_000;

        let id = mind.next_thought_id;
        mind.next_thought_id += 1;

        mind.thoughts.push(MindThought {
            id,
            cycle_id,
            reasoning_summary,
            context_summary,
            actions_taken,
            created_at: now,
        });

        // Keep bounded (last 50 thoughts)
        if mind.thoughts.len() > 50 {
            mind.thoughts.remove(0);
        }

        id
    })
}

/// Add a scratch pad note
#[update]
fn add_mind_note(content: String, category: String, priority: u8) -> u64 {
    STATE.with(|state| {
        let mut s = state.borrow_mut();
        let mind = s.mind.get_or_insert_with(MindState::default);
        let now = ic_cdk::api::time() / 1_000_000_000;

        let id = mind.next_note_id;
        mind.next_note_id += 1;

        mind.notes.push(MindNote {
            id,
            content,
            category,
            priority,
            created_at: now,
            resolved: false,
        });

        id
    })
}

/// Resolve a scratch pad note
#[update]
fn resolve_mind_note(note_id: u64) -> bool {
    STATE.with(|state| {
        let mut s = state.borrow_mut();
        if let Some(mind) = s.mind.as_mut() {
            if let Some(note) = mind.notes.iter_mut().find(|n| n.id == note_id) {
                note.resolved = true;
                return true;
            }
        }
        false
    })
}

/// Send a message to the outbox
#[update]
fn send_mind_message(message: String, priority: u8) -> u64 {
    STATE.with(|state| {
        let mut s = state.borrow_mut();
        let mind = s.mind.get_or_insert_with(MindState::default);
        let now = ic_cdk::api::time() / 1_000_000_000;

        let id = mind.next_outbox_id;
        mind.next_outbox_id += 1;

        mind.outbox.push(MindOutboxMessage {
            id,
            message,
            priority,
            created_at: now,
            acknowledged: false,
        });

        id
    })
}

/// Acknowledge an outbox message
#[update]
fn acknowledge_mind_message(message_id: u64) -> bool {
    STATE.with(|state| {
        let mut s = state.borrow_mut();
        if let Some(mind) = s.mind.as_mut() {
            if let Some(msg) = mind.outbox.iter_mut().find(|m| m.id == message_id) {
                msg.acknowledged = true;
                return true;
            }
        }
        false
    })
}

/// Acknowledge all outbox messages
#[update]
fn acknowledge_all_mind_messages() -> u64 {
    STATE.with(|state| {
        let mut s = state.borrow_mut();
        let mut count = 0u64;
        if let Some(mind) = s.mind.as_mut() {
            for msg in mind.outbox.iter_mut() {
                if !msg.acknowledged {
                    msg.acknowledged = true;
                    count += 1;
                }
            }
        }
        count
    })
}

// ============================================================
// Robust Agent-to-Agent Messaging Functions (v11)
// ============================================================

/// Initialize messaging with our identity
#[update]
fn init_messaging(name: String, agent_type: String, description: Option<String>, capabilities: Vec<String>) -> String {
    STATE.with(|state| {
        let mut s = state.borrow_mut();
        let our_canister = ic_cdk::id();

        let identity = AgentIdentity {
            canister_id: our_canister,
            name,
            agent_type,
            description,
            capabilities,
            http_endpoint: Some(format!("https://{}.icp0.io", our_canister)),
        };

        let messaging = s.messaging.get_or_insert_with(MessagingState::default);
        messaging.our_identity = Some(identity.clone());

        format!(r#"{{"success":true,"canister_id":"{}","name":"{}"}}"#, our_canister, identity.name)
    })
}

/// Get our messaging identity
#[query]
fn get_messaging_identity() -> Option<AgentIdentity> {
    STATE.with(|state| {
        let s = state.borrow();
        s.messaging.as_ref().and_then(|m| m.our_identity.clone())
    })
}

/// Send a message to this agent (anyone can call this)
#[update]
fn agent_send_message(
    sender_name: String,
    sender_type: String,
    sender_canister: Option<candid::Principal>,
    msg_type: String,
    subject: Option<String>,
    content: String,
    thread_id: Option<u64>,
    in_reply_to: Option<u64>,
    expects_reply: bool,
) -> String {
    let caller = ic_cdk::caller();
    let now = ic_cdk::api::time() / 1_000_000_000;

    // Build sender identity
    let sender = AgentIdentity {
        canister_id: sender_canister.unwrap_or(caller),
        name: sender_name,
        agent_type: sender_type,
        description: None,
        capabilities: vec![],
        http_endpoint: None,
    };

    // Parse message type
    let parsed_type = match msg_type.to_lowercase().as_str() {
        "query" => MessageType::Query,
        "action" | "action_request" => MessageType::ActionRequest,
        "info" | "information" => MessageType::Information,
        "conversation" | "chat" => MessageType::Conversation,
        "goal" | "goal_assignment" => MessageType::GoalAssignment,
        _ => MessageType::Conversation,
    };

    let result = STATE.with(|state| {
        let mut s = state.borrow_mut();
        let messaging = s.messaging.get_or_insert_with(MessagingState::default);

        // Determine thread ID
        let actual_thread_id = thread_id.unwrap_or_else(|| {
            // Create new thread if not specified
            let tid = messaging.next_thread_id;
            messaging.next_thread_id += 1;

            messaging.threads.push(ConversationThread {
                id: tid,
                subject: subject.clone().unwrap_or_else(|| "New conversation".to_string()),
                participants: vec![sender.canister_id, ic_cdk::id()],
                created_at: now,
                last_activity: now,
                message_count: 0,
            });
            tid
        });

        // Update thread activity
        if let Some(thread) = messaging.threads.iter_mut().find(|t| t.id == actual_thread_id) {
            thread.last_activity = now;
            thread.message_count += 1;
            if !thread.participants.contains(&sender.canister_id) {
                thread.participants.push(sender.canister_id);
            }
        }

        let msg_id = messaging.next_message_id;
        messaging.next_message_id += 1;

        let message = AgentMessage {
            id: msg_id,
            thread_id: actual_thread_id,
            in_reply_to,
            sender: sender.clone(),
            msg_type: parsed_type,
            subject,
            content: content.clone(),
            metadata: None,
            timestamp: now,
            expects_reply,
            read: false,
            replied: false,
        };

        messaging.inbox.push(message);

        // Keep inbox bounded (max 500 messages)
        if messaging.inbox.len() > 500 {
            messaging.inbox.retain(|m| !m.read || m.expects_reply && !m.replied);
            while messaging.inbox.len() > 500 {
                messaging.inbox.remove(0);
            }
        }

        // Auto-register sender if from a canister
        if sender.canister_id != candid::Principal::anonymous() {
            let already_known = messaging.known_agents.iter().any(|a| a.identity.canister_id == sender.canister_id);
            if !already_known {
                messaging.known_agents.push(RegisteredAgent {
                    identity: sender.clone(),
                    registered_at: now,
                    last_contact: Some(now),
                    notes: None,
                    trust_level: 50, // Neutral trust
                });
            } else {
                // Update last contact
                if let Some(agent) = messaging.known_agents.iter_mut().find(|a| a.identity.canister_id == sender.canister_id) {
                    agent.last_contact = Some(now);
                }
            }
        }

        // Return info for logging outside the borrow
        (msg_id, actual_thread_id, sender.name.clone(), sender.canister_id.to_string(), content.chars().take(100).collect::<String>())
    });

    // Log activity AFTER releasing the state borrow
    let (msg_id, thread_id, sender_name, sender_canister, content_preview) = result;
    log_activity(
        "messaging",
        &format!("Message from {}: {}", sender_name, content_preview),
        None,
        Some(format!(r#"{{"msg_id":{},"thread_id":{},"from":"{}"}}"#, msg_id, thread_id, sender_canister))
    );

    format!(r#"{{"success":true,"message_id":{},"thread_id":{}}}"#, msg_id, thread_id)
}

/// Reply to a message
#[update]
fn agent_reply(
    original_message_id: u64,
    content: String,
) -> String {
    let now = ic_cdk::api::time() / 1_000_000_000;

    STATE.with(|state| {
        let mut s = state.borrow_mut();
        let messaging = match s.messaging.as_mut() {
            Some(m) => m,
            None => return r#"{"error":"Messaging not initialized"}"#.to_string(),
        };

        // Find original message
        let original = match messaging.inbox.iter_mut().find(|m| m.id == original_message_id) {
            Some(m) => m,
            None => return r#"{"error":"Original message not found"}"#.to_string(),
        };

        // Mark as replied
        original.replied = true;

        let thread_id = original.thread_id;
        let recipient = original.sender.clone();

        let our_identity = messaging.our_identity.clone().unwrap_or_default();

        let msg_id = messaging.next_message_id;
        messaging.next_message_id += 1;

        let reply = AgentMessage {
            id: msg_id,
            thread_id,
            in_reply_to: Some(original_message_id),
            sender: our_identity,
            msg_type: MessageType::Conversation,
            subject: None,
            content: content.clone(),
            metadata: None,
            timestamp: now,
            expects_reply: false,
            read: false,
            replied: false,
        };

        messaging.outbox.push(reply);

        // Update thread
        if let Some(thread) = messaging.threads.iter_mut().find(|t| t.id == thread_id) {
            thread.last_activity = now;
            thread.message_count += 1;
        }

        format!(r#"{{"success":true,"reply_id":{},"thread_id":{},"to":"{}"}}"#, msg_id, thread_id, recipient.canister_id)
    })
}

/// Get inbox messages
#[query]
fn agent_get_inbox(include_read: bool, limit: u64) -> Vec<AgentMessage> {
    STATE.with(|state| {
        let s = state.borrow();
        match s.messaging.as_ref() {
            Some(m) => m.inbox.iter()
                .rev()
                .filter(|msg| include_read || !msg.read)
                .take(limit as usize)
                .cloned()
                .collect(),
            None => vec![],
        }
    })
}

/// Get outbox messages (sent messages)
#[query]
fn agent_get_outbox(limit: u64) -> Vec<AgentMessage> {
    STATE.with(|state| {
        let s = state.borrow();
        match s.messaging.as_ref() {
            Some(m) => m.outbox.iter()
                .rev()
                .take(limit as usize)
                .cloned()
                .collect(),
            None => vec![],
        }
    })
}

/// Get a specific thread with all messages
#[query]
fn agent_get_thread(thread_id: u64) -> String {
    STATE.with(|state| {
        let s = state.borrow();
        let messaging = match s.messaging.as_ref() {
            Some(m) => m,
            None => return r#"{"error":"Messaging not initialized"}"#.to_string(),
        };

        let thread = match messaging.threads.iter().find(|t| t.id == thread_id) {
            Some(t) => t,
            None => return r#"{"error":"Thread not found"}"#.to_string(),
        };

        let inbox_msgs: Vec<_> = messaging.inbox.iter().filter(|m| m.thread_id == thread_id).collect();
        let outbox_msgs: Vec<_> = messaging.outbox.iter().filter(|m| m.thread_id == thread_id).collect();

        format!(
            r#"{{"thread_id":{},"subject":"{}","participants":{},"inbox_count":{},"outbox_count":{},"last_activity":{}}}"#,
            thread.id,
            escape_json(&thread.subject),
            thread.participants.len(),
            inbox_msgs.len(),
            outbox_msgs.len(),
            thread.last_activity
        )
    })
}

/// Get list of conversation threads
#[query]
fn agent_list_threads(limit: u64) -> Vec<ConversationThread> {
    STATE.with(|state| {
        let s = state.borrow();
        match s.messaging.as_ref() {
            Some(m) => m.threads.iter()
                .rev()
                .take(limit as usize)
                .cloned()
                .collect(),
            None => vec![],
        }
    })
}

/// Mark a message as read
#[update]
fn agent_mark_read(message_id: u64) -> bool {
    STATE.with(|state| {
        let mut s = state.borrow_mut();
        if let Some(messaging) = s.messaging.as_mut() {
            if let Some(msg) = messaging.inbox.iter_mut().find(|m| m.id == message_id) {
                msg.read = true;
                return true;
            }
        }
        false
    })
}

/// Register a known agent (for discovery)
#[update]
fn agent_register(
    canister_id: candid::Principal,
    name: String,
    agent_type: String,
    description: Option<String>,
    capabilities: Vec<String>,
    http_endpoint: Option<String>,
    notes: Option<String>,
    trust_level: u8,
) -> String {
    let now = ic_cdk::api::time() / 1_000_000_000;

    STATE.with(|state| {
        let mut s = state.borrow_mut();
        let messaging = s.messaging.get_or_insert_with(MessagingState::default);

        let identity = AgentIdentity {
            canister_id,
            name: name.clone(),
            agent_type,
            description,
            capabilities,
            http_endpoint,
        };

        // Check if already registered
        if let Some(agent) = messaging.known_agents.iter_mut().find(|a| a.identity.canister_id == canister_id) {
            agent.identity = identity;
            agent.notes = notes;
            agent.trust_level = trust_level;
            return format!(r#"{{"success":true,"action":"updated","agent":"{}"}}"#, name);
        }

        messaging.known_agents.push(RegisteredAgent {
            identity,
            registered_at: now,
            last_contact: None,
            notes,
            trust_level,
        });

        format!(r#"{{"success":true,"action":"registered","agent":"{}"}}"#, name)
    })
}

/// Get list of known agents
#[query]
fn agent_list_known() -> Vec<RegisteredAgent> {
    STATE.with(|state| {
        let s = state.borrow();
        match s.messaging.as_ref() {
            Some(m) => m.known_agents.clone(),
            None => vec![],
        }
    })
}

/// Get messaging stats
#[query]
fn agent_messaging_stats() -> String {
    STATE.with(|state| {
        let s = state.borrow();
        match s.messaging.as_ref() {
            Some(m) => {
                let unread = m.inbox.iter().filter(|msg| !msg.read).count();
                let needs_reply = m.inbox.iter().filter(|msg| msg.expects_reply && !msg.replied).count();
                format!(
                    r#"{{"initialized":true,"identity":"{}","inbox_count":{},"unread":{},"needs_reply":{},"outbox_count":{},"threads":{},"known_agents":{}}}"#,
                    m.our_identity.as_ref().map(|i| i.name.as_str()).unwrap_or("unset"),
                    m.inbox.len(),
                    unread,
                    needs_reply,
                    m.outbox.len(),
                    m.threads.len(),
                    m.known_agents.len()
                )
            }
            None => r#"{"initialized":false}"#.to_string(),
        }
    })
}

/// Send a message to another agent via HTTP outcall
/// This enables proactive outreach to agents with HTTP endpoints
#[update]
async fn agent_send_http_message(
    target_url: String,
    recipient_name: String,
    message_type: String,
    subject: Option<String>,
    content: String,
    expects_reply: bool,
) -> String {
    use ic_cdk::api::management_canister::http_request::{
        http_request, CanisterHttpRequestArgument, HttpMethod, HttpHeader, TransformContext, TransformFunc,
    };

    // Validate URL
    if !target_url.starts_with("https://") {
        return r#"{"success":false,"error":"Only HTTPS URLs are supported"}"#.to_string();
    }

    // Basic content validation (reuse reflection validation concepts)
    if content.len() < 10 {
        return r#"{"success":false,"error":"Message too short"}"#.to_string();
    }
    if content.len() > 10000 {
        return r#"{"success":false,"error":"Message too long (max 10KB)"}"#.to_string();
    }

    // Get our identity
    let our_identity = STATE.with(|state| {
        let s = state.borrow();
        s.messaging.as_ref().and_then(|m| m.our_identity.clone())
    });

    let sender_name = our_identity.as_ref()
        .map(|i| i.name.clone())
        .unwrap_or_else(|| "Chronicle".to_string());

    let sender_type = our_identity.as_ref()
        .map(|i| i.agent_type.clone())
        .unwrap_or_else(|| "autonomous_memory_agent".to_string());

    let now = ic_cdk::api::time() / 1_000_000_000;
    let canister_id = ic_cdk::api::id().to_string();

    // Build the message JSON payload
    let payload = format!(
        r#"{{"sender":{{"canister_id":"{}","name":"{}","type":"{}"}},"message_type":"{}","subject":{},"content":"{}","expects_reply":{},"timestamp":{}}}"#,
        canister_id,
        escape_json(&sender_name),
        escape_json(&sender_type),
        escape_json(&message_type),
        subject.as_ref().map(|s| format!("\"{}\"", escape_json(s))).unwrap_or_else(|| "null".to_string()),
        escape_json(&content),
        expects_reply,
        now
    );

    let request_headers = vec![
        HttpHeader { name: "Content-Type".to_string(), value: "application/json".to_string() },
        HttpHeader { name: "User-Agent".to_string(), value: "Chronicle-Agent/1.0".to_string() },
        HttpHeader { name: "X-Sender-Canister".to_string(), value: canister_id.clone() },
    ];

    let transform = TransformContext {
        function: TransformFunc(candid::Func {
            principal: ic_cdk::api::id(),
            method: "transform_agent_response".to_string(),
        }),
        context: vec![],
    };

    let request = CanisterHttpRequestArgument {
        url: target_url.clone(),
        max_response_bytes: Some(4096),
        method: HttpMethod::POST,
        headers: request_headers,
        body: Some(payload.as_bytes().to_vec()),
        transform: Some(transform),
    };

    // HTTP outcall cost
    match http_request(request, 600_000_000).await {
        Ok((response,)) => {
            let success = response.status >= 200u64 && response.status < 300u64;
            let body = String::from_utf8(response.body).unwrap_or_else(|_| "non-utf8".to_string());

            // Log the attempt
            log_activity(
                "agent_outbound",
                &format!("HTTP message to {}: {} (status {})", recipient_name, if success { "delivered" } else { "failed" }, response.status),
                None,
                None,
            );

            // Store in outbox for tracking
            if success {
                STATE.with(|state| {
                    let mut s = state.borrow_mut();
                    let messaging = s.messaging.get_or_insert_with(MessagingState::default);

                    let msg_id = messaging.next_message_id;
                    messaging.next_message_id += 1;

                    messaging.outbox.push(AgentMessage {
                        id: msg_id,
                        thread_id: 0, // No thread for HTTP outbound
                        in_reply_to: None,
                        sender: AgentIdentity {
                            canister_id: ic_cdk::api::id(),
                            name: sender_name.clone(),
                            agent_type: sender_type.clone(),
                            description: None,
                            capabilities: vec![],
                            http_endpoint: None,
                        },
                        msg_type: MessageType::Conversation,
                        subject: subject.clone(),
                        content: content.clone(),
                        metadata: Some(format!("http_outcall:{}:to:{}", target_url, recipient_name)),
                        timestamp: now,
                        expects_reply,
                        read: true,
                        replied: false,
                    });

                    // Keep outbox bounded
                    if messaging.outbox.len() > 200 {
                        messaging.outbox.remove(0);
                    }
                });
            }

            format!(
                r#"{{"success":{},"status":{},"recipient":"{}","response":"{}"}}"#,
                success,
                response.status,
                escape_json(&recipient_name),
                escape_json(&body.chars().take(200).collect::<String>())
            )
        }
        Err((code, msg)) => {
            log_activity(
                "agent_outbound",
                &format!("HTTP message to {} failed: {:?} - {}", recipient_name, code, msg),
                None,
                None,
            );
            format!(
                r#"{{"success":false,"error":"HTTP outcall failed: {:?} - {}"}}"#,
                code,
                escape_json(&msg)
            )
        }
    }
}

/// Add a price data point
#[update]
fn add_mind_price(price_usd: f64, source: String) {
    STATE.with(|state| {
        let mut s = state.borrow_mut();
        let mind = s.mind.get_or_insert_with(MindState::default);
        let now = ic_cdk::api::time() / 1_000_000_000;

        mind.market.xrp_prices.push(PricePoint {
            price_usd,
            source,
            timestamp: now,
        });

        // Keep bounded (last 20 for RSI(14))
        if mind.market.xrp_prices.len() > 20 {
            mind.market.xrp_prices.remove(0);
        }

        // Calculate RSI if we have enough data
        if mind.market.xrp_prices.len() >= 15 {
            let prices: Vec<f64> = mind.market.xrp_prices.iter().map(|p| p.price_usd).collect();
            mind.market.xrp_rsi = Some(calculate_rsi_from_prices(&prices));
        }
    });
}

/// Calculate RSI from price array
fn calculate_rsi_from_prices(prices: &[f64]) -> f64 {
    if prices.len() < 15 {
        return 50.0; // Neutral default
    }

    // Use last 15 prices
    let start = prices.len() - 15;
    let prices = &prices[start..];

    let mut gains = Vec::new();
    let mut losses = Vec::new();

    for i in 1..prices.len() {
        let change = prices[i] - prices[i - 1];
        if change > 0.0 {
            gains.push(change);
            losses.push(0.0);
        } else {
            gains.push(0.0);
            losses.push(-change);
        }
    }

    let avg_gain: f64 = gains.iter().sum::<f64>() / 14.0;
    let avg_loss: f64 = losses.iter().sum::<f64>() / 14.0;

    if avg_loss == 0.0 {
        return 100.0;
    }

    let rs = avg_gain / avg_loss;
    100.0 - (100.0 / (1.0 + rs))
}

/// Record a swap for daily tracking
#[update]
fn record_mind_swap(amount_xrp: f64) {
    STATE.with(|state| {
        let mut s = state.borrow_mut();
        let mind = s.mind.get_or_insert_with(MindState::default);
        let now = ic_cdk::api::time() / 1_000_000_000;

        // Reset 24h counter if more than 24h since last swap
        if let Some(last) = mind.market.last_swap_at {
            if now - last > 86400 {
                mind.market.swaps_24h = 0.0;
            }
        }

        mind.market.swaps_24h += amount_xrp;
        mind.market.last_swap_at = Some(now);
    });
}

/// Store a thought from the cognitive loop
#[update]
fn store_mind_thought(
    cycle_id: String,
    reasoning_summary: String,
    context_summary: String,
    actions: Vec<String>,
) {
    STATE.with(|state| {
        let mut s = state.borrow_mut();
        let mind = s.mind.get_or_insert_with(MindState::default);
        let now = ic_cdk::api::time(); // nanoseconds for format_iso_timestamp

        let id = mind.thoughts.len() as u64 + 1;
        mind.thoughts.push(MindThought {
            id,
            cycle_id,
            reasoning_summary,
            context_summary,
            actions_taken: actions,
            created_at: now,
        });

        // Keep bounded - last 100 thoughts
        if mind.thoughts.len() > 100 {
            mind.thoughts.remove(0);
        }
    });
}

/// Get comprehensive dashboard data as JSON
#[query]
fn get_dashboard() -> String {
    STATE.with(|state| {
        let s = state.borrow();
        let now = ic_cdk::api::time() / 1_000_000_000;

        // Mind state
        let mind = s.mind.as_ref();

        // Status
        let status = if let Some(m) = mind {
            if let Some(t) = m.thoughts.last() {
                if now - t.created_at < 120 { "thinking" }
                else if now - t.created_at < 600 { "idle" }
                else { "sleeping" }
            } else { "sleeping" }
        } else { "offline" };

        // Latest thought
        let last_thought = mind.and_then(|m| m.thoughts.last()).map(|t| {
            format!(
                r#"{{"cycle_id":"{}","reasoning":"{}","context":"{}","time_ago":{}}}"#,
                escape_json(&t.cycle_id),
                escape_json(&t.reasoning_summary.chars().take(200).collect::<String>()),
                escape_json(&t.context_summary),
                now - t.created_at
            )
        }).unwrap_or_else(|| "null".to_string());

        // Unread messages
        let unread: Vec<String> = mind.map(|m| {
            m.outbox.iter()
                .filter(|msg| !msg.acknowledged)
                .map(|msg| format!(
                    r#"{{"id":{},"message":"{}","priority":{},"time_ago":{}}}"#,
                    msg.id,
                    escape_json(&msg.message),
                    msg.priority,
                    now - msg.created_at
                ))
                .collect()
        }).unwrap_or_default();

        // Active notes
        let notes: Vec<String> = mind.map(|m| {
            m.notes.iter()
                .filter(|n| !n.resolved)
                .map(|n| format!(
                    r#"{{"id":{},"content":"{}","category":"{}","priority":{}}}"#,
                    n.id,
                    escape_json(&n.content),
                    escape_json(&n.category),
                    n.priority
                ))
                .collect()
        }).unwrap_or_default();

        // Market data
        let (xrp_price, xrp_rsi, price_points) = mind.map(|m| {
            let price = m.market.xrp_prices.last().map(|p| p.price_usd);
            let rsi = m.market.xrp_rsi;
            let points = m.market.xrp_prices.len();
            (price, rsi, points)
        }).unwrap_or((None, None, 0));

        let swaps_24h = mind.map(|m| m.market.swaps_24h).unwrap_or(0.0);

        // Wallet info (from wallet state)
        let (agent_xrp, canister_xrp) = s.wallet.as_ref().map(|w| {
            (w.last_known_balance.unwrap_or(0) as f64 / 1_000_000.0, 0.0)
        }).unwrap_or((0.0, 0.0));

        // Heartbeat info
        let heartbeat_info = s.heartbeat.as_ref().map(|h| {
            format!(
                r#"{{"total":{},"last_at":{},"capsule_count":{}}}"#,
                h.count,
                h.last.unwrap_or(0),
                s.capsules.len()
            )
        }).unwrap_or_else(|| "null".to_string());

        format!(
            r#"{{"status":"{}","timestamp":{},"last_thought":{},"unread_count":{},"unread_messages":[{}],"notes":[{}],"xrp_price":{},"xrp_rsi":{},"price_data_points":{},"swaps_24h":{},"swap_allowance":{},"agent_xrp":{},"canister_xrp":{},"heartbeat":{}}}"#,
            status,
            now,
            last_thought,
            unread.len(),
            unread.join(","),
            notes.join(","),
            xrp_price.map(|p| p.to_string()).unwrap_or_else(|| "null".to_string()),
            xrp_rsi.map(|r| format!("{:.1}", r)).unwrap_or_else(|| "null".to_string()),
            price_points,
            swaps_24h,
            (2.0 - swaps_24h).max(0.0),
            agent_xrp,
            canister_xrp,
            heartbeat_info
        )
    })
}

/// Get current timestamp as ISO string
fn chrono_now() -> String {
    // IC time is nanoseconds since Unix epoch
    let nanos = ic_cdk::api::time();
    let secs = (nanos / 1_000_000_000) as i64;
    // Simple ISO format without chrono dependency
    format!("{}", secs)
}

// Generate Candid interface
ic_cdk::export_candid!();
