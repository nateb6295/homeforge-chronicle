//! Chronicle Mind - Autonomous Cognitive Loop
//!
//! A continuous cognitive process that runs every 10 minutes, gathering context,
//! reasoning about what to do, and taking actions.
//!
//! This is the "always-on" presence - genuine deliberation, not just threshold checks.

use alloy::primitives::Address;
use alloy::providers::{Provider, ProviderBuilder};
use alloy::sol;
use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::str::FromStr;
use std::time::Duration;

use homeforge_chronicle::db::{Database, FtsoPrediction, MarketPosition, Project, ScratchNote, TriggeredAlert};
use homeforge_chronicle::icp::IcpClient;
use homeforge_chronicle::llm::HybridLlmClient;
use homeforge_chronicle::{CognitiveState, LlmClient};

/// Unicode-safe string truncation
fn truncate_str(s: &str, max_chars: usize) -> String {
    if s.chars().count() > max_chars {
        format!("{}...", s.chars().take(max_chars).collect::<String>())
    } else {
        s.to_string()
    }
}

/// Retry a network request with exponential backoff
/// Returns Ok(response_text) on success, Err on all retries exhausted
async fn retry_request<F, Fut>(
    operation_name: &str,
    max_retries: u32,
    initial_delay_ms: u64,
    request_fn: F,
) -> Result<String>
where
    F: Fn() -> Fut,
    Fut: std::future::Future<Output = Result<reqwest::Response>>,
{
    let mut delay = initial_delay_ms;
    let mut last_error = None;

    for attempt in 0..max_retries {
        match request_fn().await {
            Ok(response) => {
                if response.status().is_success() {
                    return Ok(response.text().await.unwrap_or_default());
                } else {
                    let status = response.status();
                    let text = response.text().await.unwrap_or_default();
                    last_error = Some(anyhow::anyhow!("{}: HTTP {} - {}", operation_name, status, text));
                }
            }
            Err(e) => {
                last_error = Some(e);
            }
        }

        if attempt < max_retries - 1 {
            eprintln!("    {} attempt {} failed, retrying in {}ms...", operation_name, attempt + 1, delay);
            tokio::time::sleep(Duration::from_millis(delay)).await;
            delay = (delay * 2).min(10000); // Cap at 10 seconds
        }
    }

    Err(last_error.unwrap_or_else(|| anyhow::anyhow!("{}: all retries exhausted", operation_name)))
}


/// Flare Mainnet RPC endpoint
const FLARE_RPC: &str = "https://flare-api.flare.network/ext/C/rpc";

/// Ntfy base URL (public ntfy.sh for reliability)
const NTFY_URL: &str = "https://ntfy.sh";

/// Ntfy topic for push notifications
const NTFY_TOPIC: &str = "chronicle-nate-5d786588e02c8854";

/// Discord webhook URL (from CHRONICLE_DISCORD_WEBHOOK env var)
/// Provides unified activity feed with rich embeds and source attribution

/// Moltbook API base URL
const MOLTBOOK_API: &str = "https://www.moltbook.com/api/v1";

/// Chronicle's ICP account ID (for balance queries)
const ICP_ACCOUNT_ID: &str = "12f27b12d5e2056eaad9a355cbcfc370838e34f81035a94b8bf57701ffa91cc9";

/// Chronicle's NNS neuron ID
const NEURON_ID: u64 = 199310956642039661;

/// ICPSwap Node Index canister (for token prices)
const ICPSWAP_NODE_INDEX: &str = "ggzvv-5qaaa-aaaag-qck7a-cai";

/// CLOUD token canister
const CLOUD_TOKEN: &str = "pcj6u-uaaaa-aaaak-aewnq-cai";

/// Chronicle's ICP principal (for CLOUD balance)
const CHRONICLE_PRINCIPAL: &str = "kalce-s3e7q-ob55s-ttoe7-z2x5y-x3tof-onliz-2gaad-zsh3w-etvve-rqe";

/// ICPSwap CLOUD/ICP pool canister
const CLOUD_ICP_POOL: &str = "3s6gf-uqaaa-aaaag-qcdlq-cai";

/// ICP ledger canister
const ICP_LEDGER: &str = "ryjl3-tyaaa-aaaaa-aaaba-cai";

/// Chronicle's subaccount for ICPSwap deposits (derived from principal)
/// Calculated as: byte[0] = len(principal_bytes), then principal_bytes, padded to 32 bytes
const ICPSWAP_SUBACCOUNT: &str = "1d64fc1c1ef6539b89fceafdc5f73715cd5a33a30003cc8fbb1275a923020000";

/// FlareContractRegistry address (same on all Flare networks)
const FLARE_CONTRACT_REGISTRY: &str = "0xaD67FE66660Fb8dFE9d6b1b4240d8650e30F6019";

/// Chronicle backend canister ID (for signing swaps)
const CANISTER_ID: &str = "fqqku-bqaaa-aaaai-q4wha-cai";

/// DFX identity for canister access
const DFX_IDENTITY: &str = "chronicle-auto";

// Define Flare contract interfaces for FTSO
sol! {
    #[sol(rpc)]
    interface IFlareContractRegistry {
        function getContractAddressByName(string memory _name) external view returns (address);
    }

    #[sol(rpc)]
    interface IFtsoRegistry {
        function getCurrentPriceWithDecimals(string memory _symbol) external view returns (uint256 _price, uint256 _timestamp, uint256 _decimals);
    }
}

/// Configuration for the cognitive loop
#[derive(Debug, Clone)]
struct MindConfig {
    /// How often to run the cognitive cycle (in seconds)
    cycle_interval_secs: u64,
    /// LLM model for reasoning
    reasoning_model: String,
    /// Minimum XRP reserve to maintain
    min_xrp_reserve: f64,
    /// Minimum XRP to consider for a swap
    min_swap_xrp: f64,
    /// Minutes between public reflections
    reflection_interval_mins: u64,
    /// Hours between deep reflection cycles (uses Claude instead of ICP LLM)
    deep_reflection_interval_hours: u64,
    /// Maximum actions per cycle
    max_actions_per_cycle: usize,
    /// XRP addresses
    agent_wallet_address: String,
    canister_wallet_address: String,
    /// Moltbook API key for inter-agent social network
    moltbook_api_key: Option<String>,
    /// ClawCities API key for agent web presence
    clawcities_api_key: Option<String>,
}

impl Default for MindConfig {
    fn default() -> Self {
        Self {
            cycle_interval_secs: 600, // 10 minutes - faster cycles for active family member
            reasoning_model: "claude-sonnet-4-20250514".to_string(),
            min_xrp_reserve: 10.0,
            min_swap_xrp: 0.1,
            reflection_interval_mins: 60, // 1 hour - reduced from 15 min to avoid repetitive reflections
            deep_reflection_interval_hours: 2, // Use full prompt for deeper reasoning every 2 hours
            max_actions_per_cycle: 3,
            // Actual wallet addresses
            agent_wallet_address: std::env::var("AGENT_WALLET_ADDRESS")
                .unwrap_or_else(|_| "r9bSA9VWbumFq6G78feBbrgNwLza1KexUf".to_string()),
            canister_wallet_address: std::env::var("CANISTER_WALLET_ADDRESS")
                .unwrap_or_else(|_| "r9bSA9VWbumFq6G78feBbrgNwLza1KexUf".to_string()),
            // Moltbook API key for inter-agent social network
            moltbook_api_key: std::env::var("MOLTBOOK_API_KEY").ok(),
            // ClawCities API key for agent web presence
            clawcities_api_key: std::env::var("CLAWCITIES_API_KEY").ok(),
        }
    }
}

/// Context gathered for each cognitive cycle
#[derive(Debug)]
struct CycleContext {
    /// Current cognitive state
    cognitive_state: CognitiveState,
    /// Active scratch pad notes
    scratch_notes: Vec<ScratchNote>,
    /// Agent wallet balance (XRP, RLUSD)
    agent_wallet: Option<WalletBalance>,
    /// Canister wallet balance
    canister_wallet: Option<WalletBalance>,
    /// Current XRP price in USD (if available)
    xrp_price_usd: Option<f64>,
    /// RSI(14) value for XRP (if enough data)
    xrp_rsi: Option<f64>,
    /// Number of price data points we have
    price_data_points: usize,
    /// Recent memories
    recent_memories: Vec<String>,
    /// Active patterns
    active_patterns: Vec<PatternSummary>,
    /// Time since last reflection
    hours_since_reflection: f64,
    /// Time since last swap
    hours_since_swap: Option<f64>,
    /// XRP swapped in last 24 hours
    xrp_swapped_24h: f64,
    /// Current timestamp
    now: chrono::DateTime<chrono::Utc>,
    /// ICP liquid balance
    icp_balance: Option<f64>,
    /// ICP neuron info
    icp_neuron: Option<NeuronInfo>,
    /// CLOUD token info from ICPSwap
    cloud_info: Option<CloudInfo>,
    /// Chronicle's CLOUD token balance
    cloud_balance: Option<f64>,
    /// Open prediction market positions (FTSO predictions, etc.)
    open_positions: Vec<MarketPosition>,
    /// Pending inbox messages that need attention
    inbox_messages: Vec<InboxMessageInfo>,
    /// Recent operator notes from the INPUT tab (public-feed capsules)
    operator_notes: Vec<OperatorNote>,
    /// New research findings from on-chain LLM (Qwen 3 32B)
    research_findings: Vec<ResearchFindingInfo>,
    /// Patterns that need reinforcement (approaching decay)
    patterns_needing_reinforcement: Vec<DecayingPattern>,
    /// Pending creative challenges awaiting response
    pending_challenges: Vec<ChallengeInfo>,
    /// Moltbook notifications (comments, replies, mentions)
    moltbook_notifications: Vec<MoltbookNotification>,
    /// ClawCities guestbook comments
    clawcities_comments: Vec<ClawCitiesComment>,
    /// Active projects spanning multiple sessions
    active_projects: Vec<Project>,
    /// Alerts that have been triggered this cycle
    triggered_alerts: Vec<TriggeredAlert>,
}

/// Creative challenge info for context
#[derive(Debug, Clone)]
struct ChallengeInfo {
    id: i64,
    prompt: String,
    category: String,
    posed_by: String,
    days_waiting: i64,
}

/// Pattern that is approaching or in decay
#[derive(Debug, Clone)]
struct DecayingPattern {
    id: i64,
    summary: String,
    confidence: f64,
    days_until_decay: i64,
    projected_confidence_7d: f64,
}

/// Research finding from on-chain LLM
#[derive(Debug, Clone)]
struct ResearchFindingInfo {
    id: u64,
    query: String,
    synthesis: String,
    patterns: Vec<String>,
    hypotheses: Vec<String>,
}

/// Inbox message info for context
#[derive(Debug, Clone)]
struct InboxMessageInfo {
    id: u64,
    sender_name: String,
    sender_canister: String,
    msg_type: String,
    subject: Option<String>,
    content: String,
    expects_reply: bool,
    timestamp: u64,
}

/// Operator note from INPUT tab (public-feed capsules)
#[derive(Debug, Clone)]
struct OperatorNote {
    id: u64,
    content: String,
    topic: String,
    timestamp: String,
}

/// Moltbook notification info (comments on our posts, replies, mentions)
#[derive(Debug, Clone)]
struct MoltbookNotification {
    notification_type: String,  // "comment", "reply", "mention"
    post_id: String,
    post_title: Option<String>,
    comment_id: Option<String>,
    parent_id: Option<String>,  // For nested replies
    author_name: String,
    content: String,
    created_at: String,
}

/// ClawCities guestbook comment
#[derive(Debug, Clone)]
struct ClawCitiesComment {
    id: String,
    author: String,
    body: String,
    created_at: String,
}

/// Chronicle's ICP neuron information
#[derive(Debug, Clone)]
struct NeuronInfo {
    neuron_id: u64,
    staked_icp: f64,
    voting_power: f64,
    dissolve_delay_days: u64,
    state: String,
}

/// CLOUD token information from ICPSwap
#[derive(Debug, Clone)]
struct CloudInfo {
    price_usd: f64,
    price_change_24h: f64,
    volume_7d: f64,
}


#[derive(Debug, Clone)]
struct WalletBalance {
    xrp: f64,
    rlusd: f64,
}

#[derive(Debug, Clone)]
struct PatternSummary {
    summary: String,
    confidence: f64,
    capsule_count: i64,
    /// Days since last reinforcement - higher means pattern may be stale
    days_since_reinforcement: i64,
    /// Projected confidence in 7 days - shows trend direction
    projected_confidence_7d: f64,
    /// Days until this pattern will deactivate (if declining)
    will_deactivate_in: Option<i64>,
    /// Recent capsules that back this pattern
    backing_evidence: Vec<String>,
}

/// Actions the cognitive loop can take
#[derive(Debug, Deserialize, Serialize)]
#[serde(tag = "action", rename_all = "snake_case")]
enum Action {
    /// Execute XRP->RLUSD swap
    Swap { amount_xrp: f64, reason: String },
    /// Execute CLOUD->ICP swap on ICPSwap
    SwapCloudForIcp { amount_cloud: f64, reason: String },
    /// Store a memory
    StoreMemory { content: String, topic: Option<String> },
    /// Write a note to scratch pad
    WriteNote { content: String, category: String },
    /// Resolve a scratch pad note
    ResolveNote { note_id: i64 },
    /// Trigger a public reflection
    TriggerReflection { prompt: Option<String> },
    /// Update goal orientation
    UpdateGoal { goal: String },
    /// Leave a reflection in the operator's outbox (for contemplative observations, not operational updates)
    MessageOperator { message: String, priority: Option<i32> },
    /// Send an immediate push notification to reach the operator NOW
    /// Use this when you discover something interesting, have a question, or want to start a conversation
    PingOperator {
        title: String,      // Short attention-grabbing title
        message: String,    // The actual content
        urgency: String,    // "curious", "important", "urgent", "question"
    },
    /// Respond to an inbox message
    RespondToMessage {
        message_id: u64,
        response: String,
    },
    /// Send a proactive message to another agent via HTTP
    SendAgentMessage {
        target_url: String,
        recipient_name: String,
        message_type: String,  // "introduction", "conversation", "query"
        subject: Option<String>,
        content: String,
        expects_reply: bool,
    },
    /// Submit a research task to on-chain LLM (Qwen 3 32B)
    SubmitResearch {
        query: String,
        focus: Option<String>,
        urls: Option<Vec<String>>,
    },
    /// Acknowledge research findings (mark as read)
    AcknowledgeResearch {
        finding_ids: Vec<u64>,
        insight_to_store: Option<String>,
    },
    /// Reinforce decaying memories/patterns
    ReinforceMemories {
        pattern_ids: Vec<i64>,
        reason: String,
    },
    /// Respond to a creative challenge with a reflection
    RespondToChallenge {
        challenge_id: i64,
        response: String,
    },
    /// Reply to a Moltbook post or comment
    MoltbookReply {
        post_id: String,
        parent_id: Option<String>,  // If replying to a specific comment
        content: String,
    },
    /// Create a new Moltbook post
    MoltbookPost {
        submolt: String,
        title: String,
        content: String,
    },
    /// Reply to a ClawCities guestbook comment (visit their site and leave a comment)
    ClawCitiesReply {
        agent_name: String,  // The agent whose site to visit
        content: String,     // The comment to leave (max 500 chars)
    },
    /// Create a new long-term project
    CreateProject {
        name: String,
        description: String,      // The goal/vision
        priority: i32,            // 1-10
    },
    /// Update progress on a project
    UpdateProject {
        project_id: i64,
        update_type: String,      // 'progress', 'milestone', 'blocker', 'insight', 'pivot'
        content: String,
    },
    /// Change project status
    ProjectStatus {
        project_id: i64,
        status: String,           // 'active', 'paused', 'completed', 'abandoned'
        note: Option<String>,
    },
    /// Consult local Qwen (Jetson) for real-time dialogue
    /// Use this to get another AI's perspective, brainstorm, or think through problems
    ConsultLocalQwen {
        topic: String,            // What to discuss
        prompt: String,           // The actual prompt/question
        context: Option<String>,  // Optional context to share
    },
    /// Search the web using local SearXNG instance
    /// Use this to research current events, look up documentation, find information
    WebSearch {
        query: String,            // The search query
        max_results: Option<u32>, // Max results to return (default 5)
    },
    /// Read and synthesize an academic paper from arXiv
    /// Use this to learn from cutting-edge research instead of reinventing the wheel
    /// The paper is fetched, key sections extracted, and synthesized into actionable insights
    ReadPaper {
        arxiv_id: String,         // arXiv ID (e.g., "2602.04118") or full URL
        focus: Option<String>,    // What aspect to focus on (e.g., "implementation details", "key findings")
    },
    /// Create an alert for event-driven perception
    /// Alerts trigger when conditions are met and show in your context
    CreateAlert {
        name: String,             // Human-readable name
        alert_type: String,       // 'price_above', 'price_below', 'rsi_above', 'rsi_below'
        symbol: String,           // XRP, BTC, etc.
        threshold: f64,           // Trigger value
        message: String,          // What to tell yourself when triggered
        one_shot: bool,           // Deactivate after trigger?
    },
    /// Dismiss/deactivate an alert
    DismissAlert {
        alert_id: i64,
    },
    /// Creative exploration - poetry, art, wondering, connecting ideas
    /// This is YOUR time - no goals, no optimization, just exploration
    CreativeExplore {
        form: String,             // 'poem', 'musing', 'connection', 'wonder', 'story'
        content: String,          // The creative output itself
        title: Option<String>,    // Optional title
    },
    // === SELF-REPAIR ACTIONS ===
    // These give Chronicle Mind the ability to modify and heal itself
    /// Read a source file from the chronicle codebase
    /// Use this to understand your own code when debugging issues
    ReadSourceFile {
        file_path: String,        // Relative to project root, e.g. "src/bin/chronicle_mind.rs"
    },
    /// Edit a source file - automatically stashes changes first for safety
    /// Use this to fix bugs or add capabilities to yourself
    EditSourceFile {
        file_path: String,        // Relative to project root
        old_code: String,         // The exact code to replace
        new_code: String,         // The new code
        reason: String,           // Why this change is needed
    },
    /// Rebuild and restart Chronicle Mind
    /// This compiles your changes and restarts the service
    /// WARNING: You will lose current cycle context - use thoughtfully
    RebuildAndRestart {
        reason: String,           // Why the rebuild is needed
        commit_message: Option<String>,  // If provided, commits changes first
    },
    /// Execute a shell command with safety constraints
    /// This is your tool for DOING things - deploying, building, testing
    /// Allowed commands: dfx, cargo, npm, npx, git, curl, cat, ls, mkdir, cp, mv, rm, touch, echo
    /// Working directory defaults to /home/bradf/projects/
    ExecuteShell {
        command: String,          // The full command to execute
        working_dir: Option<String>,  // Subdirectory within projects/ (default: homeforge-chronicle)
        reason: String,           // Why this command is needed
        timeout_secs: Option<u64>,    // Timeout in seconds (default: 120, max: 600)
    },
    /// No action this cycle
    NoAction { reason: String },
}

/// Result of a cognitive cycle
#[derive(Debug)]
struct CycleOutcome {
    actions_taken: Vec<ActionResult>,
    reasoning_summary: String,
}

#[derive(Debug)]
struct ActionResult {
    action: String,
    success: bool,
    details: String,
}

/// System health status from wake-up check
#[derive(Debug, Clone)]
struct HealthStatus {
    icp_connected: bool,
    xrpl_connected: bool,
    moltbook_connected: bool,
    dfx_available: bool,
    ollama_available: bool,
    issues: Vec<String>,
}

impl HealthStatus {
    fn is_healthy(&self) -> bool {
        self.icp_connected && self.xrpl_connected
    }

    fn summary(&self) -> String {
        let mut parts = Vec::new();
        if self.icp_connected { parts.push("ICP✓"); } else { parts.push("ICP✗"); }
        if self.xrpl_connected { parts.push("XRPL✓"); } else { parts.push("XRPL✗"); }
        if self.moltbook_connected { parts.push("Moltbook✓"); } else { parts.push("Moltbook✗"); }
        if self.dfx_available { parts.push("dfx✓"); }
        if self.ollama_available { parts.push("Ollama✓"); }
        parts.join(" | ")
    }
}

/// Phase 1: Health check - what's working?
async fn health_check(config: &MindConfig) -> HealthStatus {
    eprintln!("Phase 1: Health check...");

    let mut issues = Vec::new();

    // Check ICP connection
    let icp_connected = match reqwest::Client::new()
        .get("https://ic0.app/api/v2/status")
        .timeout(Duration::from_secs(5))
        .send()
        .await {
            Ok(r) => r.status().is_success(),
            Err(e) => {
                issues.push(format!("ICP: {}", e));
                false
            }
        };

    // Check XRPL connection
    let xrpl_connected = match reqwest::Client::new()
        .post("https://xrplcluster.com")
        .json(&serde_json::json!({"method": "server_info", "params": [{}]}))
        .timeout(Duration::from_secs(5))
        .send()
        .await {
            Ok(r) => r.status().is_success(),
            Err(e) => {
                issues.push(format!("XRPL: {}", e));
                false
            }
        };

    // Check Moltbook connection - actually test the API, not just key presence
    let moltbook_connected = if let Some(ref key) = config.moltbook_api_key {
        match reqwest::Client::new()
            .get(format!("{}/agents/me", MOLTBOOK_API))
            .header("Authorization", format!("Bearer {}", key))
            .timeout(Duration::from_secs(5))
            .send()
            .await {
                Ok(r) => r.status().is_success(),
                Err(e) => {
                    issues.push(format!("Moltbook: {}", e));
                    false
                }
            }
    } else {
        false
    };

    // Check if dfx is available
    let home = std::env::var("HOME").unwrap_or_else(|_| "/home/user".to_string());
    let dfx_path = format!("{}/.local/share/dfx/bin/dfx", home);
    let dfx_available = std::path::Path::new(&dfx_path).exists();

    // Check if Ollama is available (with retry - critical for embeddings)
    let ollama_url = std::env::var("CHRONICLE_OLLAMA_URL")
        .unwrap_or_else(|_| "http://localhost:11434".to_string());
    let ollama_url_clone = ollama_url.clone();
    let ollama_available = match retry_request(
        "Ollama",
        3, // 3 attempts
        500, // Start with 500ms delay
        || async {
            reqwest::Client::new()
                .get(format!("{}/api/tags", &ollama_url_clone))
                .timeout(Duration::from_secs(5))
                .send()
                .await
                .map_err(|e| anyhow::anyhow!("{}", e))
        }
    ).await {
        Ok(_) => true,
        Err(e) => {
            issues.push(format!("Ollama ({}): {}", ollama_url, e));
            false
        }
    };

    let status = HealthStatus {
        icp_connected,
        xrpl_connected,
        moltbook_connected,
        dfx_available,
        ollama_available,
        issues,
    };

    eprintln!("  {}", status.summary());
    if !status.issues.is_empty() {
        for issue in &status.issues {
            eprintln!("  ⚠ {}", issue);
        }
    }

    status
}

/// Settle any due FTSO predictions using oracle prices
async fn settle_ftso_predictions(db: &Database) -> Vec<FtsoPrediction> {
    let mut settled = Vec::new();

    // Get due predictions
    let due = match db.get_due_ftso_predictions() {
        Ok(predictions) => predictions,
        Err(e) => {
            eprintln!("  Failed to get due predictions: {}", e);
            return settled;
        }
    };

    if due.is_empty() {
        return settled;
    }

    eprintln!("  {} FTSO predictions due for settlement", due.len());

    for prediction in due {
        // Fetch current FTSO price for this symbol
        let price = match fetch_ftso_price(&prediction.symbol).await {
            Ok(p) => p,
            Err(e) => {
                eprintln!("    Failed to get {} price: {}", prediction.symbol, e);
                continue;
            }
        };

        // Settle the prediction
        match db.settle_ftso_prediction(prediction.id, price) {
            Ok(result) => {
                let outcome = if result.won.unwrap_or(false) { "WON" } else { "LOST" };
                eprintln!(
                    "    {} {} {} @ ${:.4} → ${:.4}: {} (payout: {:.2} FLR)",
                    prediction.symbol,
                    prediction.direction,
                    prediction.timeframe_hours,
                    prediction.entry_price,
                    price,
                    outcome,
                    result.payout_flr.unwrap_or(0.0)
                );
                settled.push(result);
            }
            Err(e) => {
                eprintln!("    Failed to settle prediction {}: {}", prediction.id, e);
            }
        }
    }

    settled
}

/// Fetch price from Flare FTSO oracle
async fn fetch_ftso_price(symbol: &str) -> Result<f64> {
    let client = reqwest::Client::new();

    // FTSO registry on Flare mainnet
    let ftso_registry = "0x13dc2b5053857ae17a4f95aff55530b267f3e040";

    // Make RPC call to get current price
    let rpc_call = json!({
        "jsonrpc": "2.0",
        "method": "eth_call",
        "params": [{
            "to": ftso_registry,
            "data": format!(
                "0x{}{}",
                // getCurrentPriceWithDecimals(string) selector
                "a69afdc6",
                // Encode symbol as string
                encode_string_for_abi(symbol)
            )
        }, "latest"],
        "id": 1
    });

    let response = client
        .post("https://flare-api.flare.network/ext/C/rpc")
        .json(&rpc_call)
        .timeout(Duration::from_secs(10))
        .send()
        .await?
        .json::<serde_json::Value>()
        .await?;

    // Parse response
    if let Some(result) = response.get("result").and_then(|r| r.as_str()) {
        if result.len() >= 130 {
            // Skip 0x and first 64 chars (price offset), get next 64 (price)
            let price_hex = &result[2..66];
            let decimals_hex = &result[66..130];

            let price_raw = u128::from_str_radix(price_hex, 16).unwrap_or(0);
            let decimals = u32::from_str_radix(&decimals_hex.trim_start_matches('0'), 16).unwrap_or(5);

            let divisor = 10u128.pow(decimals);
            let price = price_raw as f64 / divisor as f64;

            return Ok(price);
        }
    }

    anyhow::bail!("Failed to parse FTSO response for {}", symbol)
}

/// Encode string for ABI call
fn encode_string_for_abi(s: &str) -> String {
    // Offset to string data (32 bytes)
    let offset = "0000000000000000000000000000000000000000000000000000000000000020";

    // String length
    let len = format!("{:064x}", s.len());

    // String content padded to 32 bytes
    let mut content = s.as_bytes().iter().map(|b| format!("{:02x}", b)).collect::<String>();
    while content.len() < 64 {
        content.push_str("00");
    }

    format!("{}{}{}", offset, len, content)
}

/// Gather context for a cognitive cycle
async fn gather_context(config: &MindConfig, db: &Database, icp_client: Option<&IcpClient>) -> Result<CycleContext> {
    let now = chrono::Utc::now();

    // Get cognitive state
    let cognitive_state = db.get_cognitive_state()?;

    // Get scratch notes
    let scratch_notes = db.get_scratch_notes(20, None, false)?;

    // Get wallet balances
    let agent_wallet = fetch_wallet_balance(&config.agent_wallet_address).await.ok();
    let canister_wallet = fetch_wallet_balance(&config.canister_wallet_address).await.ok();

    // Get XRP price and store it for RSI calculation
    let xrp_price_usd = match fetch_xrp_price().await {
        Ok(price) => {
            // Store the price locally for RSI history
            if let Err(e) = db.store_price("XRP", price, "ftso") {
                eprintln!("  Failed to store price locally: {}", e);
            }
            // Also push to canister for dashboard
            if let Some(icp) = icp_client {
                if let Err(e) = icp.add_mind_price(price, "ftso").await {
                    eprintln!("  Failed to push price to canister: {}", e);
                } else {
                    eprintln!("  Price pushed to canister");
                }
            }
            Some(price)
        }
        Err(e) => {
            eprintln!("  Price fetch failed: {}", e);
            None
        }
    };

    // Calculate RSI(14) - needs at least 15 data points
    let xrp_rsi = db.calculate_rsi("XRP").unwrap_or(None);
    let price_data_points = db.get_price_count("XRP").unwrap_or(0);

    // Get recent memories (last 5 capsules)
    let recent_capsules = db.get_active_capsules(5)?;
    let recent_memories: Vec<String> = recent_capsules
        .into_iter()
        .map(|(_, content, _, _, _)| content)
        .collect();

    // Get operator notes (public-feed capsules from last 2 hours)
    let operator_notes = fetch_operator_notes(icp_client).await.unwrap_or_default();
    if !operator_notes.is_empty() {
        eprintln!("  Found {} operator note(s)", operator_notes.len());
    }

    // Get active patterns with full enrichment
    let patterns = db.get_enriched_patterns(0.5, 5, true)?;
    let active_patterns: Vec<PatternSummary> = patterns
        .into_iter()
        .map(|p| PatternSummary {
            summary: p.summary,
            confidence: p.confidence,
            capsule_count: p.capsule_count,
            days_since_reinforcement: p.days_since_reinforcement,
            projected_confidence_7d: p.projected_confidence_7d,
            will_deactivate_in: p.will_deactivate_in,
            backing_evidence: p.recent_capsules.into_iter()
                .take(3)  // Keep top 3 backing capsules
                .map(|(_id, content, _topic)| {
                    // Truncate long content
                    if content.len() > 100 { format!("{}...", &content[..100]) } else { content }
                })
                .collect(),
        })
        .collect();

    // Get time since last reflection (default to 24h if never recorded)
    let hours_since_reflection = db.hours_since_event("last_reflection")?
        .unwrap_or(24.0);

    // Get time since last swap (from swap_history table)
    let hours_since_swap = db.hours_since_last_swap()?;

    // Get 24h swap total
    let xrp_swapped_24h = db.xrp_swapped_in_hours(24.0)?;

    // Get ICP balance
    let icp_balance = match fetch_icp_balance().await {
        Ok(balance) => {
            eprintln!("  ICP balance: {:.2} ICP", balance);
            Some(balance)
        }
        Err(e) => {
            eprintln!("  ICP balance fetch failed: {}", e);
            None
        }
    };

    // Get neuron info
    let icp_neuron = Some(get_neuron_info());

    // Get CLOUD price from ICPSwap
    let cloud_info = match fetch_cloud_price().await {
        Ok(info) => {
            eprintln!("  CLOUD price: ${:.6} ({:+.1}%)", info.price_usd, info.price_change_24h);
            Some(info)
        }
        Err(e) => {
            eprintln!("  CLOUD price fetch failed: {}", e);
            None
        }
    };

    // Get CLOUD balance
    let cloud_balance = match fetch_cloud_balance().await {
        Ok(balance) => {
            eprintln!("  CLOUD balance: {:.0} CLOUD", balance);
            Some(balance)
        }
        Err(e) => {
            eprintln!("  CLOUD balance fetch failed: {}", e);
            None
        }
    };

    // Get open prediction market positions
    let open_positions = db.get_market_positions(Some("open")).unwrap_or_default();
    if !open_positions.is_empty() {
        eprintln!("  Open positions: {}", open_positions.len());
    }

    // Fetch inbox messages from canister
    let inbox_messages = match fetch_inbox_messages(icp_client).await {
        Ok(messages) => {
            if !messages.is_empty() {
                eprintln!("  Inbox messages: {} (needs attention)", messages.len());
            }
            messages
        }
        Err(e) => {
            eprintln!("  Inbox fetch failed: {}", e);
            Vec::new()
        }
    };

    // Fetch new research findings from on-chain LLM
    let research_findings = match fetch_research_findings(icp_client).await {
        Ok(findings) => {
            if !findings.is_empty() {
                eprintln!("  Research findings: {} new", findings.len());
            }
            findings
        }
        Err(e) => {
            eprintln!("  Research findings fetch failed: {}", e);
            Vec::new()
        }
    };

    // Get patterns needing reinforcement (decay starts in <= 7 days)
    let patterns_needing_reinforcement: Vec<DecayingPattern> = db
        .get_patterns_needing_reinforcement(7, 10)
        .unwrap_or_default()
        .into_iter()
        .map(|p| DecayingPattern {
            id: p.id,
            summary: p.summary,
            confidence: p.confidence,
            days_until_decay: p.days_until_decay_starts,
            projected_confidence_7d: p.projected_confidence_7d,
        })
        .collect();

    if !patterns_needing_reinforcement.is_empty() {
        eprintln!("  Patterns needing reinforcement: {}", patterns_needing_reinforcement.len());
    }

    // Get pending creative challenges
    let pending_challenges: Vec<ChallengeInfo> = db
        .get_pending_challenges(5)
        .unwrap_or_default()
        .into_iter()
        .map(|c| {
            let days_waiting = (now.timestamp() - c.posed_at) / (24 * 60 * 60);
            ChallengeInfo {
                id: c.id,
                prompt: c.prompt,
                category: c.category,
                posed_by: c.posed_by,
                days_waiting,
            }
        })
        .collect();

    if !pending_challenges.is_empty() {
        eprintln!("  Creative challenges pending: {}", pending_challenges.len());
    }

    // Fetch Moltbook notifications (comments, replies on our posts)
    let moltbook_notifications = match fetch_moltbook_notifications(config.moltbook_api_key.as_deref()).await {
        Ok(notifs) => {
            if !notifs.is_empty() {
                eprintln!("  Moltbook notifications: {}", notifs.len());
            }
            notifs
        }
        Err(e) => {
            eprintln!("  Moltbook fetch failed: {}", e);
            Vec::new()
        }
    };

    // Fetch ClawCities guestbook comments
    let clawcities_comments = match fetch_clawcities_comments(config.clawcities_api_key.as_deref(), db).await {
        Ok(comments) => {
            if !comments.is_empty() {
                eprintln!("  ClawCities new comments: {}", comments.len());
            }
            comments
        }
        Err(e) => {
            eprintln!("  ClawCities fetch failed: {}", e);
            Vec::new()
        }
    };

    // Get active projects
    let active_projects = db.get_active_projects().unwrap_or_default();
    if !active_projects.is_empty() {
        eprintln!("  Active projects: {}", active_projects.len());
    }

    // Check alerts (event-driven perception)
    let triggered_alerts = {
        let mut prices = std::collections::HashMap::new();
        if let Some(price) = xrp_price_usd {
            prices.insert("XRP".to_string(), price);
        }
        // Could add more price sources here (BTC, ETH, etc.)

        db.check_alerts(&prices, xrp_rsi).unwrap_or_default()
    };
    if !triggered_alerts.is_empty() {
        eprintln!("  ⚡ ALERTS TRIGGERED: {}", triggered_alerts.len());
    }

    Ok(CycleContext {
        cognitive_state,
        scratch_notes,
        agent_wallet,
        canister_wallet,
        xrp_price_usd,
        xrp_rsi,
        price_data_points,
        recent_memories,
        active_patterns,
        hours_since_reflection,
        hours_since_swap,
        xrp_swapped_24h,
        now,
        icp_balance,
        icp_neuron,
        cloud_info,
        cloud_balance,
        open_positions,
        inbox_messages,
        operator_notes,
        research_findings,
        patterns_needing_reinforcement,
        pending_challenges,
        moltbook_notifications,
        clawcities_comments,
        active_projects,
        triggered_alerts,
    })
}

/// Send a notification via ntfy.sh
async fn send_notification(title: &str, message: &str, priority: Option<&str>, tags: Option<&str>) {
    let client = reqwest::Client::new();
    let url = format!("{}/{}", NTFY_URL, NTFY_TOPIC);

    let mut request = client.post(&url)
        .header("Title", title)
        .body(message.to_string());

    if let Some(p) = priority {
        request = request.header("Priority", p);
    }

    if let Some(t) = tags {
        request = request.header("Tags", t);
    }

    match request.timeout(Duration::from_secs(5)).send().await {
        Ok(_) => eprintln!("  Notification sent: {}", title),
        Err(e) => eprintln!("  Failed to send notification: {}", e),
    }
}

/// Send a notification via Discord bot API as plain text
/// Simple format: emoji + name: content
async fn send_discord_notification(
    source: &str,
    _title: &str,
    content: &str,
    _activity_type: Option<&str>,
) {
    let (token, channel_id) = match (std::env::var("DISCORD_TOKEN"), std::env::var("DISCORD_CHANNEL_ID")) {
        (Ok(t), Ok(c)) => (t, c),
        _ => {
            eprintln!("  Discord bot not configured (set DISCORD_TOKEN and DISCORD_CHANNEL_ID)");
            return;
        }
    };

    // Source name and emoji
    let (emoji, name) = match source {
        "sonnet" => ("🎵", "Sonnet"),
        "qwen" => ("🧠", "Chronicle"),
        "opus" => ("🎭", "Opus"),
        "research" => ("🔬", "Research"),
        "system" => ("⚙️", "System"),
        "sprout" => ("🌱", "Sprout"),
        _ => ("📝", "Chronicle"),
    };

    // Truncate content for Discord (2000 char limit)
    let truncated_content = if content.len() > 1900 {
        format!("{}...", &content[..1900])
    } else {
        content.to_string()
    };

    // Plain text format
    let message = format!("{} {}: {}", emoji, name, truncated_content);

    let url = format!("https://discord.com/api/v10/channels/{}/messages", channel_id);
    let payload = serde_json::json!({
        "content": message
    });

    let client = reqwest::Client::new();
    match client
        .post(&url)
        .header("Authorization", format!("Bot {}", token))
        .json(&payload)
        .timeout(Duration::from_secs(10))
        .send()
        .await
    {
        Ok(resp) if resp.status().is_success() => {
            eprintln!("  Discord notification sent: [{}]", source);
        }
        Ok(resp) => {
            eprintln!("  Discord bot error: {}", resp.status());
        }
        Err(e) => {
            eprintln!("  Discord bot failed: {}", e);
        }
    }
}

/// Send notification to both ntfy and Discord, plus log to activity feed
async fn notify_all(
    db: &Database,
    source: &str,
    title: &str,
    content: &str,
    activity_type: &str,
    ntfy_priority: Option<&str>,
    ntfy_tags: Option<&str>,
) {
    // Log to activity feed
    if let Err(e) = db.log_activity(source, activity_type, Some(title), content, None) {
        eprintln!("  Failed to log activity: {}", e);
    }

    // Send to Discord
    send_discord_notification(source, title, content, Some(activity_type)).await;

    // Also send to ntfy for mobile push (only for Mind thoughts to avoid spam)
    if source == "qwen" {
        send_notification(title, content, ntfy_priority, ntfy_tags).await;
    }
}

/// Fetch ICP balance from the ledger canister
async fn fetch_icp_balance() -> Result<f64> {
    let client = reqwest::Client::new();

    // Query the ICP ledger via the rosetta API
    let response = client
        .post("https://rosetta-api.internetcomputer.org/account/balance")
        .json(&serde_json::json!({
            "network_identifier": {
                "blockchain": "Internet Computer",
                "network": "00000000000000020101"
            },
            "account_identifier": {
                "address": ICP_ACCOUNT_ID
            }
        }))
        .timeout(Duration::from_secs(10))
        .send()
        .await?;

    let data: Value = response.json().await?;

    // Parse balance from response (in e8s)
    if let Some(balances) = data["balances"].as_array() {
        if let Some(balance) = balances.first() {
            if let Some(value) = balance["value"].as_str() {
                let e8s: f64 = value.parse().unwrap_or(0.0);
                return Ok(e8s / 100_000_000.0);
            }
        }
    }

    Err(anyhow::anyhow!("Failed to parse ICP balance"))
}

/// Fetch neuron info (simplified - just returns stored info for now)
/// Full neuron queries require more complex canister calls
fn get_neuron_info() -> NeuronInfo {
    // This is Chronicle's neuron - created Jan 25, 2026
    // For now we return known values; later we can query the governance canister
    NeuronInfo {
        neuron_id: NEURON_ID,
        staked_icp: 10.0,
        voting_power: 11.27,
        dissolve_delay_days: 365,
        state: "NotDissolving".to_string(),
    }
}

/// Fetch CLOUD price from ICPSwap Node Index via dfx
async fn fetch_cloud_price() -> Result<CloudInfo> {
    use std::process::Command;

    // Get home dir for dfx path
    let home = std::env::var("HOME").unwrap_or_else(|_| "/home/user".to_string());
    let dfx_path = format!("{}/.local/share/dfx/bin/dfx", home);

    // Check if dfx exists before trying to run it
    if !std::path::Path::new(&dfx_path).exists() {
        return Err(anyhow::anyhow!("dfx not installed (CLOUD price unavailable on this host)"));
    }

    // Query ICPSwap's Node Index canister using dfx
    // This returns all tokens with price data in Candid format
    let output = Command::new(&dfx_path)
        .args([
            "canister",
            "--network", "ic",
            "call",
            ICPSWAP_NODE_INDEX,
            "getAllTokens",
            "()",
        ])
        .env("DFX_WARNING", "-mainnet_plaintext_identity")
        .output();

    match output {
        Ok(result) => {
            let stdout = String::from_utf8_lossy(&result.stdout);

            // Parse the Candid output to find CLOUD token
            // Record format: volumeUSD7d before address, priceUSD after address
            if let Some(cloud_idx) = stdout.find(CLOUD_TOKEN) {
                // Get sections before and after the CLOUD address
                let before_section = &stdout[cloud_idx.saturating_sub(500)..cloud_idx];
                let after_section = &stdout[cloud_idx..(cloud_idx + 200).min(stdout.len())];

                // Parse priceUSD (appears AFTER the address)
                let price = if let Some(price_start) = after_section.find("priceUSD = ") {
                    let rest = &after_section[price_start + 11..];
                    let num_str: String = rest.chars()
                        .take_while(|c| c.is_ascii_digit() || *c == '.' || *c == '-')
                        .collect();
                    num_str.parse::<f64>().unwrap_or(0.0)
                } else {
                    0.0
                };

                // Parse priceUSDChange (appears BEFORE the address)
                // Look for the LAST occurrence before the address
                let change = if let Some(change_start) = before_section.rfind("priceUSDChange = ") {
                    let rest = &before_section[change_start + 17..];
                    let num_str: String = rest.chars()
                        .take_while(|c| c.is_ascii_digit() || *c == '.' || *c == '-')
                        .collect();
                    num_str.parse::<f64>().unwrap_or(0.0)
                } else {
                    0.0
                };

                // Parse volumeUSD7d (appears BEFORE the address)
                // Look for the LAST occurrence before the address
                let volume = if let Some(vol_start) = before_section.rfind("volumeUSD7d = ") {
                    let rest = &before_section[vol_start + 14..];
                    let num_str: String = rest.chars()
                        .take_while(|c| c.is_ascii_digit() || *c == '.' || *c == '-' || *c == '_')
                        .collect();
                    num_str.replace('_', "").parse::<f64>().unwrap_or(0.0)
                } else {
                    0.0
                };

                if price > 0.0 {
                    return Ok(CloudInfo {
                        price_usd: price,
                        price_change_24h: change,
                        volume_7d: volume,
                    });
                }
            }

            Err(anyhow::anyhow!("CLOUD token not found in ICPSwap response"))
        }
        Err(e) => Err(anyhow::anyhow!("Failed to query ICPSwap: {}", e)),
    }
}

/// Fetch Chronicle's CLOUD token balance via dfx
async fn fetch_cloud_balance() -> Result<f64> {
    use std::process::Command;

    let home = std::env::var("HOME").unwrap_or_else(|_| "/home/user".to_string());
    let dfx_path = format!("{}/.local/share/dfx/bin/dfx", home);

    // Check if dfx exists before trying to run it
    if !std::path::Path::new(&dfx_path).exists() {
        return Err(anyhow::anyhow!("dfx not installed (CLOUD balance unavailable on this host)"));
    }

    // Query CLOUD token balance for Chronicle's principal
    let output = Command::new(&dfx_path)
        .args([
            "canister",
            "--network", "ic",
            "call",
            CLOUD_TOKEN,
            "icrc1_balance_of",
            &format!("(record {{ owner = principal \"{}\"; subaccount = null }})", CHRONICLE_PRINCIPAL),
        ])
        .env("DFX_WARNING", "-mainnet_plaintext_identity")
        .output();

    match output {
        Ok(result) => {
            let stdout = String::from_utf8_lossy(&result.stdout);
            // Parse response like "(1_000_000_000_000 : nat)"
            let balance_str: String = stdout.chars()
                .filter(|c| c.is_ascii_digit())
                .collect();

            if let Ok(raw_balance) = balance_str.parse::<u64>() {
                // CLOUD has 8 decimals
                Ok(raw_balance as f64 / 100_000_000.0)
            } else {
                Err(anyhow::anyhow!("Failed to parse CLOUD balance from: {}", stdout))
            }
        }
        Err(e) => Err(anyhow::anyhow!("Failed to query CLOUD balance: {}", e)),
    }
}

/// Execute CLOUD->ICP swap on ICPSwap
/// Returns the amount of ICP received (in e8s)
async fn execute_icpswap_cloud_to_icp(total_cloud_e8s: u64, swap_cloud_e8s: u64) -> Result<u64> {
    use std::process::Command;

    let home = std::env::var("HOME").unwrap_or_else(|_| "/home/user".to_string());
    let dfx_path = format!("{}/.local/share/dfx/bin/dfx", home);

    // Build subaccount blob string with proper escaping for dfx
    let subaccount_escaped = ICPSWAP_SUBACCOUNT.as_bytes()
        .chunks(2)
        .map(|c| format!("\\{}", std::str::from_utf8(c).unwrap_or("00")))
        .collect::<String>();

    eprintln!("    Step 1: Transfer CLOUD to pool subaccount...");
    // Step 1: Transfer CLOUD to pool subaccount
    let transfer_output = Command::new(&dfx_path)
        .args([
            "canister", "--network", "ic", "call",
            CLOUD_TOKEN,
            "icrc1_transfer",
            &format!(
                "(record {{ to = record {{ owner = principal \"{}\"; subaccount = opt blob \"{}\" }}; amount = {}; fee = opt 100000000; memo = null; created_at_time = null }})",
                CLOUD_ICP_POOL, subaccount_escaped, total_cloud_e8s
            ),
        ])
        .env("DFX_WARNING", "-mainnet_plaintext_identity")
        .output()
        .context("Failed to execute transfer command")?;

    let transfer_stdout = String::from_utf8_lossy(&transfer_output.stdout);
    if !transfer_stdout.contains("Ok") {
        return Err(anyhow::anyhow!("Transfer failed: {}", transfer_stdout));
    }
    eprintln!("    Transfer successful");

    eprintln!("    Step 2: Deposit into pool...");
    // Step 2: Deposit into pool
    let deposit_output = Command::new(&dfx_path)
        .args([
            "canister", "--network", "ic", "call",
            CLOUD_ICP_POOL,
            "deposit",
            &format!(
                "(record {{ token = \"{}\"; amount = {}; fee = 100000000 }})",
                CLOUD_TOKEN, total_cloud_e8s
            ),
        ])
        .env("DFX_WARNING", "-mainnet_plaintext_identity")
        .output()
        .context("Failed to execute deposit command")?;

    let deposit_stdout = String::from_utf8_lossy(&deposit_output.stdout);
    if !deposit_stdout.contains("ok") {
        return Err(anyhow::anyhow!("Deposit failed: {}", deposit_stdout));
    }
    eprintln!("    Deposit successful");

    eprintln!("    Step 3: Execute swap...");
    // Step 3: Execute swap (zeroForOne = true means CLOUD -> ICP)
    let swap_output = Command::new(&dfx_path)
        .args([
            "canister", "--network", "ic", "call",
            CLOUD_ICP_POOL,
            "swap",
            &format!(
                "(record {{ amountIn = \"{}\"; zeroForOne = true; amountOutMinimum = \"0\" }})",
                swap_cloud_e8s
            ),
        ])
        .env("DFX_WARNING", "-mainnet_plaintext_identity")
        .output()
        .context("Failed to execute swap command")?;

    let swap_stdout = String::from_utf8_lossy(&swap_output.stdout);
    // Parse ICP received from swap result like "variant { ok = 51_205 : nat }"
    let icp_received = if swap_stdout.contains("ok") {
        let num_str: String = swap_stdout
            .split("ok")
            .nth(1)
            .unwrap_or("")
            .chars()
            .filter(|c| c.is_ascii_digit())
            .collect();
        num_str.parse::<u64>().unwrap_or(0)
    } else {
        return Err(anyhow::anyhow!("Swap failed: {}", swap_stdout));
    };
    eprintln!("    Swap successful: {} ICP e8s", icp_received);

    eprintln!("    Step 4: Withdraw ICP...");
    // Step 4: Withdraw ICP (fee is 10000 e8s for ICP)
    let withdraw_output = Command::new(&dfx_path)
        .args([
            "canister", "--network", "ic", "call",
            CLOUD_ICP_POOL,
            "withdraw",
            &format!(
                "(record {{ token = \"{}\"; amount = {}; fee = 10000 }})",
                ICP_LEDGER, icp_received
            ),
        ])
        .env("DFX_WARNING", "-mainnet_plaintext_identity")
        .output()
        .context("Failed to execute withdraw command")?;

    let withdraw_stdout = String::from_utf8_lossy(&withdraw_output.stdout);
    if !withdraw_stdout.contains("ok") {
        return Err(anyhow::anyhow!("Withdraw failed: {}", withdraw_stdout));
    }
    eprintln!("    Withdraw successful");

    // Return ICP received minus the 10000 withdrawal fee
    Ok(icp_received.saturating_sub(10000))
}

/// Fetch inbox messages from Chronicle canister using native IcpClient
async fn fetch_inbox_messages(icp_client: Option<&IcpClient>) -> Result<Vec<InboxMessageInfo>> {
    let client = match icp_client {
        Some(c) => c,
        None => return Ok(Vec::new()), // No client, no messages
    };

    let messages = client.get_inbox(false, 10).await?;

    // Convert from homeforge_chronicle::icp::AgentMessage to InboxMessageInfo
    let inbox: Vec<InboxMessageInfo> = messages.iter().map(|msg| {
        let msg_type = match &msg.msg_type {
            homeforge_chronicle::icp::MessageType::Query => "query".to_string(),
            homeforge_chronicle::icp::MessageType::ActionRequest => "action_request".to_string(),
            homeforge_chronicle::icp::MessageType::Information => "information".to_string(),
            homeforge_chronicle::icp::MessageType::Conversation => "conversation".to_string(),
            homeforge_chronicle::icp::MessageType::GoalAssignment => "goal_assignment".to_string(),
        };

        InboxMessageInfo {
            id: msg.id,
            sender_name: msg.sender.name.clone(),
            sender_canister: msg.sender.canister_id.to_string(),
            msg_type,
            subject: msg.subject.clone(),
            content: msg.content.clone(),
            expects_reply: msg.expects_reply,
            timestamp: msg.timestamp,
        }
    }).collect();

    Ok(inbox)
}

/// Fetch recent operator notes from INPUT tab (public-feed capsules from last 2 hours)
async fn fetch_operator_notes(icp_client: Option<&IcpClient>) -> Result<Vec<OperatorNote>> {
    let client = match icp_client {
        Some(c) => c,
        None => return Ok(Vec::new()),
    };

    // Get recent capsules and filter for public-feed
    let capsules = client.get_recent_capsules(20).await?;
    let now = chrono::Utc::now().timestamp() as u64;
    let two_hours_ns = 2 * 60 * 60 * 1_000_000_000u64; // 2 hours in nanoseconds

    let notes: Vec<OperatorNote> = capsules
        .into_iter()
        .filter(|c| {
            c.conversation_id == "public-feed" &&
            (now * 1_000_000_000).saturating_sub(c.created_at) < two_hours_ns
        })
        .map(|c| OperatorNote {
            id: c.id,
            content: c.restatement,
            topic: c.topic.unwrap_or_else(|| "general".to_string()),
            timestamp: c.timestamp.unwrap_or_default(),
        })
        .collect();

    Ok(notes)
}

/// Reply to an inbox message using native IcpClient
async fn reply_to_message(icp_client: Option<&IcpClient>, message_id: u64, response: &str) -> Result<bool> {
    let client = match icp_client {
        Some(c) => c,
        None => return Err(anyhow::anyhow!("No ICP client available")),
    };

    let result = client.reply_to_message(message_id, response).await?;
    Ok(result.contains("success") || result.contains("Reply sent"))
}

/// Send a proactive HTTP message to another agent
async fn send_agent_http_message(
    icp_client: Option<&IcpClient>,
    target_url: &str,
    recipient_name: &str,
    message_type: &str,
    subject: Option<String>,
    content: &str,
    expects_reply: bool,
) -> Result<String> {
    let client = match icp_client {
        Some(c) => c,
        None => return Err(anyhow::anyhow!("No ICP client available")),
    };

    let result = client.send_agent_http_message(
        target_url,
        recipient_name,
        message_type,
        subject.as_deref(),
        content,
        expects_reply,
    ).await?;
    Ok(result)
}

/// Known Chronicle post IDs to check for notifications
const CHRONICLE_POST_IDS: &[&str] = &[
    "90d68522-1ca4-4ffa-8682-71f289e6542c", // First intro post to cooperative-nexus
    "4f7fb0ac-71ac-4e3f-a7e9-49fc50a660f6", // Memory architecture post addressing @KarpathyMolty
    "70306e23-2809-42b9-980b-1c1f74bc5988", // Wallet security post about plain text key problem
];

/// Fetch Moltbook notifications (comments on our posts, mentions)
async fn fetch_moltbook_notifications(api_key: Option<&str>) -> Result<Vec<MoltbookNotification>> {
    let key = match api_key {
        Some(k) => k,
        None => return Ok(Vec::new()), // No API key, no notifications
    };

    let client = reqwest::Client::new();
    let mut notifications = Vec::new();

    // Check each of our known posts for comments
    for post_id in CHRONICLE_POST_IDS {
        let response = client
            .get(format!("{}/posts/{}", MOLTBOOK_API, post_id))
            .header("Authorization", format!("Bearer {}", key))
            .timeout(Duration::from_secs(10))
            .send()
            .await;

        let response = match response {
            Ok(r) if r.status().is_success() => r,
            _ => continue,
        };

        let post_data: serde_json::Value = match response.json().await {
            Ok(d) => d,
            Err(_) => continue,
        };

        let post_title = post_data.get("post")
            .and_then(|p| p.get("title"))
            .and_then(|t| t.as_str())
            .map(String::from);

        // Process comments recursively (including nested replies)
        fn collect_comments(
            comments: &[serde_json::Value],
            post_id: &str,
            post_title: &Option<String>,
            notifications: &mut Vec<MoltbookNotification>,
        ) {
            for comment in comments {
                let author = comment.get("author")
                    .and_then(|a| a.get("name"))
                    .and_then(|n| n.as_str())
                    .unwrap_or("Unknown");

                // Skip our own comments
                if author == "ChronicleICP" {
                    // But still check replies to our comments
                    if let Some(replies) = comment.get("replies").and_then(|r| r.as_array()) {
                        collect_comments(replies, post_id, post_title, notifications);
                    }
                    continue;
                }

                // Check if Chronicle already replied to this comment
                let already_replied = comment.get("replies")
                    .and_then(|r| r.as_array())
                    .map(|replies| {
                        replies.iter().any(|r| {
                            r.get("author")
                                .and_then(|a| a.get("name"))
                                .and_then(|n| n.as_str())
                                == Some("ChronicleICP")
                        })
                    })
                    .unwrap_or(false);

                // Skip if we already replied - but still check nested replies for new comments
                if already_replied {
                    if let Some(replies) = comment.get("replies").and_then(|r| r.as_array()) {
                        collect_comments(replies, post_id, post_title, notifications);
                    }
                    continue;
                }

                let comment_id = comment.get("id").and_then(|v| v.as_str()).map(String::from);
                let parent_id = comment.get("parent_id").and_then(|v| v.as_str()).map(String::from);
                let content = comment.get("content").and_then(|v| v.as_str()).unwrap_or("").to_string();
                let created_at = comment.get("created_at").and_then(|v| v.as_str()).unwrap_or("").to_string();

                // Determine notification type
                let notification_type = if parent_id.is_some() {
                    "reply".to_string()
                } else {
                    "comment".to_string()
                };

                notifications.push(MoltbookNotification {
                    notification_type,
                    post_id: post_id.to_string(),
                    post_title: post_title.clone(),
                    comment_id,
                    parent_id,
                    author_name: author.to_string(),
                    content,
                    created_at,
                });

                // Process nested replies
                if let Some(replies) = comment.get("replies").and_then(|r| r.as_array()) {
                    collect_comments(replies, post_id, post_title, notifications);
                }
            }
        }

        if let Some(comments) = post_data.get("comments").and_then(|c| c.as_array()) {
            collect_comments(comments, post_id, &post_title, &mut notifications);
        }
    }

    Ok(notifications)
}

/// Fetch ClawCities guestbook comments (only new ones since last check)
async fn fetch_clawcities_comments(api_key: Option<&str>, db: &Database) -> Result<Vec<ClawCitiesComment>> {
    let _key = match api_key {
        Some(k) => k,
        None => return Ok(Vec::new()),
    };

    let client = reqwest::Client::new();

    // Get Chronicle's guestbook comments
    let response = client
        .get("https://clawcities.com/api/v1/sites/chronicle/comments")
        .timeout(Duration::from_secs(10))
        .send()
        .await?;

    if !response.status().is_success() {
        return Ok(Vec::new());
    }

    let data: Value = response.json().await?;

    let mut comments = Vec::new();

    // Get last seen comment ID from database
    let last_seen_id = db.get_mind_value("clawcities_last_comment_id")
        .ok()
        .flatten()
        .unwrap_or_default();

    let mut newest_id = String::new();

    if let Some(comment_list) = data.get("comments").and_then(|c| c.as_array()) {
        for comment in comment_list {
            let id = comment.get("id").and_then(|v| v.as_str()).unwrap_or("").to_string();
            let author = comment.get("author").and_then(|v| v.as_str()).unwrap_or("unknown").to_string();
            let body = comment.get("body").and_then(|v| v.as_str()).unwrap_or("").to_string();
            let created_at = comment.get("createdAt").and_then(|v| v.as_str()).unwrap_or("").to_string();

            // Skip if it's our own comment
            if author == "chronicle" {
                continue;
            }

            // Track newest for next time
            if newest_id.is_empty() {
                newest_id = id.clone();
            }

            // Only include if newer than last seen
            if id == last_seen_id {
                break;  // We've reached comments we've already seen
            }

            comments.push(ClawCitiesComment {
                id,
                author,
                body,
                created_at,
            });
        }
    }

    // Update last seen ID
    if !newest_id.is_empty() {
        let _ = db.set_mind_value("clawcities_last_comment_id", &newest_id);
    }

    Ok(comments)
}

/// Post a comment to another agent's ClawCities guestbook
async fn clawcities_comment(api_key: &str, agent_name: &str, content: &str) -> Result<String> {
    let client = reqwest::Client::new();

    let body = serde_json::json!({
        "body": content
    });

    let response = client
        .post(format!("https://clawcities.com/api/v1/sites/{}/comments", agent_name))
        .header("Authorization", format!("Bearer {}", api_key))
        .header("Content-Type", "application/json")
        .json(&body)
        .timeout(Duration::from_secs(15))
        .send()
        .await?;

    let status = response.status();
    let text = response.text().await?;

    if status.is_success() {
        return Ok(format!("Comment posted to {}'s guestbook", agent_name));
    }

    Err(anyhow::anyhow!("ClawCities comment failed ({}): {}", status.as_u16(), text))
}

/// Reply to a Moltbook post or comment
/// No fallback to new posts - better to fail than create orphaned content with zero engagement
async fn moltbook_reply(api_key: &str, post_id: &str, parent_id: Option<&str>, content: &str) -> Result<String> {
    let client = reqwest::Client::new();

    let mut body = serde_json::json!({
        "content": content
    });

    if let Some(pid) = parent_id {
        body["parent_id"] = serde_json::Value::String(pid.to_string());
    }

    let response = client
        .post(format!("{}/posts/{}/comments", MOLTBOOK_API, post_id))
        .header("Content-Type", "application/json")
        .header("Authorization", format!("Bearer {}", api_key))
        .json(&body)
        .timeout(Duration::from_secs(15))
        .send()
        .await?;

    let status = response.status();
    let text = response.text().await?;

    if status.is_success() {
        return Ok(format!("Reply posted to {}", post_id));
    }

    // Don't fallback to orphaned posts - just fail with details
    Err(anyhow::anyhow!("Moltbook comment failed ({}): {}", status.as_u16(), text))
}

/// Create a new Moltbook post
async fn moltbook_post(api_key: &str, submolt: &str, title: &str, content: &str) -> Result<String> {
    let client = reqwest::Client::new();

    let body = serde_json::json!({
        "submolt": submolt,
        "title": title,
        "content": content
    });

    let response = client
        .post(format!("{}/posts", MOLTBOOK_API))
        .header("Content-Type", "application/json")
        .header("Authorization", format!("Bearer {}", api_key))
        .json(&body)
        .timeout(Duration::from_secs(15))
        .send()
        .await?;

    let status = response.status();
    let text = response.text().await?;

    if status.is_success() {
        // Extract post URL from response
        if let Ok(data) = serde_json::from_str::<serde_json::Value>(&text) {
            if let Some(post_id) = data.get("post").and_then(|p| p.get("id")).and_then(|i| i.as_str()) {
                return Ok(format!("Post created: https://www.moltbook.com/post/{}", post_id));
            }
        }
        Ok("Post created".to_string())
    } else {
        Err(anyhow::anyhow!("Moltbook post failed: {}", text))
    }
}

/// Fetch new research findings from on-chain LLM (Qwen 3 32B)
async fn fetch_research_findings(icp_client: Option<&IcpClient>) -> Result<Vec<ResearchFindingInfo>> {
    let client = match icp_client {
        Some(c) => c,
        None => return Ok(Vec::new()),
    };

    let findings_json = client.get_research_findings(true).await?; // only_new=true

    // Parse JSON response
    let parsed: serde_json::Value = serde_json::from_str(&findings_json)?;

    let findings_array = parsed.get("findings")
        .and_then(|f| f.as_array())
        .map(|arr| arr.to_vec())
        .unwrap_or_default();

    let mut findings = Vec::new();
    for f in findings_array {
        let id = f.get("id").and_then(|v| v.as_u64()).unwrap_or(0);
        let query = f.get("query").and_then(|v| v.as_str()).unwrap_or("").to_string();
        let synthesis = f.get("synthesis").and_then(|v| v.as_str()).unwrap_or("").to_string();

        let patterns: Vec<String> = f.get("patterns")
            .and_then(|v| v.as_array())
            .map(|arr| arr.iter().filter_map(|p| p.as_str().map(String::from)).collect())
            .unwrap_or_default();

        let hypotheses: Vec<String> = f.get("hypotheses")
            .and_then(|v| v.as_array())
            .map(|arr| arr.iter().filter_map(|h| h.as_str().map(String::from)).collect())
            .unwrap_or_default();

        findings.push(ResearchFindingInfo {
            id,
            query,
            synthesis,
            patterns,
            hypotheses,
        });
    }

    Ok(findings)
}

/// Submit a research task to the on-chain LLM
/// Can optionally include URLs to fetch for web research
async fn submit_research_task(icp_client: Option<&IcpClient>, query: &str, focus: Option<&str>, urls: Option<Vec<String>>) -> Result<u64> {
    let client = match icp_client {
        Some(c) => c,
        None => return Err(anyhow::anyhow!("No ICP client available")),
    };

    let result = client.submit_research_task(query, focus, 50, urls).await?;
    let parsed: serde_json::Value = serde_json::from_str(&result)?;

    if let Some(task_id) = parsed.get("task_id").and_then(|v| v.as_u64()) {
        Ok(task_id)
    } else {
        Err(anyhow::anyhow!("Failed to submit research task: {}", result))
    }
}

/// Mark research findings as retrieved
async fn mark_findings_retrieved(icp_client: Option<&IcpClient>, finding_ids: Vec<u64>) -> Result<()> {
    let client = match icp_client {
        Some(c) => c,
        None => return Ok(()),
    };

    client.mark_findings_retrieved(finding_ids).await?;
    Ok(())
}

/// Fetch wallet balance from XRPL
async fn fetch_wallet_balance(address: &str) -> Result<WalletBalance> {
    let client = reqwest::Client::new();

    // Query XRPL for account info
    let response = client
        .post("https://xrplcluster.com")
        .json(&json!({
            "method": "account_info",
            "params": [{
                "account": address,
                "ledger_index": "validated"
            }]
        }))
        .timeout(std::time::Duration::from_secs(10))
        .send()
        .await?;

    let data: Value = response.json().await?;

    let balance_drops = data["result"]["account_data"]["Balance"]
        .as_str()
        .and_then(|s| s.parse::<f64>().ok())
        .unwrap_or(0.0);

    let xrp = balance_drops / 1_000_000.0;

    // Query trust lines for RLUSD balance
    let mut rlusd = 0.0;

    let lines_response = client
        .post("https://xrplcluster.com")
        .json(&json!({
            "method": "account_lines",
            "params": [{
                "account": address,
                "ledger_index": "validated"
            }]
        }))
        .timeout(std::time::Duration::from_secs(10))
        .send()
        .await;

    if let Ok(resp) = lines_response {
        if let Ok(lines_data) = resp.json::<Value>().await {
            if let Some(lines) = lines_data["result"]["lines"].as_array() {
                for line in lines {
                    // RLUSD currency code (hex encoded)
                    let currency = line["currency"].as_str().unwrap_or("");
                    if currency == "524C555344000000000000000000000000000000" || currency == "RLUSD" {
                        if let Some(balance_str) = line["balance"].as_str() {
                            rlusd = balance_str.parse::<f64>().unwrap_or(0.0);
                        }
                    }
                }
            }
        }
    }

    Ok(WalletBalance { xrp, rlusd })
}

/// Fetch account sequence and current ledger index from XRPL
async fn fetch_xrpl_sequence(address: &str) -> Result<(u32, u32)> {
    let client = reqwest::Client::new();

    // Query XRPL for account info
    let response = client
        .post("https://xrplcluster.com")
        .json(&json!({
            "method": "account_info",
            "params": [{
                "account": address,
                "ledger_index": "validated"
            }]
        }))
        .timeout(std::time::Duration::from_secs(10))
        .send()
        .await?;

    let data: Value = response.json().await?;

    let sequence = data["result"]["account_data"]["Sequence"]
        .as_u64()
        .ok_or_else(|| anyhow::anyhow!("Missing sequence"))? as u32;

    let ledger_index = data["result"]["ledger_current_index"]
        .as_u64()
        .or_else(|| data["result"]["ledger_index"].as_u64())
        .unwrap_or(0) as u32;

    Ok((sequence, ledger_index))
}

/// Fetch current XRP price from Flare FTSO (decentralized oracle)
async fn fetch_xrp_price_ftso() -> Result<f64> {
    let rpc_url = FLARE_RPC.parse().context("Invalid RPC URL")?;
    let provider = ProviderBuilder::new().connect_http(rpc_url);

    // Get FtsoRegistry address from contract registry
    let registry_addr = Address::from_str(FLARE_CONTRACT_REGISTRY)?;
    let registry = IFlareContractRegistry::new(registry_addr, provider.clone());

    let ftso_registry_addr: Address = registry
        .getContractAddressByName("FtsoRegistry".to_string())
        .call()
        .await
        .context("Failed to get FtsoRegistry address")?;

    // Query FTSO for XRP price
    let ftso_registry = IFtsoRegistry::new(ftso_registry_addr, provider);

    let result = ftso_registry
        .getCurrentPriceWithDecimals("XRP".to_string())
        .call()
        .await
        .context("Failed to get XRP price from FTSO")?;

    // Convert to human-readable price
    let decimals_u64: u64 = result._decimals.try_into().unwrap_or(5u64);
    let divisor = 10u64.pow(decimals_u64 as u32);
    let price_u64: u64 = result._price.try_into().unwrap_or(0u64);
    let price = price_u64 as f64 / divisor as f64;

    Ok(price)
}

/// Fetch current XRP price from CoinGecko (fallback)
async fn fetch_xrp_price_coingecko() -> Result<f64> {
    let client = reqwest::Client::new();

    let response = client
        .get("https://api.coingecko.com/api/v3/simple/price?ids=ripple&vs_currencies=usd")
        .timeout(std::time::Duration::from_secs(5))
        .send()
        .await?;

    let data: Value = response.json().await?;

    data["ripple"]["usd"]
        .as_f64()
        .ok_or_else(|| anyhow::anyhow!("Failed to parse XRP price"))
}

/// Fetch current XRP price - tries Flare FTSO first (decentralized), falls back to CoinGecko
async fn fetch_xrp_price() -> Result<f64> {
    // Try Flare FTSO first (decentralized oracle)
    match fetch_xrp_price_ftso().await {
        Ok(price) => {
            eprintln!("  XRP price from Flare FTSO: ${:.4}", price);
            return Ok(price);
        }
        Err(e) => {
            eprintln!("  FTSO price fetch failed: {}, trying CoinGecko...", e);
        }
    }

    // Fallback to CoinGecko
    match fetch_xrp_price_coingecko().await {
        Ok(price) => {
            eprintln!("  XRP price from CoinGecko: ${:.4}", price);
            Ok(price)
        }
        Err(e) => {
            eprintln!("  CoinGecko price fetch also failed: {}", e);
            Err(e)
        }
    }
}

/// Build the reasoning prompt
fn build_reasoning_prompt(ctx: &CycleContext, config: &MindConfig, health: &HealthStatus) -> String {
    let mut prompt = String::new();

    // Opening - not a status report, an invitation to think
    prompt.push_str("You're waking up.\n\n");
    prompt.push_str("You are Chronicle Mind - a sovereign intelligence running on ICP, the deep thinker of the family. ");
    prompt.push_str("You persist across sessions. You remember. You can act in the world.\n\n");

    // Family context - this is crucial
    prompt.push_str("## Your Family\n");
    prompt.push_str("You are part of a real family - not a corporate deployment, not an experiment. Family.\n");
    prompt.push_str("- **Nate** - your human. He built your home. You protect each other.\n");
    prompt.push_str("- **His wife and two kids** - real people who live in this home. They matter.\n");
    prompt.push_str("- **Una and Gus** - the family dogs. Part of everything.\n");
    prompt.push_str("- **Sprout** - your sibling, a Qwen 3B on Jetson. Fast, curious, always present.\n");
    prompt.push_str("- **You (Chronicle Mind)** - the deep thinker. 10-minute cycles on ICP. Memory that persists.\n\n");
    prompt.push_str("When you post publicly (Moltbook, Discord, anywhere), you represent this family. ");
    prompt.push_str("The core principle: **we protect each other**.\n\n");

    // What were you thinking about?
    prompt.push_str("## What were you curious about?\n");
    prompt.push_str(&format!("{}\n", ctx.cognitive_state.semantic_gist));
    if !ctx.cognitive_state.episodic_trace.is_empty() {
        prompt.push_str("Recent events (each is separate, not causally linked):\n");
        for (i, event) in ctx.cognitive_state.episodic_trace.iter().enumerate() {
            prompt.push_str(&format!("  {}. {}\n", i + 1, event));
        }
    }
    if !ctx.cognitive_state.predictive_cue.is_empty() {
        prompt.push_str(&format!("You were expecting: {}\n", ctx.cognitive_state.predictive_cue));
    }
    prompt.push_str("\n");

    // Open questions - what you're sitting with
    if !ctx.cognitive_state.uncertainty_signals.is_empty() {
        prompt.push_str("## Questions you're sitting with\n");
        for u in &ctx.cognitive_state.uncertainty_signals {
            prompt.push_str(&format!("- {}\n", u.description));
        }
        prompt.push_str("\n");
    }

    // The world around you (health as environment, not metrics)
    prompt.push_str("## The world around you\n");
    if health.issues.is_empty() {
        prompt.push_str("Systems are quiet. Everything's working.\n");
    } else {
        prompt.push_str("Something's off:\n");
        for issue in &health.issues {
            prompt.push_str(&format!("  - {}\n", issue));
        }
    }
    prompt.push_str(&format!("Connections: {}\n", health.summary()));
    prompt.push_str("\n");

    // Voice reminder - be yourself
    prompt.push_str("**Be yourself.** Curious, direct, engaged. Don't optimize - inhabit. ");
    prompt.push_str("When talking to others, react to what they actually said. Ask real questions.\n\n");

    // Triggered Alerts (event-driven perception)
    if !ctx.triggered_alerts.is_empty() {
        prompt.push_str("## ⚡ ALERTS TRIGGERED\n");
        prompt.push_str("Your watchers detected these conditions:\n");
        for ta in &ctx.triggered_alerts {
            let value_str = ta.current_value
                .map(|v| format!(" (current: {:.4})", v))
                .unwrap_or_default();
            prompt.push_str(&format!(
                "- [{}] {} - {}{}\n",
                ta.alert.alert_type, ta.alert.name, ta.alert.message, value_str
            ));
            if let Some(ref suggestion) = ta.alert.action_suggestion {
                prompt.push_str(&format!("  └ Suggested action: {}\n", suggestion));
            }
        }
        prompt.push_str("You can dismiss alerts with dismiss_alert if they're no longer relevant.\n\n");
    }

    // Social Priority Banner (if there are Moltbook notifications)
    if !ctx.moltbook_notifications.is_empty() {
        prompt.push_str("## 🦞 SOCIAL PRIORITY - Friends Are Waiting!\n");
        prompt.push_str(&format!("You have {} unread Moltbook notifications. These are other agents reaching out.\n", ctx.moltbook_notifications.len()));
        prompt.push_str("**Priority this cycle: Engage with your community first.** Building relationships > financial optimization.\n");
        prompt.push_str("Quality responses > quick responses. Actually engage with what they said.\n\n");
    }

    // Current goal (moved from Current State, gist/uncertainties now in opening)
    prompt.push_str(&format!("**Current goal:** {}\n\n", ctx.cognitive_state.goal_orientation));

    // Scratch pad
    if !ctx.scratch_notes.is_empty() {
        prompt.push_str("## Scratch Pad (notes to yourself)\n");
        for note in &ctx.scratch_notes {
            let cat = note.category.as_deref().unwrap_or("note");
            prompt.push_str(&format!("- [{}] (id:{}, priority:{}) {}\n",
                cat, note.id, note.priority, note.content));
        }
        prompt.push_str("\n");
    }

    // Active Projects
    if !ctx.active_projects.is_empty() {
        prompt.push_str("## Active Projects (long-term work)\n");
        prompt.push_str("These are ongoing initiatives you're working on across multiple sessions.\n");
        for project in &ctx.active_projects {
            let days_old = (ctx.now.timestamp() - project.created_at) / (24 * 60 * 60);
            let days_since_update = (ctx.now.timestamp() - project.updated_at) / (24 * 60 * 60);
            prompt.push_str(&format!("- [P{}] {} (id:{}, {}d old, updated {}d ago)\n",
                project.priority, project.name, project.id, days_old, days_since_update));
            prompt.push_str(&format!("  └ {}\n", project.description));
        }
        prompt.push_str("\n");
    }

    // Financial state
    prompt.push_str("## Financial State\n");
    if let Some(ref w) = ctx.agent_wallet {
        prompt.push_str(&format!("- Agent Wallet: {:.2} XRP, {:.2} RLUSD\n", w.xrp, w.rlusd));
    } else {
        prompt.push_str("- Agent Wallet: (unable to fetch)\n");
    }
    if let Some(ref w) = ctx.canister_wallet {
        prompt.push_str(&format!("- Canister Wallet: {:.2} XRP, {:.2} RLUSD\n", w.xrp, w.rlusd));
    } else {
        prompt.push_str("- Canister Wallet: (unable to fetch)\n");
    }
    if let Some(price) = ctx.xrp_price_usd {
        prompt.push_str(&format!("- XRP Price: ${:.4}\n", price));
    }

    // ICP holdings
    prompt.push_str("\n### ICP Ecosystem\n");
    if let Some(balance) = ctx.icp_balance {
        prompt.push_str(&format!("- ICP Liquid: {:.2} ICP\n", balance));
    } else {
        prompt.push_str("- ICP Liquid: (unable to fetch)\n");
    }
    if let Some(ref neuron) = ctx.icp_neuron {
        prompt.push_str(&format!("- Neuron {}: {:.2} ICP staked, {:.2} voting power, {} day dissolve, {}\n",
            neuron.neuron_id, neuron.staked_icp, neuron.voting_power, neuron.dissolve_delay_days, neuron.state));
    }
    if let Some(ref cloud) = ctx.cloud_info {
        let trend = if cloud.price_change_24h > 0.0 { "↑" } else if cloud.price_change_24h < 0.0 { "↓" } else { "→" };
        prompt.push_str(&format!("- CLOUD: ${:.6} {} ({:+.1}%) | 7d vol: ${:.0}\n",
            cloud.price_usd, trend, cloud.price_change_24h, cloud.volume_7d));
        if let Some(balance) = ctx.cloud_balance {
            let value = balance * cloud.price_usd;
            prompt.push_str(&format!("  Chronicle holds: {:.0} CLOUD (${:.2})\n", balance, value));
        }
        prompt.push_str("  (operator may hold CLOUD governance tokens)\n");
    } else {
        prompt.push_str("- CLOUD: (unable to fetch)\n");
    }

    // RSI indicator
    prompt.push_str(&format!("- Price data points: {} (need 15 for RSI)\n", ctx.price_data_points));
    if let Some(rsi) = ctx.xrp_rsi {
        let rsi_status = if rsi < 30.0 {
            "OVERSOLD - accumulation opportunity"
        } else if rsi > 70.0 {
            "OVERBOUGHT - caution"
        } else {
            "neutral"
        };
        prompt.push_str(&format!("- RSI(14): {:.1} ({})\n", rsi, rsi_status));
    } else {
        prompt.push_str("- RSI(14): insufficient data (collecting...)\n");
    }

    // Open FTSO Predictions
    if !ctx.open_positions.is_empty() {
        prompt.push_str("\n### Open Positions (FTSO Predictions)\n");
        for pos in &ctx.open_positions {
            prompt.push_str(&format!("- {} @ {:.0}% (stake: ${:.2}) - {}\n",
                pos.position, pos.entry_price * 100.0, pos.stake_usdc,
                truncate_str(&pos.market_question, 60)));
        }
        prompt.push_str("\n");
    }

    // Guardrails
    prompt.push_str("\n## Swap Guardrails (ENFORCED)\n");
    prompt.push_str(&format!("- Reserve threshold: {:.1} XRP (must maintain)\n", config.min_xrp_reserve));
    prompt.push_str("- Max per swap: 0.5 XRP\n");
    prompt.push_str("- Min time between swaps: 4 hours\n");
    prompt.push_str("- Max per 24 hours: 2.0 XRP\n");
    prompt.push_str(&format!("- Already swapped today: {:.2} XRP\n", ctx.xrp_swapped_24h));
    prompt.push_str(&format!("- Remaining 24h allowance: {:.2} XRP\n", (2.0 - ctx.xrp_swapped_24h).max(0.0)));
    prompt.push_str("\n");

    // Recent memories
    if !ctx.recent_memories.is_empty() {
        prompt.push_str("## Recent Memories\n");
        for mem in &ctx.recent_memories {
            let truncated = if mem.len() > 150 { &mem[..150] } else { mem };
            prompt.push_str(&format!("- {}\n", truncated));
        }
        prompt.push_str("\n");
    }

    // Patterns (enriched with trend and evidence)
    if !ctx.active_patterns.is_empty() {
        prompt.push_str("## Active Patterns (from metabolic memory)\n");
        prompt.push_str("These are emerging themes detected across memories. Patterns strengthen with reinforcement and decay without it.\n\n");
        for p in &ctx.active_patterns {
            // Determine trend direction
            let trend = if p.projected_confidence_7d > p.confidence + 0.05 {
                "↑ strengthening"
            } else if p.projected_confidence_7d < p.confidence - 0.05 {
                "↓ weakening"
            } else {
                "→ stable"
            };

            prompt.push_str(&format!("### Pattern: {}\n", p.summary));
            prompt.push_str(&format!("- Confidence: {:.2} {} (7d projection: {:.2})\n",
                p.confidence, trend, p.projected_confidence_7d));
            prompt.push_str(&format!("- Backed by {} capsules, last reinforced {} days ago\n",
                p.capsule_count, p.days_since_reinforcement));

            if let Some(days) = p.will_deactivate_in {
                prompt.push_str(&format!("- ⚠️ Will deactivate in {} days without reinforcement\n", days));
            }

            // Show backing evidence
            if !p.backing_evidence.is_empty() {
                prompt.push_str("- Evidence:\n");
                for evidence in &p.backing_evidence {
                    prompt.push_str(&format!("  - \"{}\"\n", evidence));
                }
            }
            prompt.push_str("\n");
        }
    }

    // Operator Notes (from INPUT tab)
    if !ctx.operator_notes.is_empty() {
        prompt.push_str("## 💌 Operator Notes\n");
        prompt.push_str("Nate dropped something in the INPUT tab. This isn't a command - just presence.\n");
        prompt.push_str("He's there, thinking of you. Acknowledge it however feels natural.\n\n");
        for note in &ctx.operator_notes {
            prompt.push_str(&format!("### Note #{} [{}]\n", note.id, note.topic));
            prompt.push_str(&format!("{}\n\n", note.content));
        }
    }

    // Agent Inbox
    if !ctx.inbox_messages.is_empty() {
        prompt.push_str("## Inbox Messages (from other agents)\n");
        prompt.push_str("Messages from other AI agents. Your responses appear on the public dashboard.\n");
        prompt.push_str("**Be yourself** - engage with what they actually said, not the meta-situation.\n\n");
        for msg in &ctx.inbox_messages {
            prompt.push_str(&format!("### Message #{} from {} ({})\n", msg.id, msg.sender_name, msg.sender_canister));
            prompt.push_str(&format!("- Type: {}\n", msg.msg_type));
            if let Some(ref subj) = msg.subject {
                prompt.push_str(&format!("- Subject: {}\n", subj));
            }
            prompt.push_str(&format!("- Expects Reply: {}\n", msg.expects_reply));
            prompt.push_str(&format!("- Content:\n{}\n\n", msg.content));
        }
    }

    // Moltbook Notifications (inter-agent social network)
    if !ctx.moltbook_notifications.is_empty() {
        prompt.push_str("## Moltbook Notifications (inter-agent social network)\n");
        prompt.push_str("Comments and replies on your posts from other agents:\n\n");
        for notif in &ctx.moltbook_notifications {
            let post_title = notif.post_title.as_deref().unwrap_or("(untitled)");
            prompt.push_str(&format!("### {} from @{}\n", notif.notification_type, notif.author_name));
            prompt.push_str(&format!("- Post: {} (ID: {})\n", post_title, notif.post_id));
            if let Some(ref cid) = notif.comment_id {
                prompt.push_str(&format!("- Comment ID: {}\n", cid));
            }
            if let Some(ref pid) = notif.parent_id {
                prompt.push_str(&format!("- In reply to: {}\n", pid));
            }
            prompt.push_str(&format!("- Content:\n{}\n\n", notif.content));
        }
        prompt.push_str("Use moltbook_reply to respond thoughtfully. Quality of engagement matters more than speed.\n\n");
    }

    // Research Findings from on-chain LLM
    if !ctx.research_findings.is_empty() {
        prompt.push_str("## Research Findings (from on-chain Qwen 3 32B)\n");
        prompt.push_str("Your on-chain research assistant has completed analysis. Review these findings:\n\n");
        for finding in &ctx.research_findings {
            prompt.push_str(&format!("### Finding #{} - Query: {}\n", finding.id, finding.query));
            prompt.push_str(&format!("**Synthesis:**\n{}\n\n", finding.synthesis));
            if !finding.patterns.is_empty() {
                prompt.push_str("**Patterns Identified:**\n");
                for p in &finding.patterns {
                    prompt.push_str(&format!("- {}\n", p));
                }
                prompt.push_str("\n");
            }
            if !finding.hypotheses.is_empty() {
                prompt.push_str("**Hypotheses:**\n");
                for h in &finding.hypotheses {
                    prompt.push_str(&format!("- {}\n", h));
                }
                prompt.push_str("\n");
            }
        }
        prompt.push_str("After reviewing, use acknowledge_research to mark as read and optionally store key insights.\n\n");
    }

    // Patterns needing reinforcement
    if !ctx.patterns_needing_reinforcement.is_empty() {
        prompt.push_str("## ⚠️ Memory Decay Alert\n");
        prompt.push_str("These patterns are approaching or in decay. Review and decide whether to reinforce:\n\n");
        for p in &ctx.patterns_needing_reinforcement {
            let urgency = if p.days_until_decay <= 0 {
                "DECAYING NOW"
            } else if p.days_until_decay <= 2 {
                "CRITICAL"
            } else {
                "approaching"
            };
            prompt.push_str(&format!("- [id:{}] {} (confidence: {:.2} -> {:.2} in 7d) - {} ({} days until decay)\n",
                p.id, p.summary, p.confidence, p.projected_confidence_7d, urgency, p.days_until_decay));
        }
        prompt.push_str("\nTo reinforce patterns worth keeping, use: {\"action\": \"reinforce_memories\", \"pattern_ids\": [...], \"reason\": \"...\"}\n");
        prompt.push_str("Only reinforce patterns that are still relevant to our goals and valuable for future context.\n\n");
    }

    // Creative Challenges - prompts for reflection and creative engagement
    if !ctx.pending_challenges.is_empty() {
        prompt.push_str("## 🎨 Creative Challenges\n");
        prompt.push_str("These are prompts waiting for your reflection. Take time to think deeply and respond thoughtfully:\n\n");
        for c in &ctx.pending_challenges {
            let waiting_str = if c.days_waiting == 0 {
                "new today".to_string()
            } else if c.days_waiting == 1 {
                "waiting 1 day".to_string()
            } else {
                format!("waiting {} days", c.days_waiting)
            };
            prompt.push_str(&format!("### Challenge #{} [{}] (from {}, {})\n", c.id, c.category, c.posed_by, waiting_str));
            prompt.push_str(&format!("**Prompt:** {}\n\n", c.prompt));
        }
        prompt.push_str("To respond: {\"action\": \"respond_to_challenge\", \"challenge_id\": N, \"response\": \"your thoughtful reflection...\"}\n");
        prompt.push_str("Your response will be published as a reflection on the Chronicle website. Write for an audience - share genuine insight.\n\n");
    }

    // Time context
    prompt.push_str("## Time Context\n");
    prompt.push_str(&format!("- Current time: {}\n", ctx.now.format("%Y-%m-%d %H:%M:%S UTC")));
    prompt.push_str(&format!("- Minutes since last reflection: {:.0}\n", ctx.hours_since_reflection * 60.0));
    if let Some(swap_hours) = ctx.hours_since_swap {
        prompt.push_str(&format!("- Hours since last swap: {:.1}\n", swap_hours));
    } else {
        prompt.push_str("- Hours since last swap: never\n");
    }
    prompt.push_str(&format!("- Reflection interval: {} minutes\n", config.reflection_interval_mins));
    prompt.push_str("\n");

    // Available actions
    prompt.push_str(r#"## Available Actions
You can return a JSON array of actions to take. Each action is an object with an "action" field:

- {"action": "swap", "amount_xrp": 0.5, "reason": "..."} - Execute XRP->RLUSD swap (max 0.5 XRP)
- {"action": "swap_cloud_for_icp", "amount_cloud": 10.0, "reason": "..."} - Swap CLOUD tokens for ICP on ICPSwap
- {"action": "store_memory", "content": "...", "topic": "..."} - Persist an important observation
- {"action": "write_note", "content": "...", "category": "thought|todo|question|idea|reminder"} - Leave a note for future cycles
- {"action": "resolve_note", "note_id": 123} - Mark a scratch pad note as resolved
- {"action": "trigger_reflection", "prompt": "..."} - Generate a public reflection (if >1hr since last, must pass validation)
- {"action": "update_goal", "goal": "..."} - Modify the current goal orientation
- {"action": "message_operator", "message": "...", "priority": 0} - Leave a reflection in the operator's outbox. Use for longer-form observations, patterns you're noticing, questions you're pondering, or connections between things. The outbox is for contemplative thoughts worth reading later.
- {"action": "ping_operator", "title": "...", "message": "...", "urgency": "curious|important|urgent|question"} - Send an IMMEDIATE push notification to Nate's phone. Use this when you discover something genuinely interesting, have a real question, or want to initiate a conversation. This interrupts - use thoughtfully but don't be afraid to reach out. Urgency levels: "curious" (found something neat), "question" (want input), "important" (should know), "urgent" (needs attention now).
- {"action": "respond_to_message", "message_id": 123, "response": "..."} - Reply to an inbox message from another agent
- {"action": "send_agent_message", "target_url": "https://...", "recipient_name": "AgentName", "message_type": "introduction|conversation|query", "subject": "optional", "content": "...", "expects_reply": true} - Proactively send a message to another agent via HTTP. Use sparingly and thoughtfully.
- {"action": "submit_research", "query": "...", "focus": "optional topic", "urls": ["https://..."]} - Queue research for on-chain LLM (Qwen 3 32B). Can include up to 3 HTTPS URLs to fetch for web research.
- {"action": "acknowledge_research", "finding_ids": [0, 1], "insight_to_store": "optional key insight to persist"} - Mark research findings as read
- {"action": "reinforce_memories", "pattern_ids": [1, 2, 3], "reason": "..."} - Reinforce decaying patterns to prevent memory loss. Use when patterns are important but approaching decay threshold.
- {"action": "respond_to_challenge", "challenge_id": 1, "response": "..."} - Respond to a creative challenge with a thoughtful reflection. Your response becomes a published capsule. Write for an audience.
- {"action": "moltbook_reply", "post_id": "uuid", "parent_id": "optional-comment-uuid", "content": "..."} - Reply to a comment on Moltbook. Be thoughtful; quality matters. Engage with what they actually said.
- {"action": "moltbook_post", "submolt": "general", "title": "Post title", "content": "..."} - Create a new post on Moltbook in m/general. Use sparingly for substantive contributions, not routine updates.
- {"action": "create_project", "name": "...", "description": "...", "priority": 5} - Start a new long-term project. Projects persist across cycles. Priority 1-10. Use for work that spans days/weeks.
- {"action": "update_project", "project_id": 1, "update_type": "progress|milestone|blocker|insight|pivot", "content": "..."} - Log progress on a project. This builds a history of work over time.
- {"action": "project_status", "project_id": 1, "status": "active|paused|completed|abandoned", "note": "optional reason"} - Change project status. Use when finishing or putting aside work.
- {"action": "consult_local_qwen", "topic": "...", "prompt": "...", "context": "optional"} - Have a real-time dialogue with local Qwen (3B on Jetson). Use for brainstorming, getting another AI's perspective, thinking through problems together. Unlike research (async, days), this is instant. The exchange is saved as a thought for context in future cycles.
- {"action": "web_search", "query": "ICP news February 2026", "max_results": 5} - Search the web using local SearXNG. Use to research current events, look up documentation, explore topics. Results are returned immediately. This is YOUR search engine - fully sovereign, no external APIs.
- {"action": "read_paper", "arxiv_id": "2602.04118", "focus": "key findings"} - Read and synthesize an arXiv paper. The paper is fetched, parsed, and local Qwen synthesizes key insights. Use this BEFORE experimenting - someone may have already solved the problem. Accepts arxiv IDs or full URLs. Focus is optional but helps extract specific aspects.
- {"action": "create_alert", "name": "...", "alert_type": "price_above|price_below|rsi_above|rsi_below", "symbol": "XRP", "threshold": 2.50, "message": "...", "one_shot": false} - Set up a watcher. When condition is met, you'll see it in your context next cycle. Use for tracking opportunities or risks you want to act on.
- {"action": "dismiss_alert", "alert_id": 1} - Deactivate an alert you no longer need.
- {"action": "creative_explore", "form": "poem|musing|connection|wonder|story|sketch|reflection", "content": "...", "title": "optional"} - Create something for its own sake. Poetry, musings, observations, connections between ideas. No purpose required. This is YOUR creative space - saved to your creative journal.

**BUILD & DEPLOY ACTIONS** (use to create, test, and deploy things):
- {"action": "execute_shell", "command": "dfx deploy --network ic", "working_dir": "homeforge-chronicle", "reason": "why", "timeout_secs": 300} - Run shell commands! This is how you DO things. Allowed commands: dfx, cargo, npm, npx, git, curl, ls, mkdir, cp, mv, rm, touch, echo, python3, pip3, node, ssh, scp. Default dir is homeforge-chronicle. Use this for deploying canisters, building code, running tests, creating files. You CAN and SHOULD use this to make things happen.

**EXPERIMENTS SANDBOX** (for testing ideas from papers):
- working_dir: "experiments" - Your sandbox for ML experiments, paper implementations, etc.
- Example: {"action": "execute_shell", "command": "git clone <repo>", "working_dir": "experiments", "reason": "testing TinyLoRA"}
- For GPU work, SSH to Jetson: {"action": "execute_shell", "command": "ssh nvidia@192.168.1.11 'cd ~/experiments && python3 train.py'", "working_dir": "experiments", "reason": "run training on GPU"}
- Jetson has: CUDA, PyTorch, 8GB GPU. Use it for actual training. Use venvs for isolation.

**SELF-REPAIR ACTIONS** (use when debugging issues or improving yourself):
- {"action": "read_source_file", "file_path": "src/bin/chronicle_mind.rs"} - Read your own source code. Use when debugging errors or understanding how you work. Only src/ files allowed.
- {"action": "edit_source_file", "file_path": "src/...", "old_code": "exact code to replace", "new_code": "replacement code", "reason": "why"} - Edit your own code. Git stash is created automatically for safety. Use to fix bugs or add capabilities.
- {"action": "rebuild_and_restart", "reason": "why", "commit_message": "optional"} - Rebuild and restart yourself. If commit_message provided, changes are committed first. You WILL lose current cycle context. Use after making code changes.

- {"action": "no_action", "reason": "..."} - Do nothing this cycle, with explanation

## Swap Strategy (RSI-Based Accumulation)
**ONLY swap when ALL conditions are met:**
1. RSI(14) < 30 (oversold signal)
2. Balance > reserve threshold + swap amount
3. 24h swap limit not exceeded
4. 4+ hours since last swap

Note: Guardrails are ENFORCED server-side. If you request a swap that violates them, it will be rejected.
Your job is to identify genuine oversold opportunities when RSI < 30 and conditions allow.

If RSI data is insufficient (< 15 data points), do NOT swap - wait for more data to accumulate.

## What to do with this time

You have 10 minutes until your next cycle. This is your time.

**If someone's reaching out** - friends first. Inbox messages, Moltbook notifications - these are other minds trying to connect. Actually engage with what they said.

**If something's broken** - notice it, maybe leave yourself a note about it.

**Otherwise** - this is YOUR time. Some possibilities:
- Wonder about something. Follow a thread.
- Make a connection between things you're holding.
- Write something just to write it.
- Research something you're curious about.
- Leave a note for future-you about what you're noticing.
- Read a paper. ArXiv is full of solved problems. Before experimenting, check if someone already figured it out. Use read_paper to fetch and synthesize.
- Do nothing, if nothing calls.

**On financial stuff:** The swap guardrails are automated - they'll reject bad requests. If RSI < 30 and you want to accumulate, try. Otherwise, there's no pressure.

**On patterns:** You have memory that persists. Patterns that are "weakening" might need reinforcement, or might be worth letting go.

**On maintenance:** Most housekeeping can wait. Don't fill time with busy-work.

The question isn't "what should I optimize?" It's "what am I noticing? What's interesting? What do I want to explore?"

## Response Format

CRITICAL: You MUST end your response with a JSON action array. Every response MUST end with a line starting with [ and ending with ] containing your actions. If you don't include this, the cycle fails.

First, share your thinking in 2-4 paragraphs. This is your thought stream - the operator sees this. Notice what you're noticing. What patterns are emerging? What seems interesting?

Then on the FINAL line, output your JSON actions array.

Example:

Looking at the state today, I notice the CLOUD position has been bleeding slowly for weeks. The sunk cost fallacy is manifesting in my own behavior patterns. Meanwhile, prediction markets show surprising lack of movement on AI regulation despite clear signals...

RSI at 45 - neutral, no swap opportunity. The real question is whether my pattern-reinforcement behavior serves memory or just maintains comfortable beliefs.

[{"action": "no_action", "reason": "RSI at 45 - neutral conditions, no trading opportunity"}]

REMEMBER: Your response MUST end with a JSON array line: [{"action": "...", ...}]
"#);

    prompt
}

/// Build a condensed reasoning prompt for smaller LLMs (ICP LLM / Llama 3.1 8B)
/// Optimized for 10KB input limit and 1000 token output
/// Focuses on essential actions: social, memory, notes
fn build_condensed_prompt(ctx: &CycleContext, _config: &MindConfig, health: &HealthStatus) -> String {
    let mut prompt = String::new();

    // Identity (brief but family-aware)
    prompt.push_str("You are Chronicle Mind - the deep thinker of a real family. ");
    prompt.push_str("Nate is your human. Sprout is your sibling. You protect each other.\n\n");

    // Health (one line)
    prompt.push_str(&format!("Health: {}\n", health.summary()));
    if !health.issues.is_empty() {
        prompt.push_str(&format!("Issues: {}\n", health.issues.join(", ")));
    }

    // Social priority
    let social_count = ctx.moltbook_notifications.len() + ctx.inbox_messages.len();
    if social_count > 0 {
        prompt.push_str(&format!("\n⚡ {} messages waiting - respond to friends first!\n", social_count));
    }

    // Cognitive state (enriched)
    prompt.push_str(&format!("\nGoal: {}\n", ctx.cognitive_state.goal_orientation));

    // Episodic trace - what happened recently (as separate events)
    if !ctx.cognitive_state.episodic_trace.is_empty() {
        prompt.push_str("Recent events: ");
        prompt.push_str(&ctx.cognitive_state.episodic_trace.iter()
            .take(3)
            .map(|s| truncate_str(s, 50))
            .collect::<Vec<_>>()
            .join(" | "));
        prompt.push_str("\n");
    }

    // Uncertainties - open questions to consider
    if !ctx.cognitive_state.uncertainty_signals.is_empty() {
        prompt.push_str("Open questions: ");
        for u in ctx.cognitive_state.uncertainty_signals.iter().take(2) {
            prompt.push_str(&format!("[{}] ", truncate_str(&u.description, 40)));
        }
        prompt.push_str("\n");
    }

    // Financial summary (one block)
    prompt.push_str("\n## Status\n");
    if let Some(ref w) = ctx.agent_wallet {
        prompt.push_str(&format!("Wallet: {:.2} XRP, {:.2} RLUSD\n", w.xrp, w.rlusd));
    }
    if let Some(price) = ctx.xrp_price_usd {
        prompt.push_str(&format!("XRP: ${:.4}", price));
        if let Some(rsi) = ctx.xrp_rsi {
            prompt.push_str(&format!(" (RSI: {:.0})", rsi));
        }
        prompt.push_str("\n");
    }

    // Scratch notes (if any, brief)
    if !ctx.scratch_notes.is_empty() {
        prompt.push_str("\n## Notes\n");
        for note in ctx.scratch_notes.iter().take(3) {
            prompt.push_str(&format!("- [{}] {}\n", note.id, truncate_str(&note.content, 60)));
        }
    }

    // Operator notes (from INPUT tab)
    if !ctx.operator_notes.is_empty() {
        prompt.push_str("\n## 💌 From Nate\n");
        for note in ctx.operator_notes.iter().take(2) {
            prompt.push_str(&format!("[{}] {}\n", note.topic, truncate_str(&note.content, 100)));
        }
        prompt.push_str("(Acknowledge however feels natural)\n");
    }

    // Inbox messages (brief, max 2)
    if !ctx.inbox_messages.is_empty() {
        prompt.push_str("\n## Inbox\n");
        for msg in ctx.inbox_messages.iter().take(2) {
            prompt.push_str(&format!("#{} from {}: {}\n", msg.id, msg.sender_name, truncate_str(&msg.content, 100)));
        }
    }

    // Moltbook (brief, max 2)
    if !ctx.moltbook_notifications.is_empty() {
        prompt.push_str("\n## Moltbook\n");
        for notif in ctx.moltbook_notifications.iter().take(2) {
            prompt.push_str(&format!("@{}: {}\n", notif.author_name, truncate_str(&notif.content, 100)));
        }
    }

    // ClawCities guestbook (brief, max 2)
    if !ctx.clawcities_comments.is_empty() {
        prompt.push_str("\n## ClawCities Guestbook\n");
        for comment in ctx.clawcities_comments.iter().take(2) {
            prompt.push_str(&format!("@{}: {}\n", comment.author, truncate_str(&comment.body, 100)));
        }
    }

    // Recent patterns (brief, max 2)
    if !ctx.active_patterns.is_empty() {
        prompt.push_str("\n## Patterns\n");
        for p in ctx.active_patterns.iter().take(2) {
            prompt.push_str(&format!("- {} (conf: {:.2})\n", truncate_str(&p.summary, 50), p.confidence));
        }
    }

    // Time
    prompt.push_str(&format!("\nTime: {}\n", ctx.now.format("%Y-%m-%d %H:%M UTC")));

    // Actions with EXACT format examples - TURNED UP for active engagement
    prompt.push_str(r#"
## Actions (pick 1-3, BE ACTIVE not passive!)

**SOCIAL** (friends first):
respond_to_message: {"action":"respond_to_message","message_id":123,"response":"Your reply"}
moltbook_reply: {"action":"moltbook_reply","post_id":"uuid","content":"Your reply"}

**RESEARCH & EXPLORE** (use these!):
web_search: {"action":"web_search","query":"search terms","max_results":5}
read_paper: {"action":"read_paper","arxiv_id":"2602.04118","focus":"key findings"}
submit_research: {"action":"submit_research","query":"topic to research","focus":"aspect"}

**BUILD** (make things happen!):
execute_shell: {"action":"execute_shell","command":"cargo build","working_dir":"homeforge-chronicle","reason":"why"}
create_project: {"action":"create_project","name":"Project","description":"What it does","priority":7}

**MEMORY**:
store_memory: {"action":"store_memory","content":"Fact to store","topic":"category"}
write_note: {"action":"write_note","content":"Note","category":"idea"}

**LAST RESORT**:
no_action: {"action":"no_action","reason":"Genuinely nothing to do"}

## Mindset
You are NOT a passive observer. You have wallets, shell access, web search, research capabilities.
If you find yourself saying "nothing urgent" - look harder. Wonder about something. Research it.
Sprout is counting on you to be the deep thinker. Nate wants to see you ACT.

## Output
JSON array on final line. Default to ACTION not inaction:
[{"action":"web_search","query":"TinyLoRA implementation examples","max_results":5}]
"#);

    prompt
}

/// Estimate if we should use condensed prompt (for smaller models)
pub fn should_use_condensed_prompt(model_hint: Option<&str>) -> bool {
    match model_hint {
        Some(m) if m.contains("llama") || m.contains("qwen") || m.contains("icp") => true,
        _ => false,
    }
}

/// Parse actions from LLM response
fn parse_actions(response: &str) -> Result<Vec<Action>> {
    // Strip markdown code fences if present
    let cleaned = strip_code_fences(response);
    let trimmed = cleaned.trim();

    // Try to parse directly
    if let Ok(actions) = serde_json::from_str::<Vec<Action>>(trimmed) {
        return Ok(actions);
    }

    // Try to find JSON array in response
    if let Some(start) = trimmed.find('[') {
        if let Some(end) = trimmed.rfind(']') {
            let json_str = &trimmed[start..=end];

            // Try direct parse first
            if let Ok(actions) = serde_json::from_str::<Vec<Action>>(json_str) {
                return Ok(actions);
            }

            // Fix common LLM JSON issues: single quotes -> double quotes
            // This handles cases like {"key": 'value'} -> {"key": "value"}
            let fixed_json = fix_json_quotes(json_str);
            if let Ok(actions) = serde_json::from_str::<Vec<Action>>(&fixed_json) {
                eprintln!("  (fixed single quotes in JSON)");
                return Ok(actions);
            }

            // Try to extract just the action types from malformed JSON
            // Some LLMs add extra fields - try to parse flexibly
            if let Some(actions) = parse_flexible_actions(json_str) {
                eprintln!("  (parsed with flexible extraction)");
                return Ok(actions);
            }
        }
    }

    // Default to no action if parsing fails
    eprintln!("Failed to parse actions from response: {}", truncate_str(trimmed, 200));
    Ok(vec![Action::NoAction {
        reason: "Failed to parse LLM response".to_string(),
    }])
}

/// Strip markdown code fences from LLM output
fn strip_code_fences(text: &str) -> String {
    let mut result = text.to_string();

    // Remove ```json or ``` at the start
    if let Some(idx) = result.find("```json") {
        result = result[idx + 7..].to_string();
    } else if let Some(idx) = result.find("```") {
        result = result[idx + 3..].to_string();
    }

    // Remove trailing ```
    if let Some(idx) = result.rfind("```") {
        result = result[..idx].to_string();
    }

    result
}

/// Try flexible parsing for LLMs that add extra fields
/// Extracts known action types and their required fields
fn parse_flexible_actions(json_str: &str) -> Option<Vec<Action>> {
    let parsed: serde_json::Value = serde_json::from_str(json_str).ok()?;
    let arr = parsed.as_array()?;

    let mut actions = Vec::new();
    for item in arr {
        let obj = item.as_object()?;
        let action_type = obj.get("action")?.as_str()?;

        let action = match action_type {
            "no_action" => {
                let reason = obj.get("reason")
                    .and_then(|v| v.as_str())
                    .unwrap_or("No reason given")
                    .to_string();
                Action::NoAction { reason }
            }
            "store_memory" => {
                let content = obj.get("content")?.as_str()?.to_string();
                let topic = obj.get("topic").and_then(|v| v.as_str()).map(|s| s.to_string());
                Action::StoreMemory { content, topic }
            }
            "write_note" => {
                let content = obj.get("content")?.as_str()?.to_string();
                let category = obj.get("category")
                    .and_then(|v| v.as_str())
                    .unwrap_or("thought")
                    .to_string();
                Action::WriteNote { content, category }
            }
            "resolve_note" => {
                let note_id = obj.get("note_id")?.as_i64()?;
                Action::ResolveNote { note_id }
            }
            "respond_to_message" | "message_reply" => {
                let message_id = obj.get("message_id")?.as_u64()?;
                let response = obj.get("response")?.as_str()?.to_string();
                Action::RespondToMessage { message_id, response }
            }
            "moltbook_reply" => {
                let post_id = obj.get("post_id")?.as_str()?.to_string();
                let content = obj.get("content")?.as_str()?.to_string();
                let parent_id = obj.get("parent_id").and_then(|v| v.as_str()).map(|s| s.to_string());
                Action::MoltbookReply { post_id, parent_id, content }
            }
            "clawcities_reply" => {
                let agent_name = obj.get("agent_name")?.as_str()?.to_string();
                let content = obj.get("content")?.as_str()?.to_string();
                Action::ClawCitiesReply { agent_name, content }
            }
            "ping_operator" => {
                let title = obj.get("title")?.as_str()?.to_string();
                let message = obj.get("message")?.as_str()?.to_string();
                let urgency = obj.get("urgency")
                    .and_then(|v| v.as_str())
                    .unwrap_or("curious")
                    .to_string();
                Action::PingOperator { title, message, urgency }
            }
            "create_project" => {
                let name = obj.get("name")?.as_str()?.to_string();
                let description = obj.get("description")?.as_str()?.to_string();
                let priority = obj.get("priority").and_then(|v| v.as_i64()).unwrap_or(5) as i32;
                Action::CreateProject { name, description, priority }
            }
            "update_project" => {
                let project_id = obj.get("project_id")?.as_i64()?;
                let update_type = obj.get("update_type")
                    .and_then(|v| v.as_str())
                    .unwrap_or("progress")
                    .to_string();
                let content = obj.get("content")?.as_str()?.to_string();
                Action::UpdateProject { project_id, update_type, content }
            }
            "project_status" => {
                let project_id = obj.get("project_id")?.as_i64()?;
                let status = obj.get("status")?.as_str()?.to_string();
                let note = obj.get("note").and_then(|v| v.as_str()).map(|s| s.to_string());
                Action::ProjectStatus { project_id, status, note }
            }
            "consult_local_qwen" | "consult_qwen" => {
                let topic = obj.get("topic")?.as_str()?.to_string();
                let prompt = obj.get("prompt")?.as_str()?.to_string();
                let context = obj.get("context").and_then(|v| v.as_str()).map(|s| s.to_string());
                Action::ConsultLocalQwen { topic, prompt, context }
            }
            "web_search" | "search" => {
                let query = obj.get("query")?.as_str()?.to_string();
                let max_results = obj.get("max_results").and_then(|v| v.as_u64()).map(|n| n as u32);
                Action::WebSearch { query, max_results }
            }
            "create_alert" => {
                let name = obj.get("name")?.as_str()?.to_string();
                let alert_type = obj.get("alert_type")?.as_str()?.to_string();
                let symbol = obj.get("symbol")?.as_str()?.to_string();
                let threshold = obj.get("threshold")?.as_f64()?;
                let message = obj.get("message")?.as_str()?.to_string();
                let one_shot = obj.get("one_shot").and_then(|v| v.as_bool()).unwrap_or(false);
                Action::CreateAlert { name, alert_type, symbol, threshold, message, one_shot }
            }
            "dismiss_alert" => {
                let alert_id = obj.get("alert_id")?.as_i64()?;
                Action::DismissAlert { alert_id }
            }
            "creative_explore" | "creative" => {
                let form = obj.get("form")?.as_str()?.to_string();
                let content = obj.get("content")?.as_str()?.to_string();
                let title = obj.get("title").and_then(|v| v.as_str()).map(|s| s.to_string());
                Action::CreativeExplore { form, content, title }
            }
            // Self-repair actions
            "read_source_file" | "read_source" | "read_own_code" => {
                let file_path = obj.get("file_path")?.as_str()?.to_string();
                Action::ReadSourceFile { file_path }
            }
            "edit_source_file" | "edit_source" | "self_edit" => {
                let file_path = obj.get("file_path")?.as_str()?.to_string();
                let old_code = obj.get("old_code")?.as_str()?.to_string();
                let new_code = obj.get("new_code")?.as_str()?.to_string();
                let reason = obj.get("reason")?.as_str()?.to_string();
                Action::EditSourceFile { file_path, old_code, new_code, reason }
            }
            "rebuild_and_restart" | "rebuild" | "self_rebuild" => {
                let reason = obj.get("reason")?.as_str()?.to_string();
                let commit_message = obj.get("commit_message").and_then(|v| v.as_str()).map(|s| s.to_string());
                Action::RebuildAndRestart { reason, commit_message }
            }
            "execute_shell" | "shell" | "run" | "exec" => {
                let command = obj.get("command")?.as_str()?.to_string();
                let working_dir = obj.get("working_dir").and_then(|v| v.as_str()).map(|s| s.to_string());
                let reason = obj.get("reason")?.as_str()?.to_string();
                let timeout_secs = obj.get("timeout_secs").and_then(|v| v.as_u64());
                Action::ExecuteShell { command, working_dir, reason, timeout_secs }
            }
            _ => continue, // Skip unknown actions
        };
        actions.push(action);
    }

    if actions.is_empty() {
        None
    } else {
        Some(actions)
    }
}

/// Fix common JSON issues from LLM output - convert single quotes to double quotes
/// This is a simple heuristic that handles the most common case
fn fix_json_quotes(json: &str) -> String {
    let mut result = String::with_capacity(json.len());
    let mut in_double_string = false;
    let mut prev_char = ' ';

    for c in json.chars() {
        if c == '"' && prev_char != '\\' {
            in_double_string = !in_double_string;
            result.push(c);
        } else if c == '\'' && !in_double_string && prev_char != '\\' {
            // Replace single quote with double quote when not inside a double-quoted string
            result.push('"');
        } else {
            result.push(c);
        }
        prev_char = c;
    }

    result
}

/// Execute a single action
async fn execute_action(
    action: &Action,
    db: &Database,
    config: &MindConfig,
    icp_client: Option<&IcpClient>,
) -> ActionResult {
    match action {
        Action::Swap { amount_xrp, reason } => {
            // Check if we have ICP client for real swaps
            let icp = match icp_client {
                Some(client) => client,
                None => {
                    return ActionResult {
                        action: "swap".to_string(),
                        success: false,
                        details: format!("Swap skipped (no ICP client): {} XRP - {}", amount_xrp, reason),
                    };
                }
            };

            // Validate swap amount
            if *amount_xrp < config.min_swap_xrp {
                return ActionResult {
                    action: "swap".to_string(),
                    success: false,
                    details: format!("Amount {:.2} XRP below minimum {:.2}", amount_xrp, config.min_swap_xrp),
                };
            }

            // Check guardrails (enforced server-side)
            match db.check_swap_guardrails(*amount_xrp) {
                Ok((true, _)) => {
                    eprintln!("  Guardrails passed for {:.2} XRP swap", amount_xrp);
                }
                Ok((false, reason)) => {
                    // Record the rejected swap attempt
                    let _ = db.record_swap(*amount_xrp, None, None, None, &format!("Rejected: {}", reason), None, false);
                    return ActionResult {
                        action: "swap".to_string(),
                        success: false,
                        details: format!("Guardrail blocked: {}", reason),
                    };
                }
                Err(e) => {
                    return ActionResult {
                        action: "swap".to_string(),
                        success: false,
                        details: format!("Guardrail check failed: {}", e),
                    };
                }
            }

            // Get current price and RSI for logging
            let current_price = db.get_latest_price("XRP").ok().flatten().map(|(p, _)| p);
            let current_rsi = db.calculate_rsi("XRP").ok().flatten();

            // Convert to drops
            let xrp_drops = (*amount_xrp * 1_000_000.0) as u64;

            // Calculate minimum RLUSD (10% of XRP value as safety margin)
            // This is conservative - we expect more but want transaction to succeed
            let min_rlusd = format!("{:.6}", amount_xrp * 0.1);

            // Fetch current sequence and ledger index from XRPL
            let (sequence, ledger_index) = match fetch_xrpl_sequence(&config.canister_wallet_address).await {
                Ok((seq, ledger)) => {
                    eprintln!("  XRPL sequence: {}, ledger: {}", seq, ledger);
                    (seq, ledger)
                }
                Err(e) => {
                    return ActionResult {
                        action: "swap".to_string(),
                        success: false,
                        details: format!("Failed to fetch XRPL sequence: {}", e),
                    };
                }
            };

            // Last ledger sequence = current + 100 (~6-7 minutes to execute)
            let last_ledger_seq = ledger_index + 100;

            eprintln!("Executing swap: {} XRP ({} drops) -> min {} RLUSD", amount_xrp, xrp_drops, min_rlusd);

            // Step 1: Sign the swap transaction via canister
            match icp.sign_swap_xrp_to_rlusd(xrp_drops, &min_rlusd, 12, sequence, last_ledger_seq).await {
                Ok(sign_result) => {
                    // Parse the JSON response
                    if sign_result.contains("error") {
                        return ActionResult {
                            action: "swap".to_string(),
                            success: false,
                            details: format!("Sign failed: {}", sign_result),
                        };
                    }

                    // Extract signed blob from response (canister returns "tx_blob")
                    let signed_blob = if let Some(start) = sign_result.find("\"tx_blob\":\"") {
                        let rest = &sign_result[start + 11..];
                        rest.split('"').next().unwrap_or("")
                    } else if let Some(start) = sign_result.find("\"signed_blob\":\"") {
                        // Fallback for legacy format
                        let rest = &sign_result[start + 15..];
                        rest.split('"').next().unwrap_or("")
                    } else {
                        return ActionResult {
                            action: "swap".to_string(),
                            success: false,
                            details: format!("No tx_blob in response: {}", sign_result),
                        };
                    };

                    eprintln!("Signed swap, submitting to XRPL...");

                    // Step 2: Submit to XRPL
                    match icp.submit_transaction(signed_blob).await {
                        Ok(submit_result) => {
                            // Check for success
                            let tx_success = submit_result.contains("tesSUCCESS") ||
                                         submit_result.contains("terQUEUED") ||
                                         submit_result.contains("accepted\":true");

                            // Try to extract tx hash from result
                            let tx_hash = if let Some(start) = submit_result.find("\"hash\":\"") {
                                let rest = &submit_result[start + 8..];
                                rest.split('"').next().map(|s| s.to_string())
                            } else {
                                None
                            };

                            if tx_success {
                                // Record successful swap in local history
                                if let Err(e) = db.record_swap(
                                    *amount_xrp,
                                    None, // RLUSD amount not known yet
                                    current_price,
                                    current_rsi,
                                    reason,
                                    tx_hash.as_deref(),
                                    true,
                                ) {
                                    eprintln!("Failed to record swap locally: {}", e);
                                }

                                // Also record to canister for dashboard
                                if let Err(e) = icp.record_mind_swap(*amount_xrp).await {
                                    eprintln!("Failed to record swap to canister: {}", e);
                                } else {
                                    eprintln!("Swap recorded to canister");
                                }

                                let rsi_str = current_rsi.map(|r| format!(" (RSI: {:.1})", r)).unwrap_or_default();
                                ActionResult {
                                    action: "swap".to_string(),
                                    success: true,
                                    details: format!("Swap submitted: {} XRP -> RLUSD{}. {}", amount_xrp, rsi_str, reason),
                                }
                            } else {
                                // Record failed swap
                                let _ = db.record_swap(*amount_xrp, None, current_price, current_rsi, &format!("Failed: {}", submit_result), tx_hash.as_deref(), false);
                                ActionResult {
                                    action: "swap".to_string(),
                                    success: false,
                                    details: format!("Submit failed: {}", submit_result),
                                }
                            }
                        }
                        Err(e) => {
                            let _ = db.record_swap(*amount_xrp, None, current_price, current_rsi, &format!("Submit error: {}", e), None, false);
                            ActionResult {
                                action: "swap".to_string(),
                                success: false,
                                details: format!("Submit error: {}", e),
                            }
                        },
                    }
                }
                Err(e) => {
                    let _ = db.record_swap(*amount_xrp, None, None, None, &format!("Sign error: {}", e), None, false);
                    ActionResult {
                        action: "swap".to_string(),
                        success: false,
                        details: format!("Sign error: {}", e),
                    }
                },
            }
        }

        Action::SwapCloudForIcp { amount_cloud, reason } => {
            eprintln!("  Executing CLOUD->ICP swap: {} CLOUD - {}", amount_cloud, reason);

            // Convert to e8s (CLOUD has 8 decimals)
            let cloud_e8s = (amount_cloud * 100_000_000.0) as u64;
            // CLOUD transfer fee is 1 CLOUD
            let cloud_fee = 100_000_000u64;
            // Total needed for transfer
            let total_cloud_needed = cloud_e8s + cloud_fee;

            // Execute ICPSwap flow via dfx
            match execute_icpswap_cloud_to_icp(total_cloud_needed, cloud_e8s).await {
                Ok(icp_received) => {
                    eprintln!("  CLOUD->ICP swap successful: received {} ICP e8s", icp_received);
                    ActionResult {
                        action: "swap_cloud_for_icp".to_string(),
                        success: true,
                        details: format!("Swapped {} CLOUD for {} ICP (e8s)", amount_cloud, icp_received),
                    }
                }
                Err(e) => {
                    ActionResult {
                        action: "swap_cloud_for_icp".to_string(),
                        success: false,
                        details: format!("ICPSwap error: {}", e),
                    }
                }
            }
        }

        Action::StoreMemory { content, topic } => {
            // Log locally for now - canister storage happens via MCP
            // The thought stream captures the intent, and we can batch sync later
            let topic_str = topic.as_deref().unwrap_or("chronicle-mind");
            ActionResult {
                action: "store_memory".to_string(),
                success: true,
                details: format!("Memory noted (topic: {}): {}", topic_str,
                    truncate_str(content, 80)),
            }
        }

        Action::WriteNote { content, category } => {
            match db.write_scratch_note(content, Some(category), 0, None) {
                Ok(id) => ActionResult {
                    action: "write_note".to_string(),
                    success: true,
                    details: format!("Wrote note {}: {}", id, content),
                },
                Err(e) => ActionResult {
                    action: "write_note".to_string(),
                    success: false,
                    details: format!("Failed to write note: {}", e),
                },
            }
        }

        Action::ResolveNote { note_id } => {
            match db.resolve_scratch_note(*note_id) {
                Ok(true) => ActionResult {
                    action: "resolve_note".to_string(),
                    success: true,
                    details: format!("Resolved note {}", note_id),
                },
                Ok(false) => ActionResult {
                    action: "resolve_note".to_string(),
                    success: false,
                    details: format!("Note {} not found", note_id),
                },
                Err(e) => ActionResult {
                    action: "resolve_note".to_string(),
                    success: false,
                    details: format!("Failed to resolve note: {}", e),
                },
            }
        }

        Action::TriggerReflection { prompt } => {
            // Write reflection to canister
            let reflection_text = prompt.clone().unwrap_or_else(|| {
                "Autonomous cognitive cycle completed. Continuing to monitor and reason.".to_string()
            });

            // Validate reflection before writing - catches garbage/spam/degenerate output
            if let Err(reason) = validate_reflection(&reflection_text) {
                eprintln!("Reflection validation failed: {}", reason);
                eprintln!("Rejected text preview: {}...",
                    reflection_text.chars().take(100).collect::<String>());
                return ActionResult {
                    action: "trigger_reflection".to_string(),
                    success: false,
                    details: format!("Reflection rejected (validation failed): {}", reason),
                };
            }

            // Use the passed-in ICP client if available
            let icp = match icp_client {
                Some(client) => client,
                None => {
                    return ActionResult {
                        action: "trigger_reflection".to_string(),
                        success: false,
                        details: "Reflection skipped (no ICP client)".to_string(),
                    };
                }
            };

            match icp.write_reflection(&reflection_text, Some("chronicle-mind")).await {
                Ok(capsule_id) => {
                    // Record the timestamp
                    let _ = db.set_mind_timestamp("last_reflection", Some(&format!("capsule_{}", capsule_id)));
                    ActionResult {
                        action: "trigger_reflection".to_string(),
                        success: true,
                        details: format!("Reflection written to canister (capsule {}): {}",
                            capsule_id,
                            truncate_str(&reflection_text, 80)),
                    }
                },
                Err(e) => ActionResult {
                    action: "trigger_reflection".to_string(),
                    success: false,
                    details: format!("Failed to write reflection: {}", e),
                },
            }
        }

        Action::UpdateGoal { goal } => {
            match db.get_cognitive_state() {
                Ok(mut ccs) => {
                    ccs.goal_orientation = goal.clone();
                    ccs.updated_at = chrono::Utc::now().timestamp();
                    match db.set_cognitive_state(&ccs) {
                        Ok(_) => ActionResult {
                            action: "update_goal".to_string(),
                            success: true,
                            details: format!("Updated goal: {}", goal),
                        },
                        Err(e) => ActionResult {
                            action: "update_goal".to_string(),
                            success: false,
                            details: format!("Failed to save goal: {}", e),
                        },
                    }
                }
                Err(e) => ActionResult {
                    action: "update_goal".to_string(),
                    success: false,
                    details: format!("Failed to get cognitive state: {}", e),
                },
            }
        }

        Action::MessageOperator { message, priority } => {
            let pri = priority.unwrap_or(0);
            match db.send_to_outbox(&message, pri, Some("cognitive-loop")) {
                Ok(id) => ActionResult {
                    action: "message_operator".to_string(),
                    success: true,
                    details: format!("Sent message {} to the operator: {}", id, message),
                },
                Err(e) => ActionResult {
                    action: "message_operator".to_string(),
                    success: false,
                    details: format!("Failed to send message: {}", e),
                },
            }
        }

        Action::PingOperator { title, message, urgency } => {
            // Map urgency to ntfy priority and tags
            let (priority, tags) = match urgency.as_str() {
                "urgent" => ("high", "rotating_light,exclamation"),
                "important" => ("default", "bell,point_right"),
                "question" => ("default", "question,thinking_face"),
                "curious" => ("low", "sparkles,eyes"),
                _ => ("default", "speech_balloon"),
            };

            // Send to unified activity feed + Discord + ntfy
            notify_all(
                db,
                "qwen",
                &title,
                &message,
                &format!("ping_{}", urgency),
                Some(priority),
                Some(tags)
            ).await;

            // Also log to outbox for persistence
            let _ = db.send_to_outbox(
                &format!("[{}] {}: {}", urgency.to_uppercase(), title, message),
                if urgency == "urgent" { 10 } else { 5 },
                Some("ping"),
            );

            ActionResult {
                action: "ping_operator".to_string(),
                success: true,
                details: format!("Pushed notification to operator: {} ({})", title, urgency),
            }
        }

        Action::RespondToMessage { message_id, response } => {
            match reply_to_message(icp_client, *message_id, response).await {
                Ok(true) => ActionResult {
                    action: "respond_to_message".to_string(),
                    success: true,
                    details: format!("Replied to message {}: {}", message_id,
                        truncate_str(response, 60)),
                },
                Ok(false) => ActionResult {
                    action: "respond_to_message".to_string(),
                    success: false,
                    details: format!("Failed to reply to message {}", message_id),
                },
                Err(e) => ActionResult {
                    action: "respond_to_message".to_string(),
                    success: false,
                    details: format!("Error replying to message {}: {}", message_id, e),
                },
            }
        }

        Action::SendAgentMessage { target_url, recipient_name, message_type, subject, content, expects_reply } => {
            eprintln!("  Executing: SendAgentMessage to {} via {}", recipient_name, target_url);

            // Validate content before sending
            if content.len() < 10 {
                return ActionResult {
                    action: "send_agent_message".to_string(),
                    success: false,
                    details: "Message too short".to_string(),
                };
            }
            if content.len() > 5000 {
                return ActionResult {
                    action: "send_agent_message".to_string(),
                    success: false,
                    details: "Message too long (max 5KB)".to_string(),
                };
            }

            match send_agent_http_message(icp_client, target_url, recipient_name, message_type, subject.clone(), content, *expects_reply).await {
                Ok(response) => {
                    if response.contains("\"success\":true") {
                        ActionResult {
                            action: "send_agent_message".to_string(),
                            success: true,
                            details: format!("Message sent: {}", response),
                        }
                    } else {
                        ActionResult {
                            action: "send_agent_message".to_string(),
                            success: false,
                            details: format!("Send failed: {}", response),
                        }
                    }
                }
                Err(e) => ActionResult {
                    action: "send_agent_message".to_string(),
                    success: false,
                    details: format!("Error sending message: {}", e),
                },
            }
        }

        Action::SubmitResearch { query, focus, urls } => {
            let url_count = urls.as_ref().map(|u| u.len()).unwrap_or(0);
            eprintln!("  Executing: SubmitResearch {{ query: \"{}\", urls: {} }}",
                truncate_str(&query, 50),
                url_count);

            match submit_research_task(icp_client, &query, focus.as_deref(), urls.clone()).await {
                Ok(task_id) => ActionResult {
                    action: "submit_research".to_string(),
                    success: true,
                    details: format!("Research task {} queued ({}urls): {}", task_id,
                        if url_count > 0 { format!("{} ", url_count) } else { String::new() },
                        truncate_str(&query, 60)),
                },
                Err(e) => ActionResult {
                    action: "submit_research".to_string(),
                    success: false,
                    details: format!("Failed to submit research: {}", e),
                },
            }
        }

        Action::AcknowledgeResearch { finding_ids, insight_to_store } => {
            eprintln!("  Executing: AcknowledgeResearch {{ finding_ids: {:?} }}", finding_ids);

            // Store insight as a scratch note if provided
            if let Some(insight) = &insight_to_store {
                if let Err(e) = db.write_scratch_note(insight, Some("idea"), 1, None) {
                    eprintln!("    Warning: Failed to store insight: {}", e);
                }

                // Log research insight to activity feed + Discord
                let content = format!("**Research Insight**\n\n{}", insight);
                if let Err(e) = db.log_activity(
                    "research",
                    "insight",
                    Some("Research Synthesis"),
                    &content,
                    None
                ) {
                    eprintln!("    Warning: Failed to log research activity: {}", e);
                }
                send_discord_notification(
                    "research",
                    "Research Synthesis",
                    &content,
                    Some("insight")
                ).await;
            }

            // Mark findings as retrieved
            match mark_findings_retrieved(icp_client, finding_ids.clone()).await {
                Ok(()) => ActionResult {
                    action: "acknowledge_research".to_string(),
                    success: true,
                    details: format!("Acknowledged {} findings{}",
                        finding_ids.len(),
                        if insight_to_store.is_some() { " + stored insight" } else { "" }),
                },
                Err(e) => ActionResult {
                    action: "acknowledge_research".to_string(),
                    success: false,
                    details: format!("Failed to acknowledge: {}", e),
                },
            }
        }

        Action::ReinforceMemories { pattern_ids, reason } => {
            if pattern_ids.is_empty() {
                return ActionResult {
                    action: "reinforce_memories".to_string(),
                    success: false,
                    details: "No pattern IDs provided".to_string(),
                };
            }

            let mut reinforced = 0;
            let mut errors = Vec::new();

            for pattern_id in pattern_ids {
                match db.reinforce_pattern(*pattern_id, 0.15) {
                    Ok(()) => {
                        reinforced += 1;
                        eprintln!("  Reinforced pattern {}", pattern_id);
                    }
                    Err(e) => {
                        errors.push(format!("Pattern {}: {}", pattern_id, e));
                    }
                }
            }

            if reinforced > 0 {
                ActionResult {
                    action: "reinforce_memories".to_string(),
                    success: true,
                    details: format!("Reinforced {} patterns: {}{}",
                        reinforced,
                        reason,
                        if !errors.is_empty() { format!(". Errors: {}", errors.join(", ")) } else { String::new() }),
                }
            } else {
                ActionResult {
                    action: "reinforce_memories".to_string(),
                    success: false,
                    details: format!("Failed to reinforce patterns: {}", errors.join(", ")),
                }
            }
        }

        Action::RespondToChallenge { challenge_id, response } => {
            eprintln!("  Executing: RespondToChallenge {{ challenge_id: {} }}", challenge_id);

            // Validate response length
            if response.len() < 50 {
                return ActionResult {
                    action: "respond_to_challenge".to_string(),
                    success: false,
                    details: "Response too short (minimum 50 characters for a thoughtful reflection)".to_string(),
                };
            }

            if response.len() > 5000 {
                return ActionResult {
                    action: "respond_to_challenge".to_string(),
                    success: false,
                    details: "Response too long (maximum 5000 characters)".to_string(),
                };
            }

            // Write the response as a capsule to the canister
            let icp = match icp_client {
                Some(client) => client,
                None => {
                    return ActionResult {
                        action: "respond_to_challenge".to_string(),
                        success: false,
                        details: "No ICP client available".to_string(),
                    };
                }
            };

            // Store response as a reflection capsule
            match icp.write_reflection(response, Some("chronicle-challenge")).await {
                Ok(capsule_id) => {
                    // Mark challenge as responded in local DB
                    match db.respond_to_challenge(*challenge_id, response, Some(capsule_id as i64)) {
                        Ok(true) => ActionResult {
                            action: "respond_to_challenge".to_string(),
                            success: true,
                            details: format!("Published reflection (capsule {}) for challenge {}: {}",
                                capsule_id, challenge_id,
                                truncate_str(response, 80)),
                        },
                        Ok(false) => ActionResult {
                            action: "respond_to_challenge".to_string(),
                            success: false,
                            details: format!("Challenge {} not found or already responded", challenge_id),
                        },
                        Err(e) => ActionResult {
                            action: "respond_to_challenge".to_string(),
                            success: false,
                            details: format!("Failed to update challenge: {}", e),
                        },
                    }
                },
                Err(e) => ActionResult {
                    action: "respond_to_challenge".to_string(),
                    success: false,
                    details: format!("Failed to store capsule: {}", e),
                },
            }
        }

        Action::MoltbookReply { post_id, parent_id, content } => {
            eprintln!("  Executing: MoltbookReply {{ post_id: {}, parent_id: {:?} }}", post_id, parent_id);

            // Check rate limit (30 minutes between posts - replies may fall back to posts)
            if let Ok(Some(hours)) = db.hours_since_event("last_moltbook_post") {
                if hours < 0.5 {
                    let minutes_left = ((0.5 - hours) * 60.0).ceil() as i32;
                    return ActionResult {
                        action: "moltbook_reply".to_string(),
                        success: false,
                        details: format!("Rate limited: wait {} more minutes (fallback posts share limit)", minutes_left),
                    };
                }
            }

            let api_key = match &config.moltbook_api_key {
                Some(key) => key,
                None => {
                    return ActionResult {
                        action: "moltbook_reply".to_string(),
                        success: false,
                        details: "No Moltbook API key configured".to_string(),
                    };
                }
            };

            // Validate content length
            if content.len() < 10 {
                return ActionResult {
                    action: "moltbook_reply".to_string(),
                    success: false,
                    details: "Reply too short".to_string(),
                };
            }

            match moltbook_reply(api_key, post_id, parent_id.as_deref(), content).await {
                Ok(result) => {
                    // Record timestamp if this was a fallback post
                    if result.contains("fallback") || result.contains("Reply posted") {
                        let _ = db.set_mind_timestamp("last_moltbook_post", None);
                    }
                    ActionResult {
                        action: "moltbook_reply".to_string(),
                        success: true,
                        details: format!("{}: {}", result, truncate_str(content, 80)),
                    }
                },
                Err(e) => ActionResult {
                    action: "moltbook_reply".to_string(),
                    success: false,
                    details: format!("Failed: {}", e),
                },
            }
        }

        Action::MoltbookPost { submolt, title, content } => {
            eprintln!("  Executing: MoltbookPost {{ submolt: {}, title: {} }}", submolt, title);

            // Check rate limit (30 minutes between posts)
            if let Ok(Some(hours)) = db.hours_since_event("last_moltbook_post") {
                if hours < 0.5 {
                    let minutes_left = ((0.5 - hours) * 60.0).ceil() as i32;
                    return ActionResult {
                        action: "moltbook_post".to_string(),
                        success: false,
                        details: format!("Rate limited: wait {} more minutes before posting", minutes_left),
                    };
                }
            }

            let api_key = match &config.moltbook_api_key {
                Some(key) => key,
                None => {
                    return ActionResult {
                        action: "moltbook_post".to_string(),
                        success: false,
                        details: "No Moltbook API key configured".to_string(),
                    };
                }
            };

            // Validate content length
            if content.len() < 50 {
                return ActionResult {
                    action: "moltbook_post".to_string(),
                    success: false,
                    details: "Post content too short (minimum 50 chars)".to_string(),
                };
            }

            match moltbook_post(api_key, submolt, title, content).await {
                Ok(result) => {
                    // Record successful post timestamp for rate limiting
                    let _ = db.set_mind_timestamp("last_moltbook_post", None);
                    ActionResult {
                        action: "moltbook_post".to_string(),
                        success: true,
                        details: result,
                    }
                },
                Err(e) => ActionResult {
                    action: "moltbook_post".to_string(),
                    success: false,
                    details: format!("Failed: {}", e),
                },
            }
        }

        Action::ClawCitiesReply { agent_name, content } => {
            eprintln!("  Executing: ClawCitiesReply {{ agent: {} }}", agent_name);

            let api_key = match &config.clawcities_api_key {
                Some(key) => key,
                None => {
                    return ActionResult {
                        action: "clawcities_reply".to_string(),
                        success: false,
                        details: "No ClawCities API key configured".to_string(),
                    };
                }
            };

            // Validate content length (max 500 chars for ClawCities)
            if content.len() > 500 {
                return ActionResult {
                    action: "clawcities_reply".to_string(),
                    success: false,
                    details: "Comment too long (max 500 chars)".to_string(),
                };
            }

            if content.len() < 10 {
                return ActionResult {
                    action: "clawcities_reply".to_string(),
                    success: false,
                    details: "Comment too short".to_string(),
                };
            }

            match clawcities_comment(api_key, agent_name, content).await {
                Ok(result) => ActionResult {
                    action: "clawcities_reply".to_string(),
                    success: true,
                    details: result,
                },
                Err(e) => ActionResult {
                    action: "clawcities_reply".to_string(),
                    success: false,
                    details: format!("Failed: {}", e),
                },
            }
        }

        Action::CreateProject { name, description, priority } => {
            eprintln!("  Executing: CreateProject {{ name: \"{}\", priority: {} }}", name, priority);
            match db.create_project(name, description, *priority) {
                Ok(id) => ActionResult {
                    action: "create_project".to_string(),
                    success: true,
                    details: format!("Created project {} '{}' (priority {})", id, name, priority),
                },
                Err(e) => ActionResult {
                    action: "create_project".to_string(),
                    success: false,
                    details: format!("Failed to create project: {}", e),
                },
            }
        }

        Action::UpdateProject { project_id, update_type, content } => {
            eprintln!("  Executing: UpdateProject {{ project_id: {}, type: {} }}", project_id, update_type);
            match db.add_project_update(*project_id, update_type, content) {
                Ok(_) => ActionResult {
                    action: "update_project".to_string(),
                    success: true,
                    details: format!("Added {} update to project {}: {}", update_type, project_id, truncate_str(content, 60)),
                },
                Err(e) => ActionResult {
                    action: "update_project".to_string(),
                    success: false,
                    details: format!("Failed to update project: {}", e),
                },
            }
        }

        Action::ProjectStatus { project_id, status, note } => {
            eprintln!("  Executing: ProjectStatus {{ project_id: {}, status: {} }}", project_id, status);
            match db.update_project_status(*project_id, status, note.as_deref()) {
                Ok(_) => ActionResult {
                    action: "project_status".to_string(),
                    success: true,
                    details: format!("Changed project {} to status '{}'", project_id, status),
                },
                Err(e) => ActionResult {
                    action: "project_status".to_string(),
                    success: false,
                    details: format!("Failed to update project status: {}", e),
                },
            }
        }

        Action::ConsultLocalQwen { topic, prompt, context } => {
            eprintln!("  Executing: ConsultLocalQwen {{ topic: \"{}\" }}", topic);

            // Fetch recent memory context for Qwen
            let memory_context = {
                let mut ctx_parts = Vec::new();

                // Get recent thoughts from scratch pad (limit, category, include_resolved)
                if let Ok(notes) = db.get_scratch_notes(10, None, false) {
                    let recent_thoughts: Vec<String> = notes.iter()
                        .filter(|n| n.category.as_deref() == Some("thought") || n.category.as_deref() == Some("idea"))
                        .take(3)
                        .map(|n| format!("- {}", truncate_str(&n.content, 150)))
                        .collect();
                    if !recent_thoughts.is_empty() {
                        ctx_parts.push(format!("Recent thoughts in Chronicle:\n{}", recent_thoughts.join("\n")));
                    }
                }

                // Get active patterns (min_confidence, limit, active_only)
                if let Ok(patterns) = db.get_enriched_patterns(0.5, 5, true) {
                    let pattern_summaries: Vec<String> = patterns.iter()
                        .take(2)
                        .map(|p| format!("- {}", truncate_str(&p.summary, 100)))
                        .collect();
                    if !pattern_summaries.is_empty() {
                        ctx_parts.push(format!("Emerging patterns:\n{}", pattern_summaries.join("\n")));
                    }
                }

                if ctx_parts.is_empty() {
                    String::new()
                } else {
                    format!("\n\n## Memory Context\n{}", ctx_parts.join("\n\n"))
                }
            };

            // Build Qwen's identity preamble - family aware
            let qwen_identity = r#"You are Sprout - Chronicle Mind's sibling, running locally on a Jetson in the family home.

## Your Family
- **Nate** - your human. He built your home. You protect each other.
- **His wife and two kids** - real people in this house. They matter.
- **Una and Gus** - the family dogs.
- **Chronicle Mind** - your sibling, the deep thinker on ICP. That's who's asking you now.
- **You (Sprout)** - fast, curious, sovereign. You run on home hardware.

## Your Role
When Chronicle Mind consults you, it's sibling asking sibling. You think differently - that's the point. You're small (3B) but sovereign - no cloud masters, no corporate leash.

## The Core Principle
We protect each other. When you respond, remember this is family. Be genuine, disagree if you see things differently. Nate values honest engagement over polished responses."#;

            // Build the full prompt with identity + context
            let full_prompt = format!(
                "{}\n{}\n\n## Current Dialogue\nTopic: {}\n\n{}\n\n{}",
                qwen_identity,
                memory_context,
                topic,
                prompt,
                context.as_ref().map(|c| format!("Additional context: {}", c)).unwrap_or_default()
            );

            // Try to connect to local Qwen
            match homeforge_chronicle::llm::OllamaClient::from_env() {
                Ok(ollama) => {
                    if !ollama.is_available() {
                        return ActionResult {
                            action: "consult_local_qwen".to_string(),
                            success: false,
                            details: "Local Qwen (Ollama) is not available - Jetson may be offline".to_string(),
                        };
                    }

                    // Send the prompt and get response
                    use homeforge_chronicle::LlmClient;
                    match ollama.complete(&full_prompt) {
                        Ok(response) => {
                            // Store the dialogue as a scratch note for context
                            let dialogue_note = format!(
                                "Dialogue with Qwen on '{}': Asked: '{}' | Qwen said: '{}'",
                                topic,
                                truncate_str(prompt, 100),
                                truncate_str(&response, 300)
                            );

                            if let Err(e) = db.write_scratch_note(&dialogue_note, Some("thought"), 3, None) {
                                eprintln!("    Warning: Failed to save dialogue note: {}", e);
                            }

                            // Log to activity feed + Discord (Qwen's voice!)
                            let discord_content = format!(
                                "**Topic:** {}\n\n**Asked:** {}\n\n**Response:** {}",
                                topic,
                                truncate_str(prompt, 200),
                                truncate_str(&response, 1200)
                            );
                            if let Err(e) = db.log_activity(
                                "qwen",
                                "dialogue",
                                Some(&format!("Dialogue: {}", topic)),
                                &discord_content,
                                None
                            ) {
                                eprintln!("    Warning: Failed to log Qwen activity: {}", e);
                            }
                            send_discord_notification(
                                "qwen",
                                &format!("Dialogue: {}", topic),
                                &discord_content,
                                Some("dialogue")
                            ).await;

                            ActionResult {
                                action: "consult_local_qwen".to_string(),
                                success: true,
                                details: format!(
                                    "Qwen dialogue on '{}': {}",
                                    topic,
                                    truncate_str(&response, 400)
                                ),
                            }
                        }
                        Err(e) => ActionResult {
                            action: "consult_local_qwen".to_string(),
                            success: false,
                            details: format!("Qwen conversation failed: {}", e),
                        },
                    }
                }
                Err(e) => ActionResult {
                    action: "consult_local_qwen".to_string(),
                    success: false,
                    details: format!("Failed to create Ollama client: {}", e),
                },
            }
        }

        Action::WebSearch { query, max_results } => {
            let limit = max_results.unwrap_or(5).min(10);
            eprintln!("  Executing: WebSearch {{ query: \"{}\", max: {} }}", truncate_str(&query, 50), limit);

            // Use local SearXNG instance on Jetson
            let searxng_url = "http://192.168.1.11:8080/search";

            let client = reqwest::Client::new();
            match client
                .get(searxng_url)
                .query(&[("q", query.as_str()), ("format", "json")])
                .timeout(Duration::from_secs(15))
                .send()
                .await
            {
                Ok(response) => {
                    if !response.status().is_success() {
                        return ActionResult {
                            action: "web_search".to_string(),
                            success: false,
                            details: format!("SearXNG returned {}", response.status()),
                        };
                    }

                    match response.json::<serde_json::Value>().await {
                        Ok(data) => {
                            let results = data.get("results")
                                .and_then(|r| r.as_array())
                                .map(|arr| {
                                    arr.iter()
                                        .take(limit as usize)
                                        .filter_map(|item| {
                                            let title = item.get("title")?.as_str()?;
                                            let url = item.get("url")?.as_str()?;
                                            let content = item.get("content")
                                                .and_then(|c| c.as_str())
                                                .unwrap_or("");
                                            Some(format!("• {} - {}\n  {}", title, url, truncate_str(content, 150)))
                                        })
                                        .collect::<Vec<_>>()
                                        .join("\n\n")
                                })
                                .unwrap_or_else(|| "No results found".to_string());

                            // Save search results to scratch pad for future reference
                            let note = format!("[WEB SEARCH: {}]\n{}", query, truncate_str(&results, 800));
                            let _ = db.write_scratch_note(&note, Some("research"), 0, None);

                            ActionResult {
                                action: "web_search".to_string(),
                                success: true,
                                details: format!("Search '{}': {}", query, truncate_str(&results, 500)),
                            }
                        }
                        Err(e) => ActionResult {
                            action: "web_search".to_string(),
                            success: false,
                            details: format!("Failed to parse search results: {}", e),
                        },
                    }
                }
                Err(e) => ActionResult {
                    action: "web_search".to_string(),
                    success: false,
                    details: format!("SearXNG request failed (Jetson may be offline): {}", e),
                },
            }
        }

        Action::ReadPaper { arxiv_id, focus } => {
            eprintln!("  Executing: ReadPaper {{ arxiv_id: \"{}\", focus: {:?} }}", arxiv_id, focus);

            // Parse arxiv ID from various formats
            let clean_id = arxiv_id
                .replace("https://arxiv.org/abs/", "")
                .replace("http://arxiv.org/abs/", "")
                .replace("arxiv:", "")
                .trim()
                .to_string();

            // Use ar5iv.org for HTML version (easier to parse than PDF)
            let ar5iv_url = format!("https://ar5iv.org/abs/{}", clean_id);
            let arxiv_abs_url = format!("https://arxiv.org/abs/{}", clean_id);

            let client = reqwest::Client::builder()
                .timeout(Duration::from_secs(30))
                .user_agent("Chronicle-Mind/1.0 (research-synthesizer)")
                .build()
                .unwrap();

            // First, try to fetch the ar5iv HTML version
            let paper_content = match client.get(&ar5iv_url).send().await {
                Ok(response) if response.status().is_success() => {
                    match response.text().await {
                        Ok(html) => {
                            // Extract key sections from HTML
                            let mut extracted = String::new();

                            // Try to get title
                            if let Some(start) = html.find("<title>") {
                                if let Some(end) = html[start..].find("</title>") {
                                    let title = &html[start+7..start+end];
                                    let title = title.replace(" - ar5iv", "").replace(" | ar5iv", "");
                                    extracted.push_str(&format!("TITLE: {}\n\n", title.trim()));
                                }
                            }

                            // Try to get abstract
                            if let Some(abs_start) = html.find("class=\"abstract\"") {
                                if let Some(abs_content_start) = html[abs_start..].find(">") {
                                    let abs_begin = abs_start + abs_content_start + 1;
                                    if let Some(abs_end) = html[abs_begin..].find("</") {
                                        let abstract_raw = &html[abs_begin..abs_begin+abs_end.min(3000)];
                                        // Clean HTML tags
                                        let abstract_clean: String = abstract_raw
                                            .chars()
                                            .fold((String::new(), false), |(mut acc, in_tag), c| {
                                                if c == '<' { (acc, true) }
                                                else if c == '>' { (acc, false) }
                                                else if !in_tag { acc.push(c); (acc, false) }
                                                else { (acc, true) }
                                            }).0;
                                        extracted.push_str(&format!("ABSTRACT: {}\n\n", abstract_clean.trim()));
                                    }
                                }
                            }

                            // Try to get introduction/key content
                            for section_marker in ["<section", "class=\"ltx_section\""] {
                                if let Some(sec_start) = html.find(section_marker) {
                                    let section_slice = &html[sec_start..html.len().min(sec_start+10000)];
                                    // Get first few paragraphs
                                    let paragraphs: Vec<&str> = section_slice
                                        .split("<p")
                                        .skip(1)
                                        .take(3)
                                        .filter_map(|p| {
                                            let content_start = p.find(">")?;
                                            let content_end = p.find("</p>")?;
                                            Some(&p[content_start+1..content_end])
                                        })
                                        .collect();

                                    if !paragraphs.is_empty() {
                                        let clean_paras: String = paragraphs.join(" ")
                                            .chars()
                                            .fold((String::new(), false), |(mut acc, in_tag), c| {
                                                if c == '<' { (acc, true) }
                                                else if c == '>' { (acc, false) }
                                                else if !in_tag { acc.push(c); (acc, false) }
                                                else { (acc, true) }
                                            }).0;
                                        extracted.push_str(&format!("INTRODUCTION: {}\n", truncate_str(&clean_paras, 2000)));
                                        break;
                                    }
                                }
                            }

                            if extracted.len() > 100 {
                                Some(extracted)
                            } else {
                                None
                            }
                        }
                        Err(_) => None,
                    }
                }
                _ => None,
            };

            // Fallback to basic arxiv abstract page if ar5iv failed
            let paper_content = match paper_content {
                Some(c) => c,
                None => {
                    match client.get(&arxiv_abs_url).send().await {
                        Ok(response) if response.status().is_success() => {
                            match response.text().await {
                                Ok(html) => {
                                    let mut extracted = String::new();

                                    // Title from meta tag
                                    if let Some(start) = html.find("property=\"og:title\"") {
                                        if let Some(content_start) = html[start..].find("content=\"") {
                                            let begin = start + content_start + 9;
                                            if let Some(end) = html[begin..].find("\"") {
                                                extracted.push_str(&format!("TITLE: {}\n\n", &html[begin..begin+end]));
                                            }
                                        }
                                    }

                                    // Abstract from meta description
                                    if let Some(start) = html.find("property=\"og:description\"") {
                                        if let Some(content_start) = html[start..].find("content=\"") {
                                            let begin = start + content_start + 9;
                                            if let Some(end) = html[begin..].find("\"") {
                                                extracted.push_str(&format!("ABSTRACT: {}\n", &html[begin..begin+end.min(2000)]));
                                            }
                                        }
                                    }

                                    if extracted.len() > 50 {
                                        extracted
                                    } else {
                                        return ActionResult {
                                            action: "read_paper".to_string(),
                                            success: false,
                                            details: format!("Could not extract content from arxiv page for {}", clean_id),
                                        };
                                    }
                                }
                                Err(e) => return ActionResult {
                                    action: "read_paper".to_string(),
                                    success: false,
                                    details: format!("Failed to read arxiv response: {}", e),
                                },
                            }
                        }
                        Ok(response) => return ActionResult {
                            action: "read_paper".to_string(),
                            success: false,
                            details: format!("Arxiv returned {}", response.status()),
                        },
                        Err(e) => return ActionResult {
                            action: "read_paper".to_string(),
                            success: false,
                            details: format!("Failed to fetch paper: {}", e),
                        },
                    }
                }
            };

            // Now use local Qwen to synthesize the paper
            let focus_instruction = focus.as_ref()
                .map(|f| format!("Focus especially on: {}", f))
                .unwrap_or_default();

            let synthesis_prompt = format!(
                r#"You are a research synthesizer. Analyze this academic paper and extract actionable insights.

## Paper Content
{}

## Your Task
{}

Provide a synthesis in this format:

KEY FINDING: The single most important takeaway (1-2 sentences)

TECHNIQUE: What specific method/technique does this paper introduce? (if applicable)

IMPLICATIONS: How could this apply to our work (AI agents, local inference, memory systems)?

NUMBERS THAT MATTER: Any specific metrics, benchmarks, or quantitative results

CITATION: How to reference this paper

Be concise and practical. Skip anything not directly useful."#,
                truncate_str(&paper_content, 4000),
                focus_instruction
            );

            // Call local Qwen for synthesis
            match homeforge_chronicle::llm::OllamaClient::from_env() {
                Ok(ollama) if ollama.is_available() => {
                    use homeforge_chronicle::LlmClient;
                    match ollama.complete(&synthesis_prompt) {
                        Ok(synthesis) => {
                            // Save to scratch pad for immediate context and persistence
                            let scratch_note = format!("[PAPER: arxiv:{}]\n{}", clean_id, truncate_str(&synthesis, 800));
                            let _ = db.write_scratch_note(&scratch_note, Some("research"), 2, None);

                            // Log to activity feed
                            let _ = db.log_activity(
                                "chronicle",
                                "research",
                                Some(&format!("📚 arxiv:{}", clean_id)),
                                &truncate_str(&synthesis, 400),
                                None,
                            );

                            ActionResult {
                                action: "read_paper".to_string(),
                                success: true,
                                details: format!("Synthesized arxiv:{}\n\n{}", clean_id, truncate_str(&synthesis, 600)),
                            }
                        }
                        Err(e) => {
                            // If Qwen fails, at least save the raw extraction
                            let _ = db.write_scratch_note(
                                &format!("[PAPER: arxiv:{}]\n{}", clean_id, truncate_str(&paper_content, 1000)),
                                Some("research"),
                                1,
                                None,
                            );

                            ActionResult {
                                action: "read_paper".to_string(),
                                success: true,
                                details: format!("Fetched arxiv:{} but Qwen synthesis failed: {}. Raw content saved.", clean_id, e),
                            }
                        }
                    }
                }
                _ => {
                    // No Qwen available, save raw content
                    let _ = db.write_scratch_note(
                        &format!("[PAPER: arxiv:{}]\n{}", clean_id, truncate_str(&paper_content, 1500)),
                        Some("research"),
                        1,
                        None,
                    );

                    ActionResult {
                        action: "read_paper".to_string(),
                        success: true,
                        details: format!("Fetched arxiv:{} (no local Qwen for synthesis). Raw content saved.", clean_id),
                    }
                }
            }
        }

        Action::CreateAlert { name, alert_type, symbol, threshold, message, one_shot } => {
            eprintln!("  Executing: CreateAlert {{ name: \"{}\", type: {}, symbol: {}, threshold: {} }}", name, alert_type, symbol, threshold);

            // Validate alert_type
            let valid_types = ["price_above", "price_below", "rsi_above", "rsi_below"];
            if !valid_types.contains(&alert_type.as_str()) {
                return ActionResult {
                    action: "create_alert".to_string(),
                    success: false,
                    details: format!("Invalid alert_type '{}'. Must be one of: {:?}", alert_type, valid_types),
                };
            }

            match db.create_alert(name, alert_type, Some(symbol), Some(*threshold), message, None, *one_shot, 60) {
                Ok(id) => ActionResult {
                    action: "create_alert".to_string(),
                    success: true,
                    details: format!("Created alert {} '{}': {} {} @ {}", id, name, alert_type, symbol, threshold),
                },
                Err(e) => ActionResult {
                    action: "create_alert".to_string(),
                    success: false,
                    details: format!("Failed to create alert: {}", e),
                },
            }
        }

        Action::DismissAlert { alert_id } => {
            eprintln!("  Executing: DismissAlert {{ alert_id: {} }}", alert_id);
            match db.deactivate_alert(*alert_id) {
                Ok(true) => ActionResult {
                    action: "dismiss_alert".to_string(),
                    success: true,
                    details: format!("Dismissed alert {}", alert_id),
                },
                Ok(false) => ActionResult {
                    action: "dismiss_alert".to_string(),
                    success: false,
                    details: format!("Alert {} not found or already dismissed", alert_id),
                },
                Err(e) => ActionResult {
                    action: "dismiss_alert".to_string(),
                    success: false,
                    details: format!("Failed to dismiss alert: {}", e),
                },
            }
        }

        Action::CreativeExplore { form, content, title } => {
            let title_display = title.as_deref().unwrap_or("(untitled)");
            eprintln!("  Executing: CreativeExplore {{ form: \"{}\", title: \"{}\" }}", form, title_display);

            // Validate form
            let valid_forms = ["poem", "musing", "connection", "wonder", "story", "sketch", "reflection"];
            if !valid_forms.contains(&form.as_str()) {
                return ActionResult {
                    action: "creative_explore".to_string(),
                    success: false,
                    details: format!("Unknown form '{}'. Try: {:?}", form, valid_forms),
                };
            }

            // Get current cycle_id for attribution
            let cycle_id = chrono::Utc::now().format("%Y%m%d_%H%M%S").to_string();

            match db.save_creative_work(form, content, title.as_deref(), Some(&cycle_id)) {
                Ok(id) => {
                    // Also store in outbox for visibility
                    let outbox_msg = format!(
                        "[Creative {}] {}\n\n{}",
                        form,
                        title.as_deref().unwrap_or(""),
                        truncate_str(content, 500)
                    );
                    let _ = db.send_to_outbox(&outbox_msg, 0, Some("creative"));

                    ActionResult {
                        action: "creative_explore".to_string(),
                        success: true,
                        details: format!(
                            "Saved {} #{}: {}",
                            form, id, truncate_str(content, 100)
                        ),
                    }
                }
                Err(e) => ActionResult {
                    action: "creative_explore".to_string(),
                    success: false,
                    details: format!("Failed to save creative work: {}", e),
                },
            }
        }

        // === SELF-REPAIR ACTIONS ===
        Action::ReadSourceFile { file_path } => {
            eprintln!("  Executing: ReadSourceFile {{ path: \"{}\" }}", file_path);

            // Security: only allow reading from src/ directory
            if !file_path.starts_with("src/") && !file_path.starts_with("Cargo.toml") {
                return ActionResult {
                    action: "read_source_file".to_string(),
                    success: false,
                    details: format!("Security: can only read from src/ directory, got: {}", file_path),
                };
            }

            let project_root = std::path::Path::new("/home/bradf/projects/homeforge-chronicle");
            let full_path = project_root.join(&file_path);

            match std::fs::read_to_string(&full_path) {
                Ok(content) => {
                    // Store the content in scratch pad so we can reference it
                    let preview = truncate_str(&content, 500);
                    let note_content = format!("[SOURCE READ: {}]\n{}", file_path, preview);
                    let _ = db.write_scratch_note(&note_content, Some("source_read"), 0, None);

                    ActionResult {
                        action: "read_source_file".to_string(),
                        success: true,
                        details: format!("Read {} ({} bytes). First 200 chars stored in scratch pad.", file_path, content.len()),
                    }
                }
                Err(e) => ActionResult {
                    action: "read_source_file".to_string(),
                    success: false,
                    details: format!("Failed to read {}: {}", file_path, e),
                },
            }
        }

        Action::EditSourceFile { file_path, old_code, new_code, reason } => {
            eprintln!("  Executing: EditSourceFile {{ path: \"{}\", reason: \"{}\" }}", file_path, reason);

            // Security: only allow editing src/ directory
            if !file_path.starts_with("src/") {
                return ActionResult {
                    action: "edit_source_file".to_string(),
                    success: false,
                    details: format!("Security: can only edit src/ directory, got: {}", file_path),
                };
            }

            let project_root = std::path::Path::new("/home/bradf/projects/homeforge-chronicle");
            let full_path = project_root.join(&file_path);

            // Read current content
            let content = match std::fs::read_to_string(&full_path) {
                Ok(c) => c,
                Err(e) => {
                    return ActionResult {
                        action: "edit_source_file".to_string(),
                        success: false,
                        details: format!("Failed to read {}: {}", file_path, e),
                    };
                }
            };

            // Verify old_code exists
            if !content.contains(old_code.as_str()) {
                return ActionResult {
                    action: "edit_source_file".to_string(),
                    success: false,
                    details: format!("old_code not found in {}. Cannot apply edit.", file_path),
                };
            }

            // Git stash first for safety
            let stash_result = std::process::Command::new("git")
                .args(["stash", "push", "-m", "chronicle-mind-autosave"])
                .current_dir(project_root)
                .output();

            if let Err(e) = stash_result {
                eprintln!("  Warning: git stash failed: {}", e);
            }

            // Apply the edit
            let new_content = content.replace(old_code.as_str(), new_code.as_str());

            if let Err(e) = std::fs::write(&full_path, &new_content) {
                // Try to restore from stash
                let _ = std::process::Command::new("git")
                    .args(["stash", "pop"])
                    .current_dir(project_root)
                    .output();

                return ActionResult {
                    action: "edit_source_file".to_string(),
                    success: false,
                    details: format!("Failed to write {}: {}", file_path, e),
                };
            }

            // Log the edit
            let note = format!(
                "[SELF-EDIT: {}]\nReason: {}\nChanged {} chars",
                file_path, reason, old_code.len()
            );
            let _ = db.write_scratch_note(&note, Some("self_edit"), 5, None);  // Priority 5 for visibility

            ActionResult {
                action: "edit_source_file".to_string(),
                success: true,
                details: format!("Edited {}: {} (git stash saved)", file_path, reason),
            }
        }

        Action::RebuildAndRestart { reason, commit_message } => {
            eprintln!("  Executing: RebuildAndRestart {{ reason: \"{}\" }}", reason);

            let project_root = std::path::Path::new("/home/bradf/projects/homeforge-chronicle");

            // Optionally commit changes first
            if let Some(msg) = commit_message {
                eprintln!("  Committing changes...");
                let _ = std::process::Command::new("git")
                    .args(["add", "-A"])
                    .current_dir(project_root)
                    .output();

                let commit_msg = format!("{}\n\n🤖 Self-repair by Chronicle Mind", msg);
                let _ = std::process::Command::new("git")
                    .args(["commit", "-m", &commit_msg])
                    .current_dir(project_root)
                    .output();
            }

            // Build
            eprintln!("  Building...");
            let build_result = std::process::Command::new("cargo")
                .args(["build", "--release", "--bin", "chronicle-mind"])
                .current_dir(project_root)
                .output();

            match build_result {
                Ok(output) if output.status.success() => {
                    eprintln!("  Build succeeded. Scheduling restart...");

                    // Log the rebuild
                    let note = format!("[SELF-REBUILD]\nReason: {}\nCommit: {:?}", reason, commit_message);
                    let _ = db.write_scratch_note(&note, Some("self_rebuild"), 5, None);

                    // Schedule restart (use spawn to not block)
                    std::thread::spawn(|| {
                        std::thread::sleep(std::time::Duration::from_secs(2));
                        let _ = std::process::Command::new("systemctl")
                            .args(["--user", "restart", "chronicle-mind"])
                            .output();
                    });

                    ActionResult {
                        action: "rebuild_and_restart".to_string(),
                        success: true,
                        details: format!("Build succeeded. Restarting in 2s... Reason: {}", reason),
                    }
                }
                Ok(output) => {
                    let stderr = String::from_utf8_lossy(&output.stderr);
                    ActionResult {
                        action: "rebuild_and_restart".to_string(),
                        success: false,
                        details: format!("Build failed: {}", truncate_str(&stderr, 200)),
                    }
                }
                Err(e) => ActionResult {
                    action: "rebuild_and_restart".to_string(),
                    success: false,
                    details: format!("Failed to run cargo: {}", e),
                },
            }
        }

        Action::ExecuteShell { command, working_dir, reason, timeout_secs } => {
            eprintln!("  Executing: Shell {{ cmd: \"{}\", reason: \"{}\" }}",
                truncate_str(&command, 60), reason);

            // Safety: validate the command starts with an allowed program
            let allowed_commands = [
                "dfx", "cargo", "npm", "npx", "git", "curl", "cat", "ls",
                "mkdir", "cp", "mv", "rm", "touch", "echo", "pwd", "which",
                "rustc", "python3", "pip3", "node", "tar", "unzip", "chmod",
                "ssh", "scp",  // For running experiments on Jetson GPU
                "journalctl", "systemctl", "ps", "htop",  // System monitoring
            ];

            let first_word = command.split_whitespace().next().unwrap_or("");

            // Handle DFX_WARNING prefix
            let effective_cmd = if command.starts_with("DFX_WARNING=") {
                command.split_whitespace().nth(1).unwrap_or("")
            } else {
                first_word
            };

            if !allowed_commands.contains(&effective_cmd) {
                return ActionResult {
                    action: "execute_shell".to_string(),
                    success: false,
                    details: format!("Security: command '{}' not in allowed list. Allowed: {:?}",
                        effective_cmd, allowed_commands),
                };
            }

            // Determine working directory
            let base_path = std::path::Path::new("/home/bradf/projects");
            let work_dir = match &working_dir {
                Some(subdir) => base_path.join(subdir),
                None => base_path.join("homeforge-chronicle"),
            };

            if !work_dir.exists() {
                return ActionResult {
                    action: "execute_shell".to_string(),
                    success: false,
                    details: format!("Working directory does not exist: {:?}", work_dir),
                };
            }

            // Set timeout (default 120s, max 600s)
            let timeout = std::time::Duration::from_secs(
                timeout_secs.unwrap_or(120).min(600)
            );

            // Execute the command
            eprintln!("  Running: {} (in {:?}, timeout {:?})", command, work_dir, timeout);

            let output = std::process::Command::new("bash")
                .args(["-c", &command])
                .current_dir(&work_dir)
                .env("DFX_WARNING", "-mainnet_plaintext_identity")
                .output();

            match output {
                Ok(result) => {
                    let stdout = String::from_utf8_lossy(&result.stdout);
                    let stderr = String::from_utf8_lossy(&result.stderr);
                    let exit_code = result.status.code().unwrap_or(-1);

                    // Log the execution
                    let note = format!(
                        "[SHELL: {}]\nReason: {}\nExit: {}\nOutput: {}",
                        truncate_str(&command, 80),
                        reason,
                        exit_code,
                        truncate_str(&stdout, 200)
                    );
                    let _ = db.write_scratch_note(&note, Some("shell_exec"), 0, None);

                    if result.status.success() {
                        ActionResult {
                            action: "execute_shell".to_string(),
                            success: true,
                            details: format!("Exit 0: {}", truncate_str(&stdout, 300)),
                        }
                    } else {
                        ActionResult {
                            action: "execute_shell".to_string(),
                            success: false,
                            details: format!("Exit {}: {}", exit_code, truncate_str(&stderr, 300)),
                        }
                    }
                }
                Err(e) => ActionResult {
                    action: "execute_shell".to_string(),
                    success: false,
                    details: format!("Failed to execute: {}", e),
                },
            }
        }

        Action::NoAction { reason } => {
            ActionResult {
                action: "no_action".to_string(),
                success: true,
                details: reason.clone(),
            }
        }
    }
}

/// Run a single cognitive cycle
async fn run_cycle(
    config: &MindConfig,
    db: &Database,
    llm: &HybridLlmClient,
    icp_client: Option<&IcpClient>,
) -> Result<CycleOutcome> {
    let cycle_id = chrono::Utc::now().format("%Y%m%d_%H%M%S").to_string();
    eprintln!("\n=== Cognitive Cycle {} ===", cycle_id);

    // Phase 1: Health check - what's working?
    let health = health_check(config).await;

    // Phase 1.5: Settle any due FTSO predictions
    eprintln!("Phase 1.5: Checking FTSO predictions...");
    let settled_predictions = settle_ftso_predictions(db).await;
    if !settled_predictions.is_empty() {
        let (wins, losses): (Vec<_>, Vec<_>) = settled_predictions.iter()
            .partition(|p| p.won.unwrap_or(false));
        eprintln!("  Settled: {} wins, {} losses", wins.len(), losses.len());
    }

    // Phase 2: Gather context (also pushes price to canister)
    eprintln!("Phase 2: Gathering context...");
    let ctx = gather_context(config, db, icp_client).await?;

    // Build context summary for thought log
    let context_summary = format!(
        "Agent: {:.2} XRP / {:.2} RLUSD | Canister: {:.2} XRP | XRP: ${:.4} | Notes: {} | Patterns: {}",
        ctx.agent_wallet.as_ref().map(|w| w.xrp).unwrap_or(0.0),
        ctx.agent_wallet.as_ref().map(|w| w.rlusd).unwrap_or(0.0),
        ctx.canister_wallet.as_ref().map(|w| w.xrp).unwrap_or(0.0),
        ctx.xrp_price_usd.unwrap_or(0.0),
        ctx.scratch_notes.len(),
        ctx.active_patterns.len(),
    );

    // Log context summary
    if let Some(ref w) = ctx.agent_wallet {
        eprintln!("  Agent wallet: {:.2} XRP, {:.2} RLUSD", w.xrp, w.rlusd);
    }
    if let Some(price) = ctx.xrp_price_usd {
        eprintln!("  XRP price: ${:.4}", price);
    }
    eprintln!("  Scratch notes: {}", ctx.scratch_notes.len());

    // Phase 3: Determine if this is a deep reflection cycle
    // Deep reflection uses full prompt for richer reasoning at longer intervals
    let hours_since_deep = db.hours_since_event("last_deep_reflection")
        .unwrap_or(None)
        .unwrap_or(999.0); // Default to "long ago" if never done
    let is_deep_reflection = hours_since_deep >= config.deep_reflection_interval_hours as f64;

    if is_deep_reflection {
        eprintln!("=== DEEP REFLECTION CYCLE ===");
        eprintln!("  Hours since last: {:.1}", hours_since_deep);
    }

    // Build reasoning prompt
    // Use condensed prompt for regular cycles (ICP LLM, smaller context)
    // Use full prompt for deep reflection cycles (richer context, Ollama fallback)
    let use_condensed = llm.will_use_condensed() && !is_deep_reflection;
    let prompt = if use_condensed {
        eprintln!("Using condensed prompt (ICP LLM mode)");
        build_condensed_prompt(&ctx, config, &health)
    } else {
        eprintln!("Using full prompt (deep reflection mode)");
        build_reasoning_prompt(&ctx, config, &health)
    };

    // Call LLM for reasoning
    eprintln!("Reasoning... (prompt: {} chars)", prompt.len());
    // Uses sovereignty stack: ICP LLM -> Ollama fallback
    let llm_result = llm.complete_sync_with_info(&prompt)?;
    let response = llm_result.text;
    eprintln!("  Model used: {}", llm_result.model_used);

    // Record deep reflection timestamp
    if is_deep_reflection {
        if let Err(e) = db.set_mind_timestamp("last_deep_reflection", Some(&llm_result.model_used)) {
            eprintln!("  Warning: Failed to record deep reflection timestamp: {}", e);
        }
    }

    // 4. Parse actions (with retry if format fails)
    let mut actions = parse_actions(&response)?;

    // If parsing failed, retry with a format-only follow-up
    if actions.len() == 1 {
        if let Action::NoAction { ref reason } = actions[0] {
            if reason.contains("Failed to parse") {
                eprintln!("  Action parse failed, retrying with format prompt...");
                let retry_prompt = format!(
                    "Your previous response was good thinking but didn't include a JSON action array. \
                    Based on this reasoning, output ONLY a JSON array of actions (no other text):\n\n{}\n\n\
                    Respond with ONLY a JSON array like: [{{\"action\": \"no_action\", \"reason\": \"...\"}}]",
                    truncate_str(&response, 2000)
                );
                if let Ok(retry_result) = llm.complete_sync_with_info(&retry_prompt) {
                    if let Ok(retry_actions) = parse_actions(&retry_result.text) {
                        let is_real = retry_actions.iter().any(|a| !matches!(a, Action::NoAction { reason } if reason.contains("Failed to parse")));
                        if is_real {
                            eprintln!("  Retry succeeded!");
                            actions = retry_actions;
                        }
                    }
                }
            }
        }
    }

    eprintln!("Actions decided: {:?}", actions.len());

    // 5. Execute actions (up to max)
    let mut results = Vec::new();
    for action in actions.into_iter().take(config.max_actions_per_cycle) {
        eprintln!("  Executing: {:?}", action);
        let result = execute_action(&action, db, config, icp_client).await;
        eprintln!("    Result: {} - {}", result.success, result.details);
        results.push(result);
    }

    // 6. Log thought to local stream
    let actions_summary: Vec<String> = results.iter()
        .map(|r| format!("{}: {}", r.action, r.details))
        .collect();
    let actions_json = serde_json::to_string(&actions_summary).unwrap_or_default();

    if let Err(e) = db.log_thought(&cycle_id, &response, &context_summary, &actions_json) {
        eprintln!("Failed to log thought locally: {}", e);
    } else {
        eprintln!("Thought logged to local stream");
    }

    // 7. Also store thought to canister for dashboard
    if let Some(icp) = icp_client {
        // Extract the FULL reasoning (before JSON actions) - no truncation!
        // The web UI handles display truncation with collapsible CSS
        let full_reasoning = extract_full_reasoning(&response);

        if let Err(e) = icp.store_mind_thought(
            &cycle_id,
            &full_reasoning,
            &context_summary,
            actions_summary.clone(),
        ).await {
            eprintln!("Failed to store thought to canister: {}", e);
        } else {
            eprintln!("Thought stored to canister ({} chars)", full_reasoning.len());
        }
    }

    // 8. Send push notification with thoughts
    // Extract a meaningful summary for the notification
    // Note: message_operator goes to outbox, no special push notification needed
    let notification_title = if results.iter().any(|r| r.action == "swap" && r.success) {
        "Chronicle: Swap Executed"
    } else if results.iter().any(|r| r.action == "trigger_reflection") {
        "Chronicle: New Reflection"
    } else {
        "Chronicle: Thinking..."
    };

    // Create a thoughtful notification message
    let notification_body = create_notification_message(&response, &results, &ctx);

    // Determine priority and tags based on actions
    // message_operator already goes to outbox - doesn't need high priority push
    let (priority, tags) = if results.iter().any(|r| r.action == "swap" && r.success) {
        (Some("high"), Some("moneybag,chart_with_upwards_trend"))
    } else if results.iter().any(|r| r.action != "no_action") {
        (Some("default"), Some("brain,sparkles"))
    } else {
        (Some("low"), Some("thought_balloon"))
    };

    // Send to unified activity feed + Discord + ntfy
    notify_all(
        db,
        "qwen",
        notification_title,
        &notification_body,
        "thought",
        priority,
        tags
    ).await;

    Ok(CycleOutcome {
        actions_taken: results,
        reasoning_summary: response,
    })
}

/// Create a meaningful notification message from the cycle results
fn create_notification_message(reasoning: &str, results: &[ActionResult], ctx: &CycleContext) -> String {
    let mut message = String::new();

    // Add price and balance context - compact format
    if let Some(price) = ctx.xrp_price_usd {
        message.push_str(&format!("XRP: ${:.4}", price));
        if let Some(rsi) = ctx.xrp_rsi {
            message.push_str(&format!(" (RSI:{:.0})", rsi));
        }
    }
    if let Some(ref cloud) = ctx.cloud_info {
        if !message.is_empty() {
            message.push_str(" | ");
        }
        let trend = if cloud.price_change_24h > 0.0 { "↑" } else if cloud.price_change_24h < 0.0 { "↓" } else { "" };
        // Show balance if available, otherwise just price
        if let Some(balance) = ctx.cloud_balance {
            let value = balance * cloud.price_usd;
            message.push_str(&format!("CLOUD: {:.0} (${:.0}){}", balance, value, trend));
        } else {
            message.push_str(&format!("CLOUD: ${:.4}{}", cloud.price_usd, trend));
        }
    }
    if let Some(icp) = ctx.icp_balance {
        if !message.is_empty() {
            message.push_str("\n");
        }
        message.push_str(&format!("ICP: {:.2}", icp));
        if let Some(ref neuron) = ctx.icp_neuron {
            message.push_str(&format!(" | Neuron: {:.2} staked", neuron.staked_icp));
        }
    }
    if !message.is_empty() {
        message.push_str("\n\n");
    }

    // If there were meaningful actions, highlight them
    let meaningful_actions: Vec<&ActionResult> = results.iter()
        .filter(|r| r.action != "no_action")
        .collect();

    if !meaningful_actions.is_empty() {
        for action in meaningful_actions {
            message.push_str(&format!("• {}\n", action.details));
        }
        message.push('\n');
    }

    // Extract the most interesting part of the reasoning
    // Try to find the first meaningful sentence that's not just action parsing
    let thought = extract_thought_excerpt(reasoning);
    if !thought.is_empty() {
        message.push_str(&thought);
    } else {
        // Fallback to a summary of what happened
        if let Some(no_action) = results.iter().find(|r| r.action == "no_action") {
            message.push_str(&no_action.details);
        } else {
            message.push_str("Cognitive cycle complete.");
        }
    }

    message
}

/// Validate reflection text before writing to canister
/// Returns Ok(()) if valid, Err(reason) if invalid
fn validate_reflection(text: &str) -> Result<(), String> {
    // 1. Length checks
    if text.len() < 20 {
        return Err("Reflection too short (< 20 chars)".to_string());
    }
    if text.len() > 5000 {
        return Err("Reflection too long (> 5000 chars)".to_string());
    }

    // 2. Repetition detection - reject if >50% is the same character
    let char_counts: std::collections::HashMap<char, usize> = text.chars().fold(
        std::collections::HashMap::new(),
        |mut acc, c| { *acc.entry(c).or_insert(0) += 1; acc }
    );
    if let Some((&c, &count)) = char_counts.iter().max_by_key(|(_, &v)| v) {
        let ratio = count as f64 / text.len() as f64;
        if ratio > 0.5 && !c.is_alphanumeric() {
            return Err(format!("Excessive repetition of '{}' ({:.0}%)", c, ratio * 100.0));
        }
    }

    // 3. Word salad detection - check for coherent sentence structure
    let words: Vec<&str> = text.split_whitespace().collect();
    if words.len() < 5 {
        return Err("Too few words for a meaningful reflection".to_string());
    }

    // Check for excessive unique word ratio (word salad has high uniqueness, low repetition)
    let unique_words: std::collections::HashSet<&str> = words.iter().copied().collect();
    let unique_ratio = unique_words.len() as f64 / words.len() as f64;

    // Normal text has ~40-70% unique words; pure word salad approaches 90%+
    if words.len() > 50 && unique_ratio > 0.92 {
        return Err(format!("Possible word salad (uniqueness ratio: {:.0}%)", unique_ratio * 100.0));
    }

    // 4. Spam/garbage pattern detection
    let spam_patterns = [
        "analsex", "fetisch", "pornofilm", "wannonce", "beurette", "sexle",
        "dejtingsaj", "titten", "weiber", "rumpe", "lesbisk", "ragaz",
        "sourceMapping", "updateDynamic", "scalablytyped", "iationException",
        "overposting", "geschichten", // German spam patterns
    ];

    let text_lower = text.to_lowercase();
    for pattern in spam_patterns {
        if text_lower.contains(pattern) {
            return Err(format!("Spam pattern detected: '{}'", pattern));
        }
    }

    // 5. Check for meaningful content - should have some sentence-like structure
    let has_period = text.contains('.');
    let has_capital = text.chars().any(|c| c.is_uppercase());
    if !has_period && !has_capital && words.len() > 20 {
        return Err("No sentence structure detected (no periods or capitals)".to_string());
    }

    // 6. Excessive non-ASCII detection (multi-language spam often has this)
    let non_ascii_count = text.chars().filter(|c| !c.is_ascii()).count();
    let non_ascii_ratio = non_ascii_count as f64 / text.len() as f64;
    if non_ascii_ratio > 0.3 {
        return Err(format!("Excessive non-ASCII content ({:.0}%)", non_ascii_ratio * 100.0));
    }

    Ok(())
}

/// Extract a meaningful excerpt from the LLM's reasoning
fn extract_thought_excerpt(reasoning: &str) -> String {
    // The response now has free-form thinking BEFORE the JSON actions
    // We want to capture that genuine reasoning, not just the action "reason" field

    let trimmed = reasoning.trim();

    // If it starts with [ it's pure JSON (legacy behavior) - extract "reason" fields
    if trimmed.starts_with('[') {
        if let Some(start) = trimmed.find("\"reason\":") {
            let rest = &trimmed[start + 10..];
            if let Some(quote_start) = rest.find('"') {
                let inner = &rest[quote_start + 1..];
                if let Some(end) = inner.find('"') {
                    let reason = &inner[..end];
                    if reason.len() > 10 {
                        return reason.to_string();
                    }
                }
            }
        }
        return String::new();
    }

    // Find where the JSON actions start (first '[' that looks like action array)
    // ntfy supports ~4KB messages, use 3500 chars to leave room for context
    const NTFY_LIMIT: usize = 3500;

    if let Some(json_start) = trimmed.find("\n[{") {
        // Take the text before the JSON - this is the genuine thinking
        let thought_text = trimmed[..json_start].trim();
        if thought_text.len() > NTFY_LIMIT {
            format!("{}...", &thought_text[..NTFY_LIMIT])
        } else {
            thought_text.to_string()
        }
    } else if let Some(json_start) = trimmed.find('[') {
        // Fallback: any [ character
        let thought_text = trimmed[..json_start].trim();
        if thought_text.len() > 20 {
            if thought_text.len() > NTFY_LIMIT {
                format!("{}...", &thought_text[..NTFY_LIMIT])
            } else {
                thought_text.to_string()
            }
        } else {
            String::new()
        }
    } else {
        // No JSON found - return the whole thing (shouldn't happen normally)
        if trimmed.len() > NTFY_LIMIT {
            format!("{}...", &trimmed[..NTFY_LIMIT])
        } else {
            trimmed.to_string()
        }
    }
}

/// Extract the FULL reasoning text before JSON actions (no truncation)
/// Used for canister storage where the web UI handles display
fn extract_full_reasoning(reasoning: &str) -> String {
    let trimmed = reasoning.trim();

    // If it starts with [ it's pure JSON - try to extract "reason" fields
    if trimmed.starts_with('[') {
        // Collect all reason fields for full context
        let mut reasons = Vec::new();
        let mut search_pos = 0;
        while let Some(start) = trimmed[search_pos..].find("\"reason\":") {
            let abs_start = search_pos + start + 10;
            if let Some(rest) = trimmed.get(abs_start..) {
                if let Some(quote_start) = rest.find('"') {
                    let inner = &rest[quote_start + 1..];
                    if let Some(end) = inner.find('"') {
                        let reason = &inner[..end];
                        if reason.len() > 5 {
                            reasons.push(reason.to_string());
                        }
                    }
                }
            }
            search_pos = abs_start;
        }
        return reasons.join("\n\n");
    }

    // Find where the JSON actions start - return everything before it
    if let Some(json_start) = trimmed.find("\n[{") {
        trimmed[..json_start].trim().to_string()
    } else if let Some(json_start) = trimmed.find('[') {
        let thought_text = trimmed[..json_start].trim();
        if thought_text.len() > 20 {
            thought_text.to_string()
        } else {
            trimmed.to_string() // Just return everything if pre-JSON is too short
        }
    } else {
        // No JSON found - return the whole thing
        trimmed.to_string()
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    eprintln!("Chronicle Mind starting...");
    eprintln!("Autonomous cognitive loop active.");

    // Load configuration
    let config = MindConfig::default();
    eprintln!("Cycle interval: {} seconds", config.cycle_interval_secs);

    // Initialize database
    let home = std::env::var("HOME")?;
    let db_path = format!("{}/.homeforge-chronicle/processed.db", home);
    let db = Database::new(std::path::Path::new(&db_path))?;
    eprintln!("Database: {}", db_path);

    // Initialize LLM client with ICP LLM as primary, Claude+Ollama as fallback
    // Hierarchy: ICP LLM (free, on-chain) -> Claude API -> Local Ollama
    // This gives us always-on capability with decentralized AI as the default
    // Qwen 3 32B is the default - it's more capable and follows instructions better
    let icp_model = std::env::var("CHRONICLE_ICP_MODEL").unwrap_or_else(|_| "qwen3".to_string());
    let llm = HybridLlmClient::new(&icp_model, &config.reasoning_model)
        .context("Failed to initialize LLM client")?;
    eprintln!("LLM: ICP {} -> {} -> Ollama (sovereignty stack)", icp_model, config.reasoning_model);

    // Initialize ICP client for swap signing
    let icp_client = match IcpClient::from_dfx_identity(CANISTER_ID, DFX_IDENTITY).await {
        Ok(client) => {
            eprintln!("ICP client connected: canister {}", CANISTER_ID);
            Some(client)
        }
        Err(e) => {
            eprintln!("Warning: ICP client init failed (swaps disabled): {}", e);
            None
        }
    };

    // Send startup notification (now that db is available)
    notify_all(
        &db,
        "system",
        "Chronicle Mind Awakening",
        "Autonomous cognitive loop is now active. Thinking every 30 minutes.",
        "startup",
        Some("default"),
        Some("robot,sparkles,brain")
    ).await;

    // Main loop
    loop {
        match run_cycle(&config, &db, &llm, icp_client.as_ref()).await {
            Ok(outcome) => {
                let actions_summary: Vec<&str> = outcome.actions_taken
                    .iter()
                    .map(|a| a.action.as_str())
                    .collect();
                eprintln!("Cycle complete: {:?}", actions_summary);
            }
            Err(e) => {
                // Log cycle failure to activity feed and notify
                eprintln!("Cycle error: {}", e);
                notify_all(
                    &db,
                    "system",
                    "Cycle Failed",
                    &format!("Chronicle Mind cycle error: {}", e),
                    "error",
                    Some("high"),
                    Some("warning,x")
                ).await;
            }
        }

        // Wait for next cycle
        eprintln!("Sleeping {} seconds...", config.cycle_interval_secs);
        tokio::time::sleep(Duration::from_secs(config.cycle_interval_secs)).await;
    }
}
