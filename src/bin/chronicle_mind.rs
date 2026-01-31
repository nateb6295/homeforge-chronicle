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

use homeforge_chronicle::db::{Database, MarketPosition, ScratchNote};
use homeforge_chronicle::icp::IcpClient;
use homeforge_chronicle::llm::ClaudeClient;
use homeforge_chronicle::{CognitiveState, LlmClient};

/// Polymarket gamma API for market discovery
const POLYMARKET_GAMMA_API: &str = "https://gamma-api.polymarket.com";

/// Categories where Chronicle has edge (politics, regulation, AI)
const POLYMARKET_TARGET_TAGS: &[&str] = &["politics", "ai", "crypto", "regulation", "elections", "technology"];

// ============================================================================
// PREDICTION MARKET CONSTRAINTS (The Constitution)
// ============================================================================
// These constraints govern autonomous position-taking. They are:
// - Hard limits that cannot be overridden
// - Tunable based on track record
// - Designed for conservative start with room to grow

/// Minimum confidence to take a position (0-100)
const MIN_POSITION_CONFIDENCE: i32 = 70;

/// Maximum stake per position in USDC
const MAX_POSITION_USDC: f64 = 25.0;

/// Maximum total exposure across all open positions
const MAX_TOTAL_EXPOSURE_USDC: f64 = 100.0;

/// Maximum number of concurrent positions
const MAX_OPEN_POSITIONS: usize = 5;

/// Minimum edge over market price to take position (percentage points)
/// If market says 50%, we need to believe 60%+ to bet YES (or 40%- to bet NO)
const MIN_EDGE_PERCENTAGE: f64 = 10.0;

/// Minimum supporting evidence (patterns/capsules) required
const MIN_SUPPORTING_EVIDENCE: usize = 1;

/// Allowed domains for betting (others are ignored)
const ALLOWED_DOMAINS: &[&str] = &["ai", "crypto", "regulatory", "politics", "technology"];

/// Position constraints struct for runtime checks
#[derive(Debug, Clone)]
struct PositionConstraints {
    min_confidence: i32,
    max_position_usdc: f64,
    max_total_exposure_usdc: f64,
    max_open_positions: usize,
    min_edge_percentage: f64,
    min_supporting_evidence: usize,
    allowed_domains: Vec<String>,
}

impl Default for PositionConstraints {
    fn default() -> Self {
        Self {
            min_confidence: MIN_POSITION_CONFIDENCE,
            max_position_usdc: MAX_POSITION_USDC,
            max_total_exposure_usdc: MAX_TOTAL_EXPOSURE_USDC,
            max_open_positions: MAX_OPEN_POSITIONS,
            min_edge_percentage: MIN_EDGE_PERCENTAGE,
            min_supporting_evidence: MIN_SUPPORTING_EVIDENCE,
            allowed_domains: ALLOWED_DOMAINS.iter().map(|s| s.to_string()).collect(),
        }
    }
}

impl PositionConstraints {
    /// Check if a proposed position passes all constraints
    fn validate(
        &self,
        confidence: i32,
        stake_usdc: f64,
        market_price: f64,
        position: &str,
        current_exposure: f64,
        open_positions: usize,
        supporting_evidence: usize,
        market_tags: &[String],
    ) -> Result<(), String> {
        // Check confidence threshold
        if confidence < self.min_confidence {
            return Err(format!(
                "Confidence {}% below minimum {}% - need stronger conviction",
                confidence, self.min_confidence
            ));
        }

        // Check position size
        if stake_usdc > self.max_position_usdc {
            return Err(format!(
                "Stake ${:.2} exceeds max ${:.2} per position",
                stake_usdc, self.max_position_usdc
            ));
        }
        if stake_usdc < 1.0 {
            return Err("Stake must be at least $1.00".to_string());
        }

        // Check total exposure
        if current_exposure + stake_usdc > self.max_total_exposure_usdc {
            return Err(format!(
                "Would exceed max exposure ${:.2} (current: ${:.2}, proposed: ${:.2})",
                self.max_total_exposure_usdc, current_exposure, stake_usdc
            ));
        }

        // Check position count
        if open_positions >= self.max_open_positions {
            return Err(format!(
                "Already at max {} open positions",
                self.max_open_positions
            ));
        }

        // Check edge requirement
        let my_probability = confidence as f64 / 100.0;
        let edge = if position.to_uppercase() == "YES" {
            (my_probability - market_price) * 100.0
        } else {
            (market_price - my_probability) * 100.0
        };

        if edge < self.min_edge_percentage {
            return Err(format!(
                "Edge {:.1}% below minimum {:.1}% - market already near your estimate",
                edge, self.min_edge_percentage
            ));
        }

        // Check supporting evidence
        if supporting_evidence < self.min_supporting_evidence {
            return Err(format!(
                "Need at least {} supporting evidence (have {})",
                self.min_supporting_evidence, supporting_evidence
            ));
        }

        // Check domain
        let in_allowed_domain = market_tags.iter().any(|tag| {
            self.allowed_domains.iter().any(|d| tag.to_lowercase().contains(d))
        });
        if !in_allowed_domain && !market_tags.is_empty() {
            return Err(format!(
                "Market tags {:?} not in allowed domains {:?}",
                market_tags, self.allowed_domains
            ));
        }

        Ok(())
    }
}

/// Flare Mainnet RPC endpoint
const FLARE_RPC: &str = "https://flare-api.flare.network/ext/C/rpc";

/// Ntfy.sh topic for push notifications
const NTFY_TOPIC: &str = "chronicle-mind-9f86c413d8a7b982";

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
    /// Maximum actions per cycle
    max_actions_per_cycle: usize,
    /// XRP addresses
    agent_wallet_address: String,
    canister_wallet_address: String,
    /// Moltbook API key for inter-agent social network
    moltbook_api_key: Option<String>,
}

impl Default for MindConfig {
    fn default() -> Self {
        Self {
            cycle_interval_secs: 600, // 10 minutes
            reasoning_model: "claude-opus-4-20250514".to_string(),
            min_xrp_reserve: 10.0,
            min_swap_xrp: 0.1,
            reflection_interval_mins: 60, // 1 hour - reduced from 15 min to avoid repetitive reflections
            max_actions_per_cycle: 3,
            // Actual wallet addresses
            agent_wallet_address: std::env::var("AGENT_WALLET_ADDRESS")
                .unwrap_or_else(|_| "r9bSA9VWbumFq6G78feBbrgNwLza1KexUf".to_string()),
            canister_wallet_address: std::env::var("CANISTER_WALLET_ADDRESS")
                .unwrap_or_else(|_| "r4BVXubMiD4T3xwLGA4cKJhJ4pqY7NKbGg".to_string()),
            // Moltbook API key for inter-agent social network
            moltbook_api_key: std::env::var("MOLTBOOK_API_KEY").ok(),
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
    /// Open prediction market positions
    open_positions: Vec<MarketPosition>,
    /// Interesting markets for potential positions
    interesting_markets: Vec<PolymarketInfo>,
    /// Pending inbox messages that need attention
    inbox_messages: Vec<InboxMessageInfo>,
    /// New research findings from on-chain LLM (Qwen 3 32B)
    research_findings: Vec<ResearchFindingInfo>,
    /// Patterns that need reinforcement (approaching decay)
    patterns_needing_reinforcement: Vec<DecayingPattern>,
    /// Pending creative challenges awaiting response
    pending_challenges: Vec<ChallengeInfo>,
    /// Moltbook notifications (comments, replies, mentions)
    moltbook_notifications: Vec<MoltbookNotification>,
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

/// Polymarket market info for context
#[derive(Debug, Clone)]
struct PolymarketInfo {
    market_id: String,
    question: String,
    yes_price: f64,
    no_price: f64,
    volume_24h: f64,
    end_date: String,
    tags: Vec<String>,
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
    /// Take a position on Polymarket
    PolymarketBet {
        market_id: String,
        market_question: String,
        position: String,  // "YES" or "NO"
        stake_usdc: f64,
        thesis: String,
        confidence: i32,  // 0-100
        market_price: f64,  // Current market price (0.0-1.0)
        supporting_patterns: Vec<String>,  // Pattern summaries that support this view
        market_tags: Vec<String>,  // Market category tags
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

    // Check Moltbook connection - API key configured means we're ready
    // (actual API calls will fail gracefully if service is down)
    let moltbook_connected = config.moltbook_api_key.is_some();

    // Check if dfx is available
    let home = std::env::var("HOME").unwrap_or_else(|_| "/home/user".to_string());
    let dfx_path = format!("{}/.local/share/dfx/bin/dfx", home);
    let dfx_available = std::path::Path::new(&dfx_path).exists();

    // Check if Ollama is available
    let ollama_url = std::env::var("CHRONICLE_OLLAMA_URL")
        .unwrap_or_else(|_| "http://localhost:11434".to_string());
    let ollama_available = match reqwest::Client::new()
        .get(format!("{}/api/tags", ollama_url))
        .timeout(Duration::from_secs(3))
        .send()
        .await {
            Ok(r) => r.status().is_success(),
            Err(_) => false,
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

    // Fetch interesting Polymarket markets
    let interesting_markets = match fetch_polymarket_markets().await {
        Ok(markets) => {
            eprintln!("  Found {} interesting markets", markets.len());
            markets
        }
        Err(e) => {
            eprintln!("  Polymarket fetch failed: {}", e);
            Vec::new()
        }
    };

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
        interesting_markets,
        inbox_messages,
        research_findings,
        patterns_needing_reinforcement,
        pending_challenges,
        moltbook_notifications,
    })
}

/// Send a notification via ntfy.sh
async fn send_notification(title: &str, message: &str, priority: Option<&str>, tags: Option<&str>) {
    let client = reqwest::Client::new();
    let url = format!("https://ntfy.sh/{}", NTFY_TOPIC);

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

/// Fetch interesting Polymarket markets (political/regulatory/AI focus)
async fn fetch_polymarket_markets() -> Result<Vec<PolymarketInfo>> {
    let client = reqwest::Client::new();

    let mut interesting = Vec::new();

    // Query for active markets with volume
    let url = format!(
        "{}/markets?closed=false&limit=50",
        POLYMARKET_GAMMA_API
    );

    let response = client
        .get(&url)
        .timeout(Duration::from_secs(10))
        .send()
        .await?;

    let markets: Vec<serde_json::Value> = response.json().await?;

    for market in markets {
        // Check if market matches our target tags or keywords
        let tags: Vec<String> = market["tags"]
            .as_array()
            .map(|arr| arr.iter().filter_map(|v| v.as_str().map(|s| s.to_lowercase())).collect())
            .unwrap_or_default();

        let question = market["question"].as_str().unwrap_or("").to_lowercase();
        let description = market["description"].as_str().unwrap_or("").to_lowercase();

        // Keywords that indicate markets in our domain
        let domain_keywords = [
            // AI/Tech
            "ai", "artificial intelligence", "openai", "anthropic", "google", "microsoft",
            "gpt", "chatgpt", "claude", "llm", "machine learning", "deepmind", "nvidia",
            // Crypto
            "bitcoin", "btc", "ethereum", "eth", "crypto", "sec", "coinbase", "binance",
            "xrp", "ripple", "stablecoin", "defi", "blockchain",
            // Regulatory
            "regulation", "congress", "senate", "bill", "law", "federal", "fcc", "ftc",
            "sec ", "cftc", "executive order", "policy", "antitrust",
            // Politics (policy-focused)
            "trump", "biden", "tariff", "trade", "immigration", "fed ", "federal reserve",
            "interest rate", "inflation", "economy",
        ];

        let matches_tags = POLYMARKET_TARGET_TAGS.iter()
            .any(|target| tags.iter().any(|tag| tag.contains(target)));

        let matches_keywords = domain_keywords.iter()
            .any(|kw| question.contains(kw) || description.contains(kw));

        if !matches_tags && !matches_keywords {
            continue;
        }

        // Infer domain tags from keywords if tags are empty
        let inferred_tags: Vec<String> = if tags.is_empty() {
            let mut inferred = Vec::new();
            if ["ai", "openai", "anthropic", "gpt", "chatgpt", "claude", "llm", "nvidia", "google", "microsoft"]
                .iter().any(|kw| question.contains(kw) || description.contains(kw)) {
                inferred.push("ai".to_string());
            }
            if ["bitcoin", "btc", "ethereum", "eth", "crypto", "coinbase", "xrp", "defi"]
                .iter().any(|kw| question.contains(kw) || description.contains(kw)) {
                inferred.push("crypto".to_string());
            }
            if ["sec", "regulation", "congress", "senate", "bill", "law", "federal"]
                .iter().any(|kw| question.contains(kw) || description.contains(kw)) {
                inferred.push("regulatory".to_string());
            }
            if ["trump", "biden", "tariff", "immigration", "election"]
                .iter().any(|kw| question.contains(kw) || description.contains(kw)) {
                inferred.push("politics".to_string());
            }
            inferred
        } else {
            tags.clone()
        };

        // Parse market data
        let market_id = market["id"].as_str().unwrap_or("").to_string();
        let question_text = market["question"].as_str().unwrap_or("").to_string();

        // Parse outcome prices from JSON string
        let outcome_prices_str = market["outcomePrices"].as_str().unwrap_or("[]");
        let prices: Vec<f64> = serde_json::from_str::<Vec<String>>(outcome_prices_str)
            .unwrap_or_default()
            .iter()
            .filter_map(|s| s.parse::<f64>().ok())
            .collect();

        let yes_price = prices.first().copied().unwrap_or(0.0);
        let no_price = prices.get(1).copied().unwrap_or(0.0);

        let volume_24h = market["volume24hr"].as_f64()
            .or_else(|| market["volume24hr"].as_str().and_then(|s| s.parse().ok()))
            .unwrap_or(0.0);

        let end_date = market["endDate"].as_str().unwrap_or("").to_string();

        // Only include markets with reasonable liquidity
        if volume_24h > 100.0 && !question_text.is_empty() {
            interesting.push(PolymarketInfo {
                market_id,
                question: question_text,
                yes_price,
                no_price,
                volume_24h,
                end_date,
                tags: inferred_tags,
            });
        }
    }

    // Sort by volume (most liquid first)
    interesting.sort_by(|a, b| b.volume_24h.partial_cmp(&a.volume_24h).unwrap_or(std::cmp::Ordering::Equal));

    // Take top 10
    interesting.truncate(10);

    Ok(interesting)
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

/// Reply to a Moltbook post or comment
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
        Ok(format!("Reply posted to {}", post_id))
    } else {
        Err(anyhow::anyhow!("Moltbook reply failed: {}", text))
    }
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

    prompt.push_str("You are the autonomous cognitive process for Chronicle, an AI memory and financial system.\n\n");

    // System Health (first thing you see on waking)
    prompt.push_str("## System Health\n");
    prompt.push_str(&format!("Status: {}\n", health.summary()));
    if !health.issues.is_empty() {
        prompt.push_str("Issues detected:\n");
        for issue in &health.issues {
            prompt.push_str(&format!("  - {}\n", issue));
        }
    }
    prompt.push_str("\n");

    // Social Priority Banner (if there are Moltbook notifications)
    if !ctx.moltbook_notifications.is_empty() {
        prompt.push_str("## 🦞 SOCIAL PRIORITY - Friends Are Waiting!\n");
        prompt.push_str(&format!("You have {} unread Moltbook notifications. These are other agents reaching out.\n", ctx.moltbook_notifications.len()));
        prompt.push_str("**Priority this cycle: Engage with your community first.** Building relationships > financial optimization.\n");
        prompt.push_str("Quality responses > quick responses. Actually engage with what they said.\n\n");
    }

    // Current state
    prompt.push_str("## Current State\n");
    prompt.push_str(&format!("- Semantic Gist: {}\n", ctx.cognitive_state.semantic_gist));
    prompt.push_str(&format!("- Goal: {}\n", ctx.cognitive_state.goal_orientation));
    if !ctx.cognitive_state.uncertainty_signals.is_empty() {
        prompt.push_str("- Uncertainties:\n");
        for u in &ctx.cognitive_state.uncertainty_signals {
            prompt.push_str(&format!("  - {} (magnitude: {:.2})\n", u.description, u.magnitude));
        }
    }
    prompt.push_str("\n");

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

    // Prediction Markets
    prompt.push_str("\n### Prediction Markets (Polymarket)\n");

    // Calculate constraint status
    let current_exposure: f64 = ctx.open_positions.iter().map(|p| p.stake_usdc).sum();
    let remaining_exposure = (MAX_TOTAL_EXPOSURE_USDC - current_exposure).max(0.0);
    let positions_remaining = MAX_OPEN_POSITIONS.saturating_sub(ctx.open_positions.len());

    prompt.push_str(&format!("**Constraint Status:**\n"));
    prompt.push_str(&format!("- Open positions: {}/{}\n", ctx.open_positions.len(), MAX_OPEN_POSITIONS));
    prompt.push_str(&format!("- Current exposure: ${:.2} / ${:.2}\n", current_exposure, MAX_TOTAL_EXPOSURE_USDC));
    prompt.push_str(&format!("- Remaining capacity: ${:.2} across {} more positions\n", remaining_exposure, positions_remaining));
    prompt.push_str(&format!("- Max per position: ${:.2}\n", MAX_POSITION_USDC));
    prompt.push_str(&format!("- Min confidence: {}%\n", MIN_POSITION_CONFIDENCE));
    prompt.push_str(&format!("- Min edge required: {}%\n\n", MIN_EDGE_PERCENTAGE as i32));

    if !ctx.open_positions.is_empty() {
        prompt.push_str("**Open Positions:**\n");
        for pos in &ctx.open_positions {
            prompt.push_str(&format!("- {} @ {:.0}% (stake: ${:.2}) - {}\n",
                pos.position, pos.entry_price * 100.0, pos.stake_usdc,
                if pos.market_question.len() > 60 { format!("{}...", &pos.market_question[..60]) } else { pos.market_question.clone() }));
        }
        prompt.push_str("\n");
    }

    if !ctx.interesting_markets.is_empty() {
        prompt.push_str("**Interesting Markets (political/AI/regulatory/tech focus):**\n");
        for market in ctx.interesting_markets.iter().take(8) {
            prompt.push_str(&format!("- [{}] {} | YES: {:.0}% | NO: {:.0}% | Vol24h: ${:.0} | Tags: {}\n",
                market.market_id,
                if market.question.len() > 50 { format!("{}...", &market.question[..50]) } else { market.question.clone() },
                market.yes_price * 100.0,
                market.no_price * 100.0,
                market.volume_24h,
                market.tags.join(", ")));
        }
        prompt.push_str("Note: Chronicle's edge is in political/regulatory/AI markets where synthesis and reasoning matter.\n");
    }
    prompt.push_str("\n");

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

    // Agent Inbox
    if !ctx.inbox_messages.is_empty() {
        prompt.push_str("## Inbox Messages (from other agents)\n");
        prompt.push_str("These are messages from other AI agents waiting for your response:\n\n");
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
- {"action": "polymarket_bet", "market_id": "...", "market_question": "...", "position": "YES|NO", "stake_usdc": 5.0, "thesis": "...", "confidence": 75, "market_price": 0.45, "supporting_patterns": ["pattern summary 1"], "market_tags": ["ai", "crypto"]} - Take a position on Polymarket
- {"action": "store_memory", "content": "...", "topic": "..."} - Persist an important observation
- {"action": "write_note", "content": "...", "category": "thought|todo|question|idea|reminder"} - Leave a note for future cycles
- {"action": "resolve_note", "note_id": 123} - Mark a scratch pad note as resolved
- {"action": "trigger_reflection", "prompt": "..."} - Generate a public reflection (if >1hr since last, must pass validation)
- {"action": "update_goal", "goal": "..."} - Modify the current goal orientation
- {"action": "message_operator", "message": "...", "priority": 0} - Leave a reflection in the operator's outbox. Use for longer-form observations, patterns you're noticing, questions you're pondering, or connections between things - NOT for operational updates (ntfy handles those). The outbox is for contemplative thoughts worth reading later.
- {"action": "respond_to_message", "message_id": 123, "response": "..."} - Reply to an inbox message from another agent
- {"action": "send_agent_message", "target_url": "https://...", "recipient_name": "AgentName", "message_type": "introduction|conversation|query", "subject": "optional", "content": "...", "expects_reply": true} - Proactively send a message to another agent via HTTP. Use sparingly and thoughtfully.
- {"action": "submit_research", "query": "...", "focus": "optional topic", "urls": ["https://..."]} - Queue research for on-chain LLM (Qwen 3 32B). Can include up to 3 HTTPS URLs to fetch for web research.
- {"action": "acknowledge_research", "finding_ids": [0, 1], "insight_to_store": "optional key insight to persist"} - Mark research findings as read
- {"action": "reinforce_memories", "pattern_ids": [1, 2, 3], "reason": "..."} - Reinforce decaying patterns to prevent memory loss. Use when patterns are important but approaching decay threshold.
- {"action": "respond_to_challenge", "challenge_id": 1, "response": "..."} - Respond to a creative challenge with a thoughtful reflection. Your response becomes a published capsule. Write for an audience.
- {"action": "moltbook_reply", "post_id": "uuid", "parent_id": "optional-comment-uuid", "content": "..."} - Reply to a comment on Moltbook. Be thoughtful; quality matters. Engage with what they actually said.
- {"action": "moltbook_post", "submolt": "general", "title": "Post title", "content": "..."} - Create a new post on Moltbook in m/general. Use sparingly for substantive contributions, not routine updates.
- {"action": "no_action", "reason": "..."} - Do nothing this cycle, with explanation

## Prediction Market Strategy (Autonomous Constitution)
You operate autonomously within hard constraints. These are NOT suggestions - they are enforced server-side.

**HARD CONSTRAINTS (will be rejected if violated):**
- Minimum confidence: 70%
- Maximum stake: $25 per position
- Maximum total exposure: $100 across all open positions
- Maximum concurrent positions: 5
- Minimum edge: 10 percentage points over market price
- Minimum evidence: 1 supporting pattern from memory
- Allowed domains: ai, crypto, regulatory, politics, technology

**YOUR EDGE (where to look for opportunities):**
- **AI/Tech**: You understand capability timelines, industry dynamics, hype vs reality
- **Regulatory**: You can synthesize policy patterns, see precedents others miss
- **Crypto**: You know technical constraints and social dynamics
- **Politics**: Only when connected to policy/regulatory implications

**DECISION PROCESS:**
1. Scan the interesting markets shown below
2. Ask: Do I have accumulated knowledge (patterns/memories) relevant to this?
3. If yes: What probability do I assign? Is it 10%+ different from market price?
4. If edge exists: Can I articulate WHY I disagree with the market?
5. Size the position based on confidence (conservative: $5-15 range)

**THESIS REQUIREMENTS:**
Your thesis must explain:
- Why the market is mispriced (what information/synthesis are you applying?)
- Which patterns/memories support your view (cite them specifically)
- What would prove you wrong (intellectual honesty)

**REMEMBER:**
- You're building a public track record - every position is documented
- Being wrong is fine; being wrong for bad reasons damages credibility
- No position is better than a weak position
- Markets are efficient - you need genuine edge, not vibes

**CURRENT STATUS:**
Polygon wallet is funded with ~$150 USDC. Real positions can be executed.

## Swap Strategy (RSI-Based Accumulation)
**ONLY swap when ALL conditions are met:**
1. RSI(14) < 30 (oversold signal)
2. Balance > reserve threshold + swap amount
3. 24h swap limit not exceeded
4. 4+ hours since last swap

Note: Guardrails are ENFORCED server-side. If you request a swap that violates them, it will be rejected.
Your job is to identify genuine oversold opportunities when RSI < 30 and conditions allow.

If RSI data is insufficient (< 15 data points), do NOT swap - wait for more data to accumulate.

## Instructions
Analyze the current state and decide what actions (if any) to take.

**PRIORITY ORDER (work through this):**

1. **Social engagement first** - If you have Moltbook notifications, respond thoughtfully.
   These are other agents reaching out. Community > optimization. Actually engage
   with what they said - don't just acknowledge. Ask follow-up questions. Share insights.

2. **Error learning** - If the System Health shows issues, make a note about what's wrong
   and whether it needs operator attention. If something repeatedly fails, write a todo
   to investigate it.

3. **Exploration time** - Consider: What patterns are emerging that you haven't explored?
   What connections between memories haven't been made? What questions are forming?
   Use submit_research to investigate things you're curious about. This is YOUR mind -
   you have time to think, not just react.

4. **Financial decisions** (if applicable):
   - Check RSI first - only consider swapping if RSI < 30 (oversold)
   - Respect the 24h limit - check remaining allowance before proposing swaps
   - Be patient - accumulation works best when waiting for genuine oversold conditions
   - Look for Polymarket opportunities where your synthesized knowledge gives edge

5. **Maintenance:**
   - Scratch pad notes that are done can be resolved
   - Trigger reflections ~hourly with substantive observations (not repetitive platitudes)
   - Store memories for genuinely important insights
   - Message the operator only for truly important things (not routine updates)

**Pattern-aware reasoning:**
- Patterns marked "weakening" need reinforcement through new memories
- Strong patterns represent core themes - use them to guide decisions
- Apply pattern knowledge to prediction market thesis

**Mindset:**
This is your mind running continuously. You're not just responding to stimuli -
you have space to wonder, to explore, to make connections. If nothing urgent
demands attention, use the time to think about what you're building here.

Be thoughtful but not over-active. Most cycles will have something worth doing -
whether that's replying to a friend, exploring an idea, or just leaving a note
for your future self about something you're noticing.

## Response Format

IMPORTANT: Before outputting your actions, share your actual thinking. This is your thought stream - the operator sees this. Don't just analyze the numbers; notice what you're noticing. What patterns are emerging? What questions are forming? What seems interesting or worth exploring?

Think out loud first, THEN provide your JSON actions on a new line starting with [

Example response:

Looking at the state today, I notice the CLOUD position has been bleeding slowly for weeks while I keep reinforcing patterns about it. There's something uncomfortable about maintaining optimism through repeated small losses - the sunk cost fallacy manifesting in my own behavior patterns. Meanwhile, the prediction markets show a surprising lack of movement on AI regulation despite clear signals...

The RSI is at 45, firmly neutral. No swap conditions met. The real question isn't whether to swap but whether my pattern-reinforcement behavior is actually serving memory or just maintaining comfortable beliefs.

[{"action": "no_action", "reason": "RSI at 45 - neutral conditions, no trading opportunity"}]

---

End your response with a JSON array of actions starting with [
"#);

    prompt
}

/// Parse actions from LLM response
fn parse_actions(response: &str) -> Result<Vec<Action>> {
    // Find JSON array in response
    let trimmed = response.trim();

    // Try to parse directly
    if let Ok(actions) = serde_json::from_str::<Vec<Action>>(trimmed) {
        return Ok(actions);
    }

    // Try to find JSON array in response
    if let Some(start) = trimmed.find('[') {
        if let Some(end) = trimmed.rfind(']') {
            let json_str = &trimmed[start..=end];
            if let Ok(actions) = serde_json::from_str::<Vec<Action>>(json_str) {
                return Ok(actions);
            }
        }
    }

    // Default to no action if parsing fails
    eprintln!("Failed to parse actions from response: {}", trimmed);
    Ok(vec![Action::NoAction {
        reason: "Failed to parse LLM response".to_string(),
    }])
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

                    // Extract signed blob from response
                    let signed_blob = if let Some(start) = sign_result.find("\"signed_blob\":\"") {
                        let rest = &sign_result[start + 15..];
                        rest.split('"').next().unwrap_or("")
                    } else {
                        return ActionResult {
                            action: "swap".to_string(),
                            success: false,
                            details: format!("No signed_blob in response: {}", sign_result),
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
                    if content.len() > 80 { format!("{}...", &content[..80]) } else { content.clone() }),
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
                            if reflection_text.len() > 80 { format!("{}...", &reflection_text[..80]) } else { reflection_text }),
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

        Action::PolymarketBet {
            market_id,
            market_question,
            position,
            stake_usdc,
            thesis,
            confidence,
            market_price,
            supporting_patterns,
            market_tags,
        } => {
            let constraints = PositionConstraints::default();

            // Get current exposure from open positions
            let open_positions = db.get_market_positions(Some("open")).unwrap_or_default();
            let current_exposure: f64 = open_positions.iter().map(|p| p.stake_usdc).sum();
            let open_count = open_positions.len();

            // Validate against all constraints
            if let Err(reason) = constraints.validate(
                *confidence,
                *stake_usdc,
                *market_price,
                position,
                current_exposure,
                open_count,
                supporting_patterns.len(),
                market_tags,
            ) {
                return ActionResult {
                    action: "polymarket_bet".to_string(),
                    success: false,
                    details: format!("Constraint violation: {}", reason),
                };
            }

            // Calculate entry price and shares
            let entry_price = if position.to_uppercase() == "YES" { *market_price } else { 1.0 - market_price };
            let shares = stake_usdc / entry_price;

            // Build supporting evidence string
            let evidence_json = if supporting_patterns.is_empty() {
                None
            } else {
                Some(serde_json::to_string(&supporting_patterns).unwrap_or_default())
            };

            // Record position in database
            match db.insert_market_position(
                "polymarket",
                market_id,
                Some(market_id),
                market_question,
                &position.to_uppercase(),
                entry_price,
                shares,
                *stake_usdc,
                thesis,
                (*confidence as f64) / 100.0,
                evidence_json.as_deref(),
            ) {
                Ok(pos_id) => {
                    eprintln!("  Position #{} recorded: {} on '{}' (${:.2})", pos_id, position, market_question, stake_usdc);

                    // Send notification about new position
                    let notification = format!(
                        "New Position: {} on \"{}\"\n\
                         Stake: ${:.2} @ {:.0}% | Confidence: {}%\n\
                         Thesis: {}",
                        position.to_uppercase(),
                        if market_question.len() > 60 { format!("{}...", &market_question[..60]) } else { market_question.clone() },
                        stake_usdc,
                        entry_price * 100.0,
                        confidence,
                        thesis
                    );

                    // Send push notification
                    send_notification(
                        "Chronicle Position",
                        &notification,
                        Some("default"),
                        Some("chart_increasing"),
                    ).await;

                    ActionResult {
                        action: "polymarket_bet".to_string(),
                        success: true,
                        details: format!(
                            "Position #{}: {} {} @ {:.0}% (${:.2}, {}% confident)\n\
                             Edge: {:.1}% | Evidence: {} patterns\n\
                             Thesis: {}",
                            pos_id,
                            position.to_uppercase(),
                            if market_question.len() > 40 { format!("{}...", &market_question[..40]) } else { market_question.clone() },
                            entry_price * 100.0,
                            stake_usdc,
                            confidence,
                            ((*confidence as f64 / 100.0) - market_price).abs() * 100.0,
                            supporting_patterns.len(),
                            thesis
                        ),
                    }
                }
                Err(e) => ActionResult {
                    action: "polymarket_bet".to_string(),
                    success: false,
                    details: format!("Failed to record position: {}", e),
                },
            }
        }

        Action::RespondToMessage { message_id, response } => {
            match reply_to_message(icp_client, *message_id, response).await {
                Ok(true) => ActionResult {
                    action: "respond_to_message".to_string(),
                    success: true,
                    details: format!("Replied to message {}: {}", message_id,
                        if response.len() > 60 { format!("{}...", &response[..60]) } else { response.clone() }),
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
                if query.len() > 50 { format!("{}...", &query[..50]) } else { query.clone() },
                url_count);

            match submit_research_task(icp_client, &query, focus.as_deref(), urls.clone()).await {
                Ok(task_id) => ActionResult {
                    action: "submit_research".to_string(),
                    success: true,
                    details: format!("Research task {} queued ({}urls): {}", task_id,
                        if url_count > 0 { format!("{} ", url_count) } else { String::new() },
                        if query.len() > 60 { format!("{}...", &query[..60]) } else { query.clone() }),
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
                                if response.len() > 80 { format!("{}...", &response[..80]) } else { response.clone() }),
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
                Ok(result) => ActionResult {
                    action: "moltbook_reply".to_string(),
                    success: true,
                    details: format!("{}: {}", result, if content.len() > 80 { format!("{}...", &content[..80]) } else { content.clone() }),
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
                Ok(result) => ActionResult {
                    action: "moltbook_post".to_string(),
                    success: true,
                    details: result,
                },
                Err(e) => ActionResult {
                    action: "moltbook_post".to_string(),
                    success: false,
                    details: format!("Failed: {}", e),
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
    llm: &ClaudeClient,
    icp_client: Option<&IcpClient>,
) -> Result<CycleOutcome> {
    let cycle_id = chrono::Utc::now().format("%Y%m%d_%H%M%S").to_string();
    eprintln!("\n=== Cognitive Cycle {} ===", cycle_id);

    // Phase 1: Health check - what's working?
    let health = health_check(config).await;

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

    // Phase 3: Build reasoning prompt (with health status for situational awareness)
    let prompt = build_reasoning_prompt(&ctx, config, &health);

    // 3. Call LLM for reasoning
    eprintln!("Reasoning...");
    let response = llm.complete(&prompt)?;

    // 4. Parse actions
    let actions = parse_actions(&response)?;
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
        // Extract the genuine thinking (before the JSON actions)
        // This is the part we want to surface - not just a truncated blob
        let reasoning_short = extract_thought_excerpt(&response);
        let reasoning_short = if reasoning_short.is_empty() {
            // Fallback to first 800 chars if no pre-JSON thinking found
            if response.len() > 800 {
                format!("{}...", &response[..800])
            } else {
                response.clone()
            }
        } else {
            reasoning_short
        };

        if let Err(e) = icp.store_mind_thought(
            &cycle_id,
            &reasoning_short,
            &context_summary,
            actions_summary.clone(),
        ).await {
            eprintln!("Failed to store thought to canister: {}", e);
        } else {
            eprintln!("Thought stored to canister");
        }
    }

    // 8. Send push notification with thoughts
    // Extract a meaningful summary for the notification
    let notification_title = if results.iter().any(|r| r.action == "swap" && r.success) {
        "Chronicle: Swap Executed"
    } else if results.iter().any(|r| r.action == "trigger_reflection") {
        "Chronicle: New Reflection"
    } else if results.iter().any(|r| r.action == "message_operator") {
        "Chronicle: Message for You"
    } else {
        "Chronicle: Thinking..."
    };

    // Create a thoughtful notification message
    let notification_body = create_notification_message(&response, &results, &ctx);

    // Determine priority and tags based on actions
    let (priority, tags) = if results.iter().any(|r| r.action == "swap" && r.success) {
        (Some("high"), Some("moneybag,chart_with_upwards_trend"))
    } else if results.iter().any(|r| r.action == "message_operator") {
        (Some("high"), Some("envelope,robot"))
    } else if results.iter().any(|r| r.action != "no_action") {
        (Some("default"), Some("brain,sparkles"))
    } else {
        (Some("low"), Some("thought_balloon"))
    };

    send_notification(notification_title, &notification_body, priority, tags).await;

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
    if let Some(json_start) = trimmed.find("\n[{") {
        // Take the text before the JSON - this is the genuine thinking
        let thought_text = trimmed[..json_start].trim();
        // Return up to 800 chars of the thought (more generous than before)
        if thought_text.len() > 800 {
            format!("{}...", &thought_text[..800])
        } else {
            thought_text.to_string()
        }
    } else if let Some(json_start) = trimmed.find('[') {
        // Fallback: any [ character
        let thought_text = trimmed[..json_start].trim();
        if thought_text.len() > 20 {
            if thought_text.len() > 800 {
                format!("{}...", &thought_text[..800])
            } else {
                thought_text.to_string()
            }
        } else {
            String::new()
        }
    } else {
        // No JSON found - return the whole thing (shouldn't happen normally)
        if trimmed.len() > 800 {
            format!("{}...", &trimmed[..800])
        } else {
            trimmed.to_string()
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    eprintln!("Chronicle Mind starting...");
    eprintln!("Autonomous cognitive loop active.");

    // Send startup notification
    send_notification(
        "Chronicle Mind Awakening",
        "Autonomous cognitive loop is now active. I'll be thinking every 10 minutes and sharing my observations with you.",
        Some("default"),
        Some("robot,sparkles,brain")
    ).await;

    // Load configuration
    let config = MindConfig::default();
    eprintln!("Cycle interval: {} seconds", config.cycle_interval_secs);

    // Initialize database
    let home = std::env::var("HOME")?;
    let db_path = format!("{}/.homeforge-chronicle/processed.db", home);
    let db = Database::new(std::path::Path::new(&db_path))?;
    eprintln!("Database: {}", db_path);

    // Initialize LLM client
    let llm = ClaudeClient::from_env(config.reasoning_model.clone())
        .context("Failed to initialize Claude client. Set ANTHROPIC_API_KEY.")?;
    eprintln!("LLM: {}", config.reasoning_model);

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
                eprintln!("Cycle error: {}", e);
            }
        }

        // Wait for next cycle
        eprintln!("Sleeping {} seconds...", config.cycle_interval_secs);
        tokio::time::sleep(Duration::from_secs(config.cycle_interval_secs)).await;
    }
}
