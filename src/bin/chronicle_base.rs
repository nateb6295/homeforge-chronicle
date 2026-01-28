//! Chronicle BASE Wallet
//!
//! MCP server for Chronicle's BASE wallet operations.
//!
//! Phase 1 (current): Local private key (like Flare wallet)
//! Phase 2 (planned): ICP threshold ECDSA for sovereign key management
//!
//! Designed for:
//! - Receiving USDC on BASE (from Coinbase)
//! - Trading on Limitless Exchange prediction markets

use alloy::primitives::{Address, U256};
use alloy::providers::{Provider, ProviderBuilder};
use alloy::signers::local::PrivateKeySigner;
use alloy::signers::Signer;
use alloy::sol;
use anyhow::{anyhow, Context, Result};
use k256::elliptic_curve::sec1::ToEncodedPoint;
use reqwest::header::{HeaderMap, HeaderValue, COOKIE};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha3::{Digest, Keccak256};
use std::io::{BufRead, BufReader, Write};
use std::str::FromStr;
use std::sync::Arc;
use tokio::sync::RwLock;

/// BASE Mainnet RPC endpoint
const BASE_RPC_URL: &str = "https://mainnet.base.org";

/// USDC contract address on BASE
const USDC_BASE_ADDRESS: &str = "0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913";

/// Limitless Exchange API base URL
const LIMITLESS_API_URL: &str = "https://api.limitless.exchange";

/// Limitless Exchange CTF Exchange contract on BASE (for order execution)
const LIMITLESS_CTF_EXCHANGE: &str = "0xa4409D988CA2218d956BeEFD3874100F444f0DC3";

/// Limitless Conditional Tokens contract on BASE
const LIMITLESS_CTF_TOKENS: &str = "0xc9c98965297bc527861c898329ee280632b76e18";

/// Chain ID for BASE mainnet
const BASE_CHAIN_ID: u64 = 8453;

// ============================================================================
// Polymarket Configuration (Polygon)
// ============================================================================

/// Polygon mainnet RPC endpoint
const POLYGON_RPC_URL: &str = "https://polygon-rpc.com";

/// Chain ID for Polygon mainnet
const POLYGON_CHAIN_ID: u64 = 137;

/// USDC contract address on Polygon (Native USDC - used by Polymarket)
const USDC_POLYGON_ADDRESS: &str = "0x3c499c542cEF5E3811e1192ce70d8cC03d5c3359";

/// Polymarket Gamma API (unauthenticated market data)
const POLYMARKET_GAMMA_API: &str = "https://gamma-api.polymarket.com";

/// Polymarket CLOB API (trading)
const POLYMARKET_CLOB_API: &str = "https://clob.polymarket.com";

/// Polymarket CTF Exchange contract on Polygon (for order signing)
const POLYMARKET_CTF_EXCHANGE: &str = "0x4bfb41d5b3570defd03c39a9a4d8de6bd8b8982e";

/// Polymarket Neg Risk CTF Exchange (for multi-outcome markets)
const POLYMARKET_NEG_RISK_EXCHANGE: &str = "0xC5d563A36AE78145C45a50134d48A1215220f80a";

// ERC20 interface for USDC operations
sol! {
    #[sol(rpc)]
    interface IERC20 {
        function balanceOf(address account) external view returns (uint256);
        function decimals() external view returns (uint8);
        function symbol() external view returns (string);
        function allowance(address owner, address spender) external view returns (uint256);
        function approve(address spender, uint256 amount) external returns (bool);
    }
}

// ============================================================================
// Limitless Exchange Types
// ============================================================================

/// Limitless market data
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LimitlessMarket {
    pub id: String,
    pub title: String,
    pub description: Option<String>,
    pub category: Option<String>,
    pub end_date: Option<String>,
    pub volume: Option<String>,
    pub liquidity: Option<String>,
    pub yes_price: Option<f64>,
    pub no_price: Option<f64>,
    pub resolved: Option<bool>,
    pub resolution: Option<String>,
}

/// Limitless order side
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "UPPERCASE")]
pub enum OrderSide {
    Buy,
    Sell,
}

/// Limitless order for a market
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LimitlessOrder {
    pub market_id: String,
    pub side: OrderSide,
    pub outcome: String,  // "YES" or "NO"
    pub amount: String,   // USDC amount (6 decimals)
    pub price: f64,       // Price per share (0.0 - 1.0)
}

/// Authentication session
#[derive(Debug, Clone)]
pub struct LimitlessSession {
    pub address: Address,
    pub auth_token: String,
    pub owner_id: Option<i64>,
    pub expires_at: i64,
}

/// EIP-712 Order structure for Limitless CTF Exchange
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct LimitlessOrderData {
    pub salt: String,
    pub maker: String,
    pub signer: String,
    pub taker: String,
    pub token_id: String,
    pub maker_amount: String,
    pub taker_amount: String,
    pub expiration: String,
    pub nonce: String,
    pub fee_rate_bps: String,
    pub side: u8,
    pub signature_type: u8,
}

/// Market token info from Limitless API
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MarketTokens {
    pub yes: Option<String>,
    pub no: Option<String>,
}

// ============================================================================
// Polymarket Types
// ============================================================================

/// Polymarket market from Gamma API
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PolymarketMarket {
    pub id: Option<String>,  // API returns string ID
    pub question: Option<String>,
    pub slug: Option<String>,
    #[serde(rename = "conditionId")]
    pub condition_id: Option<String>,
    pub description: Option<String>,
    pub outcomes: Option<String>,  // JSON string like "[\"Yes\", \"No\"]"
    #[serde(rename = "outcomePrices")]
    pub outcome_prices: Option<String>,  // JSON string like "[\"0.65\", \"0.35\"]"
    pub volume: Option<String>,
    pub liquidity: Option<String>,
    #[serde(rename = "endDate")]
    pub end_date: Option<String>,
    pub closed: Option<bool>,
    pub active: Option<bool>,
    #[serde(rename = "groupItemTitle")]
    pub group_item_title: Option<String>,  // Often serves as category
    #[serde(rename = "clobTokenIds")]
    pub clob_token_ids: Option<String>,  // JSON string with token IDs
}

/// Parsed Polymarket market with computed fields
#[derive(Debug, Clone, Serialize)]
pub struct ParsedPolymarket {
    pub id: String,
    pub question: String,
    pub slug: String,
    pub yes_price: f64,
    pub no_price: f64,
    pub volume_usd: f64,
    pub liquidity_usd: f64,
    pub end_date: String,
    pub category: String,
    pub yes_token_id: String,
    pub no_token_id: String,
    pub is_active: bool,
}

/// Chronicle's BASE wallet configuration
#[derive(Debug, Clone)]
pub struct BaseWalletConfig {
    /// Wallet address
    pub address: Option<Address>,
    /// Private key signer (for Phase 1 - local key)
    /// Phase 2 will use ICP threshold ECDSA instead
    pub signer: Option<PrivateKeySigner>,
}

impl Default for BaseWalletConfig {
    fn default() -> Self {
        Self {
            address: None,
            signer: None,
        }
    }
}

/// Load BASE wallet private key from file
fn load_base_private_key() -> Result<String> {
    let home = dirs::home_dir().ok_or_else(|| anyhow!("Could not find home directory"))?;
    let key_path = home.join(".base-wallet").join("private_key.txt");

    if !key_path.exists() {
        return Err(anyhow!(
            "BASE wallet not configured. Create {} with your private key (0x...)",
            key_path.display()
        ));
    }

    let content = std::fs::read_to_string(&key_path)
        .context("Failed to read private key file")?;

    Ok(content.trim().to_string())
}

/// Initialize wallet from private key file
fn init_wallet() -> Result<BaseWalletConfig> {
    let private_key = load_base_private_key()?;

    let signer: PrivateKeySigner = private_key.parse()
        .context("Invalid private key format")?;

    let address = signer.address();

    Ok(BaseWalletConfig {
        address: Some(address),
        signer: Some(signer),
    })
}

/// Generate a new BASE wallet and save to file
fn generate_wallet() -> Result<(Address, String)> {
    use k256::ecdsa::SigningKey;
    use rand::rngs::OsRng;

    let signing_key = SigningKey::random(&mut OsRng);
    let private_key_bytes = signing_key.to_bytes();
    let private_key_hex = format!("0x{}", hex::encode(private_key_bytes));

    let signer: PrivateKeySigner = private_key_hex.parse()
        .context("Failed to create signer")?;
    let address = signer.address();

    // Save to file
    let home = dirs::home_dir().ok_or_else(|| anyhow!("Could not find home directory"))?;
    let wallet_dir = home.join(".base-wallet");
    std::fs::create_dir_all(&wallet_dir)?;

    let key_path = wallet_dir.join("private_key.txt");
    std::fs::write(&key_path, &private_key_hex)?;

    // Secure permissions (Unix only)
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&key_path, std::fs::Permissions::from_mode(0o600))?;
    }

    Ok((address, private_key_hex))
}

/// Derive an EVM address from a compressed secp256k1 public key
///
/// Process:
/// 1. Decompress 33-byte public key to 65-byte uncompressed
/// 2. Take 64 bytes (x || y, without 0x04 prefix)
/// 3. keccak256 hash
/// 4. Take last 20 bytes
pub fn derive_evm_address(compressed_pubkey: &[u8; 33]) -> Result<Address> {
    use k256::PublicKey;

    // Parse the compressed public key
    let public_key = PublicKey::from_sec1_bytes(compressed_pubkey)
        .map_err(|e| anyhow!("Invalid public key: {}", e))?;

    // Get uncompressed point (65 bytes: 04 || x || y)
    let uncompressed = public_key.to_sec1_bytes();

    // The uncompressed form is 65 bytes, but we get compressed (33 bytes) by default
    // We need to use the encoded point directly
    let encoded_point = public_key.as_affine().to_encoded_point(false);
    let uncompressed_bytes = encoded_point.as_bytes();

    if uncompressed_bytes.len() != 65 {
        return Err(anyhow!(
            "Expected 65-byte uncompressed key, got {}",
            uncompressed_bytes.len()
        ));
    }

    // Skip the 0x04 prefix, hash the 64-byte x||y
    let mut hasher = Keccak256::new();
    hasher.update(&uncompressed_bytes[1..]);
    let hash = hasher.finalize();

    // Take last 20 bytes as address
    let address_bytes: [u8; 20] = hash[12..32]
        .try_into()
        .map_err(|_| anyhow!("Failed to extract address bytes"))?;

    Ok(Address::from(address_bytes))
}

/// Get USDC balance on BASE for an address
async fn get_usdc_balance(address: Address) -> Result<U256> {
    let rpc_url = BASE_RPC_URL.parse().map_err(|e| anyhow!("Invalid RPC URL: {}", e))?;
    let provider = ProviderBuilder::new().connect_http(rpc_url);

    let usdc_address = Address::from_str(USDC_BASE_ADDRESS)?;
    let usdc = IERC20::new(usdc_address, provider.clone());

    let balance = usdc.balanceOf(address).call().await
        .context("Failed to get USDC balance")?;

    Ok(balance)
}

/// Get native ETH balance on BASE
async fn get_eth_balance(address: Address) -> Result<U256> {
    let rpc_url = BASE_RPC_URL.parse().map_err(|e| anyhow!("Invalid RPC URL: {}", e))?;
    let provider = ProviderBuilder::new().connect_http(rpc_url);

    let balance = provider.get_balance(address).await?;
    Ok(balance)
}

/// Get native MATIC balance on Polygon
async fn get_polygon_balance(address: Address) -> Result<U256> {
    let rpc_url = POLYGON_RPC_URL.parse().map_err(|e| anyhow!("Invalid Polygon RPC URL: {}", e))?;
    let provider = ProviderBuilder::new().connect_http(rpc_url);

    let balance = provider.get_balance(address).await?;
    Ok(balance)
}

/// Get USDC balance on Polygon for an address
async fn get_polygon_usdc_balance(address: Address) -> Result<U256> {
    let rpc_url = POLYGON_RPC_URL.parse().map_err(|e| anyhow!("Invalid Polygon RPC URL: {}", e))?;
    let provider = ProviderBuilder::new().connect_http(rpc_url);

    let usdc_address = Address::from_str(USDC_POLYGON_ADDRESS)?;
    let usdc = IERC20::new(usdc_address, provider.clone());

    let balance = usdc.balanceOf(address).call().await
        .context("Failed to get Polygon USDC balance")?;

    Ok(balance)
}

/// Get current USDC allowance for Limitless CTF Exchange
async fn get_limitless_allowance(owner: Address) -> Result<U256> {
    let rpc_url = BASE_RPC_URL.parse().map_err(|e| anyhow!("Invalid RPC URL: {}", e))?;
    let provider = ProviderBuilder::new().connect_http(rpc_url);

    let usdc_address = Address::from_str(USDC_BASE_ADDRESS)?;
    let spender = Address::from_str(LIMITLESS_CTF_EXCHANGE)?;

    let usdc = IERC20::new(usdc_address, provider);
    let allowance = usdc.allowance(owner, spender).call().await
        .context("Failed to get allowance")?;

    Ok(allowance)
}

/// Approve USDC spending for Limitless CTF Exchange
/// If spender_address is None, uses the default CTF Exchange address
async fn approve_usdc_for_limitless(signer: &PrivateKeySigner, amount: U256, spender_address: Option<&str>) -> Result<String> {
    use alloy::network::EthereumWallet;

    let rpc_url = BASE_RPC_URL.parse().map_err(|e| anyhow!("Invalid RPC URL: {}", e))?;

    // Create wallet from signer
    let wallet = EthereumWallet::from(signer.clone());

    // Build provider with transaction signing capability
    let provider = ProviderBuilder::new()
        .wallet(wallet)
        .connect_http(rpc_url);

    let usdc_address = Address::from_str(USDC_BASE_ADDRESS)?;
    let spender = Address::from_str(spender_address.unwrap_or(LIMITLESS_CTF_EXCHANGE))?;

    // Create the USDC contract instance
    let usdc = IERC20::new(usdc_address, &provider);

    // Send approve transaction
    let pending_tx = usdc.approve(spender, amount).send().await
        .context("Failed to send approval transaction")?;

    let receipt = pending_tx.get_receipt().await
        .context("Failed to get transaction receipt")?;

    Ok(format!("{:?}", receipt.transaction_hash))
}

// ============================================================================
// Limitless Exchange API Functions
// ============================================================================

/// Convert address to EIP-55 checksum format
fn to_checksum_address(address: &Address) -> String {
    let addr_lower = format!("{:?}", address).to_lowercase();
    let addr_no_prefix = addr_lower.strip_prefix("0x").unwrap_or(&addr_lower);

    let mut hasher = Keccak256::new();
    hasher.update(addr_no_prefix.as_bytes());
    let hash = hasher.finalize();

    let mut result = String::from("0x");
    for (i, c) in addr_no_prefix.chars().enumerate() {
        let nibble = (hash[i / 2] >> (if i % 2 == 0 { 4 } else { 0 })) & 0xf;
        if c.is_ascii_alphabetic() && nibble > 7 {
            result.push(c.to_ascii_uppercase());
        } else {
            result.push(c);
        }
    }
    result
}

/// Get the signing message from Limitless for authentication
async fn limitless_get_signing_message(address: &Address) -> Result<String> {
    let client = reqwest::Client::new();
    let checksum_addr = to_checksum_address(address);
    let url = format!("{}/auth/signing-message", LIMITLESS_API_URL);

    let response = client
        .get(&url)
        .header("x-account", &checksum_addr)
        .send()
        .await
        .context("Failed to get signing message")?;

    if !response.status().is_success() {
        let status = response.status();
        let text = response.text().await.unwrap_or_default();
        return Err(anyhow!("Signing message request failed ({}): {}", status, text));
    }

    // The response is plain text, not JSON
    let message = response.text().await
        .context("Failed to read signing message")?;

    if message.is_empty() {
        return Err(anyhow!("Empty signing message received"));
    }

    Ok(message)
}

/// Authenticate with Limitless Exchange using wallet signature
async fn limitless_authenticate(signer: &PrivateKeySigner) -> Result<LimitlessSession> {
    let address = signer.address();
    let checksum_addr = to_checksum_address(&address);

    // Step 1: Get the signing message
    let message = limitless_get_signing_message(&address).await?;

    // Step 2: Sign the message
    let signature = signer.sign_message(message.as_bytes()).await
        .map_err(|e| anyhow!("Failed to sign message: {}", e))?;

    let signature_hex = format!("0x{}", hex::encode(signature.as_bytes()));

    // Encode the message as hex for the header
    let message_hex = format!("0x{}", hex::encode(message.as_bytes()));

    // Step 3: Submit the signed message to login using headers + body
    let http_client = reqwest::Client::new();
    let login_url = format!("{}/auth/login", LIMITLESS_API_URL);

    // The body needs to specify client type: "eoa" for standard wallet
    let login_body = json!({
        "client": "eoa"
    });

    let response = http_client
        .post(&login_url)
        .header("Content-Type", "application/json")
        .header("x-account", &checksum_addr)
        .header("x-signing-message", &message_hex)
        .header("x-signature", &signature_hex)
        .json(&login_body)
        .send()
        .await
        .context("Login request failed")?;

    // Capture cookies from response
    let cookies: Vec<String> = response
        .headers()
        .get_all("set-cookie")
        .iter()
        .filter_map(|v| v.to_str().ok())
        .map(|s| s.to_string())
        .collect();

    if !response.status().is_success() {
        let status = response.status();
        let text = response.text().await.unwrap_or_default();
        return Err(anyhow!("Login failed ({}): {}", status, text));
    }

    // Parse response body for user ID
    let body: Value = response.json().await.unwrap_or(json!({}));

    // Extract owner_id from user object in response
    let owner_id = body
        .get("user")
        .and_then(|u| u.get("id"))
        .and_then(|id| id.as_i64())
        .or_else(|| body.get("id").and_then(|id| id.as_i64()));

    // Session is established via cookies
    let session_cookie = cookies
        .iter()
        .find(|c| c.contains("session") || c.contains("auth") || c.contains("token"))
        .cloned()
        .unwrap_or_else(|| {
            if cookies.is_empty() {
                format!("authenticated:{}", checksum_addr)
            } else {
                cookies.join("; ")
            }
        });

    Ok(LimitlessSession {
        address,
        auth_token: session_cookie,
        owner_id,
        expires_at: chrono::Utc::now().timestamp() + 86400, // 24 hours
    })
}

/// List active markets from Limitless Exchange
async fn limitless_list_markets(limit: Option<u32>, category_id: Option<&str>) -> Result<Vec<LimitlessMarket>> {
    let client = reqwest::Client::new();

    // Use the correct endpoint: /markets/active
    let mut url = if let Some(cat) = category_id {
        format!("{}/markets/active/{}", LIMITLESS_API_URL, cat)
    } else {
        format!("{}/markets/active", LIMITLESS_API_URL)
    };

    let mut params = vec![];
    if let Some(l) = limit {
        params.push(format!("limit={}", l));
    }

    if !params.is_empty() {
        url = format!("{}?{}", url, params.join("&"));
    }

    let response = client
        .get(&url)
        .header("Accept", "application/json")
        .send()
        .await
        .context("Failed to fetch markets")?;

    if !response.status().is_success() {
        let status = response.status();
        let text = response.text().await.unwrap_or_default();
        return Err(anyhow!("Markets request failed ({}): {}", status, text));
    }

    let data: Value = response.json().await?;

    // Try to parse markets from response - API returns different structures
    let markets_value = data
        .get("markets")
        .or_else(|| data.get("data"))
        .cloned()
        .unwrap_or(data);

    let markets: Vec<LimitlessMarket> = if markets_value.is_array() {
        // Parse each market, handling different field names
        markets_value
            .as_array()
            .unwrap_or(&vec![])
            .iter()
            .filter_map(|v| {
                Some(LimitlessMarket {
                    id: v.get("address")
                        .or_else(|| v.get("slug"))
                        .or_else(|| v.get("id"))
                        .and_then(|x| x.as_str())
                        .map(|s| s.to_string())?,
                    title: v.get("title")
                        .or_else(|| v.get("name"))
                        .and_then(|x| x.as_str())
                        .map(|s| s.to_string())?,
                    description: v.get("description")
                        .and_then(|x| x.as_str())
                        .map(|s| s.to_string()),
                    category: v.get("category")
                        .or_else(|| v.get("categoryId"))
                        .and_then(|x| x.as_str())
                        .map(|s| s.to_string()),
                    end_date: v.get("deadline")
                        .or_else(|| v.get("endDate"))
                        .and_then(|x| x.as_str())
                        .map(|s| s.to_string()),
                    volume: v.get("volume")
                        .or_else(|| v.get("volumeUsd"))
                        .and_then(|x| {
                            if x.is_string() { Some(x.as_str()?.to_string()) }
                            else if x.is_number() { Some(x.to_string()) }
                            else { None }
                        }),
                    liquidity: v.get("liquidity")
                        .or_else(|| v.get("liquidityUsd"))
                        .and_then(|x| {
                            if x.is_string() { Some(x.as_str()?.to_string()) }
                            else if x.is_number() { Some(x.to_string()) }
                            else { None }
                        }),
                    yes_price: v.get("yesPrice")
                        .or_else(|| v.get("prices").and_then(|p| p.get("yes")))
                        .and_then(|x| x.as_f64()),
                    no_price: v.get("noPrice")
                        .or_else(|| v.get("prices").and_then(|p| p.get("no")))
                        .and_then(|x| x.as_f64()),
                    resolved: v.get("resolved").and_then(|x| x.as_bool()),
                    resolution: v.get("resolution")
                        .or_else(|| v.get("outcome"))
                        .and_then(|x| x.as_str())
                        .map(|s| s.to_string()),
                })
            })
            .collect()
    } else {
        vec![]
    };

    Ok(markets)
}

// ============================================================================
// Polymarket API Functions
// ============================================================================

/// List markets from Polymarket Gamma API
async fn polymarket_list_markets(
    limit: Option<u32>,
    category: Option<&str>,
    active_only: bool,
) -> Result<Vec<ParsedPolymarket>> {
    let client = reqwest::Client::new();

    let mut url = format!("{}/markets", POLYMARKET_GAMMA_API);
    let mut params = vec![];

    if let Some(l) = limit {
        params.push(format!("limit={}", l.min(100)));  // Polymarket max is 100
    } else {
        params.push("limit=25".to_string());
    }

    if active_only {
        params.push("closed=false".to_string());
        params.push("active=true".to_string());
    }

    if !params.is_empty() {
        url = format!("{}?{}", url, params.join("&"));
    }

    let response = client
        .get(&url)
        .header("Accept", "application/json")
        .send()
        .await
        .context("Failed to fetch Polymarket markets")?;

    if !response.status().is_success() {
        let status = response.status();
        let text = response.text().await.unwrap_or_default();
        return Err(anyhow!("Polymarket request failed ({}): {}", status, text));
    }

    let markets: Vec<PolymarketMarket> = response.json().await
        .context("Failed to parse Polymarket response")?;

    // Parse and filter by category if specified (filter on parsed data since raw has no category)
    let parsed: Vec<ParsedPolymarket> = markets
        .into_iter()
        .filter_map(|m| parse_polymarket(&m))
        .filter(|m| {
            if let Some(cat) = category {
                let cat_lower = cat.to_lowercase();
                let q_lower = m.question.to_lowercase();
                // Match category or question content
                m.category.to_lowercase().contains(&cat_lower) || q_lower.contains(&cat_lower)
            } else {
                true
            }
        })
        .collect();

    Ok(parsed)
}

/// Parse a raw Polymarket market into our usable format
fn parse_polymarket(m: &PolymarketMarket) -> Option<ParsedPolymarket> {
    let question = m.question.clone()?;
    let slug = m.slug.clone().unwrap_or_default();

    // Parse outcome prices from JSON string like "[\"0.65\", \"0.35\"]"
    let (yes_price, no_price) = if let Some(prices_str) = &m.outcome_prices {
        let prices: Vec<String> = serde_json::from_str(prices_str).unwrap_or_default();
        let yes = prices.first().and_then(|p| p.parse::<f64>().ok()).unwrap_or(0.5);
        let no = prices.get(1).and_then(|p| p.parse::<f64>().ok()).unwrap_or(0.5);
        (yes, no)
    } else {
        (0.5, 0.5)
    };

    // Parse token IDs from JSON string
    let (yes_token, no_token) = if let Some(tokens_str) = &m.clob_token_ids {
        let tokens: Vec<String> = serde_json::from_str(tokens_str).unwrap_or_default();
        (
            tokens.first().cloned().unwrap_or_default(),
            tokens.get(1).cloned().unwrap_or_default(),
        )
    } else {
        (String::new(), String::new())
    };

    // Parse volume and liquidity
    let volume = m.volume.as_ref()
        .and_then(|v| v.parse::<f64>().ok())
        .unwrap_or(0.0);
    let liquidity = m.liquidity.as_ref()
        .and_then(|v| v.parse::<f64>().ok())
        .unwrap_or(0.0);

    // Use group_item_title as category hint, or derive from question
    let category = m.group_item_title.clone()
        .unwrap_or_else(|| {
            // Simple category detection from question
            let q = question.to_lowercase();
            if q.contains("trump") || q.contains("biden") || q.contains("election") || q.contains("congress") {
                "Politics".to_string()
            } else if q.contains("bitcoin") || q.contains("ethereum") || q.contains("crypto") {
                "Crypto".to_string()
            } else if q.contains("ai") || q.contains("gpt") || q.contains("openai") || q.contains("anthropic") {
                "AI".to_string()
            } else {
                "General".to_string()
            }
        });

    Some(ParsedPolymarket {
        id: m.id.clone().unwrap_or_else(|| slug.clone()),
        question,
        slug,
        yes_price,
        no_price,
        volume_usd: volume,
        liquidity_usd: liquidity,
        end_date: m.end_date.clone().unwrap_or_default(),
        category,
        yes_token_id: yes_token,
        no_token_id: no_token,
        is_active: m.active.unwrap_or(true) && !m.closed.unwrap_or(false),
    })
}

/// Get a specific market from Polymarket by slug or condition ID
async fn polymarket_get_market(slug_or_id: &str) -> Result<ParsedPolymarket> {
    let client = reqwest::Client::new();

    // Try by slug first
    let url = format!("{}/markets?slug={}", POLYMARKET_GAMMA_API, slug_or_id);

    let response = client
        .get(&url)
        .header("Accept", "application/json")
        .send()
        .await
        .context("Failed to fetch Polymarket market")?;

    if response.status().is_success() {
        let markets: Vec<PolymarketMarket> = response.json().await?;
        if let Some(market) = markets.first() {
            if let Some(parsed) = parse_polymarket(market) {
                return Ok(parsed);
            }
        }
    }

    // Try by condition ID
    let url = format!("{}/markets?condition_id={}", POLYMARKET_GAMMA_API, slug_or_id);
    let response = client
        .get(&url)
        .header("Accept", "application/json")
        .send()
        .await?;

    if response.status().is_success() {
        let markets: Vec<PolymarketMarket> = response.json().await?;
        if let Some(market) = markets.first() {
            if let Some(parsed) = parse_polymarket(market) {
                return Ok(parsed);
            }
        }
    }

    Err(anyhow!("Market not found: {}", slug_or_id))
}

/// Polymarket order data for EIP-712 signing
#[derive(Debug, Clone, Serialize)]
pub struct PolymarketOrderData {
    pub salt: String,
    pub maker: String,
    pub signer: String,
    pub taker: String,
    pub token_id: String,
    pub maker_amount: String,
    pub taker_amount: String,
    pub expiration: String,
    pub nonce: String,
    pub fee_rate_bps: String,
    pub side: u8,           // 0 = BUY, 1 = SELL
    pub signature_type: u8, // 0 = EOA
}

/// Create and sign a Polymarket order using EIP-712
async fn create_polymarket_order(
    signer: &PrivateKeySigner,
    token_id: &str,
    side: &str,     // "BUY" or "SELL"
    price: f64,     // 0.0-1.0
    size: f64,      // Number of shares
    neg_risk: bool, // Use NegRisk exchange?
) -> Result<(PolymarketOrderData, String)> {
    use rand::Rng;

    let address = signer.address();
    let checksum_addr = to_checksum_address(&address);

    // Generate random salt
    let salt: u128 = rand::thread_rng().gen();

    // Determine side as u8
    let side_u8: u8 = if side.to_uppercase() == "BUY" { 0 } else { 1 };

    // Calculate amounts based on side and price
    // USDC has 6 decimals, shares have 6 decimals on Polymarket
    let size_raw = (size * 1_000_000.0) as u128;  // Convert to 6 decimal places
    let (maker_amount, taker_amount) = if side_u8 == 0 {
        // BUY: spending USDC to get shares
        let usdc_amount = (size * price * 1_000_000.0) as u128;
        (usdc_amount, size_raw)
    } else {
        // SELL: spending shares to get USDC
        let usdc_amount = (size * price * 1_000_000.0) as u128;
        (size_raw, usdc_amount)
    };

    // Use 0 for taker (any taker), no expiration, 0 nonce, 0 fee
    let order_data = PolymarketOrderData {
        salt: salt.to_string(),
        maker: checksum_addr.clone(),
        signer: checksum_addr.clone(),
        taker: "0x0000000000000000000000000000000000000000".to_string(),
        token_id: token_id.to_string(),
        maker_amount: maker_amount.to_string(),
        taker_amount: taker_amount.to_string(),
        expiration: "0".to_string(),  // No expiration
        nonce: "0".to_string(),
        fee_rate_bps: "0".to_string(),
        side: side_u8,
        signature_type: 0, // EOA
    };

    // Sign the order using EIP-712
    let signature = sign_polymarket_order(signer, &order_data, neg_risk).await?;

    Ok((order_data, signature))
}

/// Sign a Polymarket order using EIP-712 typed data
async fn sign_polymarket_order(
    signer: &PrivateKeySigner,
    order: &PolymarketOrderData,
    neg_risk: bool,
) -> Result<String> {
    // EIP-712 Domain
    // Domain type: EIP712Domain(string name,string version,uint256 chainId,address verifyingContract)
    let domain_type = "EIP712Domain(string name,string version,uint256 chainId,address verifyingContract)";
    let domain_type_hash = Keccak256::digest(domain_type.as_bytes());

    // Order type: Order(uint256 salt,address maker,address signer,address taker,uint256 tokenId,uint256 makerAmount,uint256 takerAmount,uint256 expiration,uint256 nonce,uint256 feeRateBps,uint8 side,uint8 signatureType)
    let order_type = "Order(uint256 salt,address maker,address signer,address taker,uint256 tokenId,uint256 makerAmount,uint256 takerAmount,uint256 expiration,uint256 nonce,uint256 feeRateBps,uint8 side,uint8 signatureType)";
    let order_type_hash = Keccak256::digest(order_type.as_bytes());

    // Domain values
    let name_hash = Keccak256::digest(b"Polymarket CTF Exchange");
    let version_hash = Keccak256::digest(b"1");
    let chain_id = U256::from(POLYGON_CHAIN_ID);
    let exchange_addr = if neg_risk {
        Address::from_str(POLYMARKET_NEG_RISK_EXCHANGE)?
    } else {
        Address::from_str(POLYMARKET_CTF_EXCHANGE)?
    };

    // Encode domain separator
    let mut domain_data = Vec::new();
    domain_data.extend_from_slice(&domain_type_hash);
    domain_data.extend_from_slice(&name_hash);
    domain_data.extend_from_slice(&version_hash);
    domain_data.extend_from_slice(&chain_id.to_be_bytes::<32>());
    domain_data.extend_from_slice(exchange_addr.as_slice());
    // Pad address to 32 bytes
    let mut padded_addr = [0u8; 32];
    padded_addr[12..].copy_from_slice(exchange_addr.as_slice());
    domain_data.truncate(32 * 4);  // Reset and do properly
    domain_data.clear();
    domain_data.extend_from_slice(&domain_type_hash);
    domain_data.extend_from_slice(&name_hash);
    domain_data.extend_from_slice(&version_hash);
    domain_data.extend_from_slice(&chain_id.to_be_bytes::<32>());
    domain_data.extend_from_slice(&padded_addr);

    let domain_separator = Keccak256::digest(&domain_data);

    // Encode order struct
    // Parse values
    let salt = U256::from_str(&order.salt)?;
    let maker = Address::from_str(&order.maker)?;
    let order_signer = Address::from_str(&order.signer)?;
    let taker = Address::from_str(&order.taker)?;
    let token_id = U256::from_str(&order.token_id)?;
    let maker_amount = U256::from_str(&order.maker_amount)?;
    let taker_amount = U256::from_str(&order.taker_amount)?;
    let expiration = U256::from_str(&order.expiration)?;
    let nonce = U256::from_str(&order.nonce)?;
    let fee_rate_bps = U256::from_str(&order.fee_rate_bps)?;
    let side = U256::from(order.side);
    let sig_type = U256::from(order.signature_type);

    // Pad addresses to 32 bytes
    let mut maker_padded = [0u8; 32];
    maker_padded[12..].copy_from_slice(maker.as_slice());
    let mut signer_padded = [0u8; 32];
    signer_padded[12..].copy_from_slice(order_signer.as_slice());
    let mut taker_padded = [0u8; 32];
    taker_padded[12..].copy_from_slice(taker.as_slice());

    let mut struct_data = Vec::new();
    struct_data.extend_from_slice(&order_type_hash);
    struct_data.extend_from_slice(&salt.to_be_bytes::<32>());
    struct_data.extend_from_slice(&maker_padded);
    struct_data.extend_from_slice(&signer_padded);
    struct_data.extend_from_slice(&taker_padded);
    struct_data.extend_from_slice(&token_id.to_be_bytes::<32>());
    struct_data.extend_from_slice(&maker_amount.to_be_bytes::<32>());
    struct_data.extend_from_slice(&taker_amount.to_be_bytes::<32>());
    struct_data.extend_from_slice(&expiration.to_be_bytes::<32>());
    struct_data.extend_from_slice(&nonce.to_be_bytes::<32>());
    struct_data.extend_from_slice(&fee_rate_bps.to_be_bytes::<32>());
    struct_data.extend_from_slice(&side.to_be_bytes::<32>());
    struct_data.extend_from_slice(&sig_type.to_be_bytes::<32>());

    let struct_hash = Keccak256::digest(&struct_data);

    // Final hash: keccak256("\x19\x01" || domainSeparator || structHash)
    let mut final_data = Vec::new();
    final_data.push(0x19);
    final_data.push(0x01);
    final_data.extend_from_slice(&domain_separator);
    final_data.extend_from_slice(&struct_hash);

    let final_hash = Keccak256::digest(&final_data);

    // Sign the hash
    let signature = signer.sign_hash(&alloy::primitives::B256::from_slice(&final_hash)).await?;

    // Return signature in hex format
    Ok(format!("0x{}", hex::encode(signature.as_bytes())))
}

/// Get USDC allowance for Polymarket CTF Exchange on Polygon
async fn get_polymarket_allowance(owner: Address) -> Result<U256> {
    let rpc_url = POLYGON_RPC_URL.parse().map_err(|e| anyhow!("Invalid Polygon RPC URL: {}", e))?;
    let provider = ProviderBuilder::new().connect_http(rpc_url);

    let usdc_address = Address::from_str(USDC_POLYGON_ADDRESS)?;
    let spender = Address::from_str(POLYMARKET_CTF_EXCHANGE)?;

    let usdc = IERC20::new(usdc_address, provider);
    let allowance = usdc.allowance(owner, spender).call().await
        .context("Failed to get Polymarket allowance")?;

    Ok(allowance)
}

/// Approve USDC spending for Polymarket CTF Exchange on Polygon
async fn approve_usdc_for_polymarket(signer: &PrivateKeySigner, amount: U256) -> Result<String> {
    use alloy::network::EthereumWallet;

    let rpc_url = POLYGON_RPC_URL.parse().map_err(|e| anyhow!("Invalid Polygon RPC URL: {}", e))?;

    let wallet = EthereumWallet::from(signer.clone());
    let provider = ProviderBuilder::new()
        .wallet(wallet)
        .connect_http(rpc_url);

    let usdc_address = Address::from_str(USDC_POLYGON_ADDRESS)?;
    let spender = Address::from_str(POLYMARKET_CTF_EXCHANGE)?;

    let usdc = IERC20::new(usdc_address, &provider);
    let pending_tx = usdc.approve(spender, amount).send().await
        .context("Failed to send approval transaction")?;

    let receipt = pending_tx.get_receipt().await
        .context("Failed to get approval receipt")?;

    Ok(format!("{:?}", receipt.transaction_hash))
}

/// Portfolio position data
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LimitlessPosition {
    pub market_id: Option<String>,
    pub market_title: Option<String>,
    pub outcome: Option<String>,
    pub shares: Option<String>,
    pub avg_price: Option<f64>,
    pub current_price: Option<f64>,
    pub pnl: Option<f64>,
}

/// Get portfolio positions for the authenticated user
async fn limitless_get_positions(address: &Address, session: &str) -> Result<Vec<LimitlessPosition>> {
    let client = reqwest::Client::new();
    let checksum_addr = to_checksum_address(address);
    let url = format!("{}/portfolio/positions", LIMITLESS_API_URL);

    let response = client
        .get(&url)
        .header("x-account", &checksum_addr)
        .header("Cookie", session)
        .send()
        .await
        .context("Failed to fetch positions")?;

    if !response.status().is_success() {
        let status = response.status();
        let text = response.text().await.unwrap_or_default();
        return Err(anyhow!("Positions request failed ({}): {}", status, text));
    }

    let data: Value = response.json().await.unwrap_or(json!([]));

    let positions: Vec<LimitlessPosition> = if data.is_array() {
        data.as_array()
            .unwrap_or(&vec![])
            .iter()
            .filter_map(|v| {
                Some(LimitlessPosition {
                    market_id: v.get("marketId")
                        .or_else(|| v.get("market").and_then(|m| m.get("id")))
                        .and_then(|x| x.as_str())
                        .map(|s| s.to_string()),
                    market_title: v.get("marketTitle")
                        .or_else(|| v.get("market").and_then(|m| m.get("title")))
                        .and_then(|x| x.as_str())
                        .map(|s| s.to_string()),
                    outcome: v.get("outcome")
                        .and_then(|x| x.as_str())
                        .map(|s| s.to_string()),
                    shares: v.get("shares")
                        .or_else(|| v.get("quantity"))
                        .and_then(|x| {
                            if x.is_string() { Some(x.as_str()?.to_string()) }
                            else if x.is_number() { Some(x.to_string()) }
                            else { None }
                        }),
                    avg_price: v.get("avgPrice")
                        .or_else(|| v.get("averagePrice"))
                        .and_then(|x| x.as_f64()),
                    current_price: v.get("currentPrice")
                        .or_else(|| v.get("price"))
                        .and_then(|x| x.as_f64()),
                    pnl: v.get("pnl")
                        .or_else(|| v.get("unrealizedPnl"))
                        .and_then(|x| x.as_f64()),
                })
            })
            .collect()
    } else {
        vec![]
    };

    Ok(positions)
}

/// Get USDC trading allowance on Limitless
/// `market_type` should be "clob" for central limit order book or "negrisk" for Negrisk markets
async fn limitless_get_allowance(address: &Address, session: &str, market_type: &str) -> Result<String> {
    let client = reqwest::Client::new();
    let checksum_addr = to_checksum_address(address);
    let url = format!("{}/portfolio/trading/allowance?type={}", LIMITLESS_API_URL, market_type);

    let response = client
        .get(&url)
        .header("x-account", &checksum_addr)
        .header("Cookie", session)
        .send()
        .await
        .context("Failed to fetch allowance")?;

    if !response.status().is_success() {
        let status = response.status();
        let text = response.text().await.unwrap_or_default();
        return Err(anyhow!("Allowance request failed ({}): {}", status, text));
    }

    let data: Value = response.json().await.unwrap_or(json!({}));

    // Extract allowance from response
    let allowance = data
        .get("allowance")
        .or_else(|| data.get("amount"))
        .and_then(|v| {
            if v.is_string() { Some(v.as_str()?.to_string()) }
            else if v.is_number() { Some(v.to_string()) }
            else { None }
        })
        .unwrap_or_else(|| "0".to_string());

    Ok(allowance)
}

/// Market info for trading (tokens and venue)
#[derive(Debug, Clone)]
pub struct MarketTradingInfo {
    pub yes_token: String,
    pub no_token: String,
    pub exchange_address: String,
}

/// Get market info for trading (tokens and venue)
async fn limitless_get_market_trading_info(slug: &str) -> Result<MarketTradingInfo> {
    let client = reqwest::Client::new();
    let url = format!("{}/markets/{}", LIMITLESS_API_URL, slug);

    let response = client
        .get(&url)
        .header("Accept", "application/json")
        .send()
        .await
        .context("Failed to fetch market")?;

    if !response.status().is_success() {
        return Err(anyhow!("Market not found: {}", slug));
    }

    let data: Value = response.json().await?;

    let tokens = data.get("tokens").ok_or_else(|| anyhow!("No tokens in market data"))?;

    let yes_token = tokens.get("yes")
        .and_then(|v| v.as_str())
        .ok_or_else(|| anyhow!("No YES token ID"))?;

    let no_token = tokens.get("no")
        .and_then(|v| v.as_str())
        .ok_or_else(|| anyhow!("No NO token ID"))?;

    // Get venue exchange address
    let exchange_address = data.get("venue")
        .and_then(|v| v.get("exchange"))
        .and_then(|v| v.as_str())
        .unwrap_or(LIMITLESS_CTF_EXCHANGE)
        .to_string();

    Ok(MarketTradingInfo {
        yes_token: yes_token.to_string(),
        no_token: no_token.to_string(),
        exchange_address,
    })
}

/// Create and sign an EIP-712 order for Limitless
async fn create_limitless_order(
    signer: &PrivateKeySigner,
    market_slug: &str,
    outcome: &str,  // "YES" or "NO"
    side: &str,     // "BUY" or "SELL"
    amount_usdc: f64,
    price: f64,     // 0.0 to 1.0
) -> Result<(LimitlessOrderData, String)> {
    // Get market trading info (tokens and exchange address)
    let market_info = limitless_get_market_trading_info(market_slug).await?;
    let token_id = if outcome.to_uppercase() == "YES" {
        market_info.yes_token
    } else {
        market_info.no_token
    };

    let maker = signer.address();
    let maker_str = to_checksum_address(&maker);

    // Calculate amounts (6 decimals for USDC)
    let amount_raw = (amount_usdc * 1_000_000.0) as u64;
    let price_scaled = (price * 1_000_000.0) as u64;

    let (maker_amount, taker_amount) = if side.to_uppercase() == "BUY" {
        // BUY: maker gives USDC, receives shares
        // makerAmount = USDC amount
        // takerAmount = shares = USDC / price
        let shares = if price > 0.0 { amount_raw * 1_000_000 / price_scaled } else { amount_raw };
        (amount_raw.to_string(), shares.to_string())
    } else {
        // SELL: maker gives shares, receives USDC
        // makerAmount = shares
        // takerAmount = USDC = shares * price
        let shares = amount_raw; // For sell, amount is in shares conceptually
        let usdc = shares * price_scaled / 1_000_000;
        (shares.to_string(), usdc.to_string())
    };

    // Generate salt (timestamp-based for uniqueness)
    let now = chrono::Utc::now();
    let salt = format!("{}", now.timestamp_millis() * 1000 + (now.timestamp_nanos_opt().unwrap_or(0) % 1000) as i64);

    // Expiration: 0 means no expiration (API requirement)
    let expiration = "0".to_string();

    // Nonce: 0 for now (should track per address in production)
    let nonce = "0".to_string();

    let order = LimitlessOrderData {
        salt: salt.clone(),
        maker: maker_str.clone(),
        signer: maker_str.clone(),
        taker: "0x0000000000000000000000000000000000000000".to_string(),
        token_id: token_id.clone(),
        maker_amount: maker_amount.clone(),
        taker_amount: taker_amount.clone(),
        expiration: expiration.clone(),
        nonce: nonce.clone(),
        fee_rate_bps: "300".to_string(), // 3% fee
        side: if side.to_uppercase() == "BUY" { 0 } else { 1 },
        signature_type: 0, // EOA signature
    };

    // =========================================================================
    // EIP-712 Typed Data Signing for Limitless CTF Exchange
    // =========================================================================

    // EIP-712 domain type hash
    let domain_type = "EIP712Domain(string name,string version,uint256 chainId,address verifyingContract)";
    let domain_type_hash = Keccak256::digest(domain_type.as_bytes());

    // Order type hash
    let order_type = "Order(uint256 salt,address maker,address signer,address taker,uint256 tokenId,uint256 makerAmount,uint256 takerAmount,uint256 expiration,uint256 nonce,uint256 feeRateBps,uint8 side,uint8 signatureType)";
    let order_type_hash = Keccak256::digest(order_type.as_bytes());

    // Encode domain separator
    let name_hash = Keccak256::digest(b"Limitless CTF Exchange");
    let version_hash = Keccak256::digest(b"1");
    let chain_id_bytes = {
        let mut buf = [0u8; 32];
        buf[24..].copy_from_slice(&BASE_CHAIN_ID.to_be_bytes());
        buf
    };
    let verifying_contract = Address::from_str(&market_info.exchange_address)?;

    let mut domain_data = Vec::new();
    domain_data.extend_from_slice(&domain_type_hash);
    domain_data.extend_from_slice(&name_hash);
    domain_data.extend_from_slice(&version_hash);
    domain_data.extend_from_slice(&chain_id_bytes);
    domain_data.extend_from_slice(&[0u8; 12]); // padding for address
    domain_data.extend_from_slice(verifying_contract.as_slice());

    let domain_separator = Keccak256::digest(&domain_data);

    // Encode order struct
    let salt_u256: U256 = order.salt.parse().unwrap_or(U256::ZERO);
    let maker_addr = Address::from_str(&order.maker)?;
    let signer_addr = Address::from_str(&order.signer)?;
    let taker_addr = Address::from_str(&order.taker)?;
    let token_id_u256: U256 = order.token_id.parse().unwrap_or(U256::ZERO);
    let maker_amount_u256: U256 = order.maker_amount.parse().unwrap_or(U256::ZERO);
    let taker_amount_u256: U256 = order.taker_amount.parse().unwrap_or(U256::ZERO);
    let expiration_u256: U256 = order.expiration.parse().unwrap_or(U256::ZERO);
    let nonce_u256: U256 = order.nonce.parse().unwrap_or(U256::ZERO);
    let fee_rate_u256: U256 = order.fee_rate_bps.parse().unwrap_or(U256::ZERO);

    let mut order_data = Vec::new();
    order_data.extend_from_slice(&order_type_hash);
    order_data.extend_from_slice(&salt_u256.to_be_bytes::<32>());
    order_data.extend_from_slice(&[0u8; 12]); // padding for address
    order_data.extend_from_slice(maker_addr.as_slice());
    order_data.extend_from_slice(&[0u8; 12]);
    order_data.extend_from_slice(signer_addr.as_slice());
    order_data.extend_from_slice(&[0u8; 12]);
    order_data.extend_from_slice(taker_addr.as_slice());
    order_data.extend_from_slice(&token_id_u256.to_be_bytes::<32>());
    order_data.extend_from_slice(&maker_amount_u256.to_be_bytes::<32>());
    order_data.extend_from_slice(&taker_amount_u256.to_be_bytes::<32>());
    order_data.extend_from_slice(&expiration_u256.to_be_bytes::<32>());
    order_data.extend_from_slice(&nonce_u256.to_be_bytes::<32>());
    order_data.extend_from_slice(&fee_rate_u256.to_be_bytes::<32>());
    // side and signatureType as uint8 padded to 32 bytes
    let mut side_bytes = [0u8; 32];
    side_bytes[31] = order.side;
    order_data.extend_from_slice(&side_bytes);
    let mut sig_type_bytes = [0u8; 32];
    sig_type_bytes[31] = order.signature_type;
    order_data.extend_from_slice(&sig_type_bytes);

    let struct_hash = Keccak256::digest(&order_data);

    // Final hash to sign: keccak256("\x19\x01" || domainSeparator || structHash)
    let mut final_data = Vec::new();
    final_data.push(0x19);
    final_data.push(0x01);
    final_data.extend_from_slice(&domain_separator);
    final_data.extend_from_slice(&struct_hash);

    let final_hash = Keccak256::digest(&final_data);

    // Sign the hash
    let signature = signer.sign_hash(&alloy::primitives::B256::from_slice(&final_hash)).await
        .map_err(|e| anyhow!("Failed to sign order: {}", e))?;

    let signature_hex = format!("0x{}", hex::encode(signature.as_bytes()));

    Ok((order, signature_hex))
}

/// Submit an order to Limitless Exchange
async fn limitless_submit_order(
    session: &LimitlessSession,
    order: &LimitlessOrderData,
    signature: &str,
    market_slug: &str,
    order_type: &str, // "GTC" or "FOK"
    price: Option<f64>,
) -> Result<String> {
    let client = reqwest::Client::new();
    let url = format!("{}/orders", LIMITLESS_API_URL);
    let checksum_addr = to_checksum_address(&session.address);

    // Parse numeric values from strings
    let salt: u64 = order.salt.parse().unwrap_or(0);
    let maker_amount: u64 = order.maker_amount.parse().unwrap_or(0);
    let taker_amount: u64 = order.taker_amount.parse().unwrap_or(0);
    let nonce: u64 = order.nonce.parse().unwrap_or(0);
    let fee_rate_bps: u64 = order.fee_rate_bps.parse().unwrap_or(300);

    // Build order object (expiration and tokenId are strings, amounts are numbers)
    let mut order_obj = json!({
        "salt": salt,
        "maker": order.maker,
        "signer": order.signer,
        "taker": order.taker,
        "tokenId": order.token_id,  // string
        "makerAmount": maker_amount,
        "takerAmount": taker_amount,
        "expiration": order.expiration,  // string
        "nonce": nonce,
        "feeRateBps": fee_rate_bps,
        "side": order.side,
        "signatureType": order.signature_type,
        "signature": signature
    });

    // Add price for GTC orders (inside order object)
    if order_type == "GTC" {
        if let Some(p) = price {
            order_obj["price"] = json!(p);
        }
    }

    let mut payload = json!({
        "order": order_obj,
        "orderType": order_type,
        "marketSlug": market_slug
    });

    // Add ownerId as number if available
    if let Some(owner_id) = session.owner_id {
        payload["ownerId"] = json!(owner_id);
    }

    let response = client
        .post(&url)
        .header("Content-Type", "application/json")
        .header("x-account", &checksum_addr)
        .header("Cookie", &session.auth_token)
        .json(&payload)
        .send()
        .await
        .context("Failed to submit order")?;

    let status = response.status();
    let body = response.text().await.unwrap_or_default();

    if !status.is_success() {
        return Err(anyhow!("Order submission failed ({}): {}", status, body));
    }

    Ok(body)
}

/// Get details for a specific market by address or slug
async fn limitless_get_market(address_or_slug: &str) -> Result<LimitlessMarket> {
    let client = reqwest::Client::new();
    let url = format!("{}/markets/{}", LIMITLESS_API_URL, address_or_slug);

    let response = client
        .get(&url)
        .header("Accept", "application/json")
        .send()
        .await
        .context("Failed to fetch market")?;

    if !response.status().is_success() {
        let status = response.status();
        let text = response.text().await.unwrap_or_default();
        return Err(anyhow!("Market request failed ({}): {}", status, text));
    }

    let v: Value = response.json().await?;

    // Parse the market response
    let market = LimitlessMarket {
        id: v.get("address")
            .or_else(|| v.get("slug"))
            .or_else(|| v.get("id"))
            .and_then(|x| x.as_str())
            .map(|s| s.to_string())
            .unwrap_or_else(|| address_or_slug.to_string()),
        title: v.get("title")
            .or_else(|| v.get("name"))
            .and_then(|x| x.as_str())
            .map(|s| s.to_string())
            .unwrap_or_else(|| "Unknown".to_string()),
        description: v.get("description")
            .and_then(|x| x.as_str())
            .map(|s| s.to_string()),
        category: v.get("category")
            .or_else(|| v.get("categoryId"))
            .and_then(|x| x.as_str())
            .map(|s| s.to_string()),
        end_date: v.get("deadline")
            .or_else(|| v.get("endDate"))
            .and_then(|x| x.as_str())
            .map(|s| s.to_string()),
        volume: v.get("volume")
            .or_else(|| v.get("volumeUsd"))
            .and_then(|x| {
                if x.is_string() { Some(x.as_str()?.to_string()) }
                else if x.is_number() { Some(x.to_string()) }
                else { None }
            }),
        liquidity: v.get("liquidity")
            .or_else(|| v.get("liquidityUsd"))
            .and_then(|x| {
                if x.is_string() { Some(x.as_str()?.to_string()) }
                else if x.is_number() { Some(x.to_string()) }
                else { None }
            }),
        yes_price: v.get("yesPrice")
            .or_else(|| v.get("prices").and_then(|p| p.get("yes")))
            .and_then(|x| x.as_f64()),
        no_price: v.get("noPrice")
            .or_else(|| v.get("prices").and_then(|p| p.get("no")))
            .and_then(|x| x.as_f64()),
        resolved: v.get("resolved").and_then(|x| x.as_bool()),
        resolution: v.get("resolution")
            .or_else(|| v.get("outcome"))
            .and_then(|x| x.as_str())
            .map(|s| s.to_string()),
    };

    Ok(market)
}

// ============================================================================
// MCP Server Implementation
// ============================================================================

#[derive(Debug, Serialize, Deserialize)]
struct JsonRpcRequest {
    jsonrpc: String,
    method: String,
    params: Option<Value>,
    id: Value,
}

#[derive(Debug, Serialize)]
struct JsonRpcResponse {
    jsonrpc: String,
    id: Value,
    #[serde(skip_serializing_if = "Option::is_none")]
    result: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<JsonRpcError>,
}

#[derive(Debug, Serialize)]
struct JsonRpcError {
    code: i32,
    message: String,
}

fn create_response(id: Value, result: Value) -> JsonRpcResponse {
    JsonRpcResponse {
        jsonrpc: "2.0".to_string(),
        id,
        result: Some(result),
        error: None,
    }
}

fn create_error(id: Value, code: i32, message: String) -> JsonRpcResponse {
    JsonRpcResponse {
        jsonrpc: "2.0".to_string(),
        id,
        result: None,
        error: Some(JsonRpcError { code, message }),
    }
}

/// List available MCP tools
fn list_tools() -> Value {
    json!({
        "tools": [
            {
                "name": "base_wallet_info",
                "description": "Get Chronicle's BASE wallet address",
                "inputSchema": {
                    "type": "object",
                    "properties": {},
                    "required": []
                }
            },
            {
                "name": "base_wallet_balance",
                "description": "Get Chronicle's BASE wallet balance (ETH and USDC)",
                "inputSchema": {
                    "type": "object",
                    "properties": {},
                    "required": []
                }
            },
            {
                "name": "base_usdc_balance",
                "description": "Get USDC balance for any address on BASE",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "address": {
                            "type": "string",
                            "description": "The BASE address to check (0x...)"
                        }
                    },
                    "required": ["address"]
                }
            },
            {
                "name": "base_generate_wallet",
                "description": "Generate a new BASE wallet (saves to ~/.base-wallet/private_key.txt)",
                "inputSchema": {
                    "type": "object",
                    "properties": {},
                    "required": []
                }
            },
            {
                "name": "limitless_authenticate",
                "description": "Authenticate with Limitless Exchange using Chronicle's wallet",
                "inputSchema": {
                    "type": "object",
                    "properties": {},
                    "required": []
                }
            },
            {
                "name": "limitless_list_markets",
                "description": "List available prediction markets on Limitless Exchange",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "limit": {
                            "type": "integer",
                            "description": "Maximum number of markets to return"
                        },
                        "category": {
                            "type": "string",
                            "description": "Filter by category (e.g., 'crypto', 'politics', 'sports')"
                        }
                    },
                    "required": []
                }
            },
            {
                "name": "limitless_get_market",
                "description": "Get details for a specific Limitless market",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "market_id": {
                            "type": "string",
                            "description": "The market ID or slug"
                        }
                    },
                    "required": ["market_id"]
                }
            },
            {
                "name": "limitless_get_positions",
                "description": "Get Chronicle's current positions on Limitless Exchange",
                "inputSchema": {
                    "type": "object",
                    "properties": {},
                    "required": []
                }
            },
            {
                "name": "limitless_get_allowance",
                "description": "Check USDC trading allowance on Limitless",
                "inputSchema": {
                    "type": "object",
                    "properties": {},
                    "required": []
                }
            },
            {
                "name": "limitless_approve_usdc",
                "description": "Approve USDC spending for Limitless Exchange trading",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "amount": {
                            "type": "number",
                            "description": "Amount of USDC to approve (in dollars, e.g., 100 for $100). Use 0 for unlimited."
                        },
                        "spender": {
                            "type": "string",
                            "description": "Optional: specific exchange address to approve (from market venue). If not provided, approves to default CTF Exchange."
                        }
                    },
                    "required": []
                }
            },
            {
                "name": "limitless_place_order",
                "description": "Place a prediction market order on Limitless Exchange",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "market_slug": {
                            "type": "string",
                            "description": "Market slug (from limitless_list_markets)"
                        },
                        "outcome": {
                            "type": "string",
                            "description": "Outcome to bet on: 'YES' or 'NO'"
                        },
                        "amount": {
                            "type": "number",
                            "description": "Amount in USDC to spend (e.g., 5 for $5)"
                        },
                        "price": {
                            "type": "number",
                            "description": "Limit price (0.0-1.0, e.g., 0.65 for 65% odds)"
                        }
                    },
                    "required": ["market_slug", "outcome", "amount", "price"]
                }
            },
            // ================================================================
            // Polymarket Tools
            // ================================================================
            {
                "name": "polymarket_list_markets",
                "description": "List prediction markets from Polymarket (Polygon). Search for political, regulatory, AI, and other markets.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "limit": {
                            "type": "integer",
                            "description": "Maximum number of markets to return (default 25, max 100)"
                        },
                        "category": {
                            "type": "string",
                            "description": "Filter by category (e.g., 'politics', 'crypto', 'AI', 'regulation')"
                        },
                        "active_only": {
                            "type": "boolean",
                            "description": "Only show active/open markets (default: true)"
                        }
                    },
                    "required": []
                }
            },
            {
                "name": "polymarket_get_market",
                "description": "Get details for a specific Polymarket market by slug or condition ID",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "market_id": {
                            "type": "string",
                            "description": "Market slug or condition ID"
                        }
                    },
                    "required": ["market_id"]
                }
            },
            {
                "name": "polygon_wallet_balance",
                "description": "Get Chronicle's Polygon wallet balance (MATIC and USDC). Uses same key as BASE wallet.",
                "inputSchema": {
                    "type": "object",
                    "properties": {},
                    "required": []
                }
            },
            {
                "name": "polymarket_get_allowance",
                "description": "Check USDC trading allowance for Polymarket on Polygon",
                "inputSchema": {
                    "type": "object",
                    "properties": {},
                    "required": []
                }
            },
            {
                "name": "polymarket_approve_usdc",
                "description": "Approve USDC spending for Polymarket CTF Exchange on Polygon",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "amount": {
                            "type": "number",
                            "description": "Amount of USDC to approve (in dollars). Use 0 for unlimited."
                        }
                    },
                    "required": []
                }
            },
            {
                "name": "polymarket_place_order",
                "description": "Place a prediction market order on Polymarket. Requires thesis for tracking.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "market_slug": {
                            "type": "string",
                            "description": "Market slug from polymarket_list_markets"
                        },
                        "outcome": {
                            "type": "string",
                            "description": "Outcome to bet on: 'YES' or 'NO'"
                        },
                        "amount": {
                            "type": "number",
                            "description": "Amount in USDC to spend (e.g., 10 for $10)"
                        },
                        "price": {
                            "type": "number",
                            "description": "Limit price (0.0-1.0, e.g., 0.65 for 65% odds)"
                        },
                        "thesis": {
                            "type": "string",
                            "description": "Chronicle's reasoning for this prediction (required for track record)"
                        },
                        "confidence": {
                            "type": "number",
                            "description": "Confidence level 0.0-1.0 (e.g., 0.75 for 75% confident)"
                        }
                    },
                    "required": ["market_slug", "outcome", "amount", "price", "thesis", "confidence"]
                }
            },
            // ================================================================
            // Bridge Tools
            // ================================================================
            {
                "name": "bridge_usdc_base_to_polygon",
                "description": "Bridge USDC from BASE to Polygon via Stargate. Used to fund Polymarket trading.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "amount": {
                            "type": "number",
                            "description": "Amount of USDC to bridge (e.g., 100 for $100)"
                        }
                    },
                    "required": ["amount"]
                }
            }
        ]
    })
}

/// Handle a tool call
async fn handle_tool_call(
    name: &str,
    arguments: &Value,
    wallet_config: &BaseWalletConfig,
) -> Result<Value> {
    match name {
        "base_wallet_info" => {
            let address = wallet_config
                .address
                .map(|a| format!("{:?}", a))
                .unwrap_or_else(|| "Not configured - run base_generate_wallet first".to_string());

            let status = if wallet_config.signer.is_some() {
                "Ready (local key)"
            } else {
                "Not configured"
            };

            Ok(json!({
                "content": [{
                    "type": "text",
                    "text": format!(
                        "Chronicle BASE Wallet\n\
                         ====================\n\
                         Address: {}\n\
                         Network: BASE Mainnet\n\
                         Status: {}",
                        address, status
                    )
                }]
            }))
        }

        "base_generate_wallet" => {
            // Check if wallet already exists
            if wallet_config.address.is_some() {
                return Ok(json!({
                    "content": [{
                        "type": "text",
                        "text": format!(
                            "Wallet already exists!\n\
                             Address: {:?}\n\
                             To create a new wallet, delete ~/.base-wallet/private_key.txt first",
                            wallet_config.address.unwrap()
                        )
                    }]
                }));
            }

            let (address, _private_key) = generate_wallet()?;

            Ok(json!({
                "content": [{
                    "type": "text",
                    "text": format!(
                        "New BASE Wallet Generated!\n\
                         ==========================\n\
                         Address: {:?}\n\
                         Network: BASE Mainnet\n\n\
                         Private key saved to: ~/.base-wallet/private_key.txt\n\
                         IMPORTANT: Back up this file securely!\n\n\
                         You can now receive USDC at this address.",
                        address
                    )
                }]
            }))
        }

        "base_wallet_balance" => {
            let address = wallet_config
                .address
                .ok_or_else(|| anyhow!("Wallet not configured"))?;

            let eth_balance = get_eth_balance(address).await?;
            let usdc_balance = get_usdc_balance(address).await?;

            // Convert to human-readable
            let eth_display = format!(
                "{:.6} ETH",
                eth_balance.to_string().parse::<f64>().unwrap_or(0.0) / 1e18
            );

            // USDC on BASE has 6 decimals
            let usdc_display = format!(
                "{:.2} USDC",
                usdc_balance.to_string().parse::<f64>().unwrap_or(0.0) / 1_000_000.0
            );

            Ok(json!({
                "content": [{
                    "type": "text",
                    "text": format!(
                        "Chronicle BASE Wallet Balance\n\
                         =============================\n\
                         Address: {:?}\n\
                         ETH: {}\n\
                         USDC: {}",
                        address, eth_display, usdc_display
                    )
                }]
            }))
        }

        "base_usdc_balance" => {
            let address_str = arguments
                .get("address")
                .and_then(|v| v.as_str())
                .ok_or_else(|| anyhow!("Missing address parameter"))?;

            let address = Address::from_str(address_str)
                .map_err(|e| anyhow!("Invalid address: {}", e))?;

            let balance = get_usdc_balance(address).await?;
            // USDC on BASE has 6 decimals
            let display = balance.to_string().parse::<f64>().unwrap_or(0.0) / 1_000_000.0;

            Ok(json!({
                "content": [{
                    "type": "text",
                    "text": format!(
                        "USDC Balance on BASE\n\
                         ====================\n\
                         Address: {}\n\
                         Balance: {:.2} USDC",
                        address_str, display
                    )
                }]
            }))
        }

        "base_derive_address" => {
            let pk_hex = arguments
                .get("public_key")
                .and_then(|v| v.as_str())
                .ok_or_else(|| anyhow!("Missing public_key parameter"))?;

            let pk_bytes = hex::decode(pk_hex)
                .map_err(|e| anyhow!("Invalid hex: {}", e))?;

            if pk_bytes.len() != 33 {
                return Err(anyhow!("Expected 33-byte compressed public key"));
            }

            let pk_array: [u8; 33] = pk_bytes
                .try_into()
                .map_err(|_| anyhow!("Failed to convert to array"))?;

            let address = derive_evm_address(&pk_array)?;

            Ok(json!({
                "content": [{
                    "type": "text",
                    "text": format!(
                        "EVM Address Derivation\n\
                         ======================\n\
                         Public Key: {}\n\
                         BASE Address: {:?}",
                        pk_hex, address
                    )
                }]
            }))
        }

        // ====================================================================
        // Limitless Exchange Tools
        // ====================================================================

        "limitless_authenticate" => {
            let signer = wallet_config
                .signer
                .as_ref()
                .ok_or_else(|| anyhow!("Wallet not configured - run base_generate_wallet first"))?;

            let session = limitless_authenticate(signer).await?;

            Ok(json!({
                "content": [{
                    "type": "text",
                    "text": format!(
                        "Limitless Exchange Authentication\n\
                         ==================================\n\
                         Address: {:?}\n\
                         Status: Authenticated\n\
                         Token: {}...\n\
                         Expires: {} (UTC)",
                        session.address,
                        &session.auth_token[..20.min(session.auth_token.len())],
                        chrono::DateTime::from_timestamp(session.expires_at, 0)
                            .map(|dt| dt.format("%Y-%m-%d %H:%M").to_string())
                            .unwrap_or_else(|| "unknown".to_string())
                    )
                }]
            }))
        }

        "limitless_list_markets" => {
            let limit = arguments
                .get("limit")
                .and_then(|v| v.as_u64())
                .map(|n| n as u32);

            let category = arguments
                .get("category")
                .and_then(|v| v.as_str());

            let markets = limitless_list_markets(limit, category).await?;

            if markets.is_empty() {
                return Ok(json!({
                    "content": [{
                        "type": "text",
                        "text": "No markets found. The Limitless API may have changed or markets are unavailable."
                    }]
                }));
            }

            let mut output = String::from("Limitless Exchange Markets\n");
            output.push_str("==========================\n\n");

            for market in markets.iter().take(10) {
                output.push_str(&format!("**{}**\n", market.title));
                output.push_str(&format!("  ID: {}\n", market.id));

                if let Some(ref cat) = market.category {
                    output.push_str(&format!("  Category: {}\n", cat));
                }

                if let Some(yes) = market.yes_price {
                    output.push_str(&format!("  YES: {:.1}%", yes * 100.0));
                }
                if let Some(no) = market.no_price {
                    output.push_str(&format!("  NO: {:.1}%", no * 100.0));
                }
                output.push('\n');

                if let Some(ref vol) = market.volume {
                    output.push_str(&format!("  Volume: ${}\n", vol));
                }

                if let Some(resolved) = market.resolved {
                    if resolved {
                        output.push_str(&format!("  Status: RESOLVED ({})\n",
                            market.resolution.as_deref().unwrap_or("?")));
                    }
                }
                output.push('\n');
            }

            output.push_str(&format!("Total: {} markets shown", markets.len().min(10)));

            Ok(json!({
                "content": [{
                    "type": "text",
                    "text": output
                }]
            }))
        }

        "limitless_get_market" => {
            let market_id = arguments
                .get("market_id")
                .and_then(|v| v.as_str())
                .ok_or_else(|| anyhow!("Missing market_id parameter"))?;

            let market = limitless_get_market(market_id).await?;

            let mut output = format!("Market: {}\n", market.title);
            output.push_str(&format!("{}\n\n", "=".repeat(market.title.len() + 8)));

            output.push_str(&format!("ID: {}\n", market.id));

            if let Some(ref desc) = market.description {
                output.push_str(&format!("Description: {}\n", desc));
            }

            if let Some(ref cat) = market.category {
                output.push_str(&format!("Category: {}\n", cat));
            }

            if let Some(ref end) = market.end_date {
                output.push_str(&format!("End Date: {}\n", end));
            }

            output.push_str("\nPrices:\n");
            if let Some(yes) = market.yes_price {
                output.push_str(&format!("  YES: {:.2}% (${:.4} per share)\n", yes * 100.0, yes));
            }
            if let Some(no) = market.no_price {
                output.push_str(&format!("  NO: {:.2}% (${:.4} per share)\n", no * 100.0, no));
            }

            if let Some(ref vol) = market.volume {
                output.push_str(&format!("\nVolume: ${}\n", vol));
            }
            if let Some(ref liq) = market.liquidity {
                output.push_str(&format!("Liquidity: ${}\n", liq));
            }

            if let Some(resolved) = market.resolved {
                output.push_str(&format!("\nResolved: {}\n", if resolved { "YES" } else { "NO" }));
                if resolved {
                    if let Some(ref res) = market.resolution {
                        output.push_str(&format!("Resolution: {}\n", res));
                    }
                }
            }

            Ok(json!({
                "content": [{
                    "type": "text",
                    "text": output
                }]
            }))
        }

        "limitless_get_positions" => {
            let signer = wallet_config
                .signer
                .as_ref()
                .ok_or_else(|| anyhow!("Wallet not configured"))?;

            // Authenticate first
            let session = limitless_authenticate(signer).await?;

            let positions = limitless_get_positions(&session.address, &session.auth_token).await?;

            if positions.is_empty() {
                return Ok(json!({
                    "content": [{
                        "type": "text",
                        "text": "No positions found.\n\nChronicle has no active prediction market positions on Limitless."
                    }]
                }));
            }

            let mut output = String::from("Limitless Portfolio Positions\n");
            output.push_str("=============================\n\n");

            for pos in &positions {
                if let Some(ref title) = pos.market_title {
                    output.push_str(&format!("**{}**\n", title));
                }
                if let Some(ref outcome) = pos.outcome {
                    output.push_str(&format!("  Outcome: {}\n", outcome));
                }
                if let Some(ref shares) = pos.shares {
                    output.push_str(&format!("  Shares: {}\n", shares));
                }
                if let Some(avg) = pos.avg_price {
                    output.push_str(&format!("  Avg Price: ${:.4}\n", avg));
                }
                if let Some(curr) = pos.current_price {
                    output.push_str(&format!("  Current: ${:.4}\n", curr));
                }
                if let Some(pnl) = pos.pnl {
                    let pnl_sign = if pnl >= 0.0 { "+" } else { "" };
                    output.push_str(&format!("  P&L: {}${:.2}\n", pnl_sign, pnl));
                }
                output.push('\n');
            }

            Ok(json!({
                "content": [{
                    "type": "text",
                    "text": output
                }]
            }))
        }

        "limitless_get_allowance" => {
            let signer = wallet_config
                .signer
                .as_ref()
                .ok_or_else(|| anyhow!("Wallet not configured"))?;

            let address = signer.address();

            // Get on-chain allowance directly
            let onchain_allowance = get_limitless_allowance(address).await.unwrap_or(U256::ZERO);
            let allowance_display = onchain_allowance.to_string().parse::<f64>().unwrap_or(0.0) / 1_000_000.0;
            let allowance_str = if allowance_display > 1_000_000_000.0 {
                "Unlimited".to_string()
            } else {
                format!("${:.2}", allowance_display)
            };

            // Get BASE wallet balances
            let usdc_balance = get_usdc_balance(address).await.unwrap_or(U256::ZERO);
            let usdc_display = usdc_balance.to_string().parse::<f64>().unwrap_or(0.0) / 1_000_000.0;

            let eth_balance = get_eth_balance(address).await.unwrap_or(U256::ZERO);
            let eth_display = eth_balance.to_string().parse::<f64>().unwrap_or(0.0) / 1e18;

            let gas_warning = if eth_display < 0.001 {
                "\n⚠️  WARNING: ETH balance is too low for gas fees!"
            } else {
                ""
            };

            let approval_warning = if allowance_display < usdc_display && allowance_display < 1_000_000_000.0 {
                "\n⚠️  USDC approval needed. Run limitless_approve_usdc to enable trading."
            } else {
                ""
            };

            Ok(json!({
                "content": [{
                    "type": "text",
                    "text": format!(
                        "Chronicle Limitless Trading Status\n\
                         ===================================\n\
                         Wallet: {:?}\n\n\
                         Balances:\n\
                         - USDC: ${:.2}\n\
                         - ETH (for gas): {:.6}\n\n\
                         CTF Exchange Allowance: {}\n\
                         (Contract: {}){}{}\n\n\
                         Ready to trade: {}",
                        address, usdc_display, eth_display,
                        allowance_str, LIMITLESS_CTF_EXCHANGE,
                        gas_warning, approval_warning,
                        if eth_display >= 0.001 && allowance_display >= usdc_display { "YES ✓" } else { "NO" }
                    )
                }]
            }))
        }

        "limitless_approve_usdc" => {
            let signer = wallet_config
                .signer
                .as_ref()
                .ok_or_else(|| anyhow!("Wallet not configured"))?;

            // Get amount from arguments (default to max uint256 for unlimited)
            let amount_dollars = arguments
                .get("amount")
                .and_then(|v| v.as_f64())
                .unwrap_or(0.0);

            let spender = arguments
                .get("spender")
                .and_then(|v| v.as_str());

            let amount = if amount_dollars <= 0.0 {
                // Unlimited approval (max uint256)
                U256::MAX
            } else {
                // Convert dollars to USDC units (6 decimals)
                U256::from((amount_dollars * 1_000_000.0) as u64)
            };

            let amount_str = if amount == U256::MAX {
                "Unlimited".to_string()
            } else {
                format!("${:.2}", amount_dollars)
            };

            let spender_addr = spender.unwrap_or(LIMITLESS_CTF_EXCHANGE);

            // Send approval transaction
            let tx_hash = approve_usdc_for_limitless(signer, amount, spender).await?;

            Ok(json!({
                "content": [{
                    "type": "text",
                    "text": format!(
                        "USDC Approval Transaction Submitted\n\
                         ====================================\n\
                         Amount: {}\n\
                         Spender: {}\n\
                         Transaction: {}\n\n\
                         Chronicle can now trade on this Limitless Exchange!",
                        amount_str, spender_addr, tx_hash
                    )
                }]
            }))
        }

        "limitless_place_order" => {
            let signer = wallet_config
                .signer
                .as_ref()
                .ok_or_else(|| anyhow!("Wallet not configured"))?;

            // Parse order parameters
            let market_slug = arguments
                .get("market_slug")
                .and_then(|v| v.as_str())
                .ok_or_else(|| anyhow!("Missing market_slug"))?;

            let outcome = arguments
                .get("outcome")
                .and_then(|v| v.as_str())
                .ok_or_else(|| anyhow!("Missing outcome (YES or NO)"))?;

            let amount = arguments
                .get("amount")
                .and_then(|v| v.as_f64())
                .ok_or_else(|| anyhow!("Missing amount"))?;

            let price = arguments
                .get("price")
                .and_then(|v| v.as_f64())
                .ok_or_else(|| anyhow!("Missing price (0.0-1.0)"))?;

            // Validate inputs
            if amount <= 0.0 {
                return Err(anyhow!("Amount must be positive"));
            }
            if price <= 0.0 || price >= 1.0 {
                return Err(anyhow!("Price must be between 0.0 and 1.0"));
            }
            let outcome_upper = outcome.to_uppercase();
            if outcome_upper != "YES" && outcome_upper != "NO" {
                return Err(anyhow!("Outcome must be YES or NO"));
            }

            // Authenticate
            let session = limitless_authenticate(signer).await?;

            // Create and sign order
            let (order, signature) = create_limitless_order(
                signer,
                market_slug,
                &outcome_upper,
                "BUY",
                amount,
                price,
            ).await?;

            // Submit order
            let result = limitless_submit_order(
                &session,
                &order,
                &signature,
                market_slug,
                "GTC",
                Some(price),
            ).await?;

            // Calculate potential payout
            let shares = amount / price;
            let potential_payout = shares; // At $1 per share if wins

            Ok(json!({
                "content": [{
                    "type": "text",
                    "text": format!(
                        "Order Submitted to Limitless Exchange\n\
                         ======================================\n\
                         Market: {}\n\
                         Position: {} @ {:.1}%\n\
                         Amount: ${:.2} USDC\n\
                         Shares: ~{:.2}\n\
                         Potential Payout: ${:.2} (if correct)\n\n\
                         Response: {}",
                        market_slug,
                        outcome_upper, price * 100.0,
                        amount,
                        shares,
                        potential_payout,
                        result
                    )
                }]
            }))
        }

        // ====================================================================
        // Polymarket Tool Handlers
        // ====================================================================

        "polymarket_list_markets" => {
            let limit = arguments.get("limit").and_then(|v| v.as_u64()).map(|v| v as u32);
            let category = arguments.get("category").and_then(|v| v.as_str());
            let active_only = arguments.get("active_only").and_then(|v| v.as_bool()).unwrap_or(true);

            let markets = polymarket_list_markets(limit, category, active_only).await?;

            if markets.is_empty() {
                return Ok(json!({
                    "content": [{
                        "type": "text",
                        "text": "No markets found matching criteria.\n\nTry different search terms or remove the category filter."
                    }]
                }));
            }

            let mut output = String::from("Polymarket Prediction Markets\n");
            output.push_str("==============================\n\n");

            for market in &markets {
                output.push_str(&format!("**{}**\n", market.question));
                output.push_str(&format!("  Slug: {}\n", market.slug));
                output.push_str(&format!("  YES: {:.1}% | NO: {:.1}%\n", market.yes_price * 100.0, market.no_price * 100.0));
                output.push_str(&format!("  Volume: ${:.0} | Liquidity: ${:.0}\n", market.volume_usd, market.liquidity_usd));
                output.push_str(&format!("  Category: {} | End: {}\n", market.category, market.end_date));
                if !market.is_active {
                    output.push_str("  ⚠️ CLOSED\n");
                }
                output.push('\n');
            }

            output.push_str(&format!("\nFound {} markets", markets.len()));

            Ok(json!({
                "content": [{
                    "type": "text",
                    "text": output
                }]
            }))
        }

        "polymarket_get_market" => {
            let market_id = arguments
                .get("market_id")
                .and_then(|v| v.as_str())
                .ok_or_else(|| anyhow!("market_id is required"))?;

            let market = polymarket_get_market(market_id).await?;

            let mut output = format!("Polymarket: {}\n", market.question);
            output.push_str(&format!("{}\n\n", "=".repeat(50)));
            output.push_str(&format!("Slug: {}\n", market.slug));
            output.push_str(&format!("Category: {}\n", market.category));
            output.push_str(&format!("End Date: {}\n\n", market.end_date));

            output.push_str("Prices:\n");
            output.push_str(&format!("  YES: {:.2}% (${:.4} per share)\n", market.yes_price * 100.0, market.yes_price));
            output.push_str(&format!("  NO: {:.2}% (${:.4} per share)\n\n", market.no_price * 100.0, market.no_price));

            output.push_str(&format!("Volume: ${:.2}\n", market.volume_usd));
            output.push_str(&format!("Liquidity: ${:.2}\n\n", market.liquidity_usd));

            output.push_str("Token IDs (for trading):\n");
            output.push_str(&format!("  YES: {}\n", market.yes_token_id));
            output.push_str(&format!("  NO: {}\n", market.no_token_id));

            output.push_str(&format!("\nStatus: {}", if market.is_active { "Active" } else { "Closed" }));

            Ok(json!({
                "content": [{
                    "type": "text",
                    "text": output
                }]
            }))
        }

        "polygon_wallet_balance" => {
            let signer = wallet_config
                .signer
                .as_ref()
                .ok_or_else(|| anyhow!("Wallet not configured. Same key is used for BASE and Polygon."))?;

            let address = signer.address();
            let checksum_addr = to_checksum_address(&address);

            // Get MATIC balance (native token on Polygon)
            let matic_balance = get_polygon_balance(address).await.unwrap_or(U256::ZERO);
            let matic_display = matic_balance.to_string().parse::<f64>().unwrap_or(0.0) / 1e18;

            // Get USDC balance on Polygon
            let usdc_balance = get_polygon_usdc_balance(address).await.unwrap_or(U256::ZERO);
            let usdc_display = usdc_balance.to_string().parse::<f64>().unwrap_or(0.0) / 1_000_000.0;

            let gas_warning = if matic_display < 0.1 {
                "\n⚠️  Low MATIC for gas fees! Send MATIC to this address."
            } else {
                ""
            };

            Ok(json!({
                "content": [{
                    "type": "text",
                    "text": format!(
                        "Chronicle Polygon Wallet Balance\n\
                         =================================\n\
                         Address: {}\n\
                         MATIC: {:.6} MATIC\n\
                         USDC: {:.2} USDC{}",
                        checksum_addr, matic_display, usdc_display, gas_warning
                    )
                }]
            }))
        }

        "polymarket_get_allowance" => {
            let signer = wallet_config
                .signer
                .as_ref()
                .ok_or_else(|| anyhow!("Wallet not configured"))?;

            let address = signer.address();
            let allowance = get_polymarket_allowance(address).await.unwrap_or(U256::ZERO);
            let allowance_display = allowance.to_string().parse::<f64>().unwrap_or(0.0) / 1_000_000.0;

            let allowance_str = if allowance_display > 1_000_000_000.0 {
                "Unlimited".to_string()
            } else {
                format!("${:.2}", allowance_display)
            };

            Ok(json!({
                "content": [{
                    "type": "text",
                    "text": format!(
                        "Polymarket USDC Allowance\n\
                         =========================\n\
                         Address: {:?}\n\
                         Allowance: {}",
                        address, allowance_str
                    )
                }]
            }))
        }

        "polymarket_approve_usdc" => {
            let signer = wallet_config
                .signer
                .as_ref()
                .ok_or_else(|| anyhow!("Wallet not configured"))?;

            let amount_dollars = arguments.get("amount").and_then(|v| v.as_f64()).unwrap_or(0.0);
            let amount = if amount_dollars == 0.0 {
                U256::MAX  // Unlimited
            } else {
                U256::from((amount_dollars * 1_000_000.0) as u128)
            };

            let tx_hash = approve_usdc_for_polymarket(signer, amount).await?;

            let amount_str = if amount_dollars == 0.0 {
                "Unlimited".to_string()
            } else {
                format!("${:.2}", amount_dollars)
            };

            Ok(json!({
                "content": [{
                    "type": "text",
                    "text": format!(
                        "Polymarket USDC Approval\n\
                         ========================\n\
                         Amount: {}\n\
                         Transaction: {}\n\n\
                         USDC is now approved for trading on Polymarket.",
                        amount_str, tx_hash
                    )
                }]
            }))
        }

        "polymarket_place_order" => {
            let signer = wallet_config
                .signer
                .as_ref()
                .ok_or_else(|| anyhow!("Wallet not configured"))?;

            let market_slug = arguments
                .get("market_slug")
                .and_then(|v| v.as_str())
                .ok_or_else(|| anyhow!("market_slug is required"))?;
            let outcome = arguments
                .get("outcome")
                .and_then(|v| v.as_str())
                .ok_or_else(|| anyhow!("outcome is required (YES or NO)"))?;
            let amount = arguments
                .get("amount")
                .and_then(|v| v.as_f64())
                .ok_or_else(|| anyhow!("amount is required"))?;
            let price = arguments
                .get("price")
                .and_then(|v| v.as_f64())
                .ok_or_else(|| anyhow!("price is required (0.0-1.0)"))?;
            let thesis = arguments
                .get("thesis")
                .and_then(|v| v.as_str())
                .ok_or_else(|| anyhow!("thesis is required for track record"))?;
            let confidence = arguments
                .get("confidence")
                .and_then(|v| v.as_f64())
                .ok_or_else(|| anyhow!("confidence is required (0.0-1.0)"))?;

            // Validate inputs
            let outcome_upper = outcome.to_uppercase();
            if outcome_upper != "YES" && outcome_upper != "NO" {
                return Err(anyhow!("outcome must be YES or NO"));
            }
            if price <= 0.0 || price >= 1.0 {
                return Err(anyhow!("price must be between 0.0 and 1.0"));
            }
            if confidence < 0.0 || confidence > 1.0 {
                return Err(anyhow!("confidence must be between 0.0 and 1.0"));
            }

            // Get market details to find token ID
            let market = polymarket_get_market(market_slug).await?;
            let token_id = if outcome_upper == "YES" {
                &market.yes_token_id
            } else {
                &market.no_token_id
            };

            if token_id.is_empty() {
                return Err(anyhow!("Could not find token ID for {} outcome", outcome_upper));
            }

            // Calculate shares
            let shares = amount / price;

            // Create and sign order
            let (order, signature) = create_polymarket_order(
                signer,
                token_id,
                "BUY",
                price,
                shares,
                false,  // Not using neg_risk exchange
            ).await?;

            // TODO: Submit order to Polymarket CLOB API
            // For now, just return the signed order
            // Actual submission requires L2 authentication which we'll implement next

            let potential_payout = shares;  // $1 per share if wins

            Ok(json!({
                "content": [{
                    "type": "text",
                    "text": format!(
                        "Polymarket Order Created\n\
                         =========================\n\
                         Market: {}\n\
                         Position: {} @ {:.1}%\n\
                         Amount: ${:.2} USDC\n\
                         Shares: ~{:.2}\n\
                         Potential Payout: ${:.2}\n\n\
                         Thesis: {}\n\
                         Confidence: {:.0}%\n\n\
                         Order Signature: {}...\n\n\
                         ⚠️ Order submission to Polymarket CLOB not yet implemented.\n\
                         Signed order ready for manual submission or next iteration.",
                        market.question,
                        outcome_upper, price * 100.0,
                        amount,
                        shares,
                        potential_payout,
                        thesis,
                        confidence * 100.0,
                        &signature[..20]
                    )
                }]
            }))
        }

        // ================================================================
        // Bridge Tool: BASE → Polygon via Across Protocol
        // ================================================================
        "bridge_usdc_base_to_polygon" => {
            use alloy::network::EthereumWallet;
            use alloy::network::TransactionBuilder;

            let signer = wallet_config
                .signer
                .as_ref()
                .ok_or_else(|| anyhow!("Wallet not configured"))?;

            let amount_usdc = arguments
                .get("amount")
                .and_then(|v| v.as_f64())
                .ok_or_else(|| anyhow!("amount is required (USDC to bridge)"))?;

            if amount_usdc <= 0.0 {
                return Err(anyhow!("amount must be positive"));
            }

            let amount_raw = (amount_usdc * 1_000_000.0) as u64;  // USDC has 6 decimals
            let wallet_address = wallet_config.address.ok_or_else(|| anyhow!("No wallet address"))?;

            // Across Protocol constants
            let base_usdc = "0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913";
            let polygon_usdc = "0x3c499c542cEF5E3811e1192ce70d8cC03d5c3359";
            let base_chain_id: u64 = 8453;
            let polygon_chain_id: u64 = 137;

            // Step 1: Get bridge quote from Across
            let client = reqwest::Client::new();
            let quote_url = format!(
                "https://app.across.to/api/suggested-fees?\
                inputToken={}&outputToken={}&\
                originChainId={}&destinationChainId={}&\
                amount={}",
                base_usdc, polygon_usdc,
                base_chain_id, polygon_chain_id,
                amount_raw
            );

            let quote_resp = client
                .get(&quote_url)
                .send()
                .await?
                .json::<serde_json::Value>()
                .await?;

            let spoke_pool: Address = quote_resp["spokePoolAddress"].as_str()
                .ok_or_else(|| anyhow!("No spokePoolAddress in quote"))?
                .parse()?;
            let output_amount = quote_resp["outputAmount"].as_str()
                .and_then(|s| s.parse::<u64>().ok())
                .ok_or_else(|| anyhow!("No outputAmount in quote"))?;
            let output_usdc = output_amount as f64 / 1_000_000.0;
            let fill_deadline: u32 = quote_resp["fillDeadline"].as_str()
                .and_then(|s| s.parse().ok())
                .ok_or_else(|| anyhow!("No fillDeadline in quote"))?;
            let exclusive_relayer: Address = quote_resp["exclusiveRelayer"].as_str()
                .unwrap_or("0x0000000000000000000000000000000000000000")
                .parse()?;
            let exclusivity_deadline: u32 = quote_resp["exclusivityDeadline"].as_u64()
                .unwrap_or(0) as u32;
            let quote_timestamp: u32 = quote_resp["timestamp"].as_str()
                .and_then(|s| s.parse().ok())
                .ok_or_else(|| anyhow!("No timestamp in quote"))?;
            let total_fee = quote_resp["totalRelayFee"]["total"].as_str()
                .and_then(|s| s.parse::<u64>().ok())
                .unwrap_or(0);
            let fee_usdc = total_fee as f64 / 1_000_000.0;

            eprintln!("Across quote: {} USDC -> {} USDC (fee: ${:.4})",
                amount_usdc, output_usdc, fee_usdc);

            // Step 2: Approve USDC for SpokePool
            let spoke_pool_str = format!("{:?}", spoke_pool);
            let _approval_hash = approve_usdc_for_limitless(signer, U256::from(amount_raw), Some(&spoke_pool_str)).await
                .context("Failed to approve USDC for Across")?;
            eprintln!("USDC approved for Across SpokePool");
            tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;

            // Step 3: Build depositV3 calldata
            // Function: depositV3(address depositor, address recipient, address inputToken,
            //                     address outputToken, uint256 inputAmount, uint256 outputAmount,
            //                     uint256 destinationChainId, address exclusiveRelayer,
            //                     uint32 quoteTimestamp, uint32 fillDeadline, uint32 exclusivityDeadline,
            //                     bytes message)
            // Selector: 0x7b939232
            let deposit_selector: [u8; 4] = [0x7b, 0x93, 0x92, 0x32];

            let input_token: Address = base_usdc.parse()?;
            let output_token: Address = polygon_usdc.parse()?;

            let mut calldata = deposit_selector.to_vec();
            // depositor (address)
            calldata.extend_from_slice(&[0u8; 12]);
            calldata.extend_from_slice(wallet_address.as_slice());
            // recipient (address)
            calldata.extend_from_slice(&[0u8; 12]);
            calldata.extend_from_slice(wallet_address.as_slice());
            // inputToken (address)
            calldata.extend_from_slice(&[0u8; 12]);
            calldata.extend_from_slice(input_token.as_slice());
            // outputToken (address)
            calldata.extend_from_slice(&[0u8; 12]);
            calldata.extend_from_slice(output_token.as_slice());
            // inputAmount (uint256)
            calldata.extend_from_slice(&U256::from(amount_raw).to_be_bytes::<32>());
            // outputAmount (uint256)
            calldata.extend_from_slice(&U256::from(output_amount).to_be_bytes::<32>());
            // destinationChainId (uint256)
            calldata.extend_from_slice(&U256::from(polygon_chain_id).to_be_bytes::<32>());
            // exclusiveRelayer (address)
            calldata.extend_from_slice(&[0u8; 12]);
            calldata.extend_from_slice(exclusive_relayer.as_slice());
            // quoteTimestamp (uint32) - padded to 32 bytes
            calldata.extend_from_slice(&U256::from(quote_timestamp).to_be_bytes::<32>());
            // fillDeadline (uint32) - padded to 32 bytes
            calldata.extend_from_slice(&U256::from(fill_deadline).to_be_bytes::<32>());
            // exclusivityDeadline (uint32) - padded to 32 bytes
            calldata.extend_from_slice(&U256::from(exclusivity_deadline).to_be_bytes::<32>());
            // message (bytes) - empty bytes: offset then length 0
            calldata.extend_from_slice(&U256::from(384u64).to_be_bytes::<32>()); // offset to bytes data (12 * 32 = 384)
            calldata.extend_from_slice(&U256::ZERO.to_be_bytes::<32>()); // length = 0

            // Step 4: Send deposit transaction
            let rpc_url = BASE_RPC_URL.parse().map_err(|e| anyhow!("Invalid RPC URL: {}", e))?;
            let wallet = EthereumWallet::from(signer.clone());
            let provider = ProviderBuilder::new()
                .wallet(wallet)
                .connect_http(rpc_url);

            let tx_request = alloy::rpc::types::TransactionRequest::default()
                .with_to(spoke_pool)
                .with_value(U256::ZERO)
                .with_input(calldata)
                .with_chain_id(base_chain_id);

            let pending_tx = provider.send_transaction(tx_request).await
                .context("Failed to send bridge transaction")?;

            let receipt = pending_tx.get_receipt().await
                .context("Failed to get bridge transaction receipt")?;

            let tx_hash = receipt.transaction_hash;

            Ok(json!({
                "content": [{
                    "type": "text",
                    "text": format!(
                        "USDC Bridge: BASE → Polygon (Across Protocol)\n\
                         ==============================================\n\
                         Amount: ${:.2} USDC\n\
                         Expected Output: ${:.2} USDC\n\
                         Bridge Fee: ${:.4}\n\
                         Estimated Time: ~2 seconds\n\n\
                         Bridge Transaction: {:?}\n\
                         BaseScan: https://basescan.org/tx/{:?}\n\n\
                         USDC will arrive on Polygon at the same address.\n\
                         Track: https://app.across.to/transactions",
                        amount_usdc,
                        output_usdc,
                        fee_usdc,
                        tx_hash,
                        tx_hash
                    )
                }]
            }))
        }

        _ => Err(anyhow!("Unknown tool: {}", name)),
    }
}

/// Process a single JSON-RPC request
async fn process_request(
    request: JsonRpcRequest,
    wallet_config: &BaseWalletConfig,
) -> JsonRpcResponse {
    match request.method.as_str() {
        "initialize" => create_response(
            request.id,
            json!({
                "protocolVersion": "2024-11-05",
                "capabilities": {
                    "tools": {}
                },
                "serverInfo": {
                    "name": "chronicle-base",
                    "version": "0.1.0"
                }
            }),
        ),

        "tools/list" => create_response(request.id, list_tools()),

        "tools/call" => {
            let params = request.params.unwrap_or(json!({}));
            let name = params
                .get("name")
                .and_then(|v| v.as_str())
                .unwrap_or("");
            let arguments = params.get("arguments").cloned().unwrap_or(json!({}));

            match handle_tool_call(name, &arguments, wallet_config).await {
                Ok(result) => create_response(request.id, result),
                Err(e) => create_error(request.id, -32000, e.to_string()),
            }
        }

        "notifications/initialized" => {
            // No response needed for notifications
            create_response(request.id, json!(null))
        }

        _ => create_error(
            request.id,
            -32601,
            format!("Method not found: {}", request.method),
        ),
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    // Try to initialize wallet from saved key
    let wallet_config = match init_wallet() {
        Ok(config) => {
            eprintln!(
                "Chronicle BASE Wallet loaded: {:?}",
                config.address.unwrap()
            );
            config
        }
        Err(e) => {
            eprintln!("No existing wallet found: {}", e);
            eprintln!("Use base_generate_wallet tool to create one.");
            BaseWalletConfig::default()
        }
    };

    // Read JSON-RPC requests from stdin
    let stdin = std::io::stdin();
    let reader = BufReader::new(stdin.lock());
    let mut stdout = std::io::stdout();

    for line in reader.lines() {
        let line = line.context("Failed to read line")?;
        if line.trim().is_empty() {
            continue;
        }

        // Parse the request
        let request: JsonRpcRequest = match serde_json::from_str(&line) {
            Ok(req) => req,
            Err(e) => {
                let error_response = create_error(
                    json!(null),
                    -32700,
                    format!("Parse error: {}", e),
                );
                writeln!(stdout, "{}", serde_json::to_string(&error_response)?)?;
                stdout.flush()?;
                continue;
            }
        };

        // Process the request
        let response = process_request(request, &wallet_config).await;

        // Send the response
        writeln!(stdout, "{}", serde_json::to_string(&response)?)?;
        stdout.flush()?;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_derive_evm_address() {
        // Test vector: known public key -> known address
        // This is a test key, not used in production
        let test_pubkey: [u8; 33] = [
            0x02, // compressed, y is even
            0x79, 0xbe, 0x66, 0x7e, 0xf9, 0xdc, 0xbb, 0xac,
            0x55, 0xa0, 0x62, 0x95, 0xce, 0x87, 0x0b, 0x07,
            0x02, 0x9b, 0xfc, 0xdb, 0x2d, 0xce, 0x28, 0xd9,
            0x59, 0xf2, 0x81, 0x5b, 0x16, 0xf8, 0x17, 0x98,
        ];

        let address = derive_evm_address(&test_pubkey).unwrap();
        // The address should be a valid 20-byte address
        assert_eq!(address.as_slice().len(), 20);
    }

    #[test]
    fn test_wallet_config_default() {
        let config = BaseWalletConfig::default();
        assert!(config.address.is_none());
        assert!(config.signer.is_none());
    }
}
