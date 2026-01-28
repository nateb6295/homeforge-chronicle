//! Chronicle Flare - MCP Server for Flare Network
//!
//! Provides tools for interacting with Flare blockchain:
//! - Wallet balance queries
//! - FTSO price oracle feeds (decentralized price data)
//! - FLR transfers
//!
//! This gives the cognitive loop access to decentralized oracle data.

use alloy::primitives::{Address, U256};
use alloy::providers::{Provider, ProviderBuilder};
use alloy::network::EthereumWallet;
use alloy::signers::local::PrivateKeySigner;
use alloy::sol;
use alloy::sol_types::SolEvent;
use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::io::{BufRead, BufReader, Write};
use std::str::FromStr;

/// Flare Mainnet RPC endpoint
const FLARE_RPC: &str = "https://flare-api.flare.network/ext/C/rpc";

/// FlareContractRegistry address (same on all Flare networks)
const FLARE_CONTRACT_REGISTRY: &str = "0xaD67FE66660Fb8dFE9d6b1b4240d8650e30F6019";

/// Chronicle's Flare wallet address (recovered 2026-01-24, derived from seed)
/// Seed backup: ~/.flare-wallet/seed.txt
const CHRONICLE_FLARE_WALLET: &str = "0x2C6D9E36d12fbb77dD8EDcA73739C0db075f078d";

/// FXRP Token address on Flare Mainnet (known deployed address)
const FXRP_TOKEN: &str = "0xAd552A648C74D49E10027AB8a618A3ad4901c5bE";

/// FDC Contract addresses (discovered from FlareContractRegistry)
const FDC_HUB: &str = "0xc25c749DC27Efb1864Cb3DADa8845B7687eB2d44";
const FDC_VERIFICATION: &str = "0x5C14FE9D73Ab763F4d4a76f334bf7029DDD20Ecc";
const FDC_REQUEST_FEE_CONFIG: &str = "0x259852Ae6d5085bDc0650D3887825f7b76F0c4fe";

/// XRPL API endpoint for fetching transaction data
const XRPL_API: &str = "https://s1.ripple.com:51234";

/// FDC Verifier URL for mainnet
const FDC_VERIFIER_URL: &str = "https://fdc-verifiers-mainnet.flare.network";

/// Public FDC Verifier API key (rate-limited but functional)
const FDC_VERIFIER_API_KEY: &str = "00000000-0000-0000-0000-000000000000";

/// Decode an XRPL address from a bytes32 hex string
/// The bytes32 contains the raw address bytes encoded by FAssets
fn decode_xrpl_address_from_bytes32(hex_str: &str) -> Option<String> {
    // Remove 0x prefix if present
    let hex_clean = hex_str.strip_prefix("0x").unwrap_or(hex_str);

    // Decode hex to bytes
    let bytes = hex::decode(hex_clean).ok()?;

    // The FAssets contract encodes XRPL addresses as:
    // - First byte is length indicator or version
    // - Following bytes are the address payload
    // XRPL addresses are base58check encoded with version byte 0x00

    // Try to find the actual address bytes (skip leading zeros/padding)
    // XRPL addresses are typically 20 bytes (account ID) + checksum

    // For now, try to decode as UTF-8 in case it's stored as string bytes
    if let Ok(s) = std::str::from_utf8(&bytes) {
        let trimmed = s.trim_matches('\0');
        if trimmed.starts_with('r') && trimmed.len() >= 25 {
            return Some(trimmed.to_string());
        }
    }

    // Try base58check encoding of the raw bytes (skip padding zeros)
    let non_zero_start = bytes.iter().position(|&b| b != 0).unwrap_or(0);
    let payload = &bytes[non_zero_start..];

    if payload.len() >= 20 {
        // XRPL uses base58check with version byte 0x00 for account addresses
        // The account ID is 20 bytes, and we need to add version + checksum
        let account_id = &payload[..20.min(payload.len())];

        // Build full address: version(1) + account_id(20) + checksum(4)
        let mut with_version = vec![0x00u8]; // XRPL mainnet version byte
        with_version.extend_from_slice(account_id);

        // Calculate double SHA256 checksum
        use sha2::{Sha256, Digest};
        let hash1 = Sha256::digest(&with_version);
        let hash2 = Sha256::digest(&hash1);
        let checksum = &hash2[..4];

        with_version.extend_from_slice(checksum);

        // Base58 encode
        let address = bs58::encode(&with_version)
            .with_alphabet(bs58::Alphabet::RIPPLE)
            .into_string();

        if address.starts_with('r') {
            return Some(address);
        }
    }

    None
}

// Define the FlareContractRegistry interface
sol! {
    #[sol(rpc)]
    interface IFlareContractRegistry {
        function getContractAddressByName(string memory _name) external view returns (address);
    }
}

// Define the FtsoRegistry interface (legacy but still functional)
sol! {
    #[sol(rpc)]
    interface IFtsoRegistry {
        function getCurrentPriceWithDecimals(string memory _symbol) external view returns (uint256 _price, uint256 _timestamp, uint256 _decimals);
        function getSupportedSymbols() external view returns (string[] memory);
    }
}

// FAssets AssetManager interface and Payment proof structs - ALL IN ONE BLOCK
// (alloy's sol! macro requires types to be in same block to reference each other)
sol! {
    // Request body for Payment attestation
    struct PaymentRequestBody {
        bytes32 transactionId;
        uint256 inUtxo;
        uint256 utxo;
    }

    // Response body for Payment attestation (from @flarenetwork/flare-periphery-contracts)
    struct PaymentResponseBody {
        uint64 blockNumber;
        uint64 blockTimestamp;
        bytes32 sourceAddressHash;
        bytes32 sourceAddressesRoot;     // Merkle tree root of source addresses
        bytes32 receivingAddressHash;
        bytes32 intendedReceivingAddressHash;
        int256 spentAmount;
        int256 intendedSpentAmount;
        int256 receivedAmount;
        int256 intendedReceivedAmount;
        bytes32 standardPaymentReference;
        bool oneToOne;
        uint8 status;
    }

    // Full response for Payment attestation
    struct PaymentResponse {
        bytes32 attestationType;
        bytes32 sourceId;
        uint64 votingRound;
        uint64 lowestUsedTimestamp;
        PaymentRequestBody requestBody;
        PaymentResponseBody responseBody;
    }

    // Proof structure for Payment attestation
    struct PaymentProof {
        bytes32[] merkleProof;
        PaymentResponse data;
    }

    // Available agent info struct (matches Flare FAssets contract)
    struct AvailableAgentInfo {
        address agentVault;
        address ownerManagementAddress;
        uint256 feeBIPS;           // Agent fee in basis points
        uint256 mintingVaultCollateralRatioBIPS;
        uint256 mintingPoolCollateralRatioBIPS;
        uint256 freeCollateralLots; // Available lots for minting
        uint8 status;              // AgentInfo.Status enum
    }

    #[sol(rpc)]
    interface IAssetManager {
        // Get the lot size (minimum minting unit)
        function lotSize() external view returns (uint256);

        // Get list of available agents with details
        function getAvailableAgentsDetailedList(uint256 start, uint256 end) external view returns (AvailableAgentInfo[] memory agents, uint256 totalLength);

        // Calculate collateral reservation fee
        function collateralReservationFee(uint256 lots) external view returns (uint256);

        // Get the FAsset token address
        function fAsset() external view returns (address);

        // Reserve collateral for minting (payable - send CRF in msg.value)
        // Returns the collateral reservation ID
        function reserveCollateral(
            address _agentVault,
            uint256 _lots,
            uint256 _maxMintingFeeBIPS,
            address _executor
        ) external payable returns (uint256 _collateralReservationId);

        // Execute minting with FDC payment proof
        function executeMinting(
            PaymentProof calldata _payment,
            uint256 _collateralReservationId
        ) external;
    }

    // Note: Full AgentInfo struct has 40 fields which exceeds alloy's tuple limit.
    // We query the underlying address via a separate AgentVault contract call instead.
}

// CollateralReserved event - defined separately for decode access
sol! {
    event CollateralReserved(
        address indexed agentVault,
        address indexed minter,
        uint64 collateralReservationId,
        uint256 valueUBA,
        uint256 feeUBA,
        uint64 firstUnderlyingBlock,
        uint64 lastUnderlyingBlock,
        uint64 lastUnderlyingTimestamp,
        bytes32 paymentAddress,
        bytes32 paymentReference,
        address executor,
        bytes32 executorFeeNatGWei
    );
}

// ERC20 interface for FXRP token balance
sol! {
    #[sol(rpc)]
    interface IERC20 {
        function balanceOf(address account) external view returns (uint256);
        function decimals() external view returns (uint8);
        function symbol() external view returns (string memory);
        function totalSupply() external view returns (uint256);
    }
}

// FAsset token interface (extends ERC20 with assetManager reference)
sol! {
    #[sol(rpc)]
    interface IFAsset {
        function assetManager() external view returns (address);
        function totalSupply() external view returns (uint256);
        function balanceOf(address account) external view returns (uint256);
    }
}

// FDC Hub interface for submitting attestation requests
sol! {
    #[sol(rpc)]
    interface IFdcHub {
        /// Submit an attestation request
        /// Returns true if request was accepted
        function requestAttestation(bytes calldata _data) external payable returns (bool);
    }
}

// FDC Request Fee Configuration interface
sol! {
    #[sol(rpc)]
    interface IFdcRequestFeeConfigurations {
        /// Get the fee for an attestation request
        function getRequestFee(bytes calldata _data) external view returns (uint256);
    }
}

// Note: IFdcVerification interface removed - we verify via DA layer proofs
// The PaymentProof struct is already defined above for use with IAssetManager

/// JSON-RPC request structure
#[derive(Debug, Deserialize)]
#[allow(dead_code)]
struct JsonRpcRequest {
    jsonrpc: String,
    method: String,
    params: Option<Value>,
    id: Value,
}

/// JSON-RPC response structure
#[derive(Debug, Serialize)]
struct JsonRpcResponse {
    jsonrpc: String,
    result: Option<Value>,
    error: Option<JsonRpcError>,
    id: Value,
}

#[derive(Debug, Serialize)]
struct JsonRpcError {
    code: i32,
    message: String,
}

/// Tool information for MCP
#[derive(Serialize)]
struct Tool {
    name: String,
    description: String,
    #[serde(rename = "inputSchema")]
    input_schema: Value,
}

impl JsonRpcResponse {
    fn success(id: Value, result: Value) -> Self {
        Self {
            jsonrpc: "2.0".to_string(),
            result: Some(result),
            error: None,
            id,
        }
    }

    fn error(id: Value, code: i32, message: &str) -> Self {
        Self {
            jsonrpc: "2.0".to_string(),
            result: None,
            error: Some(JsonRpcError {
                code,
                message: message.to_string(),
            }),
            id,
        }
    }
}

/// Get the list of available tools
fn get_tools() -> Vec<Tool> {
    vec![
        Tool {
            name: "flare_health".to_string(),
            description: "Check Flare network connection and get current block number".to_string(),
            input_schema: json!({
                "type": "object",
                "properties": {},
                "required": []
            }),
        },
        Tool {
            name: "flare_balance".to_string(),
            description: "Get FLR balance for an address".to_string(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "address": {
                        "type": "string",
                        "description": "The Flare address to check (0x...)"
                    }
                },
                "required": ["address"]
            }),
        },
        Tool {
            name: "ftso_price".to_string(),
            description: "Get current price from Flare's decentralized FTSO oracle. Supported symbols include: XRP, BTC, ETH, FLR, SGB, DOGE, ADA, ALGO, XLM, LTC, FIL, ARB, AVAX, BNB, MATIC, SOL, USDC, USDT".to_string(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "symbol": {
                        "type": "string",
                        "description": "The price symbol (e.g., 'XRP', 'BTC', 'ETH', 'FLR')"
                    }
                },
                "required": ["symbol"]
            }),
        },
        Tool {
            name: "ftso_supported_symbols".to_string(),
            description: "Get all price symbols supported by the FTSO oracle".to_string(),
            input_schema: json!({
                "type": "object",
                "properties": {},
                "required": []
            }),
        },
        Tool {
            name: "flare_agent_wallet".to_string(),
            description: "Get Chronicle's Flare agent wallet balance and info".to_string(),
            input_schema: json!({
                "type": "object",
                "properties": {},
                "required": []
            }),
        },
        Tool {
            name: "fassets_info".to_string(),
            description: "Get FAssets system info including lot size and FXRP token address".to_string(),
            input_schema: json!({
                "type": "object",
                "properties": {},
                "required": []
            }),
        },
        Tool {
            name: "fassets_agents".to_string(),
            description: "List available FAssets agents for FXRP minting with their fees and capacity".to_string(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "limit": {
                        "type": "integer",
                        "description": "Maximum number of agents to return (default: 10)"
                    }
                },
                "required": []
            }),
        },
        Tool {
            name: "fxrp_balance".to_string(),
            description: "Get FXRP token balance for an address".to_string(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "address": {
                        "type": "string",
                        "description": "The Flare address to check (0x...)"
                    }
                },
                "required": ["address"]
            }),
        },
        Tool {
            name: "fassets_reservation_fee".to_string(),
            description: "Calculate the collateral reservation fee for minting a given number of lots".to_string(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "lots": {
                        "type": "integer",
                        "description": "Number of lots to mint (1 lot = 10 XRP)"
                    }
                },
                "required": ["lots"]
            }),
        },
        Tool {
            name: "fassets_reserve_collateral".to_string(),
            description: "Reserve collateral from an agent to begin the FXRP minting process. Requires FLR for collateral reservation fee. Returns reservation ID and payment instructions.".to_string(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "agent_vault": {
                        "type": "string",
                        "description": "Agent vault address to reserve collateral from (use fassets_agents to find one)"
                    },
                    "lots": {
                        "type": "integer",
                        "description": "Number of lots to mint (1 lot = 10 XRP minimum)"
                    },
                    "xrp_address": {
                        "type": "string",
                        "description": "Your XRP address that will send the underlying XRP (e.g., rN7n3473SaZBCG4dFL83w7a1RXtXtbk2D9)"
                    }
                },
                "required": ["agent_vault", "lots", "xrp_address"]
            }),
        },
        Tool {
            name: "fdc_request_payment_proof".to_string(),
            description: "Request an FDC Payment attestation proof for an XRPL transaction. This submits the attestation request to the FdcHub and returns the voting round ID.".to_string(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "xrp_tx_hash": {
                        "type": "string",
                        "description": "The XRPL transaction hash to prove"
                    }
                },
                "required": ["xrp_tx_hash"]
            }),
        },
        Tool {
            name: "fdc_check_proof_status".to_string(),
            description: "Check if an FDC proof is ready (voting round finalized). Returns proof data if available.".to_string(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "voting_round": {
                        "type": "integer",
                        "description": "The voting round ID from the attestation request"
                    },
                    "xrp_tx_hash": {
                        "type": "string",
                        "description": "The XRPL transaction hash that was attested"
                    }
                },
                "required": ["voting_round", "xrp_tx_hash"]
            }),
        },
        Tool {
            name: "fassets_execute_minting".to_string(),
            description: "Execute FXRP minting using an FDC Payment proof. Completes the minting process after collateral reservation and XRP payment.".to_string(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "collateral_reservation_id": {
                        "type": "integer",
                        "description": "The collateral reservation ID from fassets_reserve_collateral"
                    },
                    "voting_round": {
                        "type": "integer",
                        "description": "The voting round where the attestation was finalized"
                    },
                    "xrp_tx_hash": {
                        "type": "string",
                        "description": "The XRPL payment transaction hash"
                    }
                },
                "required": ["collateral_reservation_id", "voting_round", "xrp_tx_hash"]
            }),
        },
    ]
}

/// Check Flare network health
async fn flare_health() -> Result<Value> {
    let rpc_url = FLARE_RPC.parse().context("Invalid RPC URL")?;
    let provider = ProviderBuilder::new().connect_http(rpc_url);

    let block_number = provider.get_block_number().await?;

    Ok(json!({
        "status": "connected",
        "network": "Flare Mainnet",
        "chain_id": 14,
        "block_number": block_number,
        "rpc": FLARE_RPC
    }))
}

/// Get FLR balance for an address
async fn flare_balance(address: &str) -> Result<Value> {
    let rpc_url = FLARE_RPC.parse().context("Invalid RPC URL")?;
    let provider = ProviderBuilder::new().connect_http(rpc_url);

    let addr = Address::from_str(address).context("Invalid Flare address")?;

    let balance: U256 = provider.get_balance(addr).await?;
    let balance_str = balance.to_string();
    let balance_flr = balance_str.parse::<f64>().unwrap_or(0.0) / 1e18;

    Ok(json!({
        "address": address,
        "balance_wei": balance_str,
        "balance_flr": balance_flr,
        "network": "Flare Mainnet"
    }))
}

/// Get FTSO price for a symbol
async fn ftso_price(symbol: &str) -> Result<Value> {
    let rpc_url = FLARE_RPC.parse().context("Invalid RPC URL")?;
    let provider = ProviderBuilder::new().connect_http(rpc_url);

    // First get FtsoRegistry address from the contract registry
    let registry_addr = Address::from_str(FLARE_CONTRACT_REGISTRY)?;
    let registry = IFlareContractRegistry::new(registry_addr, provider.clone());

    let ftso_registry_addr: Address = registry
        .getContractAddressByName("FtsoRegistry".to_string())
        .call()
        .await
        .context("Failed to get FtsoRegistry address")?;

    // Now query the FTSO registry for the price
    let ftso_registry = IFtsoRegistry::new(ftso_registry_addr, provider);

    let result = ftso_registry
        .getCurrentPriceWithDecimals(symbol.to_uppercase())
        .call()
        .await
        .context(format!("Failed to get price for {}", symbol))?;

    // Convert to human-readable price
    let decimals_u64: u64 = result._decimals.try_into().unwrap_or(5u64);
    let divisor = 10u64.pow(decimals_u64 as u32);
    let price_u64: u64 = result._price.try_into().unwrap_or(0u64);
    let price_float = price_u64 as f64 / divisor as f64;

    Ok(json!({
        "symbol": symbol.to_uppercase(),
        "price_usd": price_float,
        "price_raw": result._price.to_string(),
        "decimals": result._decimals.to_string(),
        "timestamp": result._timestamp.to_string(),
        "source": "Flare FTSO Oracle",
        "ftso_registry": format!("{:?}", ftso_registry_addr)
    }))
}

/// Get all supported FTSO symbols
async fn ftso_supported_symbols() -> Result<Value> {
    let rpc_url = FLARE_RPC.parse().context("Invalid RPC URL")?;
    let provider = ProviderBuilder::new().connect_http(rpc_url);

    // Get FtsoRegistry address
    let registry_addr = Address::from_str(FLARE_CONTRACT_REGISTRY)?;
    let registry = IFlareContractRegistry::new(registry_addr, provider.clone());

    let ftso_registry_addr: Address = registry
        .getContractAddressByName("FtsoRegistry".to_string())
        .call()
        .await
        .context("Failed to get FtsoRegistry address")?;

    // Get supported symbols
    let ftso_registry = IFtsoRegistry::new(ftso_registry_addr, provider);

    let symbols: Vec<String> = ftso_registry
        .getSupportedSymbols()
        .call()
        .await
        .context("Failed to get supported symbols")?;

    Ok(json!({
        "symbols": symbols,
        "count": symbols.len(),
        "source": "Flare FTSO Oracle"
    }))
}

/// Get the FXRP AssetManager address from the FXRP token
async fn get_fxrp_asset_manager() -> Result<(Address, impl Provider + Clone)> {
    let rpc_url = FLARE_RPC.parse().context("Invalid RPC URL")?;
    let provider = ProviderBuilder::new().connect_http(rpc_url);

    // Use the known FXRP token address and query its assetManager
    let fxrp_addr = Address::from_str(FXRP_TOKEN)?;
    let fxrp = IFAsset::new(fxrp_addr, provider.clone());

    let asset_manager_addr = fxrp
        .assetManager()
        .call()
        .await
        .context("Failed to get AssetManager from FXRP token")?;

    if asset_manager_addr == Address::ZERO {
        return Err(anyhow::anyhow!("AssetManager address is zero"));
    }

    eprintln!("Found AssetManager at {:?}", asset_manager_addr);
    Ok((asset_manager_addr, provider))
}

/// Get FAssets system info
async fn fassets_info() -> Result<Value> {
    let (asset_manager_addr, provider) = get_fxrp_asset_manager().await?;
    let asset_manager = IAssetManager::new(asset_manager_addr, provider.clone());

    let lot_size = asset_manager.lotSize().call().await
        .context("Failed to get lot size")?;

    let fxrp_token_addr = asset_manager.fAsset().call().await
        .context("Failed to get FXRP token address")?;

    // Convert lot size to XRP (assuming 6 decimals for XRP)
    let lot_size_xrp = lot_size.to_string().parse::<f64>().unwrap_or(0.0) / 1_000_000.0;

    // Get XRP price for context
    let xrp_price = ftso_price("XRP").await.ok();
    let lot_value_usd = xrp_price.as_ref().map(|p| {
        lot_size_xrp * p["price_usd"].as_f64().unwrap_or(0.0)
    });

    Ok(json!({
        "asset_manager": format!("{:?}", asset_manager_addr),
        "fxrp_token": format!("{:?}", fxrp_token_addr),
        "lot_size_drops": lot_size.to_string(),
        "lot_size_xrp": lot_size_xrp,
        "lot_value_usd": lot_value_usd,
        "network": "Flare Mainnet",
        "description": "1 lot = minimum minting unit"
    }))
}

/// List available FAssets agents
async fn fassets_agents(limit: u64) -> Result<Value> {
    let (asset_manager_addr, provider) = get_fxrp_asset_manager().await?;
    let asset_manager = IAssetManager::new(asset_manager_addr, provider);

    let result = asset_manager
        .getAvailableAgentsDetailedList(U256::from(0), U256::from(limit))
        .call()
        .await
        .context("Failed to get available agents")?;

    let agents: Vec<Value> = result.agents.iter().map(|agent| {
        let fee_bips: u64 = agent.feeBIPS.try_into().unwrap_or(0);
        let fee_percent = fee_bips as f64 / 100.0;
        let free_lots: u64 = agent.freeCollateralLots.try_into().unwrap_or(0);
        json!({
            "vault": format!("{:?}", agent.agentVault),
            "owner": format!("{:?}", agent.ownerManagementAddress),
            "fee_bips": fee_bips,
            "fee_percent": fee_percent,
            "free_lots": free_lots,
            "vault_ratio_bips": agent.mintingVaultCollateralRatioBIPS.to_string(),
            "pool_ratio_bips": agent.mintingPoolCollateralRatioBIPS.to_string(),
            "status": agent.status
        })
    }).collect();

    Ok(json!({
        "agents": agents,
        "total_agents": result.totalLength.to_string(),
        "returned": agents.len(),
        "asset_manager": format!("{:?}", asset_manager_addr)
    }))
}

/// Get FXRP balance for an address
async fn fxrp_balance(address: &str) -> Result<Value> {
    let rpc_url = FLARE_RPC.parse().context("Invalid RPC URL")?;
    let provider = ProviderBuilder::new().connect_http(rpc_url);

    let fxrp_token_addr = Address::from_str(FXRP_TOKEN)?;
    let fxrp_token = IERC20::new(fxrp_token_addr, provider);
    let addr = Address::from_str(address).context("Invalid address")?;

    let balance = fxrp_token.balanceOf(addr).call().await
        .context("Failed to get FXRP balance")?;

    // FXRP has 6 decimals (same as XRP)
    let balance_fxrp = balance.to_string().parse::<f64>().unwrap_or(0.0) / 1_000_000.0;

    // Get XRP price for USD value
    let xrp_price = ftso_price("XRP").await.ok();
    let usd_value = xrp_price.as_ref().map(|p| {
        balance_fxrp * p["price_usd"].as_f64().unwrap_or(0.0)
    });

    Ok(json!({
        "address": address,
        "balance_drops": balance.to_string(),
        "balance_fxrp": balance_fxrp,
        "usd_value": usd_value,
        "fxrp_token": FXRP_TOKEN,
        "network": "Flare Mainnet"
    }))
}

/// Calculate collateral reservation fee
async fn fassets_reservation_fee(lots: u64) -> Result<Value> {
    let (asset_manager_addr, provider) = get_fxrp_asset_manager().await?;
    let asset_manager = IAssetManager::new(asset_manager_addr, provider);

    let fee = asset_manager
        .collateralReservationFee(U256::from(lots))
        .call()
        .await
        .context("Failed to get reservation fee")?;

    let fee_wei = fee.to_string();
    let fee_flr = fee_wei.parse::<f64>().unwrap_or(0.0) / 1e18;

    // Get lot size for context
    let lot_size = asset_manager.lotSize().call().await.unwrap_or(U256::ZERO);
    let lot_size_xrp = lot_size.to_string().parse::<f64>().unwrap_or(0.0) / 1_000_000.0;
    let total_xrp = lot_size_xrp * lots as f64;

    // Get FLR price
    let flr_price = ftso_price("FLR").await.ok();
    let fee_usd = flr_price.as_ref().map(|p| {
        fee_flr * p["price_usd"].as_f64().unwrap_or(0.0)
    });

    Ok(json!({
        "lots": lots,
        "lot_size_xrp": lot_size_xrp,
        "total_xrp": total_xrp,
        "fee_wei": fee_wei,
        "fee_flr": fee_flr,
        "fee_usd": fee_usd,
        "network": "Flare Mainnet"
    }))
}

/// Load Chronicle Flare wallet private key from env file
fn load_flare_private_key() -> Result<String> {
    // Try environment variable first
    if let Ok(key) = std::env::var("FLARE_WALLET_PRIVATE_KEY") {
        return Ok(key);
    }

    // Try reading from config file
    let config_path = dirs::home_dir()
        .context("No home directory")?
        .join(".config/chronicle-mind.env");

    if config_path.exists() {
        let contents = std::fs::read_to_string(&config_path)?;
        for line in contents.lines() {
            if line.starts_with("FLARE_WALLET_PRIVATE_KEY=") {
                let key = line.trim_start_matches("FLARE_WALLET_PRIVATE_KEY=");
                return Ok(key.to_string());
            }
        }
    }

    Err(anyhow::anyhow!("FLARE_WALLET_PRIVATE_KEY not found in env or ~/.config/chronicle-mind.env"))
}

/// Reserve collateral from an agent to begin FXRP minting
async fn fassets_reserve_collateral(agent_vault: &str, lots: u64, xrp_address: &str) -> Result<Value> {
    // Load private key
    let private_key = load_flare_private_key()?;

    // Parse addresses
    let agent_vault_addr = Address::from_str(agent_vault)
        .context("Invalid agent vault address")?;

    // Create signer from private key
    let signer: PrivateKeySigner = private_key.parse()
        .context("Invalid private key format")?;
    let wallet_address = signer.address();
    let wallet = EthereumWallet::from(signer);

    // Create provider with signer
    let rpc_url = FLARE_RPC.parse().context("Invalid RPC URL")?;
    let provider = ProviderBuilder::new()
        .wallet(wallet)
        .connect_http(rpc_url);

    // Get AssetManager address
    let fxrp_token_addr = Address::from_str(FXRP_TOKEN)?;
    let fxrp_token = IFAsset::new(fxrp_token_addr, provider.clone());
    let asset_manager_addr = fxrp_token.assetManager().call().await
        .context("Failed to get AssetManager")?;

    let asset_manager = IAssetManager::new(asset_manager_addr, provider.clone());

    // Get the reservation fee
    let fee = asset_manager
        .collateralReservationFee(U256::from(lots))
        .call()
        .await
        .context("Failed to get reservation fee")?;

    // Add 10% buffer to fee for gas price fluctuations
    let fee_with_buffer = fee + (fee / U256::from(10));

    // Check wallet balance
    let balance: U256 = provider.get_balance(wallet_address).await?;
    let fee_flr = fee_with_buffer.to_string().parse::<f64>().unwrap_or(0.0) / 1e18;
    let balance_flr = balance.to_string().parse::<f64>().unwrap_or(0.0) / 1e18;

    if balance < fee_with_buffer {
        return Err(anyhow::anyhow!(
            "Insufficient FLR balance. Need {} FLR but have {} FLR",
            fee_flr,
            balance_flr
        ));
    }

    // Get lot size for info
    let lot_size = asset_manager.lotSize().call().await.unwrap_or(U256::ZERO);
    let lot_size_xrp = lot_size.to_string().parse::<f64>().unwrap_or(0.0) / 1_000_000.0;
    let total_xrp = lot_size_xrp * lots as f64;

    // Get current agent fee from available agents list
    let agents_result = asset_manager
        .getAvailableAgentsDetailedList(U256::ZERO, U256::from(20))
        .call()
        .await
        .context("Failed to get agents")?;

    // Find this agent to get their fee
    let agent_fee_bips = agents_result.agents.iter()
        .find(|a| a.agentVault == agent_vault_addr)
        .map(|a| a.feeBIPS)
        .unwrap_or(U256::from(100)); // Default 1% if not found

    // Set max fee to agent fee + 10% buffer
    let max_fee_bips = agent_fee_bips + (agent_fee_bips / U256::from(10));

    // Call reserveCollateral - this is the critical transaction
    eprintln!("Reserving collateral: {} lots from agent {}", lots, agent_vault);
    eprintln!("  Fee: {} FLR (with buffer)", fee_flr);
    eprintln!("  XRP to send after reservation: {} XRP", total_xrp);

    let tx = asset_manager
        .reserveCollateral(
            agent_vault_addr,
            U256::from(lots),
            max_fee_bips,
            Address::ZERO, // No executor (we'll execute ourselves)
        )
        .value(fee_with_buffer);

    let pending_tx = match tx.send().await {
        Ok(tx) => tx,
        Err(e) => {
            eprintln!("Transaction send error: {:?}", e);
            return Err(anyhow::anyhow!("Failed to send reserveCollateral transaction: {}", e));
        }
    };

    eprintln!("Transaction sent: {:?}", pending_tx.tx_hash());

    // Wait for transaction receipt
    let receipt = pending_tx.get_receipt().await
        .context("Failed to get transaction receipt")?;

    // Check if transaction succeeded
    let success = receipt.status();

    if !success {
        return Err(anyhow::anyhow!("Transaction reverted - reservation failed"));
    }

    let tx_hash = format!("{:?}", receipt.transaction_hash);

    // Parse CollateralReserved event from logs
    let mut reservation_id: Option<u64> = None;
    let mut payment_address_hex: Option<String> = None;
    let mut payment_reference_hex: Option<String> = None;
    let mut last_block: Option<u64> = None;
    let mut last_timestamp: Option<u64> = None;
    let mut value_uba: Option<String> = None;
    let mut fee_uba: Option<String> = None;

    for log in receipt.inner.logs() {
        // Try to decode as CollateralReserved event
        if let Ok(decoded) = CollateralReserved::decode_log(&log.inner) {
            let event = &decoded.data;
            reservation_id = Some(event.collateralReservationId);
            payment_address_hex = Some(format!("0x{}", hex::encode(event.paymentAddress.as_slice())));
            payment_reference_hex = Some(format!("0x{}", hex::encode(event.paymentReference.as_slice())));
            last_block = Some(event.lastUnderlyingBlock);
            last_timestamp = Some(event.lastUnderlyingTimestamp);
            value_uba = Some(event.valueUBA.to_string());
            fee_uba = Some(event.feeUBA.to_string());
            eprintln!("Parsed CollateralReserved event:");
            eprintln!("  Reservation ID: {}", event.collateralReservationId);
            eprintln!("  Payment Address: 0x{}", hex::encode(event.paymentAddress.as_slice()));
            eprintln!("  Payment Reference: 0x{}", hex::encode(event.paymentReference.as_slice()));
            break;
        }
    }

    // Decode payment address from bytes32 to XRPL address if possible
    let xrpl_destination = payment_address_hex.as_ref().map(|hex_addr| {
        // The bytes32 contains the raw XRPL address bytes (20 bytes + padding)
        // We'll return the hex for now - user can decode or we can add bs58 later
        decode_xrpl_address_from_bytes32(hex_addr)
    }).flatten();

    Ok(json!({
        "status": "reserved",
        "transaction_hash": tx_hash,
        "reservation_id": reservation_id,
        "lots": lots,
        "lot_size_xrp": lot_size_xrp,
        "total_xrp_to_send": total_xrp,
        "fee_flr_paid": fee_flr,
        "agent_vault": agent_vault,
        "minter_xrp_address": xrp_address,
        "payment_info": {
            "xrpl_destination": xrpl_destination,
            "xrpl_destination_hex": payment_address_hex,
            "payment_reference": payment_reference_hex,
            "value_drops": value_uba,
            "fee_drops": fee_uba,
            "deadline_block": last_block,
            "deadline_timestamp": last_timestamp
        },
        "next_steps": [
            format!("1. Send {} XRP to XRPL address with memo", total_xrp),
            "2. Use payment_reference as the memo (hex encoded)",
            "3. Wait for XRP transaction to confirm",
            "4. Request FDC proof of payment",
            "5. Call executeMinting with the proof to receive FXRP"
        ],
        "flarescan_url": format!("https://mainnet.flarescan.com/tx/{}", tx_hash),
        "network": "Flare Mainnet"
    }))
}

/// Get Chronicle's agent wallet info
async fn flare_agent_wallet() -> Result<Value> {
    let balance_info = flare_balance(CHRONICLE_FLARE_WALLET).await?;

    // Also get FLR price
    let flr_price = ftso_price("FLR").await.ok();

    let balance_flr = balance_info["balance_flr"].as_f64().unwrap_or(0.0);
    let usd_value = if let Some(ref price) = flr_price {
        let flr_usd = price["price_usd"].as_f64().unwrap_or(0.0);
        Some(balance_flr * flr_usd)
    } else {
        None
    };

    Ok(json!({
        "wallet_type": "Chronicle Flare Agent",
        "address": CHRONICLE_FLARE_WALLET,
        "balance_flr": balance_flr,
        "balance_wei": balance_info["balance_wei"],
        "flr_price_usd": flr_price.as_ref().and_then(|p| p["price_usd"].as_f64()),
        "usd_value": usd_value,
        "network": "Flare Mainnet"
    }))
}

/// Fetch XRPL transaction data
async fn fetch_xrpl_transaction(tx_hash: &str) -> Result<Value> {
    let client = reqwest::Client::new();
    let response = client
        .post(XRPL_API)
        .json(&json!({
            "method": "tx",
            "params": [{
                "transaction": tx_hash
            }]
        }))
        .send()
        .await
        .context("Failed to fetch XRPL transaction")?;

    let data: Value = response.json().await.context("Failed to parse XRPL response")?;

    if let Some(result) = data.get("result") {
        if result.get("validated").and_then(|v| v.as_bool()).unwrap_or(false) {
            return Ok(result.clone());
        } else {
            return Err(anyhow::anyhow!("Transaction not validated"));
        }
    }

    Err(anyhow::anyhow!("Invalid XRPL response"))
}

/// Calculate the Message Integrity Code (MIC) for a Payment attestation
/// MIC = keccak256(abi.encode(response) + "Flare")
fn calculate_payment_mic(
    tx_hash: &str,
    block_number: u64,
    block_timestamp: u64,
    source_addr_hash: [u8; 32],
    receiving_addr_hash: [u8; 32],
    spent_amount: i128,
    received_amount: i128,
    payment_reference: [u8; 32],
    status: u8,
) -> [u8; 32] {
    use alloy::primitives::keccak256;

    // Build the response data structure for hashing
    // attestationType: "Payment" as bytes32 (left-aligned, zero-padded right)
    let mut attestation_type = [0u8; 32];
    let payment_bytes = b"Payment";
    attestation_type[..payment_bytes.len()].copy_from_slice(payment_bytes);

    // sourceId: "XRP" as bytes32 (must be uppercase!)
    let mut source_id = [0u8; 32];
    let xrp_bytes = b"XRP";
    source_id[..xrp_bytes.len()].copy_from_slice(xrp_bytes);

    // transactionId: tx_hash as bytes32
    let mut tx_id = [0u8; 32];
    if let Ok(decoded) = hex::decode(tx_hash) {
        let len = decoded.len().min(32);
        tx_id[..len].copy_from_slice(&decoded[..len]);
    }

    // Build the full response for hashing
    // The MIC is calculated as: keccak256(abi.encode(response) ++ "Flare")
    // For simplicity, we concatenate all fields and hash with Flare salt
    let mut data = Vec::new();
    data.extend_from_slice(&attestation_type);
    data.extend_from_slice(&source_id);
    data.extend_from_slice(&tx_id);
    data.extend_from_slice(&[0u8; 32]); // inUtxo = 0
    data.extend_from_slice(&[0u8; 32]); // utxo = 0
    data.extend_from_slice(&block_number.to_be_bytes());
    data.extend_from_slice(&block_timestamp.to_be_bytes());
    data.extend_from_slice(&source_addr_hash);
    data.extend_from_slice(&receiving_addr_hash);
    data.extend_from_slice(&spent_amount.to_be_bytes());
    data.extend_from_slice(&received_amount.to_be_bytes());
    data.extend_from_slice(&payment_reference);
    data.push(if spent_amount == received_amount { 1 } else { 0 }); // oneToOne
    data.push(status);
    data.extend_from_slice(b"Flare");

    // Use proper keccak256 as required by FDC
    let hash = keccak256(&data);
    let mut result = [0u8; 32];
    result.copy_from_slice(hash.as_slice());
    result
}

/// Hash an XRPL address to bytes32 (standardized format)
fn hash_xrpl_address(address: &str) -> [u8; 32] {
    use sha2::{Sha256, Digest};
    let hash = Sha256::digest(address.as_bytes());
    let mut result = [0u8; 32];
    result.copy_from_slice(&hash);
    result
}

/// Request an FDC Payment attestation proof
async fn fdc_request_payment_proof(xrp_tx_hash: &str) -> Result<Value> {
    // Step 1: Use the FDC Verifier API to prepare the request with correct MIC
    // The verifier calculates the MIC correctly based on the expected response
    let client = reqwest::Client::new();

    // Build request for verifier prepareRequest endpoint
    let verifier_request = json!({
        "attestationType": "0x5061796d656e7400000000000000000000000000000000000000000000000000",
        "sourceId": "0x5852500000000000000000000000000000000000000000000000000000000000",
        "requestBody": {
            "transactionId": format!("0x{}", xrp_tx_hash),
            "inUtxo": "0",
            "utxo": "0"
        }
    });

    eprintln!("Calling FDC verifier prepareRequest...");

    let verifier_response = client
        .post(format!("{}/verifier/xrp/Payment/prepareRequest", FDC_VERIFIER_URL))
        .header("Content-Type", "application/json")
        .header("X-API-KEY", FDC_VERIFIER_API_KEY)
        .json(&verifier_request)
        .send()
        .await
        .context("Failed to call verifier")?;

    let verifier_result: serde_json::Value = verifier_response
        .json()
        .await
        .context("Failed to parse verifier response")?;

    let status = verifier_result["status"].as_str().unwrap_or("UNKNOWN");
    if status != "VALID" {
        return Err(anyhow::anyhow!(
            "Transaction not valid for attestation: {}",
            verifier_result
        ));
    }

    let abi_encoded_request = verifier_result["abiEncodedRequest"]
        .as_str()
        .context("Missing abiEncodedRequest in verifier response")?;

    eprintln!("Verifier returned VALID request with MIC");

    // Decode the hex request data
    let request_data = hex::decode(abi_encoded_request.trim_start_matches("0x"))
        .context("Failed to decode abiEncodedRequest")?;

    // Extract MIC from the request (bytes 64-96)
    let mic = if request_data.len() >= 96 {
        &request_data[64..96]
    } else {
        return Err(anyhow::anyhow!("Request data too short"));
    };

    eprintln!("MIC: 0x{}", hex::encode(mic));

    // Also fetch transaction details for the response
    let tx_data = fetch_xrpl_transaction(xrp_tx_hash).await
        .context("Failed to fetch XRPL transaction")?;

    let ledger_index = tx_data.get("ledger_index")
        .and_then(|v| v.as_u64())
        .unwrap_or(0);

    let source_address = tx_data.get("Account")
        .and_then(|v| v.as_str())
        .unwrap_or("");

    let destination = tx_data.get("Destination")
        .and_then(|v| v.as_str())
        .unwrap_or("");

    let amount = tx_data.get("meta")
        .and_then(|m| m.get("delivered_amount"))
        .and_then(|a| a.as_str())
        .and_then(|s| s.parse::<i128>().ok())
        .unwrap_or(0);

    eprintln!("Built attestation request: {} bytes", request_data.len());

    // Step 3: Get the fee and submit to FdcHub
    let rpc_url = FLARE_RPC.parse().context("Invalid RPC URL")?;

    // Load wallet
    let private_key = load_flare_private_key()?;
    let signer: PrivateKeySigner = private_key.parse().context("Invalid private key")?;
    let wallet = EthereumWallet::from(signer);

    let provider = ProviderBuilder::new()
        .wallet(wallet)
        .connect_http(rpc_url);

    // Get fee
    let fee_config_addr = Address::from_str(FDC_REQUEST_FEE_CONFIG)?;
    let fee_config = IFdcRequestFeeConfigurations::new(fee_config_addr, provider.clone());

    let fee = fee_config
        .getRequestFee(request_data.clone().into())
        .call()
        .await
        .unwrap_or(U256::from(100000000000000000u64)); // Default 0.1 FLR if fee query fails

    eprintln!("Attestation fee: {} wei", fee);

    // Submit to FdcHub
    let fdc_hub_addr = Address::from_str(FDC_HUB)?;
    let fdc_hub = IFdcHub::new(fdc_hub_addr, provider.clone());

    let tx_builder = fdc_hub
        .requestAttestation(request_data.clone().into())
        .value(fee);

    let pending_tx = tx_builder
        .send()
        .await
        .context("Failed to send attestation request")?;

    let receipt = pending_tx
        .get_receipt()
        .await
        .context("Failed to get transaction receipt")?;

    let tx_hash = format!("{:?}", receipt.transaction_hash);

    // Get current voting round from FSP status
    let client = reqwest::Client::new();
    let voting_round = if let Ok(resp) = client
        .get(format!("{}/api/v0/fsp/status", DA_LAYER_URL))
        .send()
        .await
    {
        if let Ok(status) = resp.json::<serde_json::Value>().await {
            status["active"]["voting_round_id"].as_u64().unwrap_or(0)
        } else {
            0
        }
    } else {
        0
    };

    Ok(json!({
        "status": "submitted",
        "xrp_tx_hash": xrp_tx_hash,
        "flare_tx_hash": tx_hash,
        "fee_paid_wei": fee.to_string(),
        "voting_round_estimate": voting_round,
        "mic": format!("0x{}", hex::encode(&mic)),
        "request_size_bytes": request_data.len(),
        "transaction_details": {
            "source": source_address,
            "destination": destination,
            "amount_drops": amount,
            "ledger_index": ledger_index
        },
        "next_steps": [
            "1. Wait for voting round to finalize (~90 seconds)",
            "2. Call fdc_check_proof_status with the voting round",
            "3. Use the proof to execute minting"
        ]
    }))
}

/// DA Layer base URL for Flare mainnet
const DA_LAYER_URL: &str = "https://flr-data-availability.flare.network";

/// Check if an FDC proof is ready and retrieve it
async fn fdc_check_proof_status(voting_round: u64, xrp_tx_hash: &str) -> Result<Value> {
    // Get FSP status to find current voting round
    let client = reqwest::Client::new();
    let fsp_status: serde_json::Value = client
        .get(format!("{}/api/v0/fsp/status", DA_LAYER_URL))
        .send()
        .await?
        .json()
        .await?;

    let current_round = fsp_status["active"]["voting_round_id"]
        .as_u64()
        .unwrap_or(0);
    let latest_fdc_round = fsp_status["latest_fdc"]["voting_round_id"]
        .as_u64()
        .unwrap_or(0);

    let finalized = voting_round <= latest_fdc_round;
    let rounds_until_finalized = if voting_round > latest_fdc_round {
        voting_round - latest_fdc_round
    } else {
        0
    };

    // Build request bytes for DA layer query
    let mut request_bytes = Vec::new();

    // attestationType: "Payment" padded to 32 bytes
    let mut attestation_type = [0u8; 32];
    attestation_type[..7].copy_from_slice(b"Payment");
    request_bytes.extend_from_slice(&attestation_type);

    // sourceId: "XRP" padded to 32 bytes
    let mut source_id = [0u8; 32];
    source_id[..3].copy_from_slice(b"XRP");
    request_bytes.extend_from_slice(&source_id);

    // MIC placeholder (32 bytes of zeros - we're searching, not submitting)
    request_bytes.extend_from_slice(&[0u8; 32]);

    // transactionId
    let mut tx_id = [0u8; 32];
    if let Ok(decoded) = hex::decode(xrp_tx_hash) {
        let len = decoded.len().min(32);
        tx_id[..len].copy_from_slice(&decoded[..len]);
    }
    request_bytes.extend_from_slice(&tx_id);

    // inUtxo and utxo (0 for XRP)
    request_bytes.extend_from_slice(&[0u8; 32]);
    request_bytes.extend_from_slice(&[0u8; 32]);

    let request_hex = format!("0x{}", hex::encode(&request_bytes));

    // If finalized, try to get attestations for that round
    let mut proof_data = None;
    if finalized {
        // Query all attestations for the round and look for matching tx hash
        let attestations_url = format!(
            "{}/api/v1/fdc?voting_round_id={}",
            DA_LAYER_URL, voting_round
        );

        if let Ok(resp) = client.get(&attestations_url).send().await {
            if let Ok(attestations) = resp.json::<Vec<serde_json::Value>>().await {
                // Look for Payment attestation matching our tx hash
                let tx_hash_lower = xrp_tx_hash.to_lowercase();
                for att in &attestations {
                    if let Some(resp) = att.get("response") {
                        let att_type = resp["attestationType"].as_str().unwrap_or("");
                        // Check if it's a Payment attestation (0x5061796d656e74...)
                        if att_type.contains("5061796d656e74") {
                            if let Some(req_body) = resp.get("requestBody") {
                                let tx_id = req_body["transactionId"]
                                    .as_str()
                                    .unwrap_or("")
                                    .to_lowercase();
                                if tx_id.contains(&tx_hash_lower) || tx_hash_lower.contains(&tx_id.trim_start_matches("0x")) {
                                    proof_data = Some(att.clone());
                                    break;
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    if let Some(proof) = proof_data {
        Ok(json!({
            "status": "proof_available",
            "voting_round": voting_round,
            "current_round": current_round,
            "xrp_tx_hash": xrp_tx_hash,
            "proof": proof,
            "next_steps": [
                "Use this proof with fassets_execute_minting to complete the FXRP mint"
            ]
        }))
    } else if finalized {
        Ok(json!({
            "status": "not_found",
            "voting_round": voting_round,
            "current_round": current_round,
            "latest_fdc_round": latest_fdc_round,
            "xrp_tx_hash": xrp_tx_hash,
            "message": "Voting round is finalized but attestation not found. This may mean the MIC calculation was incorrect or the attestation was rejected.",
            "manual_fallback": "Use fassets.au.cc web interface to complete minting"
        }))
    } else {
        Ok(json!({
            "status": "pending",
            "voting_round": voting_round,
            "current_round": current_round,
            "latest_fdc_round": latest_fdc_round,
            "rounds_until_finalized": rounds_until_finalized,
            "estimated_wait_seconds": rounds_until_finalized * 90,
            "xrp_tx_hash": xrp_tx_hash,
            "message": format!("Waiting for voting round {} to finalize. Current: {}", voting_round, latest_fdc_round)
        }))
    }
}

/// Execute FXRP minting with FDC payment proof
async fn fassets_execute_minting(
    reservation_id: u64,
    voting_round: u64,
    xrp_tx_hash: &str,
) -> Result<Value> {
    // First, get the proof from the DA layer
    let client = reqwest::Client::new();
    let attestations_url = format!(
        "{}/api/v1/fdc?voting_round_id={}",
        DA_LAYER_URL, voting_round
    );

    let attestations: Vec<serde_json::Value> = client
        .get(&attestations_url)
        .send()
        .await?
        .json()
        .await?;

    // Find our payment attestation
    let tx_hash_lower = xrp_tx_hash.to_lowercase();
    let mut proof_data: Option<serde_json::Value> = None;

    for att in &attestations {
        if let Some(resp) = att.get("response") {
            let att_type = resp["attestationType"].as_str().unwrap_or("");
            if att_type.contains("5061796d656e74") {
                if let Some(req_body) = resp.get("requestBody") {
                    let tx_id = req_body["transactionId"]
                        .as_str()
                        .unwrap_or("")
                        .to_lowercase();
                    if tx_id.contains(&tx_hash_lower) || tx_hash_lower.contains(&tx_id.trim_start_matches("0x")) {
                        proof_data = Some(att.clone());
                        break;
                    }
                }
            }
        }
    }

    let proof = proof_data.context("Payment proof not found in DA layer")?;

    // Extract proof components
    let merkle_proof: Vec<String> = proof["proof"]
        .as_array()
        .context("Missing merkle proof")?
        .iter()
        .filter_map(|v| v.as_str().map(|s| s.to_string()))
        .collect();

    let response = &proof["response"];
    let response_body = &response["responseBody"];
    let request_body = &response["requestBody"];

    eprintln!("Building Payment proof for executeMinting...");
    eprintln!("Merkle proof: {:?}", merkle_proof);

    // Load wallet and connect to Flare
    let rpc_url = FLARE_RPC.parse().context("Invalid RPC URL")?;
    let private_key = load_flare_private_key()?;
    let signer: PrivateKeySigner = private_key.parse().context("Invalid private key")?;
    let wallet_address = signer.address();
    let wallet = EthereumWallet::from(signer);

    let provider = ProviderBuilder::new()
        .wallet(wallet)
        .connect_http(rpc_url);

    // Get AssetManager address
    let (asset_manager_addr, _) = get_fxrp_asset_manager().await?;
    // Use IAssetManager interface for executeMinting function
    let asset_manager = IAssetManager::new(asset_manager_addr, provider.clone());

    // Build the proof struct
    // Convert merkle proof strings to FixedBytes<32>
    let merkle_bytes: Vec<alloy::primitives::FixedBytes<32>> = merkle_proof
        .iter()
        .filter_map(|s| {
            let hex_str = s.trim_start_matches("0x");
            hex::decode(hex_str).ok().and_then(|bytes| {
                if bytes.len() == 32 {
                    let mut arr = [0u8; 32];
                    arr.copy_from_slice(&bytes);
                    Some(alloy::primitives::FixedBytes(arr))
                } else {
                    None
                }
            })
        })
        .collect();

    // Parse response fields
    let attestation_type = {
        let hex = response["attestationType"].as_str().unwrap_or("").trim_start_matches("0x");
        let mut bytes = [0u8; 32];
        if let Ok(decoded) = hex::decode(hex) {
            let len = decoded.len().min(32);
            bytes[..len].copy_from_slice(&decoded[..len]);
        }
        alloy::primitives::FixedBytes(bytes)
    };

    let source_id = {
        let hex = response["sourceId"].as_str().unwrap_or("").trim_start_matches("0x");
        let mut bytes = [0u8; 32];
        if let Ok(decoded) = hex::decode(hex) {
            let len = decoded.len().min(32);
            bytes[..len].copy_from_slice(&decoded[..len]);
        }
        alloy::primitives::FixedBytes(bytes)
    };

    let tx_id = {
        let hex = request_body["transactionId"].as_str().unwrap_or("").trim_start_matches("0x");
        let mut bytes = [0u8; 32];
        if let Ok(decoded) = hex::decode(hex) {
            let len = decoded.len().min(32);
            bytes[..len].copy_from_slice(&decoded[..len]);
        }
        alloy::primitives::FixedBytes(bytes)
    };

    let parse_bytes32 = |v: &serde_json::Value| -> alloy::primitives::FixedBytes<32> {
        let hex = v.as_str().unwrap_or("").trim_start_matches("0x");
        let mut bytes = [0u8; 32];
        if let Ok(decoded) = hex::decode(hex) {
            let len = decoded.len().min(32);
            bytes[..len].copy_from_slice(&decoded[..len]);
        }
        alloy::primitives::FixedBytes(bytes)
    };

    // Use Signed type for int256 values
    use alloy::primitives::I256;

    let parse_i256 = |v: &serde_json::Value| -> I256 {
        // Try string first (for large numbers), then i64
        if let Some(s) = v.as_str() {
            I256::from_dec_str(s).unwrap_or(I256::ZERO)
        } else {
            I256::try_from(v.as_i64().unwrap_or(0)).unwrap_or(I256::ZERO)
        }
    };

    let payment_proof = PaymentProof {
        merkleProof: merkle_bytes,
        data: PaymentResponse {
            attestationType: attestation_type,
            sourceId: source_id,
            votingRound: response["votingRound"].as_u64().unwrap_or(0),
            lowestUsedTimestamp: response["lowestUsedTimestamp"].as_u64().unwrap_or(0),
            requestBody: PaymentRequestBody {
                transactionId: tx_id,
                inUtxo: U256::from(request_body["inUtxo"].as_u64().unwrap_or(0)),
                utxo: U256::from(request_body["utxo"].as_u64().unwrap_or(0)),
            },
            responseBody: PaymentResponseBody {
                blockNumber: response_body["blockNumber"].as_u64().unwrap_or(0),
                blockTimestamp: response_body["blockTimestamp"].as_u64().unwrap_or(0),
                sourceAddressHash: parse_bytes32(&response_body["sourceAddressHash"]),
                sourceAddressesRoot: parse_bytes32(&response_body["sourceAddressesRoot"]),
                receivingAddressHash: parse_bytes32(&response_body["receivingAddressHash"]),
                intendedReceivingAddressHash: parse_bytes32(&response_body["intendedReceivingAddressHash"]),
                spentAmount: parse_i256(&response_body["spentAmount"]),
                intendedSpentAmount: parse_i256(&response_body["intendedSpentAmount"]),
                receivedAmount: parse_i256(&response_body["receivedAmount"]),
                intendedReceivedAmount: parse_i256(&response_body["intendedReceivedAmount"]),
                standardPaymentReference: parse_bytes32(&response_body["standardPaymentReference"]),
                oneToOne: response_body["oneToOne"].as_bool().unwrap_or(false),
                status: response_body["status"].as_u64().unwrap_or(0) as u8,
            },
        },
    };

    eprintln!("Calling executeMinting on AssetManager...");
    eprintln!("Reservation ID: {}", reservation_id);

    // Call executeMinting via IAssetManager
    let tx_builder = asset_manager
        .executeMinting(payment_proof, U256::from(reservation_id));

    let pending_tx = match tx_builder.send().await {
        Ok(tx) => tx,
        Err(e) => {
            eprintln!("executeMinting transaction error: {:?}", e);
            return Err(anyhow::anyhow!("Failed to send executeMinting transaction: {}", e));
        }
    };

    let receipt = pending_tx
        .get_receipt()
        .await
        .context("Failed to get transaction receipt")?;

    let tx_hash = format!("{:?}", receipt.transaction_hash);
    let success = receipt.status();

    // Check FXRP balance after minting
    let fxrp_balance = fxrp_balance(&format!("{:?}", wallet_address)).await.ok();

    if success {
        Ok(json!({
            "status": "success",
            "message": "FXRP minting completed successfully!",
            "flare_tx_hash": tx_hash,
            "reservation_id": reservation_id,
            "xrp_tx_hash": xrp_tx_hash,
            "fxrp_balance": fxrp_balance,
            "explorer_url": format!("https://flare-explorer.flare.network/tx/{}", tx_hash)
        }))
    } else {
        Ok(json!({
            "status": "failed",
            "message": "executeMinting transaction failed",
            "flare_tx_hash": tx_hash,
            "reservation_id": reservation_id
        }))
    }
}

/// Handle a tool call
async fn handle_tool_call(name: &str, args: &Value) -> Result<Value> {
    match name {
        "flare_health" => flare_health().await,
        "flare_balance" => {
            let address = args["address"]
                .as_str()
                .context("Missing address parameter")?;
            flare_balance(address).await
        }
        "ftso_price" => {
            let symbol = args["symbol"]
                .as_str()
                .context("Missing symbol parameter")?;
            ftso_price(symbol).await
        }
        "ftso_supported_symbols" => ftso_supported_symbols().await,
        "flare_agent_wallet" => flare_agent_wallet().await,
        "fassets_info" => fassets_info().await,
        "fassets_agents" => {
            let limit = args["limit"].as_u64().unwrap_or(10);
            fassets_agents(limit).await
        }
        "fxrp_balance" => {
            let address = args["address"]
                .as_str()
                .context("Missing address parameter")?;
            fxrp_balance(address).await
        }
        "fassets_reservation_fee" => {
            let lots = args["lots"]
                .as_u64()
                .context("Missing lots parameter")?;
            fassets_reservation_fee(lots).await
        }
        "fassets_reserve_collateral" => {
            let agent_vault = args["agent_vault"]
                .as_str()
                .context("Missing agent_vault parameter")?;
            let lots = args["lots"]
                .as_u64()
                .context("Missing lots parameter")?;
            let xrp_address = args["xrp_address"]
                .as_str()
                .context("Missing xrp_address parameter")?;
            fassets_reserve_collateral(agent_vault, lots, xrp_address).await
        }
        "fdc_request_payment_proof" => {
            let xrp_tx_hash = args["xrp_tx_hash"]
                .as_str()
                .context("Missing xrp_tx_hash parameter")?;
            fdc_request_payment_proof(xrp_tx_hash).await
        }
        "fdc_check_proof_status" => {
            let voting_round = args["voting_round"]
                .as_u64()
                .context("Missing voting_round parameter")?;
            let xrp_tx_hash = args["xrp_tx_hash"]
                .as_str()
                .context("Missing xrp_tx_hash parameter")?;
            fdc_check_proof_status(voting_round, xrp_tx_hash).await
        }
        "fassets_execute_minting" => {
            let reservation_id = args["collateral_reservation_id"]
                .as_u64()
                .context("Missing collateral_reservation_id parameter")?;
            let voting_round = args["voting_round"]
                .as_u64()
                .context("Missing voting_round parameter")?;
            let xrp_tx_hash = args["xrp_tx_hash"]
                .as_str()
                .context("Missing xrp_tx_hash parameter")?;
            fassets_execute_minting(reservation_id, voting_round, xrp_tx_hash).await
        }
        _ => Err(anyhow::anyhow!("Unknown tool: {}", name)),
    }
}

/// Handle a JSON-RPC request
async fn handle_request(request: JsonRpcRequest) -> JsonRpcResponse {
    match request.method.as_str() {
        "initialize" => JsonRpcResponse::success(
            request.id,
            json!({
                "protocolVersion": "2024-11-05",
                "capabilities": {
                    "tools": {}
                },
                "serverInfo": {
                    "name": "chronicle-flare",
                    "version": "0.1.0"
                }
            }),
        ),
        "notifications/initialized" => JsonRpcResponse::success(request.id, json!({})),
        "tools/list" => JsonRpcResponse::success(
            request.id,
            json!({
                "tools": get_tools()
            }),
        ),
        "tools/call" => {
            let params = request.params.unwrap_or(json!({}));
            let tool_name = params["name"].as_str().unwrap_or("");
            let arguments = params["arguments"].clone();

            match handle_tool_call(tool_name, &arguments).await {
                Ok(result) => JsonRpcResponse::success(
                    request.id,
                    json!({
                        "content": [{
                            "type": "text",
                            "text": serde_json::to_string_pretty(&result).unwrap_or_default()
                        }]
                    }),
                ),
                Err(e) => JsonRpcResponse::success(
                    request.id,
                    json!({
                        "content": [{
                            "type": "text",
                            "text": format!("Error: {}", e)
                        }],
                        "isError": true
                    }),
                ),
            }
        }
        "ping" => JsonRpcResponse::success(request.id, json!({})),
        _ => JsonRpcResponse::error(request.id, -32601, &format!("Method not found: {}", request.method)),
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    eprintln!("Chronicle Flare MCP Server starting...");
    eprintln!("Providing access to Flare Network and FTSO Oracle");

    let stdin = std::io::stdin();
    let reader = BufReader::new(stdin.lock());
    let mut stdout = std::io::stdout();

    for line in reader.lines() {
        let line = match line {
            Ok(l) => l,
            Err(e) => {
                eprintln!("Error reading line: {}", e);
                continue;
            }
        };

        if line.trim().is_empty() {
            continue;
        }

        let request: JsonRpcRequest = match serde_json::from_str(&line) {
            Ok(r) => r,
            Err(e) => {
                eprintln!("Error parsing request: {} - Line: {}", e, line);
                continue;
            }
        };

        let response = handle_request(request).await;
        let response_str = serde_json::to_string(&response)?;

        writeln!(stdout, "{}", response_str)?;
        stdout.flush()?;
    }

    Ok(())
}
