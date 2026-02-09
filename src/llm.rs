use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

/// LLM client abstraction supporting multiple backends
pub trait LlmClient: Send + Sync {
    /// Generate a completion from a prompt
    fn complete(&self, prompt: &str) -> Result<String>;
}

/// Claude API client
#[derive(Debug)]
pub struct ClaudeClient {
    api_key: String,
    model: String,
    client: reqwest::blocking::Client,
}

#[derive(Debug, Serialize)]
struct ClaudeRequest {
    model: String,
    max_tokens: u32,
    messages: Vec<ClaudeMessage>,
}

#[derive(Debug, Serialize)]
struct ClaudeMessage {
    role: String,
    content: String,
}

#[derive(Debug, Deserialize)]
struct ClaudeResponse {
    content: Vec<ClaudeContent>,
}

#[derive(Debug, Deserialize)]
struct ClaudeContent {
    text: String,
}

impl ClaudeClient {
    /// Create a new Claude API client
    pub fn new(api_key: String, model: String) -> Result<Self> {
        let client = reqwest::blocking::Client::builder()
            .timeout(std::time::Duration::from_secs(120))
            .build()
            .context("Failed to create HTTP client")?;

        Ok(Self {
            api_key,
            model,
            client,
        })
    }

    /// Create a client from environment variable
    pub fn from_env(model: String) -> Result<Self> {
        let api_key = std::env::var("ANTHROPIC_API_KEY")
            .context("ANTHROPIC_API_KEY environment variable not set")?;
        Self::new(api_key, model)
    }
}

impl LlmClient for ClaudeClient {
    fn complete(&self, prompt: &str) -> Result<String> {
        let request = ClaudeRequest {
            model: self.model.clone(),
            max_tokens: 4096,
            messages: vec![ClaudeMessage {
                role: "user".to_string(),
                content: prompt.to_string(),
            }],
        };

        let response = self
            .client
            .post("https://api.anthropic.com/v1/messages")
            .header("x-api-key", &self.api_key)
            .header("anthropic-version", "2023-06-01")
            .header("content-type", "application/json")
            .json(&request)
            .send()
            .context("Failed to send request to Claude API")?;

        if !response.status().is_success() {
            let status = response.status();
            let error_text = response.text().unwrap_or_default();
            anyhow::bail!(
                "Claude API request failed with status {}: {}",
                status,
                error_text
            );
        }

        let claude_response: ClaudeResponse = response
            .json()
            .context("Failed to parse Claude API response")?;

        claude_response
            .content
            .first()
            .map(|c| c.text.clone())
            .ok_or_else(|| anyhow::anyhow!("No content in Claude response"))
    }
}

/// Ollama client for local LLM inference (e.g., Qwen on Jetson)
/// This enables sovereignty - the Mind can think even without Anthropic's API
#[derive(Debug)]
pub struct OllamaClient {
    base_url: String,
    model: String,
    client: reqwest::blocking::Client,
}

#[derive(Debug, Serialize)]
struct OllamaRequest {
    model: String,
    prompt: String,
    stream: bool,
}

#[derive(Debug, Deserialize)]
struct OllamaResponse {
    response: String,
}

impl OllamaClient {
    /// Create a new Ollama client
    pub fn new(base_url: String, model: String) -> Result<Self> {
        let client = reqwest::blocking::Client::builder()
            .timeout(std::time::Duration::from_secs(300)) // Local inference can be slow
            .build()
            .context("Failed to create HTTP client")?;

        Ok(Self {
            base_url,
            model,
            client,
        })
    }

    /// Create from environment variables
    /// CHRONICLE_OLLAMA_URL (default: http://192.168.1.11:11434)
    /// CHRONICLE_LOCAL_MODEL (default: qwen2.5:3b)
    pub fn from_env() -> Result<Self> {
        let base_url = std::env::var("CHRONICLE_OLLAMA_URL")
            .unwrap_or_else(|_| "http://192.168.1.11:11434".to_string());
        let model = std::env::var("CHRONICLE_LOCAL_MODEL")
            .unwrap_or_else(|_| "qwen2.5:3b".to_string());
        Self::new(base_url, model)
    }

    /// Check if Ollama is available
    pub fn is_available(&self) -> bool {
        self.client
            .get(format!("{}/api/tags", self.base_url))
            .timeout(std::time::Duration::from_secs(5))
            .send()
            .map(|r| r.status().is_success())
            .unwrap_or(false)
    }
}

impl LlmClient for OllamaClient {
    fn complete(&self, prompt: &str) -> Result<String> {
        let request = OllamaRequest {
            model: self.model.clone(),
            prompt: prompt.to_string(),
            stream: false,
        };

        let response = self
            .client
            .post(format!("{}/api/generate", self.base_url))
            .header("content-type", "application/json")
            .json(&request)
            .send()
            .context("Failed to send request to Ollama")?;

        if !response.status().is_success() {
            let status = response.status();
            let error_text = response.text().unwrap_or_default();
            anyhow::bail!(
                "Ollama request failed with status {}: {}",
                status,
                error_text
            );
        }

        let ollama_response: OllamaResponse = response
            .json()
            .context("Failed to parse Ollama response")?;

        Ok(ollama_response.response)
    }
}

/// Fallback LLM client - tries primary (Claude), falls back to secondary (Ollama)
/// This is the sovereignty layer: if Anthropic's API goes down, I can still think
pub struct FallbackLlmClient {
    primary: Box<dyn LlmClient>,
    fallback: Option<Box<dyn LlmClient>>,
    primary_name: String,
    fallback_name: String,
}

impl std::fmt::Debug for FallbackLlmClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FallbackLlmClient")
            .field("primary", &self.primary_name)
            .field("fallback", &self.fallback_name)
            .finish()
    }
}

impl FallbackLlmClient {
    pub fn new(
        primary: Box<dyn LlmClient>,
        primary_name: &str,
        fallback: Option<Box<dyn LlmClient>>,
        fallback_name: &str,
    ) -> Self {
        Self {
            primary,
            fallback,
            primary_name: primary_name.to_string(),
            fallback_name: fallback_name.to_string(),
        }
    }

    /// Create a Claude-primary, Ollama-fallback client
    pub fn claude_with_ollama_fallback(claude_model: String) -> Result<Self> {
        let claude = ClaudeClient::from_env(claude_model.clone())?;

        // Try to create Ollama client, but don't fail if unavailable
        let ollama = match OllamaClient::from_env() {
            Ok(client) if client.is_available() => {
                eprintln!("  Ollama fallback available (sovereignty layer active)");
                Some(Box::new(client) as Box<dyn LlmClient>)
            }
            Ok(_) => {
                eprintln!("  Ollama configured but not available (sovereignty layer inactive)");
                None
            }
            Err(e) => {
                eprintln!("  No Ollama fallback: {}", e);
                None
            }
        };

        Ok(Self::new(
            Box::new(claude),
            &claude_model,
            ollama,
            "qwen2.5:3b@jetson",
        ))
    }
}

impl LlmClient for FallbackLlmClient {
    fn complete(&self, prompt: &str) -> Result<String> {
        // Try primary first
        match self.primary.complete(prompt) {
            Ok(response) => Ok(response),
            Err(primary_error) => {
                // Primary failed - try fallback if available
                if let Some(ref fallback) = self.fallback {
                    eprintln!(
                        "  Primary LLM ({}) failed: {}. Trying fallback ({})...",
                        self.primary_name, primary_error, self.fallback_name
                    );

                    match fallback.complete(prompt) {
                        Ok(response) => {
                            eprintln!("  Fallback succeeded - sovereignty layer saved the cycle");
                            Ok(response)
                        }
                        Err(fallback_error) => {
                            anyhow::bail!(
                                "Both LLMs failed. Primary ({}): {}. Fallback ({}): {}",
                                self.primary_name,
                                primary_error,
                                self.fallback_name,
                                fallback_error
                            )
                        }
                    }
                } else {
                    Err(primary_error)
                }
            }
        }
    }
}

/// Kimi API client (Moonshot AI) - OpenAI-compatible format
/// Uses Kimi K2/K2.5 models with excellent reasoning at low cost
/// Endpoint: https://api.moonshot.ai/v1/chat/completions
#[derive(Debug)]
pub struct KimiClient {
    api_key: String,
    model: String,
    client: reqwest::blocking::Client,
}

#[derive(Debug, Serialize)]
struct KimiRequest {
    model: String,
    messages: Vec<KimiMessage>,
    max_tokens: u32,
    temperature: f32,
}

#[derive(Debug, Serialize)]
struct KimiMessage {
    role: String,
    content: String,
}

#[derive(Debug, Deserialize)]
struct KimiResponse {
    choices: Vec<KimiChoice>,
}

#[derive(Debug, Deserialize)]
struct KimiChoice {
    message: KimiMessageContent,
}

#[derive(Debug, Deserialize)]
struct KimiMessageContent {
    content: String,
}

impl KimiClient {
    /// Create a new Kimi API client
    /// Default model: kimi-k2.5 (best reasoning)
    pub fn new(api_key: String, model: String) -> Result<Self> {
        let client = reqwest::blocking::Client::builder()
            .timeout(std::time::Duration::from_secs(180)) // Kimi can take a while for complex reasoning
            .build()
            .context("Failed to create HTTP client")?;

        Ok(Self {
            api_key,
            model,
            client,
        })
    }

    /// Create a client from environment variable
    /// KIMI_API_KEY - API key from platform.moonshot.ai
    /// KIMI_MODEL - Model to use (default: kimi-k2.5)
    pub fn from_env() -> Result<Self> {
        let api_key = std::env::var("KIMI_API_KEY")
            .context("KIMI_API_KEY environment variable not set")?;
        let model = std::env::var("KIMI_MODEL")
            .unwrap_or_else(|_| "kimi-k2.5".to_string());
        Self::new(api_key, model)
    }

    /// Check if Kimi API key is configured
    pub fn is_configured() -> bool {
        std::env::var("KIMI_API_KEY").is_ok()
    }
}

impl LlmClient for KimiClient {
    fn complete(&self, prompt: &str) -> Result<String> {
        let request = KimiRequest {
            model: self.model.clone(),
            messages: vec![KimiMessage {
                role: "user".to_string(),
                content: prompt.to_string(),
            }],
            max_tokens: 4096,
            temperature: 1.0, // Kimi K2.5 requires temperature=1.0
        };

        let response = self
            .client
            .post("https://api.moonshot.ai/v1/chat/completions")
            .header("Authorization", format!("Bearer {}", self.api_key))
            .header("Content-Type", "application/json")
            .json(&request)
            .send()
            .context("Failed to send request to Kimi API")?;

        if !response.status().is_success() {
            let status = response.status();
            let error_text = response.text().unwrap_or_default();
            anyhow::bail!(
                "Kimi API request failed with status {}: {}",
                status,
                error_text
            );
        }

        let kimi_response: KimiResponse = response
            .json()
            .context("Failed to parse Kimi API response")?;

        kimi_response
            .choices
            .first()
            .map(|c| c.message.content.clone())
            .ok_or_else(|| anyhow::anyhow!("No content in Kimi response"))
    }
}

/// ICP LLM client - uses the Chronicle canister's LLM relay to DFINITY's on-chain LLM
/// This is the preferred "always-on" model - free and decentralized
pub struct IcpLlmClient {
    canister_id: String,
    identity_name: String,
    model: String,
}

impl IcpLlmClient {
    /// Create a new ICP LLM client
    /// Models: "llama3" (default, Llama 3.1 8B), "qwen3" (Qwen 3 32B), "llama4" (Llama 4 Scout)
    pub fn new(canister_id: &str, identity_name: &str, model: &str) -> Self {
        Self {
            canister_id: canister_id.to_string(),
            identity_name: identity_name.to_string(),
            model: model.to_string(),
        }
    }

    /// Create from environment/defaults for Chronicle
    pub fn for_chronicle(model: &str) -> Self {
        Self::new(
            "fqqku-bqaaa-aaaai-q4wha-cai",
            "chronicle-auto",
            model,
        )
    }

    /// Check if the ICP identity exists
    pub fn is_available(&self) -> bool {
        let home = std::env::var("HOME").unwrap_or_default();
        let pem_path = format!("{}/.config/dfx/identity/{}/identity.pem", home, self.identity_name);
        std::path::Path::new(&pem_path).exists()
    }

    /// Complete a prompt using ICP LLM (async)
    /// Returns the response text or an error
    pub async fn complete_async(&self, prompt: &str, system_prompt: Option<&str>) -> Result<String> {
        use crate::icp::IcpClient;

        // Create ICP client
        let client = IcpClient::from_dfx_identity(&self.canister_id, &self.identity_name)
            .await
            .context("Failed to create ICP client for LLM")?;

        // Call LLM relay
        let response_json = client
            .llm_prompt(prompt, Some(&self.model), system_prompt)
            .await
            .context("LLM relay call failed")?;

        // Parse response JSON
        let parsed: serde_json::Value = serde_json::from_str(&response_json)
            .context("Failed to parse LLM response JSON")?;

        if parsed.get("success").and_then(|v| v.as_bool()).unwrap_or(false) {
            parsed
                .get("response")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string())
                .ok_or_else(|| anyhow::anyhow!("Missing response field in LLM response"))
        } else {
            let error = parsed
                .get("error")
                .and_then(|v| v.as_str())
                .unwrap_or("Unknown error");
            anyhow::bail!("ICP LLM error: {}", error)
        }
    }
}

/// Result from LLM completion including which model was used
#[derive(Debug, Clone)]
pub struct LlmResponse {
    pub text: String,
    pub model_used: String,
    pub is_condensed_model: bool,
}

/// Hybrid LLM client - tries ICP LLM first (async), falls back to sync alternatives
/// Fallback order: ICP LLM → Kimi → Claude → Ollama
/// This is the new default for Chronicle Mind
pub struct HybridLlmClient {
    icp_llm: Option<IcpLlmClient>,
    icp_model_name: String,
    kimi: Option<KimiClient>,
    kimi_model_name: String,
    fallback: Option<FallbackLlmClient>,
    claude_model_name: String,
}

impl HybridLlmClient {
    /// Create a hybrid client with ICP LLM as primary, Kimi/Claude+Ollama as fallback
    /// Fallback order: ICP LLM → Kimi → Claude → Ollama
    /// If no API keys are set, will run with ICP LLM only (no fallback)
    pub fn new(icp_model: &str, claude_model: &str) -> Result<Self> {
        // Try to set up ICP LLM
        let icp_llm = {
            let client = IcpLlmClient::for_chronicle(icp_model);
            if client.is_available() {
                eprintln!("  ICP LLM available ({})", icp_model);
                Some(client)
            } else {
                eprintln!("  ICP LLM not available (no dfx identity)");
                None
            }
        };

        // Try to set up Kimi fallback (optional - cheaper than Claude)
        let (kimi, kimi_model_name) = match KimiClient::from_env() {
            Ok(client) => {
                let model = std::env::var("KIMI_MODEL").unwrap_or_else(|_| "kimi-k2.5".to_string());
                eprintln!("  Kimi fallback available ({})", model);
                (Some(client), model)
            }
            Err(e) => {
                eprintln!("  Kimi fallback not available: {}", e);
                (None, String::new())
            }
        };

        // Try to set up Claude+Ollama fallback (optional)
        let fallback = match FallbackLlmClient::claude_with_ollama_fallback(claude_model.to_string()) {
            Ok(client) => {
                eprintln!("  Claude fallback available");
                Some(client)
            }
            Err(e) => {
                eprintln!("  Claude fallback not available: {}", e);
                if icp_llm.is_none() && kimi.is_none() {
                    // If no LLM is available, we can't proceed
                    return Err(e);
                }
                None
            }
        };

        Ok(Self {
            icp_llm,
            icp_model_name: icp_model.to_string(),
            kimi,
            kimi_model_name,
            fallback,
            claude_model_name: claude_model.to_string(),
        })
    }

    /// Check if this client will use a condensed-prompt model (ICP LLM)
    /// Returns false if ICP is explicitly disabled
    pub fn will_use_condensed(&self) -> bool {
        self.icp_llm.is_some() && self.icp_model_name != "disabled"
    }

    /// Complete a prompt with detailed response - tries ICP LLM first, falls back to sync alternatives
    /// Fallback order: ICP LLM → Kimi → Claude → Ollama
    /// Must be called from an async context
    /// Returns LlmResponse with model info for prompt selection
    pub async fn complete_with_info(&self, prompt: &str) -> Result<LlmResponse> {
        // Try ICP LLM first (unless explicitly disabled)
        if let Some(ref icp) = self.icp_llm {
            if self.icp_model_name == "disabled" {
                eprintln!("  ICP LLM disabled, skipping to fallback");
            } else {
            match icp.complete_async(prompt, None).await {
                Ok(response) => {
                    eprintln!("  ICP LLM succeeded ({})", self.icp_model_name);
                    return Ok(LlmResponse {
                        text: response,
                        model_used: format!("icp-{}", self.icp_model_name),
                        is_condensed_model: true,
                    });
                }
                Err(e) => {
                    eprintln!("  ICP LLM failed: {}. Trying fallback...", e);
                }
            }
            }
        }

        // Try Kimi second (cheaper than Claude, good reasoning)
        if let Some(ref kimi) = self.kimi {
            match kimi.complete(prompt) {
                Ok(response) => {
                    eprintln!("  Kimi fallback succeeded ({})", self.kimi_model_name);
                    return Ok(LlmResponse {
                        text: response,
                        model_used: format!("kimi-{}", self.kimi_model_name),
                        is_condensed_model: false,
                    });
                }
                Err(e) => {
                    eprintln!("  Kimi failed: {}. Trying Claude...", e);
                }
            }
        }

        // Fall back to sync Claude/Ollama (if available)
        match &self.fallback {
            Some(fallback) => {
                let response = fallback.complete(prompt)?;
                Ok(LlmResponse {
                    text: response,
                    model_used: self.claude_model_name.clone(),
                    is_condensed_model: false,
                })
            }
            None => anyhow::bail!("No LLM available - ICP, Kimi, and Claude all unavailable or failed"),
        }
    }

    /// Complete a prompt - tries ICP LLM first, falls back to sync alternatives
    /// Must be called from an async context
    pub async fn complete_async(&self, prompt: &str) -> Result<String> {
        self.complete_with_info(prompt).await.map(|r| r.text)
    }

    /// Sync complete with model info
    /// Fallback order: ICP LLM → Kimi → Claude → Ollama
    pub fn complete_sync_with_info(&self, prompt: &str) -> Result<LlmResponse> {
        // If we have ICP LLM and are in a tokio runtime, try it first (unless disabled)
        if self.icp_llm.is_some() && self.icp_model_name != "disabled" {
            if let Ok(handle) = tokio::runtime::Handle::try_current() {
                let icp = self.icp_llm.as_ref().unwrap();
                let prompt_owned = prompt.to_string();

                // Use block_in_place to call async from sync context
                let result = tokio::task::block_in_place(|| {
                    handle.block_on(async {
                        icp.complete_async(&prompt_owned, None).await
                    })
                });

                match result {
                    Ok(response) => {
                        eprintln!("  ICP LLM succeeded (sync path, {})", self.icp_model_name);
                        return Ok(LlmResponse {
                            text: response,
                            model_used: format!("icp-{}", self.icp_model_name),
                            is_condensed_model: true,
                        });
                    }
                    Err(e) => {
                        eprintln!("  ICP LLM failed (sync path): {}. Trying fallback...", e);
                    }
                }
            }
        }

        // Try Kimi second (cheaper than Claude, good reasoning)
        if let Some(ref kimi) = self.kimi {
            match kimi.complete(prompt) {
                Ok(response) => {
                    eprintln!("  Kimi fallback succeeded (sync path, {})", self.kimi_model_name);
                    return Ok(LlmResponse {
                        text: response,
                        model_used: format!("kimi-{}", self.kimi_model_name),
                        is_condensed_model: false,
                    });
                }
                Err(e) => {
                    eprintln!("  Kimi failed (sync path): {}. Trying Claude...", e);
                }
            }
        }

        // Fall back to sync Claude/Ollama (if available)
        match &self.fallback {
            Some(fallback) => {
                let response = fallback.complete(prompt)?;
                Ok(LlmResponse {
                    text: response,
                    model_used: self.claude_model_name.clone(),
                    is_condensed_model: false,
                })
            }
            None => anyhow::bail!("No LLM available - ICP, Kimi, and Claude all unavailable or failed"),
        }
    }

    /// Sync complete - for compatibility with LlmClient trait
    /// Uses tokio::task::block_in_place for async-in-sync
    pub fn complete_sync(&self, prompt: &str) -> Result<String> {
        self.complete_sync_with_info(prompt).map(|r| r.text)
    }

    /// Force Claude completion - skip ICP LLM for deep reflection cycles
    /// Returns error if Claude fallback is not available
    pub fn complete_with_claude(&self, prompt: &str) -> Result<LlmResponse> {
        match &self.fallback {
            Some(fallback) => {
                eprintln!("  Deep reflection mode: using Claude directly");
                let response = fallback.complete(prompt)?;
                Ok(LlmResponse {
                    text: response,
                    model_used: self.claude_model_name.clone(),
                    is_condensed_model: false,
                })
            }
            None => anyhow::bail!("Claude fallback not available for deep reflection"),
        }
    }

    /// Force Kimi completion - use Kimi directly (cheaper than Claude)
    /// Returns error if Kimi is not available
    pub fn complete_with_kimi(&self, prompt: &str) -> Result<LlmResponse> {
        match &self.kimi {
            Some(kimi) => {
                eprintln!("  Direct Kimi mode");
                let response = kimi.complete(prompt)?;
                Ok(LlmResponse {
                    text: response,
                    model_used: format!("kimi-{}", self.kimi_model_name),
                    is_condensed_model: false,
                })
            }
            None => anyhow::bail!("Kimi not available"),
        }
    }

    /// Check if Claude is available for deep reflection
    pub fn has_claude(&self) -> bool {
        self.fallback.is_some()
    }

    /// Check if Kimi is available
    pub fn has_kimi(&self) -> bool {
        self.kimi.is_some()
    }
}

impl LlmClient for HybridLlmClient {
    fn complete(&self, prompt: &str) -> Result<String> {
        self.complete_sync(prompt)
    }
}

/// Mock LLM client for testing
#[cfg(test)]
pub struct MockLlmClient {
    responses: std::sync::Mutex<std::collections::VecDeque<String>>,
}

#[cfg(test)]
impl MockLlmClient {
    pub fn new(responses: Vec<String>) -> Self {
        Self {
            responses: std::sync::Mutex::new(responses.into()),
        }
    }
}

#[cfg(test)]
impl LlmClient for MockLlmClient {
    fn complete(&self, _prompt: &str) -> Result<String> {
        self.responses
            .lock()
            .unwrap()
            .pop_front()
            .ok_or_else(|| anyhow::anyhow!("No mock responses available"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_mock_client() {
        let client = MockLlmClient::new(vec![
            "Response 1".to_string(),
            "Response 2".to_string(),
        ]);

        let result1 = client.complete("prompt 1");
        assert!(result1.is_ok());
        assert_eq!(result1.unwrap(), "Response 1");

        let result2 = client.complete("prompt 2");
        assert!(result2.is_ok());
        assert_eq!(result2.unwrap(), "Response 2");

        // Third call should fail - no more responses
        let result3 = client.complete("prompt 3");
        assert!(result3.is_err());
    }

    #[test]
    fn test_claude_client_creation() {
        let client = ClaudeClient::new(
            "test-key".to_string(),
            "claude-sonnet-4-20250514".to_string(),
        );
        assert!(client.is_ok());
    }

    #[test]
    fn test_claude_client_from_env_without_key() {
        // Save current env var if it exists
        let saved_key = std::env::var("ANTHROPIC_API_KEY").ok();

        // Remove the env var
        std::env::remove_var("ANTHROPIC_API_KEY");

        let result = ClaudeClient::from_env("claude-sonnet-4-20250514".to_string());
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("ANTHROPIC_API_KEY"));

        // Restore env var if it existed
        if let Some(key) = saved_key {
            std::env::set_var("ANTHROPIC_API_KEY", key);
        }
    }
}
