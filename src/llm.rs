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
