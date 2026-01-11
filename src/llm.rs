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
