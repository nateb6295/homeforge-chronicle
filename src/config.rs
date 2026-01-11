use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Config {
    pub input: InputConfig,
    pub extraction: ExtractionConfig,
    pub output: OutputConfig,
    pub deployment: DeploymentConfig,
    pub schedule: ScheduleConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct InputConfig {
    pub watch_directory: PathBuf,
    pub processed_db: PathBuf,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ExtractionConfig {
    pub llm_backend: LlmBackend,
    pub llm_model: String,
    #[serde(default)]
    pub claude_api_key: Option<String>,
    pub themes: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum LlmBackend {
    ClaudeApi,
    Ollama,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct OutputConfig {
    pub build_directory: PathBuf,
    pub site_title: String,
    pub author: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct DeploymentConfig {
    pub canister_id: String,
    pub network: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ScheduleConfig {
    pub auto_deploy: bool,
    #[serde(default)]
    pub notify_on_completion: Option<bool>,
    #[serde(default)]
    pub notify_webhook: Option<String>,
}

impl Config {
    /// Load configuration from a TOML file
    pub fn from_file<P: AsRef<Path>>(path: P) -> Result<Self> {
        let contents = std::fs::read_to_string(path.as_ref())
            .context("Failed to read config file")?;
        Self::from_str(&contents)
    }

    /// Parse configuration from a TOML string
    pub fn from_str(contents: &str) -> Result<Self> {
        toml::from_str(contents).context("Failed to parse config TOML")
    }

    /// Create a default configuration
    pub fn default_config() -> Self {
        let home = std::env::var_os("HOME")
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from("."));
        let chronicle_dir = home.join(".homeforge-chronicle");

        Config {
            input: InputConfig {
                watch_directory: home.join("claude-exports"),
                processed_db: chronicle_dir.join("processed.db"),
            },
            extraction: ExtractionConfig {
                llm_backend: LlmBackend::ClaudeApi,
                llm_model: "claude-sonnet-4-20250514".to_string(),
                claude_api_key: None,
                themes: vec![
                    "lattice".to_string(),
                    "homeforge".to_string(),
                    "icp".to_string(),
                    "xrp".to_string(),
                    "ai-agency".to_string(),
                    "construction".to_string(),
                    "family".to_string(),
                ],
            },
            output: OutputConfig {
                build_directory: chronicle_dir.join("build"),
                site_title: "Homeforge Chronicle".to_string(),
                author: "Nate".to_string(),
            },
            deployment: DeploymentConfig {
                canister_id: "xxxxx-xxxxx-xxxxx-xxxxx-cai".to_string(),
                network: "ic".to_string(),
            },
            schedule: ScheduleConfig {
                auto_deploy: true,
                notify_on_completion: Some(false),
                notify_webhook: None,
            },
        }
    }

    /// Write configuration to a TOML file
    pub fn to_file<P: AsRef<Path>>(&self, path: P) -> Result<()> {
        let contents = toml::to_string_pretty(self)
            .context("Failed to serialize config to TOML")?;
        std::fs::write(path.as_ref(), contents)
            .context("Failed to write config file")?;
        Ok(())
    }
}

// Need to add dirs crate for home_dir
// For now, let's create a simpler version without dirs

impl Config {
    /// Expand tilde in paths to home directory
    pub fn expand_paths(&mut self) {
        if let Some(home) = std::env::var_os("HOME") {
            let home = PathBuf::from(home);
            self.input.watch_directory = expand_tilde(&self.input.watch_directory, &home);
            self.input.processed_db = expand_tilde(&self.input.processed_db, &home);
            self.output.build_directory = expand_tilde(&self.output.build_directory, &home);
        }
    }
}

fn expand_tilde(path: &Path, home: &Path) -> PathBuf {
    if let Ok(stripped) = path.strip_prefix("~") {
        home.join(stripped)
    } else {
        path.to_path_buf()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const TEST_CONFIG: &str = r#"
[input]
watch_directory = "~/claude-exports"
processed_db = "~/.homeforge-chronicle/processed.db"

[extraction]
llm_backend = "claude-api"
llm_model = "claude-sonnet-4-20250514"
claude_api_key = "sk-test-key"
themes = ["lattice", "homeforge", "icp"]

[output]
build_directory = "~/.homeforge-chronicle/build"
site_title = "Test Chronicle"
author = "Test Author"

[deployment]
canister_id = "test-canister-id"
network = "local"

[schedule]
auto_deploy = false
notify_webhook = "https://example.com/webhook"
"#;

    #[test]
    fn test_config_parses_from_toml() {
        let config = Config::from_str(TEST_CONFIG);
        assert!(config.is_ok(), "Failed to parse config: {:?}", config.err());

        let config = config.unwrap();
        assert_eq!(config.extraction.llm_backend, LlmBackend::ClaudeApi);
        assert_eq!(config.extraction.llm_model, "claude-sonnet-4-20250514");
        assert_eq!(config.output.author, "Test Author");
        assert_eq!(config.schedule.auto_deploy, false);
    }

    #[test]
    fn test_config_serializes_to_toml() {
        let config = Config::from_str(TEST_CONFIG).unwrap();
        let serialized = toml::to_string(&config);
        assert!(serialized.is_ok());

        // Should be able to parse it back
        let reparsed = Config::from_str(&serialized.unwrap());
        assert!(reparsed.is_ok());
        assert_eq!(config, reparsed.unwrap());
    }

    #[test]
    fn test_default_config_creates() {
        let config = Config::default_config();
        assert_eq!(config.extraction.llm_backend, LlmBackend::ClaudeApi);
        assert!(!config.extraction.themes.is_empty());
        assert_eq!(config.output.site_title, "Homeforge Chronicle");
    }

    #[test]
    fn test_expand_paths_with_tilde() {
        let mut config = Config::from_str(TEST_CONFIG).unwrap();
        config.expand_paths();

        // After expansion, paths should not contain ~
        assert!(!config.input.watch_directory.to_string_lossy().contains('~'));
        assert!(!config.input.processed_db.to_string_lossy().contains('~'));
    }

    #[test]
    fn test_llm_backend_enum() {
        let claude = toml::from_str::<ExtractionConfig>(r#"
llm_backend = "claude-api"
llm_model = "test"
themes = []
        "#);
        assert!(claude.is_ok());
        assert_eq!(claude.unwrap().llm_backend, LlmBackend::ClaudeApi);

        let ollama = toml::from_str::<ExtractionConfig>(r#"
llm_backend = "ollama"
llm_model = "test"
themes = []
        "#);
        assert!(ollama.is_ok());
        assert_eq!(ollama.unwrap().llm_backend, LlmBackend::Ollama);
    }
}
