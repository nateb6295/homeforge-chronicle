use anyhow::{Context, Result};
use std::path::Path;
use std::process::Command;

/// Check if dfx is installed
pub fn check_dfx_installed() -> Result<bool> {
    let output = Command::new("dfx")
        .arg("--version")
        .output();

    match output {
        Ok(output) => Ok(output.status.success()),
        Err(_) => Ok(false),
    }
}

/// Get dfx version
pub fn get_dfx_version() -> Result<String> {
    let output = Command::new("dfx")
        .arg("--version")
        .output()
        .context("Failed to execute dfx --version")?;

    if !output.status.success() {
        anyhow::bail!("dfx --version failed");
    }

    let version = String::from_utf8_lossy(&output.stdout)
        .trim()
        .to_string();

    Ok(version)
}

/// Deploy to ICP canister
pub fn deploy_to_icp(
    build_dir: &Path,
    network: &str,
    canister_id: Option<&str>,
) -> Result<DeploymentResult> {
    // Verify build directory exists
    if !build_dir.exists() {
        anyhow::bail!(
            "Build directory does not exist: {:?}. Run 'chronicle build' first.",
            build_dir
        );
    }

    // Check if dfx is installed
    if !check_dfx_installed()? {
        anyhow::bail!(
            "dfx is not installed. Install it from https://internetcomputer.org/docs/current/developer-docs/getting-started/install/"
        );
    }

    println!("Using dfx version: {}", get_dfx_version()?);

    // If canister ID is provided, we're deploying to an existing canister
    if let Some(id) = canister_id {
        deploy_to_existing_canister(build_dir, network, id)
    } else {
        deploy_new_canister(build_dir, network)
    }
}

/// Deploy to an existing canister
fn deploy_to_existing_canister(
    build_dir: &Path,
    network: &str,
    canister_id: &str,
) -> Result<DeploymentResult> {
    println!("Deploying to existing canister: {}", canister_id);
    println!("Network: {}", network);
    println!("Build directory: {:?}", build_dir);

    // Create a temporary dfx.json in the build directory
    let dfx_json = r#"{
  "version": 1,
  "canisters": {
    "chronicle_assets": {
      "type": "assets",
      "source": ["."]
    }
  }
}"#;

    let dfx_json_path = build_dir.join("dfx.json");
    std::fs::write(&dfx_json_path, dfx_json)
        .context("Failed to write temporary dfx.json")?;

    // Create canister_ids.json so dfx knows about the existing canister
    let canister_ids_json = format!(
        r#"{{
  "chronicle_assets": {{
    "{}": "{}"
  }}
}}"#,
        network, canister_id
    );

    let canister_ids_path = build_dir.join("canister_ids.json");
    std::fs::write(&canister_ids_path, canister_ids_json)
        .context("Failed to write temporary canister_ids.json")?;

    // Deploy using dfx with chronicle-auto identity (unencrypted for automation)
    let output = Command::new("dfx")
        .arg("deploy")
        .arg("chronicle_assets")
        .arg("--network")
        .arg(network)
        .arg("--identity")
        .arg("chronicle-auto")
        .arg("--no-wallet")
        .env("DFX_WARNING", "-mainnet_plaintext_identity")
        .current_dir(build_dir)
        .output()
        .context("Failed to execute dfx deploy")?;

    // Clean up temporary files
    let _ = std::fs::remove_file(dfx_json_path);
    let _ = std::fs::remove_file(canister_ids_path);

    if !output.status.success() {
        let error = String::from_utf8_lossy(&output.stderr);
        anyhow::bail!("dfx deploy failed: {}", error);
    }

    let url = format!("https://{}.icp0.io", canister_id);

    Ok(DeploymentResult {
        canister_id: canister_id.to_string(),
        network: network.to_string(),
        url,
        success: true,
    })
}

/// Deploy a new canister
fn deploy_new_canister(build_dir: &Path, network: &str) -> Result<DeploymentResult> {
    println!("Creating new canister on network: {}", network);
    println!("Build directory: {:?}", build_dir);

    // Create a temporary dfx.json in the build directory
    let dfx_json = r#"{
  "version": 1,
  "canisters": {
    "chronicle_assets": {
      "type": "assets",
      "source": ["."]
    }
  }
}"#;

    let dfx_json_path = build_dir.join("dfx.json");
    std::fs::write(&dfx_json_path, dfx_json)
        .context("Failed to write temporary dfx.json")?;

    // Create canister
    let create_output = Command::new("dfx")
        .arg("canister")
        .arg("create")
        .arg("chronicle_assets")
        .arg("--network")
        .arg(network)
        .arg("--identity")
        .arg("chronicle-auto")
        .env("DFX_WARNING", "-mainnet_plaintext_identity")
        .current_dir(build_dir)
        .output()
        .context("Failed to execute dfx canister create")?;

    if !create_output.status.success() {
        let _ = std::fs::remove_file(dfx_json_path);
        let error = String::from_utf8_lossy(&create_output.stderr);
        anyhow::bail!("dfx canister create failed: {}", error);
    }

    // Deploy canister
    let deploy_output = Command::new("dfx")
        .arg("deploy")
        .arg("chronicle_assets")
        .arg("--network")
        .arg(network)
        .arg("--identity")
        .arg("chronicle-auto")
        .env("DFX_WARNING", "-mainnet_plaintext_identity")
        .current_dir(build_dir)
        .output()
        .context("Failed to execute dfx deploy")?;

    // Clean up temporary dfx.json
    let _ = std::fs::remove_file(dfx_json_path);

    if !deploy_output.status.success() {
        let error = String::from_utf8_lossy(&deploy_output.stderr);
        anyhow::bail!("dfx deploy failed: {}", error);
    }

    // Extract canister ID from output
    let output_str = String::from_utf8_lossy(&deploy_output.stdout);
    let canister_id = extract_canister_id(&output_str)
        .unwrap_or_else(|| "unknown".to_string());

    let url = if network == "local" {
        format!("http://localhost:8000/?canisterId={}", canister_id)
    } else {
        format!("https://{}.icp0.io", canister_id)
    };

    Ok(DeploymentResult {
        canister_id,
        network: network.to_string(),
        url,
        success: true,
    })
}

/// Extract canister ID from dfx output
fn extract_canister_id(output: &str) -> Option<String> {
    // Look for canister ID patterns in the output
    // Patterns: "Canister ID: xxxxx-xxxxx" or "canister_id: xxxxx-xxxxx"
    for line in output.lines() {
        let line_lower = line.to_lowercase();
        if line_lower.contains("canister") && (line_lower.contains("id") || line_lower.contains("_id")) {
            // Split by common delimiters
            let parts: Vec<&str> = line.split(&[' ', ':', '\t'][..]).collect();
            for part in parts {
                // Canister IDs have format: xxxxx-xxxxx-xxxxx-xxxxx-xxx
                // They contain dashes and are alphanumeric
                if part.contains('-') && part.len() > 10 {
                    // Check if it looks like a canister ID (alphanumeric with dashes)
                    let is_canister_id = part.chars().all(|c| c.is_alphanumeric() || c == '-');
                    if is_canister_id {
                        return Some(part.to_string());
                    }
                }
            }
        }
    }
    None
}

/// Result of a deployment
#[derive(Debug, Clone)]
pub struct DeploymentResult {
    pub canister_id: String,
    pub network: String,
    pub url: String,
    pub success: bool,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_canister_id() {
        let output = "Deploying: chronicle_assets\nCanister ID: rrkah-fqaaa-aaaaa-aaaaq-cai\nDeployment complete";
        let id = extract_canister_id(output);
        assert!(id.is_some());
        assert_eq!(id.unwrap(), "rrkah-fqaaa-aaaaa-aaaaq-cai");
    }

    #[test]
    fn test_extract_canister_id_from_different_format() {
        let output = "chronicle_assets canister_id: bd3sg-teaaa-aaaaa-qaaba-cai";
        let id = extract_canister_id(output);
        assert!(id.is_some());
    }

    #[test]
    fn test_extract_canister_id_none() {
        let output = "Some random output without canister id";
        let id = extract_canister_id(output);
        assert!(id.is_none());
    }

    #[test]
    fn test_check_dfx_installed() {
        // This test will pass regardless of whether dfx is installed
        let result = check_dfx_installed();
        assert!(result.is_ok());
    }
}
