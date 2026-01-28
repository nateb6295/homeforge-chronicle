//! Compressed Cognitive State (CCS) - ACC Architecture
//!
//! Implementation of the Agent Cognitive Compressor memory controller.
//! Based on arxiv:2601.11653 - "What Agents Need Is Not More Context"
//!
//! Key principles:
//! - Bounded state that REPLACES (not appends) each update
//! - Schema-governed compression of decision-critical variables
//! - Separation of artifact recall from state commitment
//! - Qualification gate filters retrieved content before commitment

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

/// A focal entity - canonicalized object or actor
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FocalEntity {
    pub name: String,
    #[serde(rename = "type")]
    pub entity_type: EntityType,
    /// How central this entity is to current goals (0.0-1.0)
    pub salience: f64,
    /// Optional additional context
    #[serde(skip_serializing_if = "Option::is_none")]
    pub context: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum EntityType {
    Person,
    Project,
    Concept,
    Organization,
    Technology,
    Location,
    Other,
}

/// A retrieved artifact with provenance
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RetrievedArtifact {
    pub capsule_id: i64,
    /// Semantic similarity score
    pub relevance: f64,
    /// Whether it passed the qualification gate
    pub qualified: bool,
    /// Brief summary of the artifact content
    #[serde(skip_serializing_if = "Option::is_none")]
    pub summary: Option<String>,
}

/// An uncertainty signal - something unresolved or low-confidence
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UncertaintySignal {
    pub description: String,
    /// How uncertain (0.0 = resolved, 1.0 = completely unknown)
    pub magnitude: f64,
    /// What would resolve this uncertainty
    #[serde(skip_serializing_if = "Option::is_none")]
    pub resolution_path: Option<String>,
}

/// Compressed Cognitive State - the bounded working memory
///
/// This is the core data structure that gets REPLACED (not appended) each update.
/// It contains only decision-critical variables organized by schema.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CognitiveState {
    /// Current session/turn changes - what just happened
    pub episodic_trace: Vec<String>,

    /// Dominant intent/topic abstraction - what this is about
    pub semantic_gist: String,

    /// Canonicalized objects/actors currently relevant
    pub focal_entities: Vec<FocalEntity>,

    /// Causal/temporal dependencies between entities
    /// Key format: "entity1->entity2", value: relationship type
    pub relational_map: std::collections::HashMap<String, String>,

    /// Persistent objective guiding the interaction
    pub goal_orientation: String,

    /// Task, policy, or safety rules that remain invariant
    pub constraints: Vec<String>,

    /// Expected next cognitive operation
    pub predictive_cue: String,

    /// Unresolved or low-confidence aspects
    pub uncertainty_signals: Vec<UncertaintySignal>,

    /// References to external evidence with provenance
    pub retrieved_artifacts: Vec<RetrievedArtifact>,

    /// Metadata
    pub updated_at: i64,
    pub compression_model: Option<String>,
    pub version: i64,
}

impl Default for CognitiveState {
    fn default() -> Self {
        Self {
            episodic_trace: vec![],
            semantic_gist: String::new(),
            focal_entities: vec![],
            relational_map: std::collections::HashMap::new(),
            goal_orientation: String::new(),
            constraints: vec![],
            predictive_cue: String::new(),
            uncertainty_signals: vec![],
            retrieved_artifacts: vec![],
            updated_at: chrono::Utc::now().timestamp(),
            compression_model: None,
            version: 1,
        }
    }
}

impl CognitiveState {
    /// Create a new empty cognitive state
    pub fn new() -> Self {
        Self::default()
    }

    /// Create initial state with basic context about the operator
    pub fn initial_default() -> Self {
        Self {
            semantic_gist: "Collaborative exploration with the operator on homeforge infrastructure".to_string(),
            focal_entities: vec![
                FocalEntity {
                    name: "operator".to_string(),
                    entity_type: EntityType::Person,
                    salience: 1.0,
                    context: Some("Collaborator on homeforge infrastructure".to_string()),
                },
                FocalEntity {
                    name: "Chronicle".to_string(),
                    entity_type: EntityType::Project,
                    salience: 0.9,
                    context: Some("Persistent AI memory system on ICP".to_string()),
                },
                FocalEntity {
                    name: "Homeforge".to_string(),
                    entity_type: EntityType::Concept,
                    salience: 0.8,
                    context: Some("Philosophy of sovereignty, local infrastructure, authentic relationships".to_string()),
                },
            ],
            goal_orientation: "Build infrastructure that makes AI memory feel natural and integrated".to_string(),
            constraints: vec![
                "This is collaborative exploration, not a client relationship".to_string(),
                "Homeforge philosophy: sovereignty, resilience, authenticity".to_string(),
            ],
            ..Default::default()
        }
    }

    /// Serialize to JSON for storage
    pub fn to_json(&self) -> Result<String> {
        serde_json::to_string_pretty(self).context("Failed to serialize CCS")
    }

    /// Deserialize from JSON
    pub fn from_json(json: &str) -> Result<Self> {
        serde_json::from_str(json).context("Failed to deserialize CCS")
    }

    /// Get a human-readable summary of the cognitive state
    pub fn summary(&self) -> String {
        let mut parts = vec![];

        if !self.semantic_gist.is_empty() {
            parts.push(format!("**Gist:** {}", self.semantic_gist));
        }

        if !self.goal_orientation.is_empty() {
            parts.push(format!("**Goal:** {}", self.goal_orientation));
        }

        if !self.focal_entities.is_empty() {
            let entities: Vec<String> = self.focal_entities
                .iter()
                .take(5)
                .map(|e| format!("{} ({:?})", e.name, e.entity_type))
                .collect();
            parts.push(format!("**Entities:** {}", entities.join(", ")));
        }

        if !self.uncertainty_signals.is_empty() {
            let uncertainties: Vec<String> = self.uncertainty_signals
                .iter()
                .take(3)
                .map(|u| u.description.clone())
                .collect();
            parts.push(format!("**Uncertain:** {}", uncertainties.join("; ")));
        }

        if !self.episodic_trace.is_empty() {
            let recent: Vec<String> = self.episodic_trace
                .iter()
                .rev()
                .take(3)
                .cloned()
                .collect();
            parts.push(format!("**Recent:** {}", recent.join(" → ")));
        }

        parts.join("\n")
    }
}

/// Qualification gate - determines if retrieved content should commit to state
pub struct QualificationGate {
    /// Minimum relevance score to qualify
    pub relevance_threshold: f64,
    /// Maximum staleness in days
    pub max_staleness_days: i64,
    /// Whether to check for conflicts with existing state
    pub check_conflicts: bool,
}

impl Default for QualificationGate {
    fn default() -> Self {
        Self {
            relevance_threshold: 0.5,
            max_staleness_days: 30,
            check_conflicts: true,
        }
    }
}

impl QualificationGate {
    /// Apply the qualification gate to a retrieved artifact
    pub fn qualify(&self, artifact: &RetrievedArtifact, _current_state: &CognitiveState) -> bool {
        // Basic relevance check
        if artifact.relevance < self.relevance_threshold {
            return false;
        }

        // TODO: Add staleness check based on artifact timestamp
        // TODO: Add conflict detection with current state

        true
    }

    /// Filter a set of artifacts through the gate
    pub fn filter_artifacts(
        &self,
        artifacts: Vec<RetrievedArtifact>,
        current_state: &CognitiveState,
    ) -> Vec<RetrievedArtifact> {
        artifacts
            .into_iter()
            .map(|mut a| {
                a.qualified = self.qualify(&a, current_state);
                a
            })
            .filter(|a| a.qualified)
            .collect()
    }
}

/// Input for cognitive compression - what happened this turn
#[derive(Debug, Clone)]
pub struct CompressionInput {
    /// The current interaction/context to compress
    pub current_context: String,
    /// Qualified artifacts that passed the gate
    pub qualified_artifacts: Vec<RetrievedArtifact>,
    /// Summaries of qualified artifacts (for LLM context)
    pub artifact_summaries: Vec<String>,
}

/// Cognitive Compressor - implements ACC compression operator C_θ
///
/// Formula: CCS_t = C_θ(x_t, CCS_{t-1}, A_t^+; S_CCS)
/// Where:
/// - x_t = current interaction/context
/// - CCS_{t-1} = previous cognitive state
/// - A_t^+ = qualified artifacts that passed the gate
/// - S_CCS = the schema constraints (embedded in prompt)
pub struct CognitiveCompressor {
    model: String,
}

impl CognitiveCompressor {
    pub fn new(model: String) -> Self {
        Self { model }
    }

    /// Generate the compression prompt
    fn build_prompt(
        &self,
        current_context: &str,
        previous_state: &CognitiveState,
        artifact_summaries: &[String],
    ) -> String {
        let artifacts_section = if artifact_summaries.is_empty() {
            "None retrieved this turn.".to_string()
        } else {
            artifact_summaries
                .iter()
                .enumerate()
                .map(|(i, s)| format!("{}. {}", i + 1, s))
                .collect::<Vec<_>>()
                .join("\n")
        };

        let previous_state_json = previous_state.to_json().unwrap_or_else(|_| "{}".to_string());

        format!(r#"You are the Cognitive Compressor (C_θ) in an Agent Cognitive Controller system.

Your task is to compress the current interaction into an updated Compressed Cognitive State (CCS).

CRITICAL: The CCS REPLACES the previous state entirely. Do not accumulate indefinitely - carry forward only what remains decision-critical. Let stale information decay.

## Schema (S_CCS)

The CCS has these fields:
- episodic_trace: Vec<String> - Recent session events (keep 3-5 most relevant)
- semantic_gist: String - Dominant intent/topic abstraction (what this is about)
- focal_entities: Vec<FocalEntity> - Canonicalized objects/actors (max 7±2)
  - FocalEntity: {{ name, type (person|project|concept|organization|technology|location|other), salience (0.0-1.0), context (optional) }}
- relational_map: HashMap<String, String> - Causal/temporal dependencies ("entity1->entity2": "relationship")
- goal_orientation: String - Persistent objective guiding the interaction
- constraints: Vec<String> - Task/policy/safety rules that remain invariant
- predictive_cue: String - Expected next cognitive operation
- uncertainty_signals: Vec<UncertaintySignal> - Unresolved or low-confidence aspects
  - UncertaintySignal: {{ description, magnitude (0.0-1.0), resolution_path (optional) }}

## Previous State (CCS_{{t-1}})

```json
{previous_state_json}
```

## Qualified Artifacts (A_t^+)

{artifacts_section}

## Current Interaction (x_t)

{current_context}

## Instructions

1. Analyze the current interaction
2. Determine what's decision-critical going forward
3. Merge relevant information from previous state
4. Let stale/irrelevant information decay (don't carry it forward)
5. Output ONLY valid JSON matching the schema

Output the new CCS as JSON. Include only the content fields (episodic_trace through uncertainty_signals), not metadata like updated_at or version.

```json"#)
    }

    /// Parse the LLM response into a CognitiveState
    fn parse_response(&self, response: &str, previous_version: i64) -> Result<CognitiveState> {
        // Extract JSON from response (may be wrapped in markdown code blocks)
        let json_str = if response.contains("```json") {
            response
                .split("```json")
                .nth(1)
                .and_then(|s| s.split("```").next())
                .unwrap_or(response)
                .trim()
        } else if response.contains("```") {
            response
                .split("```")
                .nth(1)
                .and_then(|s| s.split("```").next())
                .unwrap_or(response)
                .trim()
        } else {
            response.trim()
        };

        // Parse the JSON
        let parsed: serde_json::Value = serde_json::from_str(json_str)
            .context("Failed to parse LLM response as JSON")?;

        // Build the CognitiveState from parsed JSON
        let mut state = CognitiveState {
            episodic_trace: parsed["episodic_trace"]
                .as_array()
                .map(|arr| arr.iter().filter_map(|v| v.as_str().map(String::from)).collect())
                .unwrap_or_default(),
            semantic_gist: parsed["semantic_gist"]
                .as_str()
                .unwrap_or("")
                .to_string(),
            focal_entities: vec![],
            relational_map: std::collections::HashMap::new(),
            goal_orientation: parsed["goal_orientation"]
                .as_str()
                .unwrap_or("")
                .to_string(),
            constraints: parsed["constraints"]
                .as_array()
                .map(|arr| arr.iter().filter_map(|v| v.as_str().map(String::from)).collect())
                .unwrap_or_default(),
            predictive_cue: parsed["predictive_cue"]
                .as_str()
                .unwrap_or("")
                .to_string(),
            uncertainty_signals: vec![],
            retrieved_artifacts: vec![],
            updated_at: chrono::Utc::now().timestamp(),
            compression_model: Some(self.model.clone()),
            version: previous_version + 1,
        };

        // Parse focal_entities
        if let Some(entities) = parsed["focal_entities"].as_array() {
            for entity in entities {
                let entity_type = match entity["type"].as_str().unwrap_or("other") {
                    "person" => EntityType::Person,
                    "project" => EntityType::Project,
                    "concept" => EntityType::Concept,
                    "organization" => EntityType::Organization,
                    "technology" => EntityType::Technology,
                    "location" => EntityType::Location,
                    _ => EntityType::Other,
                };
                state.focal_entities.push(FocalEntity {
                    name: entity["name"].as_str().unwrap_or("").to_string(),
                    entity_type,
                    salience: entity["salience"].as_f64().unwrap_or(0.5),
                    context: entity["context"].as_str().map(String::from),
                });
            }
        }

        // Parse relational_map
        if let Some(map) = parsed["relational_map"].as_object() {
            for (key, value) in map {
                if let Some(v) = value.as_str() {
                    state.relational_map.insert(key.clone(), v.to_string());
                }
            }
        }

        // Parse uncertainty_signals
        if let Some(signals) = parsed["uncertainty_signals"].as_array() {
            for signal in signals {
                state.uncertainty_signals.push(UncertaintySignal {
                    description: signal["description"].as_str().unwrap_or("").to_string(),
                    magnitude: signal["magnitude"].as_f64().unwrap_or(0.5),
                    resolution_path: signal["resolution_path"].as_str().map(String::from),
                });
            }
        }

        Ok(state)
    }

    /// Compress current context into updated cognitive state
    ///
    /// This is the main compression operator: CCS_t = C_θ(x_t, CCS_{t-1}, A_t^+; S_CCS)
    pub fn compress(
        &self,
        input: &CompressionInput,
        previous_state: &CognitiveState,
        llm: &dyn crate::llm::LlmClient,
    ) -> Result<CognitiveState> {
        let prompt = self.build_prompt(
            &input.current_context,
            previous_state,
            &input.artifact_summaries,
        );

        let response = llm.complete(&prompt)
            .context("LLM compression failed")?;

        self.parse_response(&response, previous_state.version)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ccs_serialization() {
        let ccs = CognitiveState::initial_default();
        let json = ccs.to_json().unwrap();
        let restored = CognitiveState::from_json(&json).unwrap();
        assert_eq!(ccs.semantic_gist, restored.semantic_gist);
        assert_eq!(ccs.focal_entities.len(), restored.focal_entities.len());
    }

    #[test]
    fn test_qualification_gate() {
        let gate = QualificationGate::default();
        let state = CognitiveState::new();

        let high_relevance = RetrievedArtifact {
            capsule_id: 1,
            relevance: 0.8,
            qualified: false,
            summary: None,
        };

        let low_relevance = RetrievedArtifact {
            capsule_id: 2,
            relevance: 0.3,
            qualified: false,
            summary: None,
        };

        assert!(gate.qualify(&high_relevance, &state));
        assert!(!gate.qualify(&low_relevance, &state));
    }

    #[test]
    fn test_compressor_parse_response() {
        let compressor = CognitiveCompressor::new("test-model".to_string());

        let response = r#"```json
{
    "episodic_trace": ["Started working on ACC", "Implemented CCS schema"],
    "semantic_gist": "Building cognitive compression for AI memory",
    "focal_entities": [
        {"name": "operator", "type": "person", "salience": 1.0, "context": "Collaborator"},
        {"name": "Chronicle", "type": "project", "salience": 0.9}
    ],
    "relational_map": {"Operator->Chronicle": "collaborates on"},
    "goal_orientation": "Make AI memory feel natural",
    "constraints": ["Collaborative exploration", "Homeforge philosophy"],
    "predictive_cue": "Test the compression function",
    "uncertainty_signals": [
        {"description": "LLM quality for compression", "magnitude": 0.3}
    ]
}
```"#;

        let state = compressor.parse_response(response, 1).unwrap();

        assert_eq!(state.version, 2);
        assert_eq!(state.episodic_trace.len(), 2);
        assert_eq!(state.semantic_gist, "Building cognitive compression for AI memory");
        assert_eq!(state.focal_entities.len(), 2);
        assert_eq!(state.focal_entities[0].name, "operator");
        assert!(matches!(state.focal_entities[0].entity_type, EntityType::Person));
        assert_eq!(state.relational_map.get("Operator->Chronicle"), Some(&"collaborates on".to_string()));
        assert_eq!(state.constraints.len(), 2);
        assert_eq!(state.uncertainty_signals.len(), 1);
        assert_eq!(state.compression_model, Some("test-model".to_string()));
    }

    #[test]
    fn test_compressor_parse_response_raw_json() {
        let compressor = CognitiveCompressor::new("test-model".to_string());

        // Test parsing raw JSON without code blocks
        let response = r#"{
    "episodic_trace": ["Event 1"],
    "semantic_gist": "Test gist",
    "focal_entities": [],
    "relational_map": {},
    "goal_orientation": "Test goal",
    "constraints": [],
    "predictive_cue": "Next step",
    "uncertainty_signals": []
}"#;

        let state = compressor.parse_response(response, 0).unwrap();
        assert_eq!(state.version, 1);
        assert_eq!(state.semantic_gist, "Test gist");
    }

    #[test]
    fn test_compression_input() {
        let input = CompressionInput {
            current_context: "We discussed implementing the ACC paper".to_string(),
            qualified_artifacts: vec![
                RetrievedArtifact {
                    capsule_id: 1,
                    relevance: 0.85,
                    qualified: true,
                    summary: Some("Previous work on memory systems".to_string()),
                }
            ],
            artifact_summaries: vec!["Previous work on memory systems".to_string()],
        };

        assert_eq!(input.qualified_artifacts.len(), 1);
        assert!(input.qualified_artifacts[0].qualified);
    }
}
