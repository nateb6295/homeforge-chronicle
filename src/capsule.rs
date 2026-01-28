//! Knowledge Capsule Compression
//!
//! Converts raw conversation dialogue into atomic, self-contained facts.
//! Based on SimpleMem's Semantic Structured Compression approach.
//!
//! Key transformations:
//! - Coreference resolution (no pronouns - "he" → "Bob")
//! - Temporal anchoring (relative → absolute: "tomorrow" → "2026-01-13T14:00:00")
//! - Entity extraction (persons, companies, concepts)
//! - Keyword generation (for BM25 search)

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

use crate::llm::LlmClient;
use crate::parser::ConversationExport;

/// A compressed knowledge capsule - an atomic, self-contained fact
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KnowledgeCapsule {
    /// The lossless restatement - complete, unambiguous sentence
    pub restatement: String,

    /// Keywords for lexical search
    pub keywords: Vec<String>,

    /// Absolute timestamp (ISO 8601) if time is mentioned
    pub timestamp: Option<String>,

    /// Location if mentioned
    pub location: Option<String>,

    /// People mentioned
    pub persons: Vec<String>,

    /// Entities (companies, products, concepts) with optional type
    pub entities: Vec<(String, Option<String>)>,

    /// Topic/category of this fact
    pub topic: Option<String>,
}

/// Response from LLM containing extracted capsules
#[derive(Debug, Deserialize)]
struct CapsuleExtractionResponse {
    capsules: Vec<RawCapsule>,
}

#[derive(Debug, Deserialize)]
struct RawCapsule {
    lossless_restatement: String,
    keywords: Vec<String>,
    timestamp: Option<String>,
    location: Option<String>,
    persons: Vec<String>,
    entities: Vec<String>,
    topic: Option<String>,
}

/// Extract knowledge capsules from a conversation
pub fn extract_capsules(
    conversation: &ConversationExport,
    llm: &dyn LlmClient,
) -> Result<Vec<KnowledgeCapsule>> {
    let dialogue_text = format_dialogue_for_extraction(conversation);

    // Skip very short conversations
    if dialogue_text.len() < 100 {
        return Ok(vec![]);
    }

    let prompt = build_extraction_prompt(&dialogue_text);
    let response = llm.complete(&prompt)?;

    parse_capsule_response(&response)
}

/// Format conversation for capsule extraction
fn format_dialogue_for_extraction(conversation: &ConversationExport) -> String {
    let mut text = String::new();

    for msg in &conversation.chat_messages {
        let sender = match msg.sender {
            crate::parser::MessageSender::Human => "Human",
            crate::parser::MessageSender::Assistant => "Assistant",
        };

        // Include timestamp if available
        text.push_str(&format!("[{}] {}: {}\n\n", msg.created_at, sender, msg.get_text()));
    }

    text
}

/// Build the SimpleMem-style extraction prompt
fn build_extraction_prompt(dialogue_text: &str) -> String {
    // Truncate to avoid token limits while keeping enough context
    let truncated = if dialogue_text.len() > 12000 {
        &dialogue_text[..12000]
    } else {
        dialogue_text
    };

    format!(r#"Extract all valuable information from the following conversation and convert them into structured knowledge capsules.

[Conversation]
{truncated}

[Requirements]
1. **Complete Coverage**: Generate enough capsules to capture ALL meaningful information
2. **Force Disambiguation**: NEVER use pronouns (he, she, it, they, this, that) or relative time (yesterday, today, tomorrow, last week). Always use specific names and absolute dates.
3. **Lossless Information**: Each capsule's restatement must be a complete, independent, understandable sentence
4. **Precise Extraction**:
   - keywords: Core keywords (names, places, entities, topic words)
   - timestamp: Absolute time in ISO 8601 format (YYYY-MM-DDTHH:MM:SS) if time is mentioned
   - location: Specific location name if mentioned
   - persons: All person names mentioned
   - entities: Companies, products, organizations, concepts
   - topic: The topic category of this information

[Output Format]
Return a JSON object with a "capsules" array:

```json
{{
  "capsules": [
    {{
      "lossless_restatement": "Complete unambiguous restatement with all subjects, objects, times, locations",
      "keywords": ["keyword1", "keyword2"],
      "timestamp": "YYYY-MM-DDTHH:MM:SS or null",
      "location": "location name or null",
      "persons": ["name1", "name2"],
      "entities": ["entity1", "entity2"],
      "topic": "topic phrase or null"
    }}
  ]
}}
```

[Example]
Input dialogue:
[2025-11-15T14:30:00] Human: Hey, let's meet at Starbucks tomorrow at 2pm to discuss the new project
[2025-11-15T14:31:00] Assistant: Sure, I'll prepare the materials

Output:
```json
{{
  "capsules": [
    {{
      "lossless_restatement": "The user proposed meeting at Starbucks on 2025-11-16 at 14:00:00 to discuss a new project.",
      "keywords": ["meeting", "Starbucks", "project", "discussion"],
      "timestamp": "2025-11-16T14:00:00",
      "location": "Starbucks",
      "persons": [],
      "entities": ["new project"],
      "topic": "Meeting arrangement"
    }},
    {{
      "lossless_restatement": "The assistant agreed to attend the meeting and will prepare materials.",
      "keywords": ["materials", "preparation", "agreement"],
      "timestamp": null,
      "location": null,
      "persons": [],
      "entities": [],
      "topic": "Meeting preparation"
    }}
  ]
}}
```

Now process the conversation above. Return ONLY the JSON object, no other text."#)
}

/// Parse the LLM response into KnowledgeCapsules
fn parse_capsule_response(response: &str) -> Result<Vec<KnowledgeCapsule>> {
    // Try to extract JSON from response (handle markdown code blocks)
    let json_str = extract_json_from_response(response);

    let parsed: CapsuleExtractionResponse = serde_json::from_str(&json_str)
        .context("Failed to parse capsule extraction response")?;

    let capsules = parsed.capsules
        .into_iter()
        .map(|raw| KnowledgeCapsule {
            restatement: raw.lossless_restatement,
            keywords: raw.keywords,
            timestamp: raw.timestamp,
            location: raw.location,
            persons: raw.persons,
            entities: raw.entities.into_iter().map(|e| (e, None)).collect(),
            topic: raw.topic,
        })
        .collect();

    Ok(capsules)
}

/// Extract JSON from LLM response (handles markdown code blocks)
fn extract_json_from_response(response: &str) -> String {
    let trimmed = response.trim();

    // Check for markdown code block
    if trimmed.starts_with("```json") {
        if let Some(end) = trimmed.rfind("```") {
            let start = trimmed.find('\n').unwrap_or(7) + 1;
            if start < end {
                return trimmed[start..end].trim().to_string();
            }
        }
    }

    // Check for just ``` block
    if trimmed.starts_with("```") {
        if let Some(end) = trimmed.rfind("```") {
            let start = trimmed.find('\n').unwrap_or(3) + 1;
            if start < end {
                return trimmed[start..end].trim().to_string();
            }
        }
    }

    // Try to find JSON object directly
    if let Some(start) = trimmed.find('{') {
        if let Some(end) = trimmed.rfind('}') {
            return trimmed[start..=end].to_string();
        }
    }

    trimmed.to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_json_from_response() {
        // Test markdown code block
        let response = r#"```json
{"capsules": []}
```"#;
        assert_eq!(extract_json_from_response(response), r#"{"capsules": []}"#);

        // Test plain JSON
        let response = r#"{"capsules": []}"#;
        assert_eq!(extract_json_from_response(response), r#"{"capsules": []}"#);

        // Test with extra text
        let response = r#"Here is the result:
{"capsules": []}
Done!"#;
        assert_eq!(extract_json_from_response(response), r#"{"capsules": []}"#);
    }

    #[test]
    fn test_parse_capsule_response() {
        let response = r#"{"capsules": [
            {
                "lossless_restatement": "Test fact about something.",
                "keywords": ["test", "fact"],
                "timestamp": "2026-01-12T10:00:00",
                "location": null,
                "persons": ["Alice"],
                "entities": ["TestCorp"],
                "topic": "Testing"
            }
        ]}"#;

        let capsules = parse_capsule_response(response).unwrap();
        assert_eq!(capsules.len(), 1);
        assert_eq!(capsules[0].restatement, "Test fact about something.");
        assert_eq!(capsules[0].persons, vec!["Alice"]);
        assert_eq!(capsules[0].topic, Some("Testing".to_string()));
    }
}
