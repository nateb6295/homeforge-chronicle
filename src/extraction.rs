use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

use crate::llm::LlmClient;
use crate::parser::ConversationExport;

/// Classification types for conversations
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum Classification {
    Prediction,
    Insight,
    Milestone,
    Reflection,
    Technical,
}

impl std::fmt::Display for Classification {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Classification::Prediction => write!(f, "prediction"),
            Classification::Insight => write!(f, "insight"),
            Classification::Milestone => write!(f, "milestone"),
            Classification::Reflection => write!(f, "reflection"),
            Classification::Technical => write!(f, "technical"),
        }
    }
}

/// Extracted content from a conversation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Extraction {
    pub conversation_id: String,
    pub title: String,
    pub summary: String,
    pub themes: Vec<String>,
    pub classification: Classification,
    pub confidence_score: f64,
    pub key_quotes: Vec<String>,
}

/// Extract content from a conversation using LLM
pub fn extract_conversation(
    conversation: &ConversationExport,
    llm: &dyn LlmClient,
    known_themes: &[String],
) -> Result<Extraction> {
    // Build conversation text for analysis
    let conversation_text = format_conversation_for_analysis(conversation);

    // Extract themes
    let themes = extract_themes(&conversation_text, llm, known_themes)?;

    // Generate title
    let title = generate_title(&conversation_text, llm)?;

    // Generate summary (preserving voice)
    let summary = generate_summary(&conversation_text, llm)?;

    // Classify conversation
    let classification = classify_conversation(&conversation_text, llm)?;

    // Extract key quotes
    let key_quotes = extract_key_quotes(&conversation_text, llm)?;

    // Calculate confidence score
    let confidence_score = calculate_confidence_score(conversation);

    Ok(Extraction {
        conversation_id: conversation.uuid.clone(),
        title,
        summary,
        themes,
        classification,
        confidence_score,
        key_quotes,
    })
}

/// Format conversation for LLM analysis
fn format_conversation_for_analysis(conversation: &ConversationExport) -> String {
    let mut text = String::new();
    text.push_str(&format!("Conversation: {}\n\n", conversation.name));

    for (i, msg) in conversation.chat_messages.iter().enumerate() {
        let sender = match msg.sender {
            crate::parser::MessageSender::Human => "Human",
            crate::parser::MessageSender::Assistant => "Assistant",
        };
        text.push_str(&format!("[{}] {}: {}\n\n", i + 1, sender, msg.text));
    }

    text
}

/// Extract themes from conversation
fn extract_themes(
    conversation_text: &str,
    llm: &dyn LlmClient,
    known_themes: &[String],
) -> Result<Vec<String>> {
    let prompt = format!(
        r#"Analyze this conversation and identify the primary themes discussed.

Known themes to consider: {}

Conversation:
{}

Return ONLY a JSON array of theme names (1-3 themes). Use known themes when applicable, or suggest new ones if the conversation covers different topics.

Format: ["theme1", "theme2"]

Your response:"#,
        known_themes.join(", "),
        conversation_text.chars().take(8000).collect::<String>()
    );

    let response = llm.complete(&prompt)?;

    // Parse JSON response
    let themes: Vec<String> = serde_json::from_str(response.trim())
        .context("Failed to parse themes from LLM response")?;

    Ok(themes.into_iter().take(3).collect())
}

/// Generate a concise title
fn generate_title(conversation_text: &str, llm: &dyn LlmClient) -> Result<String> {
    let prompt = format!(
        r#"Generate a concise title (max 10 words) for this conversation that captures the main topic.

Conversation:
{}

Return ONLY the title, nothing else.

Your response:"#,
        conversation_text.chars().take(6000).collect::<String>()
    );

    let response = llm.complete(&prompt)?;
    Ok(response.trim().trim_matches('"').to_string())
}

/// Generate a summary preserving original voice
fn generate_summary(conversation_text: &str, llm: &dyn LlmClient) -> Result<String> {
    let prompt = format!(
        r#"Write a 2-3 sentence summary of this conversation. IMPORTANT: Preserve the original voice and texture of the thinking. Don't flatten it into corporate blog voice. Keep the authentic, personal tone.

Conversation:
{}

Your summary:"#,
        conversation_text.chars().take(10000).collect::<String>()
    );

    let response = llm.complete(&prompt)?;
    Ok(response.trim().to_string())
}

/// Classify the conversation type
fn classify_conversation(conversation_text: &str, llm: &dyn LlmClient) -> Result<Classification> {
    let prompt = format!(
        r#"Classify this conversation into ONE of these categories:

- prediction: Explicit claims about future events with implied or explicit timeline
- insight: Novel connection or pattern recognition
- milestone: Project completion, infrastructure change, significant decision
- reflection: Personal processing, emotional content, life context
- technical: Build logs, configuration details, troubleshooting

Conversation:
{}

Return ONLY one word: prediction, insight, milestone, reflection, or technical.

Your response:"#,
        conversation_text.chars().take(6000).collect::<String>()
    );

    let response = llm.complete(&prompt)?;
    let classification_str = response.trim().to_lowercase();

    match classification_str.as_str() {
        "prediction" => Ok(Classification::Prediction),
        "insight" => Ok(Classification::Insight),
        "milestone" => Ok(Classification::Milestone),
        "reflection" => Ok(Classification::Reflection),
        "technical" => Ok(Classification::Technical),
        _ => {
            // Default to technical if unclear
            Ok(Classification::Technical)
        }
    }
}

/// Extract key quotes from conversation
fn extract_key_quotes(conversation_text: &str, llm: &dyn LlmClient) -> Result<Vec<String>> {
    let prompt = format!(
        r#"Extract 1-3 key quotes from this conversation that capture the essential thinking. Return verbatim excerpts.

Conversation:
{}

Return ONLY a JSON array of quoted strings.

Format: ["quote 1", "quote 2"]

Your response:"#,
        conversation_text.chars().take(10000).collect::<String>()
    );

    let response = llm.complete(&prompt)?;

    // Parse JSON response
    let quotes: Vec<String> = serde_json::from_str(response.trim())
        .context("Failed to parse quotes from LLM response")?;

    Ok(quotes.into_iter().take(3).collect())
}

/// Calculate confidence score based on conversation characteristics
fn calculate_confidence_score(conversation: &ConversationExport) -> f64 {
    let message_count = conversation.message_count() as f64;
    let total_chars: usize = conversation
        .chat_messages
        .iter()
        .map(|m| m.text.len())
        .sum();
    let avg_message_length = total_chars as f64 / message_count;

    // Score based on depth and substance
    let mut score: f64 = 0.5; // Base score

    // More messages generally means more substantive
    if message_count > 10.0 {
        score += 0.2;
    } else if message_count > 5.0 {
        score += 0.1;
    }

    // Longer messages suggest deeper thinking
    if avg_message_length > 500.0 {
        score += 0.2;
    } else if avg_message_length > 200.0 {
        score += 0.1;
    }

    // Cap at 1.0
    score.min(1.0_f64)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::llm::MockLlmClient;
    use crate::parser::{ChatMessage, MessageSender};

    fn create_test_conversation() -> ConversationExport {
        ConversationExport {
            uuid: "test-123".to_string(),
            name: "Test Conversation".to_string(),
            summary: None,
            created_at: "2025-01-01T10:00:00Z".to_string(),
            updated_at: "2025-01-01T10:30:00Z".to_string(),
            chat_messages: vec![
                ChatMessage {
                    uuid: "msg-1".to_string(),
                    text: "I have an insight about lattice theory and how it applies to homeforge architecture.".to_string(),
                    sender: MessageSender::Human,
                    created_at: "2025-01-01T10:00:00Z".to_string(),
                    updated_at: None,
                    attachments: vec![],
                },
                ChatMessage {
                    uuid: "msg-2".to_string(),
                    text: "That's fascinating! Tell me more about this connection.".to_string(),
                    sender: MessageSender::Assistant,
                    created_at: "2025-01-01T10:00:30Z".to_string(),
                    updated_at: None,
                    attachments: vec![],
                },
            ],
        }
    }

    #[test]
    fn test_format_conversation() {
        let conv = create_test_conversation();
        let formatted = format_conversation_for_analysis(&conv);

        assert!(formatted.contains("Test Conversation"));
        assert!(formatted.contains("Human"));
        assert!(formatted.contains("Assistant"));
        assert!(formatted.contains("lattice theory"));
    }

    #[test]
    fn test_extract_themes() {
        let mock_llm = MockLlmClient::new(vec![r#"["lattice", "homeforge"]"#.to_string()]);

        let conversation_text = "Discussion about lattice and homeforge";
        let known_themes = vec!["lattice".to_string(), "homeforge".to_string()];

        let themes = extract_themes(conversation_text, &mock_llm, &known_themes);
        assert!(themes.is_ok());

        let themes = themes.unwrap();
        assert_eq!(themes.len(), 2);
        assert!(themes.contains(&"lattice".to_string()));
        assert!(themes.contains(&"homeforge".to_string()));
    }

    #[test]
    fn test_generate_title() {
        let mock_llm = MockLlmClient::new(vec!["Lattice Theory in Homeforge".to_string()]);

        let conversation_text = "Discussion about lattice";
        let title = generate_title(conversation_text, &mock_llm);

        assert!(title.is_ok());
        assert_eq!(title.unwrap(), "Lattice Theory in Homeforge");
    }

    #[test]
    fn test_classify_conversation() {
        let mock_llm = MockLlmClient::new(vec!["insight".to_string()]);

        let conversation_text = "I realized something interesting";
        let classification = classify_conversation(conversation_text, &mock_llm);

        assert!(classification.is_ok());
        assert_eq!(classification.unwrap(), Classification::Insight);
    }

    #[test]
    fn test_classification_display() {
        assert_eq!(Classification::Prediction.to_string(), "prediction");
        assert_eq!(Classification::Insight.to_string(), "insight");
        assert_eq!(Classification::Milestone.to_string(), "milestone");
        assert_eq!(Classification::Reflection.to_string(), "reflection");
        assert_eq!(Classification::Technical.to_string(), "technical");
    }

    #[test]
    fn test_extract_key_quotes() {
        let mock_llm = MockLlmClient::new(vec![
            r#"["This is a key insight", "Another important point"]"#.to_string(),
        ]);

        let conversation_text = "Some conversation text";
        let quotes = extract_key_quotes(conversation_text, &mock_llm);

        assert!(quotes.is_ok());
        let quotes = quotes.unwrap();
        assert_eq!(quotes.len(), 2);
    }

    #[test]
    fn test_confidence_score_calculation() {
        let conv = create_test_conversation();
        let score = calculate_confidence_score(&conv);

        assert!(score >= 0.0);
        assert!(score <= 1.0);
    }

    #[test]
    fn test_full_extraction() {
        let mock_llm = MockLlmClient::new(vec![
            r#"["lattice", "homeforge"]"#.to_string(), // themes
            "Lattice Theory Application".to_string(),  // title
            "Discussion about applying lattice theory to homeforge architecture.".to_string(), // summary
            "insight".to_string(),                      // classification
            r#"["I have an insight about lattice theory"]"#.to_string(), // quotes
        ]);

        let conv = create_test_conversation();
        let known_themes = vec!["lattice".to_string(), "homeforge".to_string()];

        let extraction = extract_conversation(&conv, &mock_llm, &known_themes);
        assert!(extraction.is_ok());

        let extraction = extraction.unwrap();
        assert_eq!(extraction.conversation_id, "test-123");
        assert!(!extraction.title.is_empty());
        assert!(!extraction.summary.is_empty());
        assert!(!extraction.themes.is_empty());
        assert_eq!(extraction.classification, Classification::Insight);
        assert!(extraction.confidence_score > 0.0);
    }
}
