use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::path::Path;

/// Claude conversation export structure (single conversation)
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ConversationExport {
    pub uuid: String,
    pub name: String,
    #[serde(default)]
    pub summary: Option<String>,
    pub created_at: String,
    pub updated_at: String,
    pub chat_messages: Vec<ChatMessage>,
}

/// Claude bulk export structure (direct array of conversations)
pub type BulkConversationExport = Vec<ConversationExport>;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ChatMessage {
    pub uuid: String,
    pub text: String,
    pub sender: MessageSender,
    pub created_at: String,
    #[serde(default)]
    pub updated_at: Option<String>,
    #[serde(default)]
    pub attachments: Vec<Attachment>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum MessageSender {
    Human,
    Assistant,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Attachment {
    pub file_name: String,
    pub file_type: String,
    pub file_size: i64,
    #[serde(default)]
    pub extracted_content: Option<String>,
}

impl ConversationExport {
    /// Parse a conversation export from a JSON file
    pub fn from_file<P: AsRef<Path>>(path: P) -> Result<Self> {
        let contents = std::fs::read_to_string(path.as_ref())
            .context("Failed to read conversation export file")?;
        Self::from_str(&contents)
    }

    /// Parse a conversation export from a JSON string
    pub fn from_str(json: &str) -> Result<Self> {
        serde_json::from_str(json).context("Failed to parse conversation JSON")
    }

    /// Get the first message timestamp
    pub fn first_message_timestamp(&self) -> Result<DateTime<Utc>> {
        self.chat_messages
            .first()
            .ok_or_else(|| anyhow::anyhow!("No messages in conversation"))
            .and_then(|msg| parse_timestamp(&msg.created_at))
    }

    /// Get the last message timestamp
    pub fn last_message_timestamp(&self) -> Result<DateTime<Utc>> {
        self.chat_messages
            .last()
            .ok_or_else(|| anyhow::anyhow!("No messages in conversation"))
            .and_then(|msg| parse_timestamp(&msg.created_at))
    }

    /// Count total messages
    pub fn message_count(&self) -> usize {
        self.chat_messages.len()
    }

    /// Estimate token count (rough approximation: 1 token ≈ 4 characters)
    pub fn estimated_tokens(&self) -> usize {
        let total_chars: usize = self
            .chat_messages
            .iter()
            .map(|msg| msg.text.len())
            .sum();
        total_chars / 4
    }

    /// Get all messages from human
    pub fn human_messages(&self) -> Vec<&ChatMessage> {
        self.chat_messages
            .iter()
            .filter(|msg| matches!(msg.sender, MessageSender::Human))
            .collect()
    }

    /// Get all messages from assistant
    pub fn assistant_messages(&self) -> Vec<&ChatMessage> {
        self.chat_messages
            .iter()
            .filter(|msg| matches!(msg.sender, MessageSender::Assistant))
            .collect()
    }
}

/// Parse a bulk conversation export from a JSON file
pub fn parse_bulk_export<P: AsRef<Path>>(path: P) -> Result<BulkConversationExport> {
    let contents = std::fs::read_to_string(path.as_ref())
        .context("Failed to read bulk conversation export file")?;
    parse_bulk_export_str(&contents)
}

/// Parse a bulk conversation export from a JSON string
pub fn parse_bulk_export_str(json: &str) -> Result<BulkConversationExport> {
    serde_json::from_str(json).context("Failed to parse bulk conversation JSON")
}

/// Parse ISO 8601 timestamp string to DateTime<Utc>
fn parse_timestamp(timestamp: &str) -> Result<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(timestamp)
        .context("Failed to parse timestamp")
        .map(|dt| dt.with_timezone(&Utc))
}

#[cfg(test)]
mod tests {
    use super::*;

    const SAMPLE_EXPORT: &str = r#"{
  "uuid": "test-conv-123",
  "name": "Test Conversation",
  "created_at": "2025-01-01T10:00:00Z",
  "updated_at": "2025-01-01T10:30:00Z",
  "chat_messages": [
    {
      "uuid": "msg-1",
      "text": "Hello, can you help me with Rust?",
      "sender": "human",
      "created_at": "2025-01-01T10:00:00Z"
    },
    {
      "uuid": "msg-2",
      "text": "Of course! I'd be happy to help you with Rust. What would you like to know?",
      "sender": "assistant",
      "created_at": "2025-01-01T10:00:30Z"
    },
    {
      "uuid": "msg-3",
      "text": "How do I parse JSON?",
      "sender": "human",
      "created_at": "2025-01-01T10:01:00Z"
    }
  ]
}"#;

    #[test]
    fn test_parse_conversation_export() {
        let export = ConversationExport::from_str(SAMPLE_EXPORT);
        assert!(export.is_ok(), "Failed to parse: {:?}", export.err());

        let export = export.unwrap();
        assert_eq!(export.uuid, "test-conv-123");
        assert_eq!(export.name, "Test Conversation");
        assert_eq!(export.chat_messages.len(), 3);
    }

    #[test]
    fn test_message_count() {
        let export = ConversationExport::from_str(SAMPLE_EXPORT).unwrap();
        assert_eq!(export.message_count(), 3);
    }

    #[test]
    fn test_first_and_last_timestamps() {
        let export = ConversationExport::from_str(SAMPLE_EXPORT).unwrap();

        let first = export.first_message_timestamp().unwrap();
        let last = export.last_message_timestamp().unwrap();

        assert!(last > first);
    }

    #[test]
    fn test_filter_by_sender() {
        let export = ConversationExport::from_str(SAMPLE_EXPORT).unwrap();

        let human_msgs = export.human_messages();
        let assistant_msgs = export.assistant_messages();

        assert_eq!(human_msgs.len(), 2);
        assert_eq!(assistant_msgs.len(), 1);
        assert_eq!(human_msgs[0].text, "Hello, can you help me with Rust?");
        assert!(assistant_msgs[0].text.contains("happy to help"));
    }

    #[test]
    fn test_estimated_tokens() {
        let export = ConversationExport::from_str(SAMPLE_EXPORT).unwrap();
        let tokens = export.estimated_tokens();

        // Should have some reasonable token estimate
        assert!(tokens > 10);
        assert!(tokens < 1000);
    }

    #[test]
    fn test_parse_with_attachments() {
        let json_with_attachment = r#"{
  "uuid": "test-conv-456",
  "name": "Conversation with Attachment",
  "created_at": "2025-01-01T10:00:00Z",
  "updated_at": "2025-01-01T10:30:00Z",
  "chat_messages": [
    {
      "uuid": "msg-1",
      "text": "Here's a file",
      "sender": "human",
      "created_at": "2025-01-01T10:00:00Z",
      "attachments": [
        {
          "file_name": "test.txt",
          "file_type": "text/plain",
          "file_size": 1024,
          "extracted_content": "File content here"
        }
      ]
    }
  ]
}"#;

        let export = ConversationExport::from_str(json_with_attachment);
        assert!(export.is_ok());

        let export = export.unwrap();
        assert_eq!(export.chat_messages[0].attachments.len(), 1);
        assert_eq!(export.chat_messages[0].attachments[0].file_name, "test.txt");
    }

    #[test]
    fn test_empty_conversation_fails() {
        let json = r#"{
  "uuid": "test-conv-789",
  "name": "Empty Conversation",
  "created_at": "2025-01-01T10:00:00Z",
  "updated_at": "2025-01-01T10:30:00Z",
  "chat_messages": []
}"#;

        let export = ConversationExport::from_str(json).unwrap();

        // Should fail when trying to get timestamps from empty conversation
        assert!(export.first_message_timestamp().is_err());
        assert!(export.last_message_timestamp().is_err());
        assert_eq!(export.message_count(), 0);
    }

    #[test]
    fn test_invalid_json_fails() {
        let invalid_json = "{ invalid json }";
        let result = ConversationExport::from_str(invalid_json);
        assert!(result.is_err());
    }

    #[test]
    fn test_missing_required_fields() {
        let json = r#"{
  "uuid": "test-conv-999",
  "name": "Incomplete"
}"#;
        let result = ConversationExport::from_str(json);
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_bulk_export() {
        let bulk_json = r#"[
    {
      "uuid": "conv-1",
      "name": "First Conversation",
      "summary": "A test conversation",
      "created_at": "2025-01-01T10:00:00Z",
      "updated_at": "2025-01-01T10:30:00Z",
      "chat_messages": [
        {
          "uuid": "msg-1",
          "text": "Hello",
          "sender": "human",
          "created_at": "2025-01-01T10:00:00Z"
        },
        {
          "uuid": "msg-2",
          "text": "Hi there!",
          "sender": "assistant",
          "created_at": "2025-01-01T10:00:30Z"
        }
      ]
    },
    {
      "uuid": "conv-2",
      "name": "Second Conversation",
      "created_at": "2025-01-02T10:00:00Z",
      "updated_at": "2025-01-02T10:30:00Z",
      "chat_messages": [
        {
          "uuid": "msg-3",
          "text": "Another conversation",
          "sender": "human",
          "created_at": "2025-01-02T10:00:00Z"
        }
      ]
    }
]"#;

        let bulk_export = parse_bulk_export_str(bulk_json);
        assert!(bulk_export.is_ok(), "Failed to parse: {:?}", bulk_export.err());

        let bulk_export = bulk_export.unwrap();
        assert_eq!(bulk_export.len(), 2);
        assert_eq!(bulk_export.iter().map(|c| c.message_count()).sum::<usize>(), 3);
        assert_eq!(bulk_export[0].uuid, "conv-1");
        assert_eq!(bulk_export[0].summary, Some("A test conversation".to_string()));
        assert_eq!(bulk_export[1].uuid, "conv-2");
        assert_eq!(bulk_export[1].summary, None);
    }

    #[test]
    fn test_empty_bulk_export() {
        let bulk_json = r#"[]"#;
        let bulk_export = parse_bulk_export_str(bulk_json);
        assert!(bulk_export.is_ok());

        let bulk_export = bulk_export.unwrap();
        assert_eq!(bulk_export.len(), 0);
    }
}
