use anyhow::{Context, Result};
use std::path::Path;

use crate::db::Database;
use crate::parser::ConversationExport;

/// Result of an ingestion attempt
#[derive(Debug, Clone, PartialEq)]
pub enum IngestResult {
    /// Conversation was successfully ingested
    Ingested {
        conversation_id: String,
        message_count: usize,
    },
    /// Conversation was already processed (duplicate)
    Duplicate {
        conversation_id: String,
    },
    /// Conversation was skipped (e.g., empty)
    Skipped {
        reason: String,
    },
}

/// Ingest a conversation export file
pub fn ingest_conversation<P: AsRef<Path>>(
    db: &Database,
    file_path: P,
) -> Result<IngestResult> {
    let path = file_path.as_ref();

    // Parse the conversation export
    let export = ConversationExport::from_file(path)
        .context("Failed to parse conversation export")?;

    // Get filename for record
    let filename = path
        .file_name()
        .and_then(|f| f.to_str())
        .unwrap_or("unknown.json");

    ingest_conversation_export(db, &export, filename)
}

/// Ingest a parsed ConversationExport struct directly
pub fn ingest_conversation_export(
    db: &Database,
    export: &ConversationExport,
    filename: &str,
) -> Result<IngestResult> {
    // Check if conversation is empty
    if export.message_count() == 0 {
        return Ok(IngestResult::Skipped {
            reason: "Conversation has no messages".to_string(),
        });
    }

    // Check for duplicates
    if db.is_conversation_processed(&export.uuid)? {
        return Ok(IngestResult::Duplicate {
            conversation_id: export.uuid.clone(),
        });
    }

    // Extract timestamps
    let first_timestamp = export.first_message_timestamp()?.timestamp();
    let last_timestamp = export.last_message_timestamp()?.timestamp();
    let message_count = export.message_count() as i64;

    // Insert into database
    db.insert_conversation(
        &export.uuid,
        filename,
        first_timestamp,
        last_timestamp,
        message_count,
    )?;

    Ok(IngestResult::Ingested {
        conversation_id: export.uuid.clone(),
        message_count: export.message_count(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::NamedTempFile;

    const SAMPLE_EXPORT: &str = r#"{
  "uuid": "test-conv-123",
  "name": "Test Conversation",
  "created_at": "2025-01-01T10:00:00Z",
  "updated_at": "2025-01-01T10:30:00Z",
  "chat_messages": [
    {
      "uuid": "msg-1",
      "text": "Hello!",
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
}"#;

    fn create_temp_export(json: &str) -> NamedTempFile {
        let mut file = NamedTempFile::new().unwrap();
        file.write_all(json.as_bytes()).unwrap();
        file.flush().unwrap();
        file
    }

    #[test]
    fn test_ingest_new_conversation() {
        let db = Database::in_memory().unwrap();
        let file = create_temp_export(SAMPLE_EXPORT);

        let result = ingest_conversation(&db, file.path()).unwrap();

        match result {
            IngestResult::Ingested { conversation_id, message_count } => {
                assert_eq!(conversation_id, "test-conv-123");
                assert_eq!(message_count, 2);
            }
            _ => panic!("Expected Ingested result, got {:?}", result),
        }

        // Verify it was stored in database
        assert!(db.is_conversation_processed("test-conv-123").unwrap());
    }

    #[test]
    fn test_ingest_duplicate_conversation() {
        let db = Database::in_memory().unwrap();
        let file = create_temp_export(SAMPLE_EXPORT);

        // Ingest once
        let result1 = ingest_conversation(&db, file.path()).unwrap();
        assert!(matches!(result1, IngestResult::Ingested { .. }));

        // Ingest again - should be duplicate
        let result2 = ingest_conversation(&db, file.path()).unwrap();

        match result2 {
            IngestResult::Duplicate { conversation_id } => {
                assert_eq!(conversation_id, "test-conv-123");
            }
            _ => panic!("Expected Duplicate result, got {:?}", result2),
        }
    }

    #[test]
    fn test_ingest_empty_conversation() {
        let db = Database::in_memory().unwrap();

        let empty_json = r#"{
  "uuid": "empty-conv",
  "name": "Empty",
  "created_at": "2025-01-01T10:00:00Z",
  "updated_at": "2025-01-01T10:00:00Z",
  "chat_messages": []
}"#;

        let file = create_temp_export(empty_json);
        let result = ingest_conversation(&db, file.path()).unwrap();

        match result {
            IngestResult::Skipped { reason } => {
                assert!(reason.contains("no messages"));
            }
            _ => panic!("Expected Skipped result, got {:?}", result),
        }

        // Should not be in database
        assert!(!db.is_conversation_processed("empty-conv").unwrap());
    }

    #[test]
    fn test_ingest_invalid_file() {
        let db = Database::in_memory().unwrap();
        let file = create_temp_export("{ invalid json }");

        let result = ingest_conversation(&db, file.path());
        assert!(result.is_err());
    }

    #[test]
    fn test_ingest_nonexistent_file() {
        let db = Database::in_memory().unwrap();
        let result = ingest_conversation(&db, "/nonexistent/file.json");
        assert!(result.is_err());
    }

    #[test]
    fn test_multiple_conversations() {
        let db = Database::in_memory().unwrap();

        // Create two different conversations
        let json1 = r#"{
  "uuid": "conv-1",
  "name": "First",
  "created_at": "2025-01-01T10:00:00Z",
  "updated_at": "2025-01-01T10:00:00Z",
  "chat_messages": [
    {
      "uuid": "msg-1",
      "text": "Message 1",
      "sender": "human",
      "created_at": "2025-01-01T10:00:00Z"
    }
  ]
}"#;

        let json2 = r#"{
  "uuid": "conv-2",
  "name": "Second",
  "created_at": "2025-01-01T11:00:00Z",
  "updated_at": "2025-01-01T11:00:00Z",
  "chat_messages": [
    {
      "uuid": "msg-2",
      "text": "Message 2",
      "sender": "human",
      "created_at": "2025-01-01T11:00:00Z"
    }
  ]
}"#;

        let file1 = create_temp_export(json1);
        let file2 = create_temp_export(json2);

        // Ingest both
        let result1 = ingest_conversation(&db, file1.path()).unwrap();
        let result2 = ingest_conversation(&db, file2.path()).unwrap();

        assert!(matches!(result1, IngestResult::Ingested { .. }));
        assert!(matches!(result2, IngestResult::Ingested { .. }));

        // Both should be in database
        assert!(db.is_conversation_processed("conv-1").unwrap());
        assert!(db.is_conversation_processed("conv-2").unwrap());
    }
}
