use homeforge_chronicle::config::{Config, LlmBackend};
use homeforge_chronicle::db::Database;
use std::path::PathBuf;
use tempfile::TempDir;

#[test]
fn test_config_round_trip() {
    let temp_dir = TempDir::new().unwrap();
    let config_path = temp_dir.path().join("test_config.toml");

    // Create and write config
    let config = Config::default_config();
    config.to_file(&config_path).unwrap();

    // Read it back
    let loaded_config = Config::from_file(&config_path).unwrap();

    assert_eq!(config, loaded_config);
}

#[test]
fn test_database_persistence() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test.db");

    // Create database
    {
        let db = Database::new(&db_path).unwrap();
        let theme_id = db.get_or_create_theme("test-theme").unwrap();
        assert!(theme_id > 0);
    }

    // Reopen database
    {
        let db = Database::new(&db_path).unwrap();
        let theme_id = db.get_or_create_theme("test-theme").unwrap();
        assert_eq!(theme_id, 1); // Should be the same ID
    }
}

#[test]
fn test_full_phase_1_workflow() {
    let temp_dir = TempDir::new().unwrap();

    // 1. Create config
    let mut config = Config::default_config();
    config.input.watch_directory = temp_dir.path().join("exports");
    config.input.processed_db = temp_dir.path().join("chronicle.db");
    config.output.build_directory = temp_dir.path().join("build");

    // 2. Verify config serialization
    let config_path = temp_dir.path().join("chronicle.toml");
    config.to_file(&config_path).unwrap();
    assert!(config_path.exists());

    // 3. Initialize database
    let db = Database::new(&config.input.processed_db).unwrap();

    // 4. Test basic database operations
    assert!(!db.is_conversation_processed("test-conv-1").unwrap());

    // Insert a conversation
    db.connection().execute(
        "INSERT INTO conversations (id, export_filename, first_message_at, last_message_at, message_count, processed_at)
         VALUES (?, ?, ?, ?, ?, ?)",
        rusqlite::params!["test-conv-1", "export1.json", 1000, 2000, 10, 3000],
    ).unwrap();

    assert!(db.is_conversation_processed("test-conv-1").unwrap());

    // 5. Test theme management
    let theme_id = db.get_or_create_theme("homeforge").unwrap();
    assert!(theme_id > 0);

    // 6. Verify config reloading
    let reloaded_config = Config::from_file(&config_path).unwrap();
    assert_eq!(config, reloaded_config);
}

#[test]
fn test_config_with_custom_values() {
    let toml_str = r#"
[input]
watch_directory = "/custom/exports"
processed_db = "/custom/db/chronicle.db"

[extraction]
llm_backend = "ollama"
llm_model = "llama2"
themes = ["custom", "themes"]

[output]
build_directory = "/custom/build"
site_title = "Custom Title"
author = "Custom Author"

[deployment]
canister_id = "custom-canister"
network = "local"

[schedule]
auto_deploy = false
"#;

    let config = Config::from_str(toml_str).unwrap();

    assert_eq!(config.input.watch_directory, PathBuf::from("/custom/exports"));
    assert_eq!(config.extraction.llm_backend, LlmBackend::Ollama);
    assert_eq!(config.extraction.llm_model, "llama2");
    assert_eq!(config.extraction.themes, vec!["custom", "themes"]);
    assert_eq!(config.output.site_title, "Custom Title");
    assert_eq!(config.deployment.network, "local");
    assert!(!config.schedule.auto_deploy);
}

#[test]
fn test_database_schema_integrity() {
    let db = Database::in_memory().unwrap();

    // Test that we can create a full data flow
    // 1. Insert conversation
    db.connection().execute(
        "INSERT INTO conversations (id, export_filename, first_message_at, last_message_at, message_count, processed_at)
         VALUES (?, ?, ?, ?, ?, ?)",
        rusqlite::params!["conv-1", "export.json", 1000, 2000, 5, 3000],
    ).unwrap();

    // 2. Insert extraction
    db.connection().execute(
        "INSERT INTO extractions (conversation_id, title, summary, classification, confidence_score, created_at)
         VALUES (?, ?, ?, ?, ?, ?)",
        rusqlite::params!["conv-1", "Test Title", "Test summary", "insight", 0.85, 4000],
    ).unwrap();

    let extraction_id = db.connection().last_insert_rowid();

    // 3. Create theme and link it
    let theme_id = db.get_or_create_theme("test-theme").unwrap();
    db.connection().execute(
        "INSERT INTO extraction_themes (extraction_id, theme_id) VALUES (?, ?)",
        rusqlite::params![extraction_id, theme_id],
    ).unwrap();

    // 4. Add quote
    db.connection().execute(
        "INSERT INTO quotes (extraction_id, content) VALUES (?, ?)",
        rusqlite::params![extraction_id, "This is a test quote"],
    ).unwrap();

    // 5. Add prediction
    db.connection().execute(
        "INSERT INTO predictions (extraction_id, claim, date_made, timeline, status)
         VALUES (?, ?, ?, ?, ?)",
        rusqlite::params![extraction_id, "Test prediction", 5000, "2025 Q1", "pending"],
    ).unwrap();

    // Verify data was inserted correctly
    let mut stmt = db.connection().prepare(
        "SELECT e.title, t.name, q.content, p.claim
         FROM extractions e
         JOIN extraction_themes et ON e.id = et.extraction_id
         JOIN themes t ON et.theme_id = t.id
         JOIN quotes q ON e.id = q.extraction_id
         JOIN predictions p ON e.id = p.extraction_id
         WHERE e.id = ?"
    ).unwrap();

    let result = stmt.query_row(rusqlite::params![extraction_id], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, String>(2)?,
            row.get::<_, String>(3)?,
        ))
    }).unwrap();

    assert_eq!(result.0, "Test Title");
    assert_eq!(result.1, "test-theme");
    assert_eq!(result.2, "This is a test quote");
    assert_eq!(result.3, "Test prediction");
}

#[test]
fn test_ingest_conversation_workflow() {
    use homeforge_chronicle::{ingest_conversation, IngestResult};
    use std::io::Write;

    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test.db");

    // Create database
    let db = Database::new(&db_path).unwrap();

    // Create a test conversation file
    let export_json = r#"{
  "uuid": "integration-test-conv",
  "name": "Integration Test Conversation",
  "created_at": "2025-01-01T12:00:00Z",
  "updated_at": "2025-01-01T12:30:00Z",
  "chat_messages": [
    {
      "uuid": "msg-1",
      "text": "First message",
      "sender": "human",
      "created_at": "2025-01-01T12:00:00Z"
    },
    {
      "uuid": "msg-2",
      "text": "Response message",
      "sender": "assistant",
      "created_at": "2025-01-01T12:00:30Z"
    },
    {
      "uuid": "msg-3",
      "text": "Third message",
      "sender": "human",
      "created_at": "2025-01-01T12:01:00Z"
    }
  ]
}"#;

    let export_path = temp_dir.path().join("test_export.json");
    let mut file = std::fs::File::create(&export_path).unwrap();
    file.write_all(export_json.as_bytes()).unwrap();
    file.flush().unwrap();
    drop(file);

    // Ingest the conversation
    let result = ingest_conversation(&db, &export_path).unwrap();

    // Verify result
    match result {
        IngestResult::Ingested { conversation_id, message_count } => {
            assert_eq!(conversation_id, "integration-test-conv");
            assert_eq!(message_count, 3);
        }
        _ => panic!("Expected Ingested result, got {:?}", result),
    }

    // Verify it's in the database
    assert!(db.is_conversation_processed("integration-test-conv").unwrap());

    // Verify we can query details
    let mut stmt = db.connection().prepare(
        "SELECT export_filename, message_count, first_message_at, last_message_at
         FROM conversations WHERE id = ?"
    ).unwrap();

    let row = stmt.query_row(
        rusqlite::params!["integration-test-conv"],
        |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, i64>(1)?,
                row.get::<_, i64>(2)?,
                row.get::<_, i64>(3)?,
            ))
        }
    ).unwrap();

    assert_eq!(row.0, "test_export.json");
    assert_eq!(row.1, 3);
    assert!(row.2 < row.3); // First timestamp before last

    // Try to ingest again - should be duplicate
    let result2 = ingest_conversation(&db, &export_path).unwrap();
    match result2 {
        IngestResult::Duplicate { conversation_id } => {
            assert_eq!(conversation_id, "integration-test-conv");
        }
        _ => panic!("Expected Duplicate result on second ingest, got {:?}", result2),
    }
}
