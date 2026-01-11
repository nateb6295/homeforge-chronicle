use anyhow::{Context, Result};
use rusqlite::{Connection, params};
use std::path::Path;

/// Database manager for Chronicle
pub struct Database {
    conn: Connection,
}

impl Database {
    /// Create a new database connection and initialize schema
    pub fn new<P: AsRef<Path>>(path: P) -> Result<Self> {
        let conn = Connection::open(path.as_ref())
            .context("Failed to open database")?;

        let db = Database { conn };
        db.initialize_schema()?;

        Ok(db)
    }

    /// Create an in-memory database (useful for testing)
    pub fn in_memory() -> Result<Self> {
        let conn = Connection::open_in_memory()
            .context("Failed to create in-memory database")?;

        let db = Database { conn };
        db.initialize_schema()?;

        Ok(db)
    }

    /// Initialize database schema
    fn initialize_schema(&self) -> Result<()> {
        // Conversations table - tracks processed conversation exports
        self.conn.execute(
            "CREATE TABLE IF NOT EXISTS conversations (
                id TEXT PRIMARY KEY,
                export_filename TEXT NOT NULL,
                first_message_at INTEGER NOT NULL,
                last_message_at INTEGER NOT NULL,
                message_count INTEGER NOT NULL,
                processed_at INTEGER NOT NULL
            )",
            [],
        ).context("Failed to create conversations table")?;

        // Extractions table - stores LLM-extracted content
        self.conn.execute(
            "CREATE TABLE IF NOT EXISTS extractions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                conversation_id TEXT NOT NULL,
                title TEXT NOT NULL,
                summary TEXT NOT NULL,
                classification TEXT NOT NULL,
                confidence_score REAL NOT NULL,
                created_at INTEGER NOT NULL,
                FOREIGN KEY(conversation_id) REFERENCES conversations(id)
            )",
            [],
        ).context("Failed to create extractions table")?;

        // Themes table - tracks themes across extractions
        self.conn.execute(
            "CREATE TABLE IF NOT EXISTS themes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL UNIQUE
            )",
            [],
        ).context("Failed to create themes table")?;

        // Extraction_themes junction table - many-to-many relationship
        self.conn.execute(
            "CREATE TABLE IF NOT EXISTS extraction_themes (
                extraction_id INTEGER NOT NULL,
                theme_id INTEGER NOT NULL,
                PRIMARY KEY(extraction_id, theme_id),
                FOREIGN KEY(extraction_id) REFERENCES extractions(id),
                FOREIGN KEY(theme_id) REFERENCES themes(id)
            )",
            [],
        ).context("Failed to create extraction_themes table")?;

        // Quotes table - stores verbatim excerpts
        self.conn.execute(
            "CREATE TABLE IF NOT EXISTS quotes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                extraction_id INTEGER NOT NULL,
                content TEXT NOT NULL,
                FOREIGN KEY(extraction_id) REFERENCES extractions(id)
            )",
            [],
        ).context("Failed to create quotes table")?;

        // Predictions table - special tracking for predictions
        self.conn.execute(
            "CREATE TABLE IF NOT EXISTS predictions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                extraction_id INTEGER NOT NULL,
                claim TEXT NOT NULL,
                date_made INTEGER NOT NULL,
                timeline TEXT,
                status TEXT NOT NULL DEFAULT 'pending',
                validation_date INTEGER,
                notes TEXT,
                FOREIGN KEY(extraction_id) REFERENCES extractions(id)
            )",
            [],
        ).context("Failed to create predictions table")?;

        // Create indexes for common queries
        self.conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_extractions_conversation
             ON extractions(conversation_id)",
            [],
        ).context("Failed to create index on extractions")?;

        self.conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_extractions_created
             ON extractions(created_at)",
            [],
        ).context("Failed to create index on extractions created_at")?;

        self.conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_predictions_status
             ON predictions(status)",
            [],
        ).context("Failed to create index on predictions status")?;

        Ok(())
    }

    /// Check if a conversation has already been processed
    pub fn is_conversation_processed(&self, conversation_id: &str) -> Result<bool> {
        let mut stmt = self.conn.prepare(
            "SELECT COUNT(*) FROM conversations WHERE id = ?"
        )?;

        let count: i64 = stmt.query_row(params![conversation_id], |row| row.get(0))?;
        Ok(count > 0)
    }

    /// Get or create a theme by name
    pub fn get_or_create_theme(&self, name: &str) -> Result<i64> {
        // Try to get existing theme
        let mut stmt = self.conn.prepare("SELECT id FROM themes WHERE name = ?")?;

        match stmt.query_row(params![name], |row| row.get::<_, i64>(0)) {
            Ok(id) => Ok(id),
            Err(rusqlite::Error::QueryReturnedNoRows) => {
                // Create new theme
                self.conn.execute(
                    "INSERT INTO themes (name) VALUES (?)",
                    params![name],
                )?;
                Ok(self.conn.last_insert_rowid())
            }
            Err(e) => Err(e.into()),
        }
    }

    /// Insert a new conversation record
    pub fn insert_conversation(
        &self,
        id: &str,
        export_filename: &str,
        first_message_at: i64,
        last_message_at: i64,
        message_count: i64,
    ) -> Result<()> {
        let processed_at = chrono::Utc::now().timestamp();

        self.conn.execute(
            "INSERT INTO conversations (id, export_filename, first_message_at, last_message_at, message_count, processed_at)
             VALUES (?, ?, ?, ?, ?, ?)",
            params![id, export_filename, first_message_at, last_message_at, message_count, processed_at],
        ).context("Failed to insert conversation")?;

        Ok(())
    }

    /// Insert an extraction and return its ID
    pub fn insert_extraction(
        &self,
        conversation_id: &str,
        title: &str,
        summary: &str,
        classification: &str,
        confidence_score: f64,
        themes: &[String],
        quotes: &[String],
    ) -> Result<i64> {
        let created_at = chrono::Utc::now().timestamp();

        // Insert extraction
        self.conn.execute(
            "INSERT INTO extractions (conversation_id, title, summary, classification, confidence_score, created_at)
             VALUES (?, ?, ?, ?, ?, ?)",
            params![conversation_id, title, summary, classification, confidence_score, created_at],
        ).context("Failed to insert extraction")?;

        let extraction_id = self.conn.last_insert_rowid();

        // Link themes
        for theme_name in themes {
            let theme_id = self.get_or_create_theme(theme_name)?;
            self.conn.execute(
                "INSERT INTO extraction_themes (extraction_id, theme_id) VALUES (?, ?)",
                params![extraction_id, theme_id],
            )?;
        }

        // Insert quotes
        for quote in quotes {
            self.conn.execute(
                "INSERT INTO quotes (extraction_id, content) VALUES (?, ?)",
                params![extraction_id, quote],
            )?;
        }

        Ok(extraction_id)
    }

    /// Get conversations that haven't been extracted yet
    pub fn get_unextracted_conversations(&self) -> Result<Vec<String>> {
        let mut stmt = self.conn.prepare(
            "SELECT id FROM conversations
             WHERE id NOT IN (SELECT DISTINCT conversation_id FROM extractions)
             ORDER BY processed_at ASC"
        )?;

        let conversation_ids = stmt.query_map([], |row| row.get(0))?
            .collect::<Result<Vec<String>, _>>()?;

        Ok(conversation_ids)
    }

    /// Get conversation export filename for re-parsing
    pub fn get_conversation_filename(&self, conversation_id: &str) -> Result<Option<String>> {
        let mut stmt = self.conn.prepare(
            "SELECT export_filename FROM conversations WHERE id = ?"
        )?;

        match stmt.query_row(params![conversation_id], |row| row.get(0)) {
            Ok(filename) => Ok(Some(filename)),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    /// Get extractions from the past N days with their themes and quotes
    pub fn get_recent_extractions(&self, days: i64) -> Result<Vec<(i64, String, String, String, String, f64, i64, Vec<String>, Vec<String>)>> {
        let cutoff = chrono::Utc::now().timestamp() - (days * 24 * 60 * 60);

        let mut stmt = self.conn.prepare(
            "SELECT e.id, e.conversation_id, e.title, e.summary, e.classification, e.confidence_score, e.created_at
             FROM extractions e
             WHERE e.created_at >= ?
             ORDER BY e.created_at DESC"
        )?;

        let extraction_rows = stmt.query_map(params![cutoff], |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, String>(3)?,
                row.get::<_, String>(4)?,
                row.get::<_, f64>(5)?,
                row.get::<_, i64>(6)?,
            ))
        })?;

        let mut results = Vec::new();

        for extraction_result in extraction_rows {
            let (id, conv_id, title, summary, classification, confidence, created_at) = extraction_result?;

            // Get themes for this extraction
            let mut theme_stmt = self.conn.prepare(
                "SELECT t.name FROM themes t
                 JOIN extraction_themes et ON t.id = et.theme_id
                 WHERE et.extraction_id = ?"
            )?;

            let themes: Vec<String> = theme_stmt.query_map(params![id], |row| row.get(0))?
                .collect::<Result<Vec<_>, _>>()?;

            // Get quotes for this extraction
            let mut quote_stmt = self.conn.prepare(
                "SELECT content FROM quotes WHERE extraction_id = ?"
            )?;

            let quotes: Vec<String> = quote_stmt.query_map(params![id], |row| row.get(0))?
                .collect::<Result<Vec<_>, _>>()?;

            results.push((id, conv_id, title, summary, classification, confidence, created_at, themes, quotes));
        }

        Ok(results)
    }

    /// Get all extractions for a specific theme
    pub fn get_extractions_by_theme(&self, theme: &str) -> Result<Vec<(i64, String, String, i64, Vec<String>)>> {
        let mut stmt = self.conn.prepare(
            "SELECT e.id, e.title, e.summary, e.created_at
             FROM extractions e
             JOIN extraction_themes et ON e.id = et.extraction_id
             JOIN themes t ON et.theme_id = t.id
             WHERE t.name = ?
             ORDER BY e.created_at ASC"
        )?;

        let extraction_rows = stmt.query_map(params![theme], |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, i64>(3)?,
            ))
        })?;

        let mut results = Vec::new();

        for extraction_result in extraction_rows {
            let (id, title, summary, created_at) = extraction_result?;

            // Get quotes for this extraction
            let mut quote_stmt = self.conn.prepare(
                "SELECT content FROM quotes WHERE extraction_id = ?"
            )?;

            let quotes: Vec<String> = quote_stmt.query_map(params![id], |row| row.get(0))?
                .collect::<Result<Vec<_>, _>>()?;

            results.push((id, title, summary, created_at, quotes));
        }

        Ok(results)
    }

    /// Get all themes that have a minimum number of extractions
    pub fn get_themes_with_threshold(&self, min_count: i64) -> Result<Vec<(String, i64)>> {
        let mut stmt = self.conn.prepare(
            "SELECT t.name, COUNT(et.extraction_id) as count
             FROM themes t
             JOIN extraction_themes et ON t.id = et.theme_id
             GROUP BY t.name
             HAVING count >= ?
             ORDER BY count DESC"
        )?;

        let themes = stmt.query_map(params![min_count], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?))
        })?
        .collect::<Result<Vec<_>, _>>()?;

        Ok(themes)
    }

    /// Get all predictions from extractions classified as predictions
    pub fn get_all_predictions(&self) -> Result<Vec<(i64, i64, String, i64, Option<String>, String, Option<i64>, Option<String>)>> {
        // First get all predictions from the predictions table if they exist
        let mut stmt = self.conn.prepare(
            "SELECT p.id, p.extraction_id, p.claim, p.date_made, p.timeline, p.status, p.validation_date, p.notes
             FROM predictions p
             ORDER BY p.date_made DESC"
        )?;

        let predictions = stmt.query_map([], |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, i64>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, i64>(3)?,
                row.get::<_, Option<String>>(4)?,
                row.get::<_, String>(5)?,
                row.get::<_, Option<i64>>(6)?,
                row.get::<_, Option<String>>(7)?,
            ))
        })?
        .collect::<Result<Vec<_>, _>>()?;

        Ok(predictions)
    }

    /// Get the database connection (for advanced operations)
    pub fn connection(&self) -> &Connection {
        &self.conn
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_database_creates_in_memory() {
        let db = Database::in_memory();
        assert!(db.is_ok());
    }

    #[test]
    fn test_schema_initializes() {
        let db = Database::in_memory().unwrap();

        // Verify tables exist by querying sqlite_master
        let mut stmt = db.conn.prepare(
            "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
        ).unwrap();

        let tables: Vec<String> = stmt.query_map([], |row| row.get(0))
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();

        assert!(tables.contains(&"conversations".to_string()));
        assert!(tables.contains(&"extractions".to_string()));
        assert!(tables.contains(&"themes".to_string()));
        assert!(tables.contains(&"extraction_themes".to_string()));
        assert!(tables.contains(&"quotes".to_string()));
        assert!(tables.contains(&"predictions".to_string()));
    }

    #[test]
    fn test_is_conversation_processed() {
        let db = Database::in_memory().unwrap();

        // Should not be processed initially
        let result = db.is_conversation_processed("test-id");
        assert!(result.is_ok());
        assert!(!result.unwrap());

        // Insert a conversation
        db.conn.execute(
            "INSERT INTO conversations (id, export_filename, first_message_at, last_message_at, message_count, processed_at)
             VALUES (?, ?, ?, ?, ?, ?)",
            params!["test-id", "test.json", 1000, 2000, 10, 3000],
        ).unwrap();

        // Should be processed now
        let result = db.is_conversation_processed("test-id");
        assert!(result.is_ok());
        assert!(result.unwrap());
    }

    #[test]
    fn test_get_or_create_theme() {
        let db = Database::in_memory().unwrap();

        // Create new theme
        let id1 = db.get_or_create_theme("test-theme").unwrap();
        assert!(id1 > 0);

        // Get existing theme (should return same ID)
        let id2 = db.get_or_create_theme("test-theme").unwrap();
        assert_eq!(id1, id2);

        // Create different theme
        let id3 = db.get_or_create_theme("other-theme").unwrap();
        assert_ne!(id1, id3);
    }

    #[test]
    fn test_insert_conversation() {
        let db = Database::in_memory().unwrap();

        // Insert a conversation using the method
        let result = db.insert_conversation(
            "conv-123",
            "export.json",
            1000,
            2000,
            5,
        );
        assert!(result.is_ok());

        // Verify it was inserted
        assert!(db.is_conversation_processed("conv-123").unwrap());

        // Verify we can query the data
        let mut stmt = db.conn.prepare(
            "SELECT export_filename, message_count FROM conversations WHERE id = ?"
        ).unwrap();

        let (filename, count): (String, i64) = stmt.query_row(
            params!["conv-123"],
            |row| Ok((row.get(0)?, row.get(1)?))
        ).unwrap();

        assert_eq!(filename, "export.json");
        assert_eq!(count, 5);
    }

    #[test]
    fn test_foreign_key_constraints() {
        let db = Database::in_memory().unwrap();

        // Enable foreign keys
        db.conn.execute("PRAGMA foreign_keys = ON", []).unwrap();

        // Try to insert extraction without conversation (should fail)
        let result = db.conn.execute(
            "INSERT INTO extractions (conversation_id, title, summary, classification, confidence_score, created_at)
             VALUES (?, ?, ?, ?, ?, ?)",
            params!["nonexistent-id", "Test", "Summary", "insight", 0.8, 1000],
        );

        // This should fail due to foreign key constraint
        assert!(result.is_err());
    }

    #[test]
    fn test_indexes_created() {
        let db = Database::in_memory().unwrap();

        let mut stmt = db.conn.prepare(
            "SELECT name FROM sqlite_master WHERE type='index' AND name LIKE 'idx_%'"
        ).unwrap();

        let indexes: Vec<String> = stmt.query_map([], |row| row.get(0))
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();

        assert!(indexes.contains(&"idx_extractions_conversation".to_string()));
        assert!(indexes.contains(&"idx_extractions_created".to_string()));
        assert!(indexes.contains(&"idx_predictions_status".to_string()));
    }

    #[test]
    fn test_insert_extraction() {
        let db = Database::in_memory().unwrap();

        // First insert a conversation
        db.insert_conversation("conv-1", "test.json", 1000, 2000, 5).unwrap();

        // Insert extraction
        let themes = vec!["lattice".to_string(), "homeforge".to_string()];
        let quotes = vec!["Key insight here".to_string()];

        let extraction_id = db.insert_extraction(
            "conv-1",
            "Test Title",
            "Test summary",
            "insight",
            0.85,
            &themes,
            &quotes,
        ).unwrap();

        assert!(extraction_id > 0);

        // Verify extraction was inserted
        let mut stmt = db.conn.prepare(
            "SELECT title, summary, classification, confidence_score FROM extractions WHERE id = ?"
        ).unwrap();

        let row = stmt.query_row(params![extraction_id], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, f64>(3)?,
            ))
        }).unwrap();

        assert_eq!(row.0, "Test Title");
        assert_eq!(row.1, "Test summary");
        assert_eq!(row.2, "insight");
        assert_eq!(row.3, 0.85);

        // Verify themes were linked
        let mut stmt = db.conn.prepare(
            "SELECT t.name FROM themes t
             JOIN extraction_themes et ON t.id = et.theme_id
             WHERE et.extraction_id = ?"
        ).unwrap();

        let theme_names: Vec<String> = stmt.query_map(params![extraction_id], |row| row.get(0))
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();

        assert_eq!(theme_names.len(), 2);
        assert!(theme_names.contains(&"lattice".to_string()));
        assert!(theme_names.contains(&"homeforge".to_string()));

        // Verify quotes were inserted
        let mut stmt = db.conn.prepare(
            "SELECT content FROM quotes WHERE extraction_id = ?"
        ).unwrap();

        let quote_contents: Vec<String> = stmt.query_map(params![extraction_id], |row| row.get(0))
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();

        assert_eq!(quote_contents.len(), 1);
        assert_eq!(quote_contents[0], "Key insight here");
    }

    #[test]
    fn test_get_unextracted_conversations() {
        let db = Database::in_memory().unwrap();

        // Insert three conversations
        db.insert_conversation("conv-1", "test1.json", 1000, 2000, 5).unwrap();
        db.insert_conversation("conv-2", "test2.json", 2000, 3000, 5).unwrap();
        db.insert_conversation("conv-3", "test3.json", 3000, 4000, 5).unwrap();

        // All should be unextracted
        let unextracted = db.get_unextracted_conversations().unwrap();
        assert_eq!(unextracted.len(), 3);

        // Extract one conversation
        db.insert_extraction(
            "conv-2",
            "Title",
            "Summary",
            "insight",
            0.8,
            &["theme".to_string()],
            &[],
        ).unwrap();

        // Now only 2 should be unextracted
        let unextracted = db.get_unextracted_conversations().unwrap();
        assert_eq!(unextracted.len(), 2);
        assert!(unextracted.contains(&"conv-1".to_string()));
        assert!(unextracted.contains(&"conv-3".to_string()));
        assert!(!unextracted.contains(&"conv-2".to_string()));
    }
}
