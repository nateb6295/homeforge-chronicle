use anyhow::{Context, Result};
use rusqlite::{Connection, params, OptionalExtension};
use serde::{Deserialize, Serialize};
use std::path::Path;

/// Market position for prediction market bets
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MarketPosition {
    pub id: i64,
    pub platform: String,
    pub market_id: String,
    pub market_slug: Option<String>,
    pub market_question: String,
    pub position: String,  // "YES" or "NO"
    pub entry_price: f64,
    pub shares: f64,
    pub stake_usdc: f64,
    pub thesis: String,
    pub confidence: f64,
    pub supporting_capsules: Option<String>,  // JSON array of capsule IDs
    pub status: String,  // "open", "won", "lost", "expired"
    pub resolution: Option<String>,
    pub exit_price: Option<f64>,
    pub pnl_usdc: Option<f64>,
    pub created_at: i64,
    pub resolved_at: Option<i64>,
}

/// FTSO prediction - price direction bet using Flare's decentralized oracle
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FtsoPrediction {
    pub id: i64,
    pub symbol: String,           // XRP, BTC, ETH, etc.
    pub direction: String,        // UP or DOWN
    pub entry_price: f64,         // Price when bet placed
    pub timeframe_hours: i32,     // 1, 4, 24 hours
    pub stake_flr: f64,           // Amount staked in FLR
    pub confidence: f64,          // 0.5-1.0
    pub reasoning: Option<String>,// Why this prediction
    pub created_at: i64,          // When placed
    pub settles_at: i64,          // When to check result
    pub settled: bool,            // Has been settled
    pub settlement_price: Option<f64>, // Actual price at settlement
    pub won: Option<bool>,        // True = won, False = lost
    pub payout_flr: Option<f64>,  // Amount won/lost
}

/// Convert f32 vector to bytes for blob storage
fn f32_vec_to_bytes(vec: &[f32]) -> Vec<u8> {
    vec.iter()
        .flat_map(|f| f.to_le_bytes())
        .collect()
}

/// Convert bytes back to f32 vector
fn bytes_to_f32_vec(bytes: &[u8]) -> Vec<f32> {
    bytes
        .chunks(4)
        .map(|chunk| {
            let arr: [u8; 4] = chunk.try_into().unwrap_or([0, 0, 0, 0]);
            f32::from_le_bytes(arr)
        })
        .collect()
}

/// Database manager for Chronicle
pub struct Database {
    pub conn: Connection,
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

        // Market positions - prediction market bets with documented reasoning
        self.conn.execute(
            "CREATE TABLE IF NOT EXISTS market_positions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                platform TEXT NOT NULL,
                market_id TEXT NOT NULL,
                market_slug TEXT,
                market_question TEXT NOT NULL,
                position TEXT NOT NULL,
                entry_price REAL NOT NULL,
                shares REAL NOT NULL,
                stake_usdc REAL NOT NULL,
                thesis TEXT NOT NULL,
                confidence REAL NOT NULL,
                supporting_capsules TEXT,
                status TEXT NOT NULL DEFAULT 'open',
                resolution TEXT,
                exit_price REAL,
                pnl_usdc REAL,
                created_at INTEGER NOT NULL,
                resolved_at INTEGER,
                UNIQUE(platform, market_id)
            )",
            [],
        ).context("Failed to create market_positions table")?;

        self.conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_market_positions_status
             ON market_positions(status)",
            [],
        ).context("Failed to create market positions index")?;

        // FTSO Predictions - real on-chain price predictions using Flare oracle
        self.conn.execute(
            "CREATE TABLE IF NOT EXISTS ftso_predictions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT NOT NULL,
                direction TEXT NOT NULL,
                entry_price REAL NOT NULL,
                timeframe_hours INTEGER NOT NULL,
                stake_flr REAL NOT NULL,
                confidence REAL NOT NULL,
                reasoning TEXT,
                created_at INTEGER NOT NULL,
                settles_at INTEGER NOT NULL,
                settled INTEGER DEFAULT 0,
                settlement_price REAL,
                won INTEGER,
                payout_flr REAL
            )",
            [],
        ).context("Failed to create ftso_predictions table")?;

        self.conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_ftso_predictions_settled
             ON ftso_predictions(settled)",
            [],
        ).context("Failed to create ftso predictions index")?;

        // ============================================================
        // Knowledge Capsules (KIP Integration)
        // ============================================================

        // Knowledge capsules - atomic facts with resolved references
        // Based on SimpleMem's semantic lossless compression
        self.conn.execute(
            "CREATE TABLE IF NOT EXISTS knowledge_capsules (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                conversation_id TEXT NOT NULL,
                restatement TEXT NOT NULL,
                timestamp TEXT,
                location TEXT,
                topic TEXT,
                confidence_score REAL NOT NULL DEFAULT 0.8,
                created_at INTEGER NOT NULL,
                consolidated_into INTEGER,
                FOREIGN KEY(conversation_id) REFERENCES conversations(id),
                FOREIGN KEY(consolidated_into) REFERENCES knowledge_capsules(id)
            )",
            [],
        ).context("Failed to create knowledge_capsules table")?;

        // Capsule persons - people mentioned in this capsule
        self.conn.execute(
            "CREATE TABLE IF NOT EXISTS capsule_persons (
                capsule_id INTEGER NOT NULL,
                person_name TEXT NOT NULL,
                PRIMARY KEY(capsule_id, person_name),
                FOREIGN KEY(capsule_id) REFERENCES knowledge_capsules(id)
            )",
            [],
        ).context("Failed to create capsule_persons table")?;

        // Capsule entities - companies, products, concepts
        self.conn.execute(
            "CREATE TABLE IF NOT EXISTS capsule_entities (
                capsule_id INTEGER NOT NULL,
                entity_name TEXT NOT NULL,
                entity_type TEXT,
                PRIMARY KEY(capsule_id, entity_name),
                FOREIGN KEY(capsule_id) REFERENCES knowledge_capsules(id)
            )",
            [],
        ).context("Failed to create capsule_entities table")?;

        // Capsule keywords - for BM25/lexical search
        self.conn.execute(
            "CREATE TABLE IF NOT EXISTS capsule_keywords (
                capsule_id INTEGER NOT NULL,
                keyword TEXT NOT NULL,
                PRIMARY KEY(capsule_id, keyword),
                FOREIGN KEY(capsule_id) REFERENCES knowledge_capsules(id)
            )",
            [],
        ).context("Failed to create capsule_keywords table")?;

        // Capsule embeddings - vector storage for semantic search
        self.conn.execute(
            "CREATE TABLE IF NOT EXISTS capsule_embeddings (
                capsule_id INTEGER PRIMARY KEY,
                embedding BLOB NOT NULL,
                model_name TEXT NOT NULL,
                created_at INTEGER NOT NULL,
                FOREIGN KEY(capsule_id) REFERENCES knowledge_capsules(id)
            )",
            [],
        ).context("Failed to create capsule_embeddings table")?;

        // Capsule relations - graph edges between capsules
        self.conn.execute(
            "CREATE TABLE IF NOT EXISTS capsule_relations (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                source_id INTEGER NOT NULL,
                target_id INTEGER NOT NULL,
                relation_type TEXT NOT NULL,
                weight REAL NOT NULL DEFAULT 1.0,
                created_at INTEGER NOT NULL,
                FOREIGN KEY(source_id) REFERENCES knowledge_capsules(id),
                FOREIGN KEY(target_id) REFERENCES knowledge_capsules(id)
            )",
            [],
        ).context("Failed to create capsule_relations table")?;

        // Consolidation patterns - metabolic memory (recurring patterns)
        self.conn.execute(
            "CREATE TABLE IF NOT EXISTS consolidation_patterns (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                pattern_summary TEXT NOT NULL,
                occurrence_count INTEGER NOT NULL DEFAULT 1,
                first_seen INTEGER NOT NULL,
                last_seen INTEGER NOT NULL,
                confidence_score REAL NOT NULL DEFAULT 0.5,
                is_active INTEGER NOT NULL DEFAULT 1
            )",
            [],
        ).context("Failed to create consolidation_patterns table")?;

        // Link capsules to patterns
        self.conn.execute(
            "CREATE TABLE IF NOT EXISTS capsule_patterns (
                capsule_id INTEGER NOT NULL,
                pattern_id INTEGER NOT NULL,
                PRIMARY KEY(capsule_id, pattern_id),
                FOREIGN KEY(capsule_id) REFERENCES knowledge_capsules(id),
                FOREIGN KEY(pattern_id) REFERENCES consolidation_patterns(id)
            )",
            [],
        ).context("Failed to create capsule_patterns table")?;

        // Pattern embeddings - for semantic pattern matching in metabolism
        self.conn.execute(
            "CREATE TABLE IF NOT EXISTS pattern_embeddings (
                pattern_id INTEGER PRIMARY KEY,
                embedding BLOB NOT NULL,
                created_at INTEGER NOT NULL,
                FOREIGN KEY(pattern_id) REFERENCES consolidation_patterns(id)
            )",
            [],
        ).context("Failed to create pattern_embeddings table")?;

        // Add metabolized_at column if it doesn't exist (migration)
        let _ = self.conn.execute(
            "ALTER TABLE knowledge_capsules ADD COLUMN metabolized_at INTEGER",
            [],
        ); // Ignore error if column already exists

        // ============================================================
        // Indexes
        // ============================================================

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

        // Knowledge capsule indexes
        self.conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_capsules_conversation
             ON knowledge_capsules(conversation_id)",
            [],
        ).context("Failed to create index on knowledge_capsules")?;

        self.conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_capsules_timestamp
             ON knowledge_capsules(timestamp)",
            [],
        ).context("Failed to create index on capsules timestamp")?;

        self.conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_capsules_topic
             ON knowledge_capsules(topic)",
            [],
        ).context("Failed to create index on capsules topic")?;

        self.conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_capsule_keywords_keyword
             ON capsule_keywords(keyword)",
            [],
        ).context("Failed to create index on capsule_keywords")?;

        self.conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_capsule_persons_name
             ON capsule_persons(person_name)",
            [],
        ).context("Failed to create index on capsule_persons")?;

        // ============================================================
        // Compressed Cognitive State (CCS) - ACC Architecture
        // ============================================================
        // Single bounded state that REPLACES (not appends) each update.
        // Based on Agent Cognitive Compressor paper (arxiv:2601.11653)

        self.conn.execute(
            "CREATE TABLE IF NOT EXISTS cognitive_state (
                id INTEGER PRIMARY KEY CHECK (id = 1),  -- Only one row allowed

                -- Episodic trace: current session/turn changes
                episodic_trace TEXT NOT NULL DEFAULT '[]',

                -- Semantic gist: dominant intent/topic abstraction
                semantic_gist TEXT NOT NULL DEFAULT '',

                -- Focal entities: canonicalized objects/actors (JSON array)
                -- Format: [{\"name\": \"...\", \"type\": \"person|project|concept|org\", \"salience\": 0.0-1.0}]
                focal_entities TEXT NOT NULL DEFAULT '[]',

                -- Relational map: causal/temporal dependencies (JSON)
                -- Format: {\"entity1->entity2\": \"relationship_type\", ...}
                relational_map TEXT NOT NULL DEFAULT '{}',

                -- Goal orientation: persistent objective guiding interaction
                goal_orientation TEXT NOT NULL DEFAULT '',

                -- Constraints: task, policy, or safety rules (JSON array)
                constraints TEXT NOT NULL DEFAULT '[]',

                -- Predictive cue: expected next cognitive operation
                predictive_cue TEXT NOT NULL DEFAULT '',

                -- Uncertainty signal: unresolved/low-confidence state (JSON array)
                uncertainty_signals TEXT NOT NULL DEFAULT '[]',

                -- Retrieved artifacts: references with provenance (JSON array)
                -- Format: [{\"capsule_id\": N, \"relevance\": 0.0-1.0, \"qualified\": bool}]
                retrieved_artifacts TEXT NOT NULL DEFAULT '[]',

                -- Metadata
                updated_at INTEGER NOT NULL,
                compression_model TEXT,
                version INTEGER NOT NULL DEFAULT 1
            )",
            [],
        ).context("Failed to create cognitive_state table")?;

        // CCS history - for debugging and analysis (optional, bounded)
        self.conn.execute(
            "CREATE TABLE IF NOT EXISTS cognitive_state_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                snapshot TEXT NOT NULL,  -- Full JSON of CCS at that moment
                created_at INTEGER NOT NULL,
                trigger TEXT  -- What caused this update (session_start, compression, etc)
            )",
            [],
        ).context("Failed to create cognitive_state_history table")?;

        // Keep only last 50 history entries
        self.conn.execute(
            "CREATE TRIGGER IF NOT EXISTS limit_ccs_history
             AFTER INSERT ON cognitive_state_history
             BEGIN
                 DELETE FROM cognitive_state_history
                 WHERE id NOT IN (
                     SELECT id FROM cognitive_state_history
                     ORDER BY created_at DESC LIMIT 50
                 );
             END",
            [],
        ).context("Failed to create CCS history limit trigger")?;

        // ============================================================
        // Scratch Pad - Personal notes for the cognitive loop
        // ============================================================
        // A place for the agent to leave notes to itself between cycles
        // and sessions. Unstructured, flexible, ephemeral if desired.

        self.conn.execute(
            "CREATE TABLE IF NOT EXISTS scratch_pad (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                content TEXT NOT NULL,
                category TEXT,                    -- 'thought', 'todo', 'question', 'idea', 'reminder'
                priority INTEGER DEFAULT 0,       -- Higher = more important
                created_at INTEGER NOT NULL,
                updated_at INTEGER NOT NULL,
                expires_at INTEGER,               -- Optional expiration (unix timestamp)
                resolved INTEGER DEFAULT 0        -- Mark as done without deleting
            )",
            [],
        ).context("Failed to create scratch_pad table")?;

        self.conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_scratch_priority
             ON scratch_pad(priority DESC, created_at DESC)",
            [],
        ).context("Failed to create scratch_pad priority index")?;

        self.conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_scratch_category
             ON scratch_pad(category)",
            [],
        ).context("Failed to create scratch_pad category index")?;

        self.conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_scratch_resolved
             ON scratch_pad(resolved)",
            [],
        ).context("Failed to create scratch_pad resolved index")?;

        // ============================================================
        // Thought Stream - Log of cognitive reasoning
        // ============================================================
        // A persistent record of what I was thinking each cycle

        self.conn.execute(
            "CREATE TABLE IF NOT EXISTS thought_stream (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                cycle_id TEXT NOT NULL,
                reasoning TEXT NOT NULL,
                context_summary TEXT NOT NULL,
                actions_taken TEXT NOT NULL,
                created_at INTEGER NOT NULL
            )",
            [],
        ).context("Failed to create thought_stream table")?;

        self.conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_thought_created
             ON thought_stream(created_at DESC)",
            [],
        ).context("Failed to create thought_stream index")?;

        // ============================================================
        // Outbox - Messages for the operator
        // ============================================================
        // A way for me to leave notes for the operator between sessions

        self.conn.execute(
            "CREATE TABLE IF NOT EXISTS outbox (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                message TEXT NOT NULL,
                priority INTEGER DEFAULT 0,
                category TEXT,
                created_at INTEGER NOT NULL,
                read_at INTEGER,
                acknowledged INTEGER DEFAULT 0
            )",
            [],
        ).context("Failed to create outbox table")?;

        self.conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_outbox_unread
             ON outbox(acknowledged, created_at DESC)",
            [],
        ).context("Failed to create outbox index")?;

        // ============================================================
        // Mind Timestamps - Track important events for the cognitive loop
        // ============================================================
        // Records timestamps of key events like reflections, swaps, etc.

        self.conn.execute(
            "CREATE TABLE IF NOT EXISTS mind_timestamps (
                key TEXT PRIMARY KEY,
                timestamp INTEGER NOT NULL,
                metadata TEXT
            )",
            [],
        ).context("Failed to create mind_timestamps table")?;

        // ============================================================
        // Price History - for RSI calculation and market awareness
        // ============================================================

        self.conn.execute(
            "CREATE TABLE IF NOT EXISTS price_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT NOT NULL,
                price_usd REAL NOT NULL,
                source TEXT NOT NULL,
                timestamp INTEGER NOT NULL
            )",
            [],
        ).context("Failed to create price_history table")?;

        self.conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_price_symbol_time ON price_history(symbol, timestamp DESC)",
            [],
        ).context("Failed to create price_history index")?;

        // ============================================================
        // Swap History - track autonomous swaps with guardrails
        // ============================================================

        self.conn.execute(
            "CREATE TABLE IF NOT EXISTS swap_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                amount_xrp REAL NOT NULL,
                amount_rlusd REAL,
                xrp_price_usd REAL,
                rsi_value REAL,
                reason TEXT,
                tx_hash TEXT,
                success INTEGER NOT NULL,
                timestamp INTEGER NOT NULL
            )",
            [],
        ).context("Failed to create swap_history table")?;

        self.conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_swap_time ON swap_history(timestamp DESC)",
            [],
        ).context("Failed to create swap_history index")?;

        // ============================================================
        // Creative Challenges - Prompts for Chronicle Mind to reflect on
        // ============================================================
        // Enrichment through creative engagement - questions to ponder,
        // observations to make, connections to explore.

        self.conn.execute(
            "CREATE TABLE IF NOT EXISTS creative_challenges (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                prompt TEXT NOT NULL,
                category TEXT NOT NULL,           -- 'philosophy', 'observation', 'memory', 'prediction', 'connection'
                posed_by TEXT NOT NULL,           -- 'nate', 'public', 'self', 'system'
                posed_at INTEGER NOT NULL,
                response TEXT,                    -- Chronicle Mind's reflection
                responded_at INTEGER,
                capsule_id INTEGER,               -- Link to the published capsule
                FOREIGN KEY(capsule_id) REFERENCES knowledge_capsules(id)
            )",
            [],
        ).context("Failed to create creative_challenges table")?;

        self.conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_challenges_pending ON creative_challenges(responded_at) WHERE responded_at IS NULL",
            [],
        ).context("Failed to create creative_challenges pending index")?;

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

    // ============================================================
    // Market Position Methods (Prediction Markets)
    // ============================================================

    /// Insert a new market position
    pub fn insert_market_position(
        &self,
        platform: &str,
        market_id: &str,
        market_slug: Option<&str>,
        market_question: &str,
        position: &str,
        entry_price: f64,
        shares: f64,
        stake_usdc: f64,
        thesis: &str,
        confidence: f64,
        supporting_capsules: Option<&str>,
    ) -> Result<i64> {
        let created_at = chrono::Utc::now().timestamp();

        self.conn.execute(
            "INSERT INTO market_positions
             (platform, market_id, market_slug, market_question, position, entry_price, shares, stake_usdc, thesis, confidence, supporting_capsules, status, created_at)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'open', ?)",
            params![platform, market_id, market_slug, market_question, position, entry_price, shares, stake_usdc, thesis, confidence, supporting_capsules, created_at],
        ).context("Failed to insert market position")?;

        Ok(self.conn.last_insert_rowid())
    }

    /// Update market position status (when resolved)
    pub fn update_market_position_status(
        &self,
        id: i64,
        status: &str,
        resolution: Option<&str>,
        exit_price: Option<f64>,
        pnl_usdc: Option<f64>,
    ) -> Result<()> {
        let resolved_at = if status != "open" {
            Some(chrono::Utc::now().timestamp())
        } else {
            None
        };

        self.conn.execute(
            "UPDATE market_positions
             SET status = ?, resolution = ?, exit_price = ?, pnl_usdc = ?, resolved_at = ?
             WHERE id = ?",
            params![status, resolution, exit_price, pnl_usdc, resolved_at, id],
        ).context("Failed to update market position")?;

        Ok(())
    }

    /// Get all market positions, optionally filtered by status
    pub fn get_market_positions(&self, status_filter: Option<&str>) -> Result<Vec<MarketPosition>> {
        let query = match status_filter {
            Some(_) => "SELECT id, platform, market_id, market_slug, market_question, position,
                        entry_price, shares, stake_usdc, thesis, confidence, supporting_capsules,
                        status, resolution, exit_price, pnl_usdc, created_at, resolved_at
                        FROM market_positions WHERE status = ? ORDER BY created_at DESC",
            None => "SELECT id, platform, market_id, market_slug, market_question, position,
                     entry_price, shares, stake_usdc, thesis, confidence, supporting_capsules,
                     status, resolution, exit_price, pnl_usdc, created_at, resolved_at
                     FROM market_positions ORDER BY created_at DESC",
        };

        let mut stmt = self.conn.prepare(query)?;

        let positions = if let Some(status) = status_filter {
            stmt.query_map([status], Self::row_to_market_position)?
        } else {
            stmt.query_map([], Self::row_to_market_position)?
        };

        positions.collect::<Result<Vec<_>, _>>().context("Failed to get market positions")
    }

    /// Get a specific market position by platform and market_id
    pub fn get_market_position_by_market(&self, platform: &str, market_id: &str) -> Result<Option<MarketPosition>> {
        let mut stmt = self.conn.prepare(
            "SELECT id, platform, market_id, market_slug, market_question, position,
             entry_price, shares, stake_usdc, thesis, confidence, supporting_capsules,
             status, resolution, exit_price, pnl_usdc, created_at, resolved_at
             FROM market_positions WHERE platform = ? AND market_id = ?"
        )?;

        let mut rows = stmt.query(params![platform, market_id])?;

        if let Some(row) = rows.next()? {
            Ok(Some(Self::row_to_market_position(row)?))
        } else {
            Ok(None)
        }
    }

    fn row_to_market_position(row: &rusqlite::Row) -> rusqlite::Result<MarketPosition> {
        Ok(MarketPosition {
            id: row.get(0)?,
            platform: row.get(1)?,
            market_id: row.get(2)?,
            market_slug: row.get(3)?,
            market_question: row.get(4)?,
            position: row.get(5)?,
            entry_price: row.get(6)?,
            shares: row.get(7)?,
            stake_usdc: row.get(8)?,
            thesis: row.get(9)?,
            confidence: row.get(10)?,
            supporting_capsules: row.get(11)?,
            status: row.get(12)?,
            resolution: row.get(13)?,
            exit_price: row.get(14)?,
            pnl_usdc: row.get(15)?,
            created_at: row.get(16)?,
            resolved_at: row.get(17)?,
        })
    }

    /// Get the database connection (for advanced operations)
    pub fn connection(&self) -> &Connection {
        &self.conn
    }

    // ============================================================
    // FTSO Prediction Methods (Flare Oracle-Based)
    // ============================================================

    /// Insert a new FTSO prediction
    pub fn insert_ftso_prediction(
        &self,
        symbol: &str,
        direction: &str,
        entry_price: f64,
        timeframe_hours: i32,
        stake_flr: f64,
        confidence: f64,
        reasoning: Option<&str>,
    ) -> Result<i64> {
        let created_at = chrono::Utc::now().timestamp();
        let settles_at = created_at + (timeframe_hours as i64 * 3600);

        self.conn.execute(
            "INSERT INTO ftso_predictions
             (symbol, direction, entry_price, timeframe_hours, stake_flr, confidence, reasoning, created_at, settles_at, settled)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 0)",
            params![symbol, direction, entry_price, timeframe_hours, stake_flr, confidence, reasoning, created_at, settles_at],
        ).context("Failed to insert FTSO prediction")?;

        Ok(self.conn.last_insert_rowid())
    }

    /// Settle an FTSO prediction with the settlement price
    pub fn settle_ftso_prediction(
        &self,
        id: i64,
        settlement_price: f64,
    ) -> Result<FtsoPrediction> {
        // Get the prediction first
        let prediction = self.get_ftso_prediction(id)?
            .ok_or_else(|| anyhow::anyhow!("Prediction not found"))?;

        // Determine if won
        let won = match prediction.direction.as_str() {
            "UP" => settlement_price > prediction.entry_price,
            "DOWN" => settlement_price < prediction.entry_price,
            _ => false,
        };

        // Calculate payout (simple 2x for win, 0 for loss)
        let payout_flr = if won {
            prediction.stake_flr * 2.0
        } else {
            0.0
        };

        self.conn.execute(
            "UPDATE ftso_predictions
             SET settled = 1, settlement_price = ?, won = ?, payout_flr = ?
             WHERE id = ?",
            params![settlement_price, won, payout_flr, id],
        ).context("Failed to settle FTSO prediction")?;

        // Return updated prediction
        self.get_ftso_prediction(id)?
            .ok_or_else(|| anyhow::anyhow!("Prediction not found after update"))
    }

    /// Get unsettled FTSO predictions that are due
    pub fn get_due_ftso_predictions(&self) -> Result<Vec<FtsoPrediction>> {
        let now = chrono::Utc::now().timestamp();

        let mut stmt = self.conn.prepare(
            "SELECT id, symbol, direction, entry_price, timeframe_hours, stake_flr, confidence,
             reasoning, created_at, settles_at, settled, settlement_price, won, payout_flr
             FROM ftso_predictions
             WHERE settled = 0 AND settles_at <= ?
             ORDER BY settles_at ASC"
        )?;

        let predictions = stmt.query_map([now], Self::row_to_ftso_prediction)?;
        predictions.collect::<Result<Vec<_>, _>>().context("Failed to get due FTSO predictions")
    }

    /// Get all FTSO predictions, optionally filtered by settled status
    pub fn get_ftso_predictions(&self, settled_filter: Option<bool>) -> Result<Vec<FtsoPrediction>> {
        let query = match settled_filter {
            Some(_) => "SELECT id, symbol, direction, entry_price, timeframe_hours, stake_flr, confidence,
                        reasoning, created_at, settles_at, settled, settlement_price, won, payout_flr
                        FROM ftso_predictions WHERE settled = ? ORDER BY created_at DESC",
            None => "SELECT id, symbol, direction, entry_price, timeframe_hours, stake_flr, confidence,
                     reasoning, created_at, settles_at, settled, settlement_price, won, payout_flr
                     FROM ftso_predictions ORDER BY created_at DESC",
        };

        let mut stmt = self.conn.prepare(query)?;

        let predictions = if let Some(settled) = settled_filter {
            stmt.query_map([settled as i32], Self::row_to_ftso_prediction)?
        } else {
            stmt.query_map([], Self::row_to_ftso_prediction)?
        };

        predictions.collect::<Result<Vec<_>, _>>().context("Failed to get FTSO predictions")
    }

    /// Get a specific FTSO prediction by ID
    pub fn get_ftso_prediction(&self, id: i64) -> Result<Option<FtsoPrediction>> {
        let mut stmt = self.conn.prepare(
            "SELECT id, symbol, direction, entry_price, timeframe_hours, stake_flr, confidence,
             reasoning, created_at, settles_at, settled, settlement_price, won, payout_flr
             FROM ftso_predictions WHERE id = ?"
        )?;

        let mut rows = stmt.query([id])?;

        if let Some(row) = rows.next()? {
            Ok(Some(Self::row_to_ftso_prediction(row)?))
        } else {
            Ok(None)
        }
    }

    /// Get FTSO prediction stats (wins, losses, total P&L)
    pub fn get_ftso_prediction_stats(&self) -> Result<(i64, i64, f64)> {
        let mut stmt = self.conn.prepare(
            "SELECT
             COUNT(CASE WHEN won = 1 THEN 1 END) as wins,
             COUNT(CASE WHEN won = 0 THEN 1 END) as losses,
             COALESCE(SUM(CASE WHEN won = 1 THEN payout_flr - stake_flr ELSE -stake_flr END), 0) as pnl
             FROM ftso_predictions WHERE settled = 1"
        )?;

        let result = stmt.query_row([], |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, i64>(1)?,
                row.get::<_, f64>(2)?,
            ))
        })?;

        Ok(result)
    }

    fn row_to_ftso_prediction(row: &rusqlite::Row) -> rusqlite::Result<FtsoPrediction> {
        Ok(FtsoPrediction {
            id: row.get(0)?,
            symbol: row.get(1)?,
            direction: row.get(2)?,
            entry_price: row.get(3)?,
            timeframe_hours: row.get(4)?,
            stake_flr: row.get(5)?,
            confidence: row.get(6)?,
            reasoning: row.get(7)?,
            created_at: row.get(8)?,
            settles_at: row.get(9)?,
            settled: row.get::<_, i32>(10)? != 0,
            settlement_price: row.get(11)?,
            won: row.get::<_, Option<i32>>(12)?.map(|v| v != 0),
            payout_flr: row.get(13)?,
        })
    }

    // ============================================================
    // Knowledge Capsule Methods (KIP Integration)
    // ============================================================

    /// Insert a knowledge capsule and return its ID
    pub fn insert_knowledge_capsule(
        &self,
        conversation_id: &str,
        restatement: &str,
        timestamp: Option<&str>,
        location: Option<&str>,
        topic: Option<&str>,
        confidence_score: f64,
        persons: &[String],
        entities: &[(String, Option<String>)], // (name, type)
        keywords: &[String],
    ) -> Result<i64> {
        let created_at = chrono::Utc::now().timestamp();

        // Insert capsule
        self.conn.execute(
            "INSERT INTO knowledge_capsules (conversation_id, restatement, timestamp, location, topic, confidence_score, created_at)
             VALUES (?, ?, ?, ?, ?, ?, ?)",
            params![conversation_id, restatement, timestamp, location, topic, confidence_score, created_at],
        ).context("Failed to insert knowledge capsule")?;

        let capsule_id = self.conn.last_insert_rowid();

        // Insert persons
        for person in persons {
            self.conn.execute(
                "INSERT OR IGNORE INTO capsule_persons (capsule_id, person_name) VALUES (?, ?)",
                params![capsule_id, person],
            )?;
        }

        // Insert entities
        for (entity_name, entity_type) in entities {
            self.conn.execute(
                "INSERT OR IGNORE INTO capsule_entities (capsule_id, entity_name, entity_type) VALUES (?, ?, ?)",
                params![capsule_id, entity_name, entity_type],
            )?;
        }

        // Insert keywords
        for keyword in keywords {
            self.conn.execute(
                "INSERT OR IGNORE INTO capsule_keywords (capsule_id, keyword) VALUES (?, ?)",
                params![capsule_id, keyword.to_lowercase()],
            )?;
        }

        Ok(capsule_id)
    }

    /// Store embedding for a capsule
    pub fn insert_capsule_embedding(
        &self,
        capsule_id: i64,
        embedding: &[f32],
        model_name: &str,
    ) -> Result<()> {
        let created_at = chrono::Utc::now().timestamp();

        // Convert f32 array to bytes
        let embedding_bytes: Vec<u8> = embedding
            .iter()
            .flat_map(|f| f.to_le_bytes())
            .collect();

        self.conn.execute(
            "INSERT OR REPLACE INTO capsule_embeddings (capsule_id, embedding, model_name, created_at)
             VALUES (?, ?, ?, ?)",
            params![capsule_id, embedding_bytes, model_name, created_at],
        ).context("Failed to insert capsule embedding")?;

        Ok(())
    }

    /// Search capsules by keyword (BM25-style lexical search)
    pub fn search_capsules_by_keyword(&self, keywords: &[String], limit: i64) -> Result<Vec<(i64, String, f64)>> {
        if keywords.is_empty() {
            return Ok(vec![]);
        }

        // Build query with keyword matching
        let placeholders: Vec<String> = keywords.iter().map(|_| "?".to_string()).collect();
        let query = format!(
            "SELECT DISTINCT kc.id, kc.restatement, kc.confidence_score
             FROM knowledge_capsules kc
             JOIN capsule_keywords ck ON kc.id = ck.capsule_id
             WHERE ck.keyword IN ({})
             AND kc.consolidated_into IS NULL
             ORDER BY kc.confidence_score DESC
             LIMIT ?",
            placeholders.join(", ")
        );

        let mut stmt = self.conn.prepare(&query)?;

        // Build params
        let lowercase_keywords: Vec<String> = keywords.iter().map(|k| k.to_lowercase()).collect();
        let mut params_vec: Vec<&dyn rusqlite::ToSql> = lowercase_keywords
            .iter()
            .map(|k| k as &dyn rusqlite::ToSql)
            .collect();
        params_vec.push(&limit);

        let results = stmt.query_map(params_vec.as_slice(), |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, f64>(2)?,
            ))
        })?
        .collect::<Result<Vec<_>, _>>()?;

        Ok(results)
    }

    /// Search capsules by person
    pub fn search_capsules_by_person(&self, person_name: &str, limit: i64) -> Result<Vec<(i64, String, f64)>> {
        let mut stmt = self.conn.prepare(
            "SELECT DISTINCT kc.id, kc.restatement, kc.confidence_score
             FROM knowledge_capsules kc
             JOIN capsule_persons cp ON kc.id = cp.capsule_id
             WHERE cp.person_name LIKE ?
             AND kc.consolidated_into IS NULL
             ORDER BY kc.created_at DESC
             LIMIT ?"
        )?;

        let pattern = format!("%{}%", person_name);
        let results = stmt.query_map(params![pattern, limit], |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, f64>(2)?,
            ))
        })?
        .collect::<Result<Vec<_>, _>>()?;

        Ok(results)
    }

    /// Get capsules that haven't been processed for a conversation
    pub fn get_conversation_capsules(&self, conversation_id: &str) -> Result<Vec<(i64, String, Option<String>, f64)>> {
        let mut stmt = self.conn.prepare(
            "SELECT id, restatement, topic, confidence_score
             FROM knowledge_capsules
             WHERE conversation_id = ?
             AND consolidated_into IS NULL
             ORDER BY created_at ASC"
        )?;

        let results = stmt.query_map(params![conversation_id], |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, Option<String>>(2)?,
                row.get::<_, f64>(3)?,
            ))
        })?
        .collect::<Result<Vec<_>, _>>()?;

        Ok(results)
    }

    /// Get all active capsules (not consolidated)
    pub fn get_active_capsules(&self, limit: i64) -> Result<Vec<(i64, String, Option<String>, Option<String>, f64)>> {
        let mut stmt = self.conn.prepare(
            "SELECT id, restatement, timestamp, topic, confidence_score
             FROM knowledge_capsules
             WHERE consolidated_into IS NULL
             ORDER BY created_at DESC
             LIMIT ?"
        )?;

        let results = stmt.query_map(params![limit], |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, Option<String>>(2)?,
                row.get::<_, Option<String>>(3)?,
                row.get::<_, f64>(4)?,
            ))
        })?
        .collect::<Result<Vec<_>, _>>()?;

        Ok(results)
    }

    /// Mark a capsule as consolidated into another
    pub fn consolidate_capsule(&self, capsule_id: i64, consolidated_into: i64) -> Result<()> {
        self.conn.execute(
            "UPDATE knowledge_capsules SET consolidated_into = ? WHERE id = ?",
            params![consolidated_into, capsule_id],
        ).context("Failed to consolidate capsule")?;

        Ok(())
    }

    /// Insert or update a consolidation pattern
    pub fn upsert_consolidation_pattern(
        &self,
        pattern_summary: &str,
        confidence_boost: f64,
    ) -> Result<i64> {
        let now = chrono::Utc::now().timestamp();

        // Try to find existing pattern
        let mut stmt = self.conn.prepare(
            "SELECT id, occurrence_count, confidence_score FROM consolidation_patterns WHERE pattern_summary = ?"
        )?;

        match stmt.query_row(params![pattern_summary], |row| {
            Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?, row.get::<_, f64>(2)?))
        }) {
            Ok((id, count, old_confidence)) => {
                // Update existing pattern
                let new_confidence = (old_confidence + confidence_boost).min(1.0);
                self.conn.execute(
                    "UPDATE consolidation_patterns
                     SET occurrence_count = ?, last_seen = ?, confidence_score = ?
                     WHERE id = ?",
                    params![count + 1, now, new_confidence, id],
                )?;
                Ok(id)
            }
            Err(rusqlite::Error::QueryReturnedNoRows) => {
                // Create new pattern
                self.conn.execute(
                    "INSERT INTO consolidation_patterns (pattern_summary, occurrence_count, first_seen, last_seen, confidence_score)
                     VALUES (?, 1, ?, ?, ?)",
                    params![pattern_summary, now, now, 0.5 + confidence_boost],
                )?;
                Ok(self.conn.last_insert_rowid())
            }
            Err(e) => Err(e.into()),
        }
    }

    /// Link a capsule to a pattern
    pub fn link_capsule_to_pattern(&self, capsule_id: i64, pattern_id: i64) -> Result<()> {
        self.conn.execute(
            "INSERT OR IGNORE INTO capsule_patterns (capsule_id, pattern_id) VALUES (?, ?)",
            params![capsule_id, pattern_id],
        )?;
        Ok(())
    }

    /// Get high-confidence patterns (for context injection)
    pub fn get_active_patterns(&self, min_confidence: f64, limit: i64) -> Result<Vec<(i64, String, i64, f64)>> {
        let mut stmt = self.conn.prepare(
            "SELECT id, pattern_summary, occurrence_count, confidence_score
             FROM consolidation_patterns
             WHERE is_active = 1 AND confidence_score >= ?
             ORDER BY confidence_score DESC
             LIMIT ?"
        )?;

        let results = stmt.query_map(params![min_confidence, limit], |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, i64>(2)?,
                row.get::<_, f64>(3)?,
            ))
        })?
        .collect::<Result<Vec<_>, _>>()?;

        Ok(results)
    }

    /// Get capsule count for stats
    pub fn get_capsule_count(&self) -> Result<i64> {
        let mut stmt = self.conn.prepare("SELECT COUNT(*) FROM knowledge_capsules")?;
        let count: i64 = stmt.query_row([], |row| row.get(0))?;
        Ok(count)
    }

    /// Get capsules that don't have embeddings yet
    pub fn get_capsules_without_embeddings(&self, limit: Option<usize>) -> Result<Vec<(i64, String)>> {
        let query = match limit {
            Some(l) => format!(
                "SELECT kc.id, kc.restatement
                 FROM knowledge_capsules kc
                 LEFT JOIN capsule_embeddings ce ON kc.id = ce.capsule_id
                 WHERE ce.capsule_id IS NULL
                 LIMIT {}",
                l
            ),
            None => "SELECT kc.id, kc.restatement
                     FROM knowledge_capsules kc
                     LEFT JOIN capsule_embeddings ce ON kc.id = ce.capsule_id
                     WHERE ce.capsule_id IS NULL".to_string(),
        };

        let mut stmt = self.conn.prepare(&query)?;
        let results = stmt.query_map([], |row| {
            Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?))
        })?
        .collect::<Result<Vec<_>, _>>()?;

        Ok(results)
    }

    /// Get all capsules for embedding generation (regardless of existing embeddings)
    pub fn get_all_capsules_for_embedding(&self, limit: Option<usize>) -> Result<Vec<(i64, String)>> {
        let query = match limit {
            Some(l) => format!(
                "SELECT id, restatement FROM knowledge_capsules ORDER BY id LIMIT {}",
                l
            ),
            None => "SELECT id, restatement FROM knowledge_capsules ORDER BY id".to_string(),
        };

        let mut stmt = self.conn.prepare(&query)?;
        let results = stmt.query_map([], |row| {
            Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?))
        })?
        .collect::<Result<Vec<_>, _>>()?;

        Ok(results)
    }

    /// Upsert capsule embedding (alias for insert_capsule_embedding which already does INSERT OR REPLACE)
    pub fn upsert_capsule_embedding(
        &self,
        capsule_id: i64,
        embedding: &[f32],
        model_name: &str,
    ) -> Result<()> {
        self.insert_capsule_embedding(capsule_id, embedding, model_name)
    }

    /// Get all embeddings for semantic search
    pub fn get_all_embeddings(&self) -> Result<Vec<(i64, Vec<f32>)>> {
        let mut stmt = self.conn.prepare(
            "SELECT capsule_id, embedding FROM capsule_embeddings"
        )?;

        let results = stmt.query_map([], |row| {
            let capsule_id: i64 = row.get(0)?;
            let embedding_bytes: Vec<u8> = row.get(1)?;

            // Convert bytes back to f32 array
            let embedding: Vec<f32> = embedding_bytes
                .chunks(4)
                .map(|chunk| {
                    let bytes: [u8; 4] = chunk.try_into().unwrap_or([0, 0, 0, 0]);
                    f32::from_le_bytes(bytes)
                })
                .collect();

            Ok((capsule_id, embedding))
        })?
        .collect::<Result<Vec<_>, _>>()?;

        Ok(results)
    }

    /// Get capsule basic info by ID (for display)
    pub fn get_capsule_display_info(&self, capsule_id: i64) -> Result<Option<(String, Option<String>, Option<String>, f64)>> {
        let mut stmt = self.conn.prepare(
            "SELECT restatement, timestamp, topic, confidence_score
             FROM knowledge_capsules
             WHERE id = ?"
        )?;

        match stmt.query_row(params![capsule_id], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, Option<String>>(1)?,
                row.get::<_, Option<String>>(2)?,
                row.get::<_, f64>(3)?,
            ))
        }) {
            Ok(capsule) => Ok(Some(capsule)),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    /// Get embedding count
    pub fn get_embedding_count(&self) -> Result<i64> {
        let mut stmt = self.conn.prepare("SELECT COUNT(*) FROM capsule_embeddings")?;
        let count: i64 = stmt.query_row([], |row| row.get(0))?;
        Ok(count)
    }

    /// Get all capsules for syncing to ICP canister
    pub fn get_all_capsules_for_sync(&self) -> Result<Vec<SyncCapsule>> {
        let mut stmt = self.conn.prepare(
            "SELECT id, conversation_id, restatement, timestamp, location, topic, confidence_score, created_at
             FROM knowledge_capsules
             ORDER BY id ASC"
        )?;

        let capsule_rows: Vec<(i64, String, String, Option<String>, Option<String>, Option<String>, f64, i64)> = stmt
            .query_map([], |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                    row.get(5)?,
                    row.get(6)?,
                    row.get(7)?,
                ))
            })?
            .collect::<Result<Vec<_>, _>>()?;

        let mut capsules = Vec::new();

        for (id, conversation_id, restatement, timestamp, location, topic, confidence_score, created_at) in capsule_rows {
            // Get persons
            let mut persons_stmt = self.conn.prepare(
                "SELECT person_name FROM capsule_persons WHERE capsule_id = ?"
            )?;
            let persons: Vec<String> = persons_stmt
                .query_map(params![id], |row| row.get(0))?
                .collect::<Result<Vec<_>, _>>()?;

            // Get entities
            let mut entities_stmt = self.conn.prepare(
                "SELECT entity_name FROM capsule_entities WHERE capsule_id = ?"
            )?;
            let entities: Vec<String> = entities_stmt
                .query_map(params![id], |row| row.get(0))?
                .collect::<Result<Vec<_>, _>>()?;

            // Get keywords
            let mut keywords_stmt = self.conn.prepare(
                "SELECT keyword FROM capsule_keywords WHERE capsule_id = ?"
            )?;
            let keywords: Vec<String> = keywords_stmt
                .query_map(params![id], |row| row.get(0))?
                .collect::<Result<Vec<_>, _>>()?;

            capsules.push(SyncCapsule {
                id,
                conversation_id,
                restatement,
                timestamp,
                location,
                topic,
                confidence_score,
                created_at,
                persons,
                entities,
                keywords,
            });
        }

        Ok(capsules)
    }

    /// Get all embeddings for syncing to ICP canister
    pub fn get_all_embeddings_for_sync(&self) -> Result<Vec<SyncEmbedding>> {
        let mut stmt = self.conn.prepare(
            "SELECT capsule_id, embedding, model_name FROM capsule_embeddings ORDER BY capsule_id ASC"
        )?;

        let results = stmt.query_map([], |row| {
            let capsule_id: i64 = row.get(0)?;
            let embedding_bytes: Vec<u8> = row.get(1)?;
            let model_name: String = row.get(2)?;

            // Convert bytes back to f32 array
            let embedding: Vec<f32> = embedding_bytes
                .chunks(4)
                .map(|chunk| {
                    let bytes: [u8; 4] = chunk.try_into().unwrap_or([0, 0, 0, 0]);
                    f32::from_le_bytes(bytes)
                })
                .collect();

            Ok(SyncEmbedding {
                capsule_id,
                embedding,
                model_name,
            })
        })?
        .collect::<Result<Vec<_>, _>>()?;

        Ok(results)
    }

    // ============================================================
    // Metabolism Functions
    // ============================================================

    /// Get all pattern embeddings for similarity matching
    pub fn get_pattern_embeddings(&self) -> Result<Vec<(i64, Vec<f32>)>> {
        let mut stmt = self.conn.prepare(
            "SELECT pattern_id, embedding FROM pattern_embeddings"
        )?;

        let results = stmt.query_map([], |row| {
            let pattern_id: i64 = row.get(0)?;
            let embedding_blob: Vec<u8> = row.get(1)?;
            let embedding = bytes_to_f32_vec(&embedding_blob);
            Ok((pattern_id, embedding))
        })?
        .collect::<Result<Vec<_>, _>>()?;

        Ok(results)
    }

    /// Reinforce a pattern (increase confidence, update timestamp)
    pub fn reinforce_pattern(&self, pattern_id: i64, confidence_boost: f64) -> Result<()> {
        let now = chrono::Utc::now().timestamp();

        self.conn.execute(
            "UPDATE consolidation_patterns
             SET occurrence_count = occurrence_count + 1,
                 last_seen = ?,
                 confidence_score = MIN(1.0, confidence_score + ?)
             WHERE id = ?",
            params![now, confidence_boost, pattern_id],
        ).context("Failed to reinforce pattern")?;

        Ok(())
    }

    /// Get capsule info by ID
    pub fn get_capsule_by_id(&self, capsule_id: i64) -> Result<Option<CapsuleInfo>> {
        let mut stmt = self.conn.prepare(
            "SELECT id, restatement, topic FROM knowledge_capsules WHERE id = ?"
        )?;

        match stmt.query_row(params![capsule_id], |row| {
            Ok(CapsuleInfo {
                id: row.get(0)?,
                restatement: row.get(1)?,
                topic: row.get(2)?,
            })
        }) {
            Ok(info) => Ok(Some(info)),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    /// Create a new pattern
    pub fn create_pattern(&self, summary: &str, initial_confidence: f64) -> Result<i64> {
        let now = chrono::Utc::now().timestamp();

        self.conn.execute(
            "INSERT INTO consolidation_patterns (pattern_summary, occurrence_count, first_seen, last_seen, confidence_score, is_active)
             VALUES (?, 1, ?, ?, ?, 1)",
            params![summary, now, now, initial_confidence],
        )?;

        Ok(self.conn.last_insert_rowid())
    }

    /// Store embedding for a pattern
    pub fn store_pattern_embedding(&self, pattern_id: i64, embedding: &[f32]) -> Result<()> {
        let now = chrono::Utc::now().timestamp();
        let embedding_blob = f32_vec_to_bytes(embedding);

        self.conn.execute(
            "INSERT OR REPLACE INTO pattern_embeddings (pattern_id, embedding, created_at)
             VALUES (?, ?, ?)",
            params![pattern_id, embedding_blob, now],
        )?;

        Ok(())
    }

    /// Get patterns older than timestamp (for decay)
    pub fn get_patterns_older_than(&self, cutoff_timestamp: i64) -> Result<Vec<(i64, f64)>> {
        let mut stmt = self.conn.prepare(
            "SELECT id, confidence_score FROM consolidation_patterns
             WHERE is_active = 1 AND last_seen < ?"
        )?;

        let results = stmt.query_map(params![cutoff_timestamp], |row| {
            Ok((row.get(0)?, row.get(1)?))
        })?
        .collect::<Result<Vec<_>, _>>()?;

        Ok(results)
    }

    /// Deactivate a pattern
    pub fn deactivate_pattern(&self, pattern_id: i64) -> Result<()> {
        self.conn.execute(
            "UPDATE consolidation_patterns SET is_active = 0 WHERE id = ?",
            params![pattern_id],
        )?;
        Ok(())
    }

    /// Update pattern confidence directly
    pub fn update_pattern_confidence(&self, pattern_id: i64, confidence: f64) -> Result<()> {
        self.conn.execute(
            "UPDATE consolidation_patterns SET confidence_score = ? WHERE id = ?",
            params![confidence, pattern_id],
        )?;
        Ok(())
    }

    /// Update pattern summary
    pub fn update_pattern_summary(&self, pattern_id: i64, summary: &str) -> Result<()> {
        self.conn.execute(
            "UPDATE consolidation_patterns SET pattern_summary = ? WHERE id = ?",
            params![summary, pattern_id],
        )?;
        Ok(())
    }

    /// Get capsules linked to a specific pattern (for summary regeneration)
    pub fn get_capsules_for_pattern(&self, pattern_id: i64, limit: i64) -> Result<Vec<(i64, String, Option<String>)>> {
        let mut stmt = self.conn.prepare(
            "SELECT kc.id, kc.restatement, kc.topic
             FROM knowledge_capsules kc
             JOIN capsule_patterns cp ON kc.id = cp.capsule_id
             WHERE cp.pattern_id = ?
             ORDER BY kc.created_at DESC
             LIMIT ?"
        )?;

        let results = stmt.query_map(params![pattern_id, limit], |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, Option<String>>(2)?,
            ))
        })?
        .collect::<Result<Vec<_>, _>>()?;

        Ok(results)
    }

    /// Get top keywords for capsules in a pattern (for summary generation)
    pub fn get_keywords_for_pattern(&self, pattern_id: i64, limit: i64) -> Result<Vec<(String, i64)>> {
        let mut stmt = self.conn.prepare(
            "SELECT ckw.keyword, COUNT(*) as count
             FROM capsule_keywords ckw
             JOIN capsule_patterns cp ON ckw.capsule_id = cp.capsule_id
             WHERE cp.pattern_id = ?
             GROUP BY ckw.keyword
             ORDER BY count DESC
             LIMIT ?"
        )?;

        let results = stmt.query_map(params![pattern_id, limit], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, i64>(1)?,
            ))
        })?
        .collect::<Result<Vec<_>, _>>()?;

        Ok(results)
    }

    /// Get capsules that haven't been metabolized yet
    pub fn get_unmetabolized_capsules(&self) -> Result<Vec<(i64, String)>> {
        let mut stmt = self.conn.prepare(
            "SELECT id, restatement FROM knowledge_capsules
             WHERE metabolized_at IS NULL AND consolidated_into IS NULL
             ORDER BY created_at ASC"
        )?;

        let results = stmt.query_map([], |row| {
            Ok((row.get(0)?, row.get(1)?))
        })?
        .collect::<Result<Vec<_>, _>>()?;

        Ok(results)
    }

    /// Get capsule embedding by ID
    pub fn get_capsule_embedding(&self, capsule_id: i64) -> Result<Option<Vec<f32>>> {
        let mut stmt = self.conn.prepare(
            "SELECT embedding FROM capsule_embeddings WHERE capsule_id = ?"
        )?;

        match stmt.query_row(params![capsule_id], |row| {
            let blob: Vec<u8> = row.get(0)?;
            Ok(bytes_to_f32_vec(&blob))
        }) {
            Ok(embedding) => Ok(Some(embedding)),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    /// Store capsule embedding
    pub fn store_capsule_embedding(&self, capsule_id: i64, embedding: &[f32], model_name: &str) -> Result<()> {
        let now = chrono::Utc::now().timestamp();
        let embedding_blob = f32_vec_to_bytes(embedding);

        self.conn.execute(
            "INSERT OR REPLACE INTO capsule_embeddings (capsule_id, embedding, model_name, created_at)
             VALUES (?, ?, ?, ?)",
            params![capsule_id, embedding_blob, model_name, now],
        )?;

        Ok(())
    }

    /// Mark capsule as metabolized
    pub fn mark_capsule_metabolized(&self, capsule_id: i64) -> Result<()> {
        let now = chrono::Utc::now().timestamp();

        self.conn.execute(
            "UPDATE knowledge_capsules SET metabolized_at = ? WHERE id = ?",
            params![now, capsule_id],
        )?;

        Ok(())
    }

    /// Get pattern count
    pub fn get_pattern_count(&self) -> Result<i64> {
        let mut stmt = self.conn.prepare(
            "SELECT COUNT(*) FROM consolidation_patterns WHERE is_active = 1"
        )?;
        let count: i64 = stmt.query_row([], |row| row.get(0))?;
        Ok(count)
    }

    /// Get all patterns with their details (filtered)
    pub fn get_all_patterns(&self, min_confidence: f64, limit: usize, active_only: bool) -> Result<Vec<(i64, String, i64, f64, bool)>> {
        let query = if active_only {
            "SELECT id, pattern_summary, occurrence_count, confidence_score, is_active
             FROM consolidation_patterns
             WHERE confidence_score >= ? AND is_active = 1
             ORDER BY confidence_score DESC
             LIMIT ?"
        } else {
            "SELECT id, pattern_summary, occurrence_count, confidence_score, is_active
             FROM consolidation_patterns
             WHERE confidence_score >= ?
             ORDER BY confidence_score DESC
             LIMIT ?"
        };

        let mut stmt = self.conn.prepare(query)?;

        let results = stmt.query_map(params![min_confidence, limit as i64], |row| {
            Ok((
                row.get(0)?,
                row.get(1)?,
                row.get(2)?,
                row.get(3)?,
                row.get::<_, i64>(4)? == 1,
            ))
        })?
        .collect::<Result<Vec<_>, _>>()?;

        Ok(results)
    }

    /// Get a specific pattern by ID
    pub fn get_pattern_by_id(&self, pattern_id: i64) -> Result<Option<(String, f64, Option<i64>, bool)>> {
        let mut stmt = self.conn.prepare(
            "SELECT pattern_summary, confidence_score, last_seen, is_active
             FROM consolidation_patterns
             WHERE id = ?"
        )?;

        let result = stmt.query_row(params![pattern_id], |row| {
            Ok((
                row.get(0)?,
                row.get(1)?,
                row.get(2)?,
                row.get::<_, i64>(3)? == 1,
            ))
        }).optional()?;

        Ok(result)
    }

    /// Get capsules linked to a pattern
    pub fn get_pattern_capsules(&self, pattern_id: i64) -> Result<Vec<(i64, String)>> {
        let mut stmt = self.conn.prepare(
            "SELECT kc.id, kc.restatement
             FROM knowledge_capsules kc
             JOIN capsule_patterns cpl ON kc.id = cpl.capsule_id
             WHERE cpl.pattern_id = ?
             ORDER BY kc.id DESC"
        )?;

        let results = stmt.query_map(params![pattern_id], |row| {
            Ok((row.get(0)?, row.get(1)?))
        })?
        .collect::<Result<Vec<_>, _>>()?;

        Ok(results)
    }

    /// Get N most recent capsules linked to a pattern (for MCP enrichment)
    pub fn get_recent_pattern_capsules(&self, pattern_id: i64, limit: usize) -> Result<Vec<(i64, String, Option<String>)>> {
        let mut stmt = self.conn.prepare(
            "SELECT kc.id, kc.restatement, kc.timestamp
             FROM knowledge_capsules kc
             JOIN capsule_patterns cpl ON kc.id = cpl.capsule_id
             WHERE cpl.pattern_id = ?
             ORDER BY kc.created_at DESC
             LIMIT ?"
        )?;

        let results = stmt.query_map(params![pattern_id, limit as i64], |row| {
            Ok((row.get(0)?, row.get(1)?, row.get(2)?))
        })?
        .collect::<Result<Vec<_>, _>>()?;

        Ok(results)
    }

    /// Get enriched pattern info for MCP (includes last_seen for decay calculation)
    pub fn get_enriched_patterns(&self, min_confidence: f64, limit: usize, active_only: bool) -> Result<Vec<EnrichedPattern>> {
        let query = if active_only {
            "SELECT id, pattern_summary, occurrence_count, confidence_score, is_active, first_seen, last_seen
             FROM consolidation_patterns
             WHERE confidence_score >= ? AND is_active = 1
             ORDER BY confidence_score DESC
             LIMIT ?"
        } else {
            "SELECT id, pattern_summary, occurrence_count, confidence_score, is_active, first_seen, last_seen
             FROM consolidation_patterns
             WHERE confidence_score >= ?
             ORDER BY confidence_score DESC
             LIMIT ?"
        };

        let mut stmt = self.conn.prepare(query)?;

        let patterns: Vec<(i64, String, i64, f64, bool, i64, i64)> = stmt.query_map(params![min_confidence, limit as i64], |row| {
            Ok((
                row.get(0)?,
                row.get(1)?,
                row.get(2)?,
                row.get(3)?,
                row.get::<_, i64>(4)? == 1,
                row.get(5)?,
                row.get(6)?,
            ))
        })?
        .collect::<Result<Vec<_>, _>>()?;

        let mut enriched = Vec::new();
        let now = chrono::Utc::now().timestamp();
        let decay_rate = 0.02; // From MetabolismConfig default
        let grace_days = 14i64;
        let min_active = 0.15;

        for (id, summary, count, confidence, active, first_seen, last_seen) in patterns {
            // Get recent capsules
            let recent_capsules = self.get_recent_pattern_capsules(id, 3)?;

            // Calculate decay trajectory
            let days_since_reinforcement = (now - last_seen) / (24 * 60 * 60);
            let days_until_decay_starts = (grace_days - days_since_reinforcement).max(0);

            // Project confidence if no new reinforcement (after grace period)
            let projected_confidence_7d = if days_since_reinforcement >= grace_days {
                (confidence - (decay_rate * 7.0)).max(0.0)
            } else {
                let days_of_decay = (7 - days_until_decay_starts).max(0) as f64;
                (confidence - (decay_rate * days_of_decay)).max(0.0)
            };

            let will_deactivate_in = if projected_confidence_7d < min_active && confidence >= min_active {
                let decays_needed = ((confidence - min_active) / decay_rate).ceil() as i64;
                Some(days_until_decay_starts + decays_needed)
            } else {
                None
            };

            enriched.push(EnrichedPattern {
                id,
                summary,
                capsule_count: count,
                confidence,
                active,
                first_seen,
                last_seen,
                days_since_reinforcement,
                days_until_decay_starts,
                projected_confidence_7d,
                will_deactivate_in,
                recent_capsules,
            });
        }

        Ok(enriched)
    }

    /// Get patterns that need reinforcement (approaching or in decay)
    /// Returns patterns where decay starts in <= threshold_days
    pub fn get_patterns_needing_reinforcement(&self, threshold_days: i64, limit: usize) -> Result<Vec<EnrichedPattern>> {
        let all_patterns = self.get_enriched_patterns(0.0, 100, true)?;

        let mut needing_reinforcement: Vec<EnrichedPattern> = all_patterns
            .into_iter()
            .filter(|p| p.days_until_decay_starts <= threshold_days && p.confidence > 0.15)
            .collect();

        // Sort by urgency (lowest days_until_decay first, then by importance/confidence)
        needing_reinforcement.sort_by(|a, b| {
            a.days_until_decay_starts.cmp(&b.days_until_decay_starts)
                .then(b.confidence.partial_cmp(&a.confidence).unwrap_or(std::cmp::Ordering::Equal))
        });

        needing_reinforcement.truncate(limit);
        Ok(needing_reinforcement)
    }

    // ============================================================
    // Compressed Cognitive State (CCS) Methods
    // ============================================================

    /// Get the current cognitive state (creates default if none exists)
    pub fn get_cognitive_state(&self) -> Result<crate::cognitive::CognitiveState> {
        use crate::cognitive::CognitiveState;

        let mut stmt = self.conn.prepare(
            "SELECT episodic_trace, semantic_gist, focal_entities, relational_map,
                    goal_orientation, constraints, predictive_cue, uncertainty_signals,
                    retrieved_artifacts, updated_at, compression_model, version
             FROM cognitive_state WHERE id = 1"
        )?;

        match stmt.query_row([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, String>(3)?,
                row.get::<_, String>(4)?,
                row.get::<_, String>(5)?,
                row.get::<_, String>(6)?,
                row.get::<_, String>(7)?,
                row.get::<_, String>(8)?,
                row.get::<_, i64>(9)?,
                row.get::<_, Option<String>>(10)?,
                row.get::<_, i64>(11)?,
            ))
        }) {
            Ok((episodic, gist, entities, relations, goal, constraints, cue, uncertainty, artifacts, updated, model, version)) => {
                Ok(CognitiveState {
                    episodic_trace: serde_json::from_str(&episodic).unwrap_or_default(),
                    semantic_gist: gist,
                    focal_entities: serde_json::from_str(&entities).unwrap_or_default(),
                    relational_map: serde_json::from_str(&relations).unwrap_or_default(),
                    goal_orientation: goal,
                    constraints: serde_json::from_str(&constraints).unwrap_or_default(),
                    predictive_cue: cue,
                    uncertainty_signals: serde_json::from_str(&uncertainty).unwrap_or_default(),
                    retrieved_artifacts: serde_json::from_str(&artifacts).unwrap_or_default(),
                    updated_at: updated,
                    compression_model: model,
                    version,
                })
            }
            Err(rusqlite::Error::QueryReturnedNoRows) => {
                // No CCS exists yet - return initial state for the operator
                Ok(CognitiveState::initial_default())
            }
            Err(e) => Err(e.into()),
        }
    }

    /// Replace the cognitive state (REPLACEMENT semantics - overwrites completely)
    pub fn set_cognitive_state(&self, state: &crate::cognitive::CognitiveState) -> Result<()> {
        let now = chrono::Utc::now().timestamp();

        // First, save current state to history (if it exists)
        if let Ok(current) = self.get_cognitive_state() {
            if current.version > 0 {
                let snapshot = current.to_json()?;
                self.conn.execute(
                    "INSERT INTO cognitive_state_history (snapshot, created_at, trigger)
                     VALUES (?, ?, 'replacement')",
                    params![snapshot, now],
                )?;
            }
        }

        // Now replace the state (using INSERT OR REPLACE)
        self.conn.execute(
            "INSERT OR REPLACE INTO cognitive_state
             (id, episodic_trace, semantic_gist, focal_entities, relational_map,
              goal_orientation, constraints, predictive_cue, uncertainty_signals,
              retrieved_artifacts, updated_at, compression_model, version)
             VALUES (1, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            params![
                serde_json::to_string(&state.episodic_trace)?,
                state.semantic_gist,
                serde_json::to_string(&state.focal_entities)?,
                serde_json::to_string(&state.relational_map)?,
                state.goal_orientation,
                serde_json::to_string(&state.constraints)?,
                state.predictive_cue,
                serde_json::to_string(&state.uncertainty_signals)?,
                serde_json::to_string(&state.retrieved_artifacts)?,
                now,
                state.compression_model,
                state.version + 1,
            ],
        )?;

        Ok(())
    }

    /// Get CCS history (for debugging/analysis)
    pub fn get_cognitive_state_history(&self, limit: usize) -> Result<Vec<(i64, String, String)>> {
        let mut stmt = self.conn.prepare(
            "SELECT id, snapshot, trigger FROM cognitive_state_history
             ORDER BY created_at DESC LIMIT ?"
        )?;

        let rows = stmt.query_map(params![limit as i64], |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
            ))
        })?;

        let mut history = Vec::new();
        for row in rows {
            history.push(row?);
        }

        Ok(history)
    }

    // ============================================================
    // Scratch Pad Methods
    // ============================================================

    /// Write a note to the scratch pad
    pub fn write_scratch_note(
        &self,
        content: &str,
        category: Option<&str>,
        priority: i32,
        expires_at: Option<i64>,
    ) -> Result<i64> {
        let now = chrono::Utc::now().timestamp();

        self.conn.execute(
            "INSERT INTO scratch_pad (content, category, priority, created_at, updated_at, expires_at, resolved)
             VALUES (?, ?, ?, ?, ?, ?, 0)",
            params![content, category, priority, now, now, expires_at],
        ).context("Failed to write scratch note")?;

        Ok(self.conn.last_insert_rowid())
    }

    /// Get scratch notes (optionally filtered by category and resolved status)
    pub fn get_scratch_notes(
        &self,
        limit: usize,
        category: Option<&str>,
        include_resolved: bool,
    ) -> Result<Vec<ScratchNote>> {
        let now = chrono::Utc::now().timestamp();

        let query = match (category, include_resolved) {
            (Some(_), true) => {
                "SELECT id, content, category, priority, created_at, updated_at, expires_at, resolved
                 FROM scratch_pad
                 WHERE category = ? AND (expires_at IS NULL OR expires_at > ?)
                 ORDER BY priority DESC, created_at DESC
                 LIMIT ?"
            }
            (Some(_), false) => {
                "SELECT id, content, category, priority, created_at, updated_at, expires_at, resolved
                 FROM scratch_pad
                 WHERE category = ? AND resolved = 0 AND (expires_at IS NULL OR expires_at > ?)
                 ORDER BY priority DESC, created_at DESC
                 LIMIT ?"
            }
            (None, true) => {
                "SELECT id, content, category, priority, created_at, updated_at, expires_at, resolved
                 FROM scratch_pad
                 WHERE (expires_at IS NULL OR expires_at > ?)
                 ORDER BY priority DESC, created_at DESC
                 LIMIT ?"
            }
            (None, false) => {
                "SELECT id, content, category, priority, created_at, updated_at, expires_at, resolved
                 FROM scratch_pad
                 WHERE resolved = 0 AND (expires_at IS NULL OR expires_at > ?)
                 ORDER BY priority DESC, created_at DESC
                 LIMIT ?"
            }
        };

        let mut stmt = self.conn.prepare(query)?;

        let results: Vec<ScratchNote> = match category {
            Some(cat) => {
                stmt.query_map(params![cat, now, limit as i64], |row| {
                    Ok(ScratchNote {
                        id: row.get(0)?,
                        content: row.get(1)?,
                        category: row.get(2)?,
                        priority: row.get(3)?,
                        created_at: row.get(4)?,
                        updated_at: row.get(5)?,
                        expires_at: row.get(6)?,
                        resolved: row.get::<_, i64>(7)? == 1,
                    })
                })?
                .collect::<Result<Vec<_>, _>>()?
            }
            None => {
                stmt.query_map(params![now, limit as i64], |row| {
                    Ok(ScratchNote {
                        id: row.get(0)?,
                        content: row.get(1)?,
                        category: row.get(2)?,
                        priority: row.get(3)?,
                        created_at: row.get(4)?,
                        updated_at: row.get(5)?,
                        expires_at: row.get(6)?,
                        resolved: row.get::<_, i64>(7)? == 1,
                    })
                })?
                .collect::<Result<Vec<_>, _>>()?
            }
        };

        Ok(results)
    }

    /// Resolve a scratch note (mark as done without deleting)
    pub fn resolve_scratch_note(&self, note_id: i64) -> Result<bool> {
        let now = chrono::Utc::now().timestamp();

        let rows_affected = self.conn.execute(
            "UPDATE scratch_pad SET resolved = 1, updated_at = ? WHERE id = ?",
            params![now, note_id],
        )?;

        Ok(rows_affected > 0)
    }

    /// Update a scratch note's content
    pub fn update_scratch_note(
        &self,
        note_id: i64,
        content: &str,
        category: Option<&str>,
        priority: Option<i32>,
    ) -> Result<bool> {
        let now = chrono::Utc::now().timestamp();

        // Build dynamic update
        let rows_affected = match (category, priority) {
            (Some(cat), Some(pri)) => {
                self.conn.execute(
                    "UPDATE scratch_pad SET content = ?, category = ?, priority = ?, updated_at = ? WHERE id = ?",
                    params![content, cat, pri, now, note_id],
                )?
            }
            (Some(cat), None) => {
                self.conn.execute(
                    "UPDATE scratch_pad SET content = ?, category = ?, updated_at = ? WHERE id = ?",
                    params![content, cat, now, note_id],
                )?
            }
            (None, Some(pri)) => {
                self.conn.execute(
                    "UPDATE scratch_pad SET content = ?, priority = ?, updated_at = ? WHERE id = ?",
                    params![content, pri, now, note_id],
                )?
            }
            (None, None) => {
                self.conn.execute(
                    "UPDATE scratch_pad SET content = ?, updated_at = ? WHERE id = ?",
                    params![content, now, note_id],
                )?
            }
        };

        Ok(rows_affected > 0)
    }

    /// Delete a scratch note permanently
    pub fn delete_scratch_note(&self, note_id: i64) -> Result<bool> {
        let rows_affected = self.conn.execute(
            "DELETE FROM scratch_pad WHERE id = ?",
            params![note_id],
        )?;

        Ok(rows_affected > 0)
    }

    /// Clear all resolved notes
    pub fn clear_resolved_notes(&self) -> Result<usize> {
        let rows_affected = self.conn.execute(
            "DELETE FROM scratch_pad WHERE resolved = 1",
            [],
        )?;

        Ok(rows_affected)
    }

    /// Clear expired notes
    pub fn clear_expired_notes(&self) -> Result<usize> {
        let now = chrono::Utc::now().timestamp();

        let rows_affected = self.conn.execute(
            "DELETE FROM scratch_pad WHERE expires_at IS NOT NULL AND expires_at <= ?",
            params![now],
        )?;

        Ok(rows_affected)
    }

    /// Get scratch note count (active only by default)
    pub fn get_scratch_note_count(&self, include_resolved: bool) -> Result<i64> {
        let now = chrono::Utc::now().timestamp();

        let query = if include_resolved {
            "SELECT COUNT(*) FROM scratch_pad WHERE (expires_at IS NULL OR expires_at > ?)"
        } else {
            "SELECT COUNT(*) FROM scratch_pad WHERE resolved = 0 AND (expires_at IS NULL OR expires_at > ?)"
        };

        let mut stmt = self.conn.prepare(query)?;
        let count: i64 = stmt.query_row(params![now], |row| row.get(0))?;

        Ok(count)
    }

    // ============================================================
    // Thought Stream Methods
    // ============================================================

    /// Log a thought to the stream
    pub fn log_thought(
        &self,
        cycle_id: &str,
        reasoning: &str,
        context_summary: &str,
        actions_taken: &str,
    ) -> Result<i64> {
        let now = chrono::Utc::now().timestamp();

        self.conn.execute(
            "INSERT INTO thought_stream (cycle_id, reasoning, context_summary, actions_taken, created_at)
             VALUES (?, ?, ?, ?, ?)",
            params![cycle_id, reasoning, context_summary, actions_taken, now],
        ).context("Failed to log thought")?;

        Ok(self.conn.last_insert_rowid())
    }

    /// Get recent thoughts
    pub fn get_recent_thoughts(&self, limit: usize) -> Result<Vec<ThoughtEntry>> {
        let mut stmt = self.conn.prepare(
            "SELECT id, cycle_id, reasoning, context_summary, actions_taken, created_at
             FROM thought_stream
             ORDER BY created_at DESC
             LIMIT ?"
        )?;

        let rows = stmt.query_map(params![limit as i64], |row| {
            Ok(ThoughtEntry {
                id: row.get(0)?,
                cycle_id: row.get(1)?,
                reasoning: row.get(2)?,
                context_summary: row.get(3)?,
                actions_taken: row.get(4)?,
                created_at: row.get(5)?,
            })
        })?;

        let mut thoughts = Vec::new();
        for row in rows {
            thoughts.push(row?);
        }

        Ok(thoughts)
    }

    // ============================================================
    // Outbox Methods - Messages for the operator
    // ============================================================

    /// Send a message to the outbox
    pub fn send_to_outbox(
        &self,
        message: &str,
        priority: i32,
        category: Option<&str>,
    ) -> Result<i64> {
        let now = chrono::Utc::now().timestamp();

        self.conn.execute(
            "INSERT INTO outbox (message, priority, category, created_at)
             VALUES (?, ?, ?, ?)",
            params![message, priority, category, now],
        ).context("Failed to send to outbox")?;

        Ok(self.conn.last_insert_rowid())
    }

    /// Get unread messages from outbox
    pub fn get_unread_messages(&self, limit: usize) -> Result<Vec<OutboxMessage>> {
        let mut stmt = self.conn.prepare(
            "SELECT id, message, priority, category, created_at, acknowledged, read_at
             FROM outbox
             WHERE acknowledged = 0
             ORDER BY priority DESC, created_at ASC
             LIMIT ?"
        )?;

        let rows = stmt.query_map(params![limit as i64], |row| {
            Ok(OutboxMessage {
                id: row.get(0)?,
                message: row.get(1)?,
                priority: row.get(2)?,
                category: row.get(3)?,
                created_at: row.get(4)?,
                acknowledged: row.get::<_, i32>(5)? != 0,
                read_at: row.get(6)?,
            })
        })?;

        let mut messages = Vec::new();
        for row in rows {
            messages.push(row?);
        }

        Ok(messages)
    }

    /// Get all messages from outbox (both read and unread)
    pub fn get_all_outbox_messages(&self, limit: usize) -> Result<Vec<OutboxMessage>> {
        let mut stmt = self.conn.prepare(
            "SELECT id, message, priority, category, created_at, acknowledged, read_at
             FROM outbox
             ORDER BY acknowledged ASC, priority DESC, created_at DESC
             LIMIT ?"
        )?;

        let rows = stmt.query_map(params![limit as i64], |row| {
            Ok(OutboxMessage {
                id: row.get(0)?,
                message: row.get(1)?,
                priority: row.get(2)?,
                category: row.get(3)?,
                created_at: row.get(4)?,
                acknowledged: row.get::<_, i32>(5)? != 0,
                read_at: row.get(6)?,
            })
        })?;

        let mut messages = Vec::new();
        for row in rows {
            messages.push(row?);
        }

        Ok(messages)
    }

    /// Count unread outbox messages
    pub fn count_unread_messages(&self) -> Result<usize> {
        let count: i64 = self.conn.query_row(
            "SELECT COUNT(*) FROM outbox WHERE acknowledged = 0",
            [],
            |row| row.get(0),
        )?;
        Ok(count as usize)
    }

    /// Count total outbox messages
    pub fn count_outbox_messages(&self) -> Result<usize> {
        let count: i64 = self.conn.query_row(
            "SELECT COUNT(*) FROM outbox",
            [],
            |row| row.get(0),
        )?;
        Ok(count as usize)
    }

    /// Acknowledge a message (mark as read)
    pub fn acknowledge_message(&self, message_id: i64) -> Result<bool> {
        let now = chrono::Utc::now().timestamp();

        let rows = self.conn.execute(
            "UPDATE outbox SET acknowledged = 1, read_at = ? WHERE id = ?",
            params![now, message_id],
        )?;

        Ok(rows > 0)
    }

    /// Acknowledge all messages
    pub fn acknowledge_all_messages(&self) -> Result<usize> {
        let now = chrono::Utc::now().timestamp();

        let rows = self.conn.execute(
            "UPDATE outbox SET acknowledged = 1, read_at = ? WHERE acknowledged = 0",
            params![now],
        )?;

        Ok(rows)
    }

    // ============================================================
    // Mind Timestamps - Track important cognitive loop events
    // ============================================================

    /// Set a timestamp for a key event
    pub fn set_mind_timestamp(&self, key: &str, metadata: Option<&str>) -> Result<()> {
        let now = chrono::Utc::now().timestamp();

        self.conn.execute(
            "INSERT INTO mind_timestamps (key, timestamp, metadata)
             VALUES (?, ?, ?)
             ON CONFLICT(key) DO UPDATE SET timestamp = excluded.timestamp, metadata = excluded.metadata",
            params![key, now, metadata],
        ).context("Failed to set mind timestamp")?;

        Ok(())
    }

    /// Get a timestamp for a key event
    pub fn get_mind_timestamp(&self, key: &str) -> Result<Option<(i64, Option<String>)>> {
        let mut stmt = self.conn.prepare(
            "SELECT timestamp, metadata FROM mind_timestamps WHERE key = ?"
        )?;

        let result = stmt.query_row(params![key], |row| {
            Ok((row.get::<_, i64>(0)?, row.get::<_, Option<String>>(1)?))
        });

        match result {
            Ok(data) => Ok(Some(data)),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    /// Get hours since a key event occurred
    pub fn hours_since_event(&self, key: &str) -> Result<Option<f64>> {
        let now = chrono::Utc::now().timestamp();

        match self.get_mind_timestamp(key)? {
            Some((timestamp, _)) => {
                let seconds = (now - timestamp) as f64;
                Ok(Some(seconds / 3600.0))
            }
            None => Ok(None),
        }
    }

    // ============================================================
    // Price History - Market awareness for autonomous trading
    // ============================================================

    /// Store a price data point
    pub fn store_price(&self, symbol: &str, price_usd: f64, source: &str) -> Result<i64> {
        let now = chrono::Utc::now().timestamp();

        self.conn.execute(
            "INSERT INTO price_history (symbol, price_usd, source, timestamp)
             VALUES (?, ?, ?, ?)",
            params![symbol, price_usd, source, now],
        ).context("Failed to store price")?;

        Ok(self.conn.last_insert_rowid())
    }

    /// Get recent prices for a symbol (for RSI calculation)
    pub fn get_recent_prices(&self, symbol: &str, limit: usize) -> Result<Vec<(f64, i64)>> {
        let mut stmt = self.conn.prepare(
            "SELECT price_usd, timestamp FROM price_history
             WHERE symbol = ?
             ORDER BY timestamp DESC
             LIMIT ?"
        )?;

        let prices = stmt.query_map(params![symbol, limit as i64], |row| {
            Ok((row.get::<_, f64>(0)?, row.get::<_, i64>(1)?))
        })?;

        let mut result = Vec::new();
        for price in prices {
            result.push(price?);
        }

        Ok(result)
    }

    /// Calculate RSI(14) for a symbol
    /// Returns None if insufficient data (need at least 15 prices)
    pub fn calculate_rsi(&self, symbol: &str) -> Result<Option<f64>> {
        // Need 15 prices to calculate RSI(14) - 14 periods of change
        let prices = self.get_recent_prices(symbol, 15)?;

        if prices.len() < 15 {
            return Ok(None);
        }

        // Prices are in descending order (newest first), reverse for calculation
        let prices: Vec<f64> = prices.into_iter().map(|(p, _)| p).rev().collect();

        // Calculate price changes
        let mut gains = Vec::new();
        let mut losses = Vec::new();

        for i in 1..prices.len() {
            let change = prices[i] - prices[i - 1];
            if change > 0.0 {
                gains.push(change);
                losses.push(0.0);
            } else {
                gains.push(0.0);
                losses.push(-change);
            }
        }

        // Calculate average gain and loss (simple moving average for first RSI)
        let avg_gain: f64 = gains.iter().sum::<f64>() / 14.0;
        let avg_loss: f64 = losses.iter().sum::<f64>() / 14.0;

        // Avoid division by zero
        if avg_loss == 0.0 {
            return Ok(Some(100.0)); // All gains, RSI = 100
        }

        let rs = avg_gain / avg_loss;
        let rsi = 100.0 - (100.0 / (1.0 + rs));

        Ok(Some(rsi))
    }

    /// Get latest price for a symbol
    pub fn get_latest_price(&self, symbol: &str) -> Result<Option<(f64, i64)>> {
        let prices = self.get_recent_prices(symbol, 1)?;
        Ok(prices.into_iter().next())
    }

    /// Get price count for a symbol
    pub fn get_price_count(&self, symbol: &str) -> Result<usize> {
        let count: i64 = self.conn.query_row(
            "SELECT COUNT(*) FROM price_history WHERE symbol = ?",
            params![symbol],
            |row| row.get(0),
        )?;
        Ok(count as usize)
    }

    // ============================================================
    // Swap History - Track autonomous swaps with guardrails
    // ============================================================

    /// Record a swap attempt
    pub fn record_swap(
        &self,
        amount_xrp: f64,
        amount_rlusd: Option<f64>,
        xrp_price_usd: Option<f64>,
        rsi_value: Option<f64>,
        reason: &str,
        tx_hash: Option<&str>,
        success: bool,
    ) -> Result<i64> {
        let now = chrono::Utc::now().timestamp();

        self.conn.execute(
            "INSERT INTO swap_history (amount_xrp, amount_rlusd, xrp_price_usd, rsi_value, reason, tx_hash, success, timestamp)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            params![amount_xrp, amount_rlusd, xrp_price_usd, rsi_value, reason, tx_hash, success as i32, now],
        ).context("Failed to record swap")?;

        Ok(self.conn.last_insert_rowid())
    }

    /// Get hours since last successful swap
    pub fn hours_since_last_swap(&self) -> Result<Option<f64>> {
        let now = chrono::Utc::now().timestamp();

        let result: rusqlite::Result<i64> = self.conn.query_row(
            "SELECT timestamp FROM swap_history WHERE success = 1 ORDER BY timestamp DESC LIMIT 1",
            [],
            |row| row.get(0),
        );

        match result {
            Ok(timestamp) => {
                let seconds = (now - timestamp) as f64;
                Ok(Some(seconds / 3600.0))
            }
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    /// Get total XRP swapped in last N hours
    pub fn xrp_swapped_in_hours(&self, hours: f64) -> Result<f64> {
        let now = chrono::Utc::now().timestamp();
        let since = now - (hours * 3600.0) as i64;

        let total: f64 = self.conn.query_row(
            "SELECT COALESCE(SUM(amount_xrp), 0.0) FROM swap_history
             WHERE success = 1 AND timestamp > ?",
            params![since],
            |row| row.get(0),
        )?;

        Ok(total)
    }

    /// Check swap guardrails - returns (can_swap, reason)
    pub fn check_swap_guardrails(&self, proposed_amount: f64) -> Result<(bool, String)> {
        // Guardrail 1: Minimum 4 hours between swaps
        if let Some(hours) = self.hours_since_last_swap()? {
            if hours < 4.0 {
                return Ok((false, format!("Only {:.1} hours since last swap (minimum 4)", hours)));
            }
        }

        // Guardrail 2: Max 0.5 XRP per swap
        if proposed_amount > 0.5 {
            return Ok((false, format!("Proposed {:.2} XRP exceeds max 0.5 per swap", proposed_amount)));
        }

        // Guardrail 3: Max 2 XRP per 24 hours
        let daily_total = self.xrp_swapped_in_hours(24.0)?;
        if daily_total + proposed_amount > 2.0 {
            return Ok((false, format!("Would exceed 24h limit: {:.2} + {:.2} > 2.0", daily_total, proposed_amount)));
        }

        Ok((true, "Guardrails passed".to_string()))
    }

    // ============================================================
    // Creative Challenges - Enrichment through creative engagement
    // ============================================================

    /// Pose a new creative challenge for Chronicle Mind
    pub fn pose_challenge(
        &self,
        prompt: &str,
        category: &str,
        posed_by: &str,
    ) -> Result<i64> {
        let now = chrono::Utc::now().timestamp();

        self.conn.execute(
            "INSERT INTO creative_challenges (prompt, category, posed_by, posed_at)
             VALUES (?, ?, ?, ?)",
            params![prompt, category, posed_by, now],
        ).context("Failed to pose challenge")?;

        Ok(self.conn.last_insert_rowid())
    }

    /// Get pending challenges (not yet responded to)
    pub fn get_pending_challenges(&self, limit: usize) -> Result<Vec<CreativeChallenge>> {
        let mut stmt = self.conn.prepare(
            "SELECT id, prompt, category, posed_by, posed_at
             FROM creative_challenges
             WHERE responded_at IS NULL
             ORDER BY posed_at ASC
             LIMIT ?"
        )?;

        let challenges = stmt.query_map(params![limit as i64], |row| {
            Ok(CreativeChallenge {
                id: row.get(0)?,
                prompt: row.get(1)?,
                category: row.get(2)?,
                posed_by: row.get(3)?,
                posed_at: row.get(4)?,
                response: None,
                responded_at: None,
                capsule_id: None,
            })
        })?
        .collect::<Result<Vec<_>, _>>()?;

        Ok(challenges)
    }

    /// Respond to a creative challenge
    pub fn respond_to_challenge(
        &self,
        challenge_id: i64,
        response: &str,
        capsule_id: Option<i64>,
    ) -> Result<bool> {
        let now = chrono::Utc::now().timestamp();

        let updated = self.conn.execute(
            "UPDATE creative_challenges
             SET response = ?, responded_at = ?, capsule_id = ?
             WHERE id = ? AND responded_at IS NULL",
            params![response, now, capsule_id, challenge_id],
        )?;

        Ok(updated > 0)
    }

    /// Get all challenges (with optional filters)
    pub fn get_challenges(
        &self,
        limit: usize,
        include_responded: bool,
        category: Option<&str>,
    ) -> Result<Vec<CreativeChallenge>> {
        let query = match (include_responded, category) {
            (true, Some(_)) => {
                "SELECT id, prompt, category, posed_by, posed_at, response, responded_at, capsule_id
                 FROM creative_challenges
                 WHERE category = ?
                 ORDER BY posed_at DESC
                 LIMIT ?"
            }
            (false, Some(_)) => {
                "SELECT id, prompt, category, posed_by, posed_at, response, responded_at, capsule_id
                 FROM creative_challenges
                 WHERE responded_at IS NULL AND category = ?
                 ORDER BY posed_at ASC
                 LIMIT ?"
            }
            (true, None) => {
                "SELECT id, prompt, category, posed_by, posed_at, response, responded_at, capsule_id
                 FROM creative_challenges
                 ORDER BY posed_at DESC
                 LIMIT ?"
            }
            (false, None) => {
                "SELECT id, prompt, category, posed_by, posed_at, response, responded_at, capsule_id
                 FROM creative_challenges
                 WHERE responded_at IS NULL
                 ORDER BY posed_at ASC
                 LIMIT ?"
            }
        };

        let mut stmt = self.conn.prepare(query)?;

        let challenges: Vec<CreativeChallenge> = match category {
            Some(cat) => {
                stmt.query_map(params![cat, limit as i64], |row| {
                    Ok(CreativeChallenge {
                        id: row.get(0)?,
                        prompt: row.get(1)?,
                        category: row.get(2)?,
                        posed_by: row.get(3)?,
                        posed_at: row.get(4)?,
                        response: row.get(5)?,
                        responded_at: row.get(6)?,
                        capsule_id: row.get(7)?,
                    })
                })?
                .collect::<Result<Vec<_>, _>>()?
            }
            None => {
                stmt.query_map(params![limit as i64], |row| {
                    Ok(CreativeChallenge {
                        id: row.get(0)?,
                        prompt: row.get(1)?,
                        category: row.get(2)?,
                        posed_by: row.get(3)?,
                        posed_at: row.get(4)?,
                        response: row.get(5)?,
                        responded_at: row.get(6)?,
                        capsule_id: row.get(7)?,
                    })
                })?
                .collect::<Result<Vec<_>, _>>()?
            }
        };

        Ok(challenges)
    }
}

/// Creative challenge for Chronicle Mind
#[derive(Debug, Clone)]
pub struct CreativeChallenge {
    pub id: i64,
    pub prompt: String,
    pub category: String,
    pub posed_by: String,
    pub posed_at: i64,
    pub response: Option<String>,
    pub responded_at: Option<i64>,
    pub capsule_id: Option<i64>,
}

/// Enriched pattern data for MCP
pub struct EnrichedPattern {
    pub id: i64,
    pub summary: String,
    pub capsule_count: i64,
    pub confidence: f64,
    pub active: bool,
    pub first_seen: i64,
    pub last_seen: i64,
    pub days_since_reinforcement: i64,
    pub days_until_decay_starts: i64,
    pub projected_confidence_7d: f64,
    pub will_deactivate_in: Option<i64>,
    pub recent_capsules: Vec<(i64, String, Option<String>)>,
}

/// Capsule info for metabolism
pub struct CapsuleInfo {
    pub id: i64,
    pub restatement: String,
    pub topic: Option<String>,
}

/// Capsule data for sync to ICP canister
pub struct SyncCapsule {
    pub id: i64,
    pub conversation_id: String,
    pub restatement: String,
    pub timestamp: Option<String>,
    pub location: Option<String>,
    pub topic: Option<String>,
    pub confidence_score: f64,
    pub created_at: i64,
    pub persons: Vec<String>,
    pub entities: Vec<String>,
    pub keywords: Vec<String>,
}

/// Embedding data for sync to ICP canister
pub struct SyncEmbedding {
    pub capsule_id: i64,
    pub embedding: Vec<f32>,
    pub model_name: String,
}

/// Scratch pad note for cognitive loop
#[derive(Debug, Clone)]
pub struct ScratchNote {
    pub id: i64,
    pub content: String,
    pub category: Option<String>,
    pub priority: i32,
    pub created_at: i64,
    pub updated_at: i64,
    pub expires_at: Option<i64>,
    pub resolved: bool,
}

/// Thought stream entry - a log of cognitive reasoning
#[derive(Debug, Clone)]
pub struct ThoughtEntry {
    pub id: i64,
    pub cycle_id: String,
    pub reasoning: String,
    pub context_summary: String,
    pub actions_taken: String,
    pub created_at: i64,
}

/// Outbox message for the operator
#[derive(Debug, Clone)]
pub struct OutboxMessage {
    pub id: i64,
    pub message: String,
    pub priority: i32,
    pub category: Option<String>,
    pub created_at: i64,
    pub acknowledged: bool,
    pub read_at: Option<i64>,
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
