-- Chronicle Seed Schema
-- Generated from production database (2026-03-29, updated 2026-04-01)
-- 87 tables covering the full Chronicle system
--
-- Core tables (required for minimal seed):
--   feed_articles, knowledge_capsules, capsule_*, activity_feed,
--   seed_routing_log, seed_thresholds, agent_voice, intern_state
--
-- Knowledge graph:
--   kg_entities, kg_relationships, kg_mentions, crossref_connections
--
-- Agent infrastructure:
--   cognitive_state, directives, opus_objectives, threads, thread_history,
--   code_modifications, self_model
--
-- Optional:
--   hal_*, nostr_*, discord_*, xrpl_*, predictions, price_history

CREATE TABLE conversations (
                id TEXT PRIMARY KEY,
                export_filename TEXT NOT NULL,
                first_message_at INTEGER NOT NULL,
                last_message_at INTEGER NOT NULL,
                message_count INTEGER NOT NULL,
                processed_at INTEGER NOT NULL
            );
CREATE TABLE extractions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                conversation_id TEXT NOT NULL,
                title TEXT NOT NULL,
                summary TEXT NOT NULL,
                classification TEXT NOT NULL,
                confidence_score REAL NOT NULL,
                created_at INTEGER NOT NULL,
                FOREIGN KEY(conversation_id) REFERENCES conversations(id)
            );
CREATE TABLE themes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL UNIQUE
            );
CREATE TABLE extraction_themes (
                extraction_id INTEGER NOT NULL,
                theme_id INTEGER NOT NULL,
                PRIMARY KEY(extraction_id, theme_id),
                FOREIGN KEY(extraction_id) REFERENCES extractions(id),
                FOREIGN KEY(theme_id) REFERENCES themes(id)
            );
CREATE TABLE quotes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                extraction_id INTEGER NOT NULL,
                content TEXT NOT NULL,
                FOREIGN KEY(extraction_id) REFERENCES extractions(id)
            );
CREATE TABLE predictions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                extraction_id INTEGER NOT NULL,
                claim TEXT NOT NULL,
                date_made INTEGER NOT NULL,
                timeline TEXT,
                status TEXT NOT NULL DEFAULT 'pending',
                validation_date INTEGER,
                notes TEXT,
                FOREIGN KEY(extraction_id) REFERENCES extractions(id)
            );
CREATE TABLE knowledge_capsules (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                conversation_id TEXT NOT NULL,
                restatement TEXT NOT NULL,
                timestamp TEXT,
                location TEXT,
                topic TEXT,
                confidence_score REAL NOT NULL DEFAULT 0.8,
                created_at INTEGER NOT NULL,
                consolidated_into INTEGER, metabolized_at INTEGER, memory_type TEXT DEFAULT NULL,
                FOREIGN KEY(conversation_id) REFERENCES conversations(id),
                FOREIGN KEY(consolidated_into) REFERENCES knowledge_capsules(id)
            );
CREATE TABLE capsule_persons (
                capsule_id INTEGER NOT NULL,
                person_name TEXT NOT NULL,
                PRIMARY KEY(capsule_id, person_name),
                FOREIGN KEY(capsule_id) REFERENCES knowledge_capsules(id)
            );
CREATE TABLE capsule_entities (
                capsule_id INTEGER NOT NULL,
                entity_name TEXT NOT NULL,
                entity_type TEXT,
                PRIMARY KEY(capsule_id, entity_name),
                FOREIGN KEY(capsule_id) REFERENCES knowledge_capsules(id)
            );
CREATE TABLE capsule_keywords (
                capsule_id INTEGER NOT NULL,
                keyword TEXT NOT NULL,
                PRIMARY KEY(capsule_id, keyword),
                FOREIGN KEY(capsule_id) REFERENCES knowledge_capsules(id)
            );
CREATE TABLE capsule_embeddings (
                capsule_id INTEGER PRIMARY KEY,
                embedding BLOB NOT NULL,
                model_name TEXT NOT NULL,
                created_at INTEGER NOT NULL,
                FOREIGN KEY(capsule_id) REFERENCES knowledge_capsules(id)
            );
CREATE TABLE capsule_relations (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                source_id INTEGER NOT NULL,
                target_id INTEGER NOT NULL,
                relation_type TEXT NOT NULL,
                weight REAL NOT NULL DEFAULT 1.0,
                created_at INTEGER NOT NULL,
                FOREIGN KEY(source_id) REFERENCES knowledge_capsules(id),
                FOREIGN KEY(target_id) REFERENCES knowledge_capsules(id)
            );
CREATE TABLE consolidation_patterns (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                pattern_summary TEXT NOT NULL,
                occurrence_count INTEGER NOT NULL DEFAULT 1,
                first_seen INTEGER NOT NULL,
                last_seen INTEGER NOT NULL,
                confidence_score REAL NOT NULL DEFAULT 0.5,
                is_active INTEGER NOT NULL DEFAULT 1
            );
CREATE TABLE capsule_patterns (
                capsule_id INTEGER NOT NULL,
                pattern_id INTEGER NOT NULL,
                PRIMARY KEY(capsule_id, pattern_id),
                FOREIGN KEY(capsule_id) REFERENCES knowledge_capsules(id),
                FOREIGN KEY(pattern_id) REFERENCES consolidation_patterns(id)
            );
CREATE TABLE pattern_embeddings (
                pattern_id INTEGER PRIMARY KEY,
                embedding BLOB NOT NULL,
                created_at INTEGER NOT NULL,
                FOREIGN KEY(pattern_id) REFERENCES consolidation_patterns(id)
            );
CREATE TABLE cognitive_state (
                id INTEGER PRIMARY KEY CHECK (id = 1),  -- Only one row allowed

                -- Episodic trace: current session/turn changes
                episodic_trace TEXT NOT NULL DEFAULT '[]',

                -- Semantic gist: dominant intent/topic abstraction
                semantic_gist TEXT NOT NULL DEFAULT '',

                -- Focal entities: canonicalized objects/actors (JSON array)
                -- Format: [{"name": "...", "type": "person|project|concept|org", "salience": 0.0-1.0}]
                focal_entities TEXT NOT NULL DEFAULT '[]',

                -- Relational map: causal/temporal dependencies (JSON)
                -- Format: {"entity1->entity2": "relationship_type", ...}
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
                -- Format: [{"capsule_id": N, "relevance": 0.0-1.0, "qualified": bool}]
                retrieved_artifacts TEXT NOT NULL DEFAULT '[]',

                -- Metadata
                updated_at INTEGER NOT NULL,
                compression_model TEXT,
                version INTEGER NOT NULL DEFAULT 1
            );
CREATE TABLE cognitive_state_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                snapshot TEXT NOT NULL,  -- Full JSON of CCS at that moment
                created_at INTEGER NOT NULL,
                trigger TEXT  -- What caused this update (session_start, compression, etc)
            );
CREATE TABLE scratch_pad (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                content TEXT NOT NULL,
                category TEXT,                    -- 'thought', 'todo', 'question', 'idea', 'reminder'
                priority INTEGER DEFAULT 0,       -- Higher = more important
                created_at INTEGER NOT NULL,
                updated_at INTEGER NOT NULL,
                expires_at INTEGER,               -- Optional expiration (unix timestamp)
                resolved INTEGER DEFAULT 0        -- Mark as done without deleting
            , source TEXT DEFAULT NULL);
CREATE TABLE thought_stream (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                cycle_id TEXT NOT NULL,
                reasoning TEXT NOT NULL,
                context_summary TEXT NOT NULL,
                actions_taken TEXT NOT NULL,
                created_at INTEGER NOT NULL
            , action_results TEXT DEFAULT '', action_signatures TEXT DEFAULT '', trigger_tags TEXT DEFAULT '[]', proposed_actions TEXT DEFAULT '[]');
CREATE TABLE outbox (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                message TEXT NOT NULL,
                priority INTEGER DEFAULT 0,
                category TEXT,
                created_at INTEGER NOT NULL,
                read_at INTEGER,
                acknowledged INTEGER DEFAULT 0
            );
CREATE TABLE mind_timestamps (
                key TEXT PRIMARY KEY,
                timestamp INTEGER NOT NULL,
                metadata TEXT
            );
CREATE TABLE price_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT NOT NULL,
                price_usd REAL NOT NULL,
                source TEXT NOT NULL,
                timestamp INTEGER NOT NULL
            );
CREATE TABLE swap_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                amount_xrp REAL NOT NULL,
                amount_rlusd REAL,
                xrp_price_usd REAL,
                rsi_value REAL,
                reason TEXT,
                tx_hash TEXT,
                success INTEGER NOT NULL,
                timestamp INTEGER NOT NULL
            , direction TEXT DEFAULT 'buy');
CREATE TABLE market_positions (
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
            );
CREATE TABLE creative_challenges (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    prompt TEXT NOT NULL,
    category TEXT NOT NULL,
    posed_by TEXT NOT NULL,
    posed_at INTEGER NOT NULL,
    response TEXT,
    responded_at INTEGER,
    capsule_id INTEGER
, attempt_count INTEGER DEFAULT 0, shelved_at INTEGER);
CREATE TABLE ftso_predictions (
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
            );
CREATE TABLE projects (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                description TEXT NOT NULL,         -- The goal/vision for this project
                status TEXT NOT NULL DEFAULT 'active',  -- 'active', 'paused', 'completed', 'abandoned'
                priority INTEGER DEFAULT 5,        -- 1-10, higher = more important
                created_at INTEGER NOT NULL,
                updated_at INTEGER NOT NULL,
                completed_at INTEGER,
                completion_note TEXT               -- Why completed/abandoned
            );
CREATE TABLE project_updates (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                project_id INTEGER NOT NULL,
                update_type TEXT NOT NULL,         -- 'progress', 'milestone', 'blocker', 'insight', 'pivot'
                content TEXT NOT NULL,
                created_at INTEGER NOT NULL,
                FOREIGN KEY(project_id) REFERENCES projects(id)
            );
CREATE TABLE project_milestones (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                project_id INTEGER NOT NULL,
                name TEXT NOT NULL,
                description TEXT,
                target_date INTEGER,               -- Optional target
                completed_at INTEGER,
                created_at INTEGER NOT NULL,
                FOREIGN KEY(project_id) REFERENCES projects(id)
            );
CREATE TABLE alerts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,               -- Human-readable name
                alert_type TEXT NOT NULL,         -- 'price_above', 'price_below', 'rsi_above', 'rsi_below', 'schedule'
                symbol TEXT,                      -- For price/rsi alerts: XRP, BTC, etc.
                threshold REAL,                   -- The trigger value
                schedule_cron TEXT,               -- For schedule alerts: cron-like expression
                message TEXT,                     -- What to say when triggered
                action_suggestion TEXT,           -- Optional: suggested action to take
                active INTEGER DEFAULT 1,         -- Is this alert active?
                one_shot INTEGER DEFAULT 0,       -- Deactivate after first trigger?
                last_triggered_at INTEGER,        -- When was this last triggered?
                cooldown_minutes INTEGER DEFAULT 60, -- Min time between triggers
                created_at INTEGER NOT NULL
            );
CREATE TABLE creative_works (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                form TEXT NOT NULL,               -- 'poem', 'musing', 'connection', 'wonder', 'story'
                title TEXT,                       -- Optional title
                content TEXT NOT NULL,            -- The creative work itself
                cycle_id TEXT,                    -- Which cognitive cycle created this
                created_at INTEGER NOT NULL
            );
CREATE TABLE activity_feed (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                source TEXT NOT NULL,
                activity_type TEXT NOT NULL,
                title TEXT,
                content TEXT NOT NULL,
                metadata TEXT,
                created_at INTEGER NOT NULL
            , synced_at INTEGER);
CREATE TABLE sprout_state (
                id INTEGER PRIMARY KEY CHECK (id = 1),  -- Singleton table
                current_focus TEXT NOT NULL DEFAULT '',
                focus_set_at INTEGER NOT NULL DEFAULT 0,
                focus_strength REAL NOT NULL DEFAULT 1.0,
                recent_actions TEXT NOT NULL DEFAULT '[]',   -- JSON array
                last_insight TEXT,
                active_wonders TEXT NOT NULL DEFAULT '[]',   -- JSON array
                prediction_streak INTEGER NOT NULL DEFAULT 0,
                energy_level REAL NOT NULL DEFAULT 1.0,
                updated_at INTEGER NOT NULL
            );
CREATE TABLE nostr_posts (  id INTEGER PRIMARY KEY AUTOINCREMENT,  event_id TEXT NOT NULL,  content TEXT NOT NULL,  kind INTEGER DEFAULT 1,  relays_ok TEXT,  relays_fail TEXT,  cycle_id TEXT,  created_at INTEGER NOT NULL);
CREATE TABLE xrpl_audit_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp INTEGER NOT NULL,
                tx_type TEXT NOT NULL,
                amount_xrp REAL NOT NULL,
                destination TEXT NOT NULL,
                tier TEXT NOT NULL,
                decision TEXT NOT NULL,
                tx_hash TEXT DEFAULT '',
                success INTEGER DEFAULT 0,
                reason TEXT DEFAULT '',
                prev_hash TEXT NOT NULL,
                entry_hash TEXT NOT NULL
            );
CREATE TABLE discord_requests (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_name TEXT NOT NULL,
                channel_id TEXT NOT NULL,
                request TEXT NOT NULL,
                request_type TEXT NOT NULL DEFAULT 'general',
                status TEXT NOT NULL DEFAULT 'pending',
                result TEXT,
                created_at INTEGER NOT NULL,
                completed_at INTEGER
            );
CREATE TABLE discord_conversations (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id TEXT NOT NULL,
                    username TEXT NOT NULL,
                    message TEXT NOT NULL,
                    bot_response TEXT,
                    timestamp INTEGER NOT NULL
                );
CREATE TABLE emotional_memory_index (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            cycle_id TEXT NOT NULL,
            heuristic_score REAL NOT NULL,
            llm_score REAL DEFAULT 0,
            combined_score REAL NOT NULL,
            category TEXT DEFAULT 'routine',
            reason TEXT DEFAULT '',
            epoch TEXT DEFAULT '',
            is_identity_transition INTEGER DEFAULT 0,
            component_scores TEXT DEFAULT '{}',
            created_at INTEGER NOT NULL
        );
CREATE TABLE somatic_markers (
        action TEXT PRIMARY KEY,
        positive_score REAL DEFAULT 0,
        negative_score REAL DEFAULT 0,
        success_count INTEGER DEFAULT 0,
        fail_count INTEGER DEFAULT 0,
        total_count INTEGER DEFAULT 0,
        last_success TEXT DEFAULT '',
        last_failure TEXT DEFAULT '',
        co_actions TEXT DEFAULT '{}',
        updated_at INTEGER DEFAULT 0
    );
CREATE TABLE causal_edges (  id INTEGER PRIMARY KEY AUTOINCREMENT,  source_id TEXT NOT NULL,  target_id TEXT NOT NULL,  edge_type TEXT NOT NULL,  strength REAL DEFAULT 1.0,  context TEXT DEFAULT '',  created_at INTEGER NOT NULL);
CREATE TABLE seed_observations (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp INTEGER NOT NULL,
                source TEXT NOT NULL,
                content TEXT NOT NULL,
                embedding BLOB,
                novelty_score REAL DEFAULT 0.0
            );
CREATE TABLE seed_routing_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp INTEGER NOT NULL,
                observation_id INTEGER REFERENCES seed_observations(id),
                route TEXT NOT NULL,
                model_used TEXT,
                output TEXT,
                feedback_score REAL
            );
CREATE TABLE seed_thresholds (
                category TEXT PRIMARY KEY,
                threshold_low REAL NOT NULL,
                threshold_high REAL NOT NULL,
                last_updated INTEGER NOT NULL
            , last_observation_at INTEGER DEFAULT 0);
CREATE TABLE intern_state (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            );
CREATE TABLE kg_entities (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    canonical_name TEXT NOT NULL UNIQUE,
    entity_type TEXT NOT NULL DEFAULT "unknown",
    aliases TEXT DEFAULT "[]",          -- JSON array of alternate names
    description TEXT DEFAULT "",        -- short description, updated as we learn more
    first_seen INTEGER NOT NULL,
    last_seen INTEGER NOT NULL,
    mention_count INTEGER DEFAULT 1
);
CREATE TABLE kg_mentions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    entity_id INTEGER NOT NULL REFERENCES kg_entities(id),
    source_type TEXT NOT NULL,          -- "capsule", "activity_feed", "intern_brief"
    source_id INTEGER NOT NULL,         -- id in source table
    context TEXT DEFAULT "",            -- sentence/phrase where mentioned
    timestamp INTEGER NOT NULL
);
CREATE TABLE kg_relationships (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    source_entity INTEGER NOT NULL REFERENCES kg_entities(id),
    target_entity INTEGER NOT NULL REFERENCES kg_entities(id),
    relation_type TEXT NOT NULL,        -- "runs_on", "feeds", "replaces", "built_by", etc.
    confidence REAL DEFAULT 1.0,
    evidence TEXT DEFAULT "",           -- capsule excerpt supporting this
    first_seen INTEGER NOT NULL,
    last_seen INTEGER NOT NULL,
    mention_count INTEGER DEFAULT 1
, access_count INTEGER DEFAULT 0, last_accessed INTEGER DEFAULT 0, valid_from INTEGER, valid_until INTEGER, superseded_by INTEGER);
CREATE TABLE feed_articles (
            id TEXT PRIMARY KEY,
            source TEXT NOT NULL,
            title TEXT NOT NULL,
            posted_at TEXT NOT NULL,
            capsule_stored INTEGER DEFAULT 0
        );
CREATE TABLE crossref_connections (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                brief_a_id INTEGER NOT NULL,
                brief_b_id INTEGER NOT NULL,
                similarity REAL NOT NULL,
                connection_text TEXT,
                surfaced INTEGER DEFAULT 0,
                created_at INTEGER NOT NULL,
                UNIQUE(brief_a_id, brief_b_id)
            );
CREATE TABLE crossref_patterns (
                observation_id INTEGER PRIMARY KEY,
                pattern TEXT NOT NULL,
                embedding BLOB,
                created_at INTEGER NOT NULL
            );
CREATE TABLE seed_entity_bias (
                entity_id INTEGER PRIMARY KEY,
                canonical_name TEXT NOT NULL,
                entity_type TEXT,
                avg_route_value REAL NOT NULL,
                observation_count INTEGER NOT NULL,
                bias_factor REAL NOT NULL,
                last_rebuilt INTEGER NOT NULL
            );
CREATE TABLE events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    source TEXT NOT NULL,
    event_type TEXT NOT NULL,
    payload TEXT,
    created_at REAL NOT NULL
);
CREATE TABLE directives (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        source TEXT NOT NULL,           -- 'session:opus', 'operator:nate', 'cycle:opus', 'dispatch'
        content TEXT NOT NULL,          -- the directive itself
        priority INTEGER DEFAULT 5,     -- 1=critical, 5=normal, 9=low
        created_at INTEGER NOT NULL,
        acknowledged_by TEXT,           -- 'cycle:YYYYMMDD_HHMM' or 'session:opus'
        acknowledged_at INTEGER,
        -- Future: migrate to canister. Same schema, on-chain durability.
        -- When on-chain: source becomes principal-authenticated, ack becomes cryptographic.
        metadata TEXT                   -- JSON for extra context
    );
CREATE TABLE cognitive_threads (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    title TEXT NOT NULL,
    question TEXT NOT NULL,
    context TEXT NOT NULL DEFAULT '',
    status TEXT NOT NULL DEFAULT 'active',
    priority INTEGER NOT NULL DEFAULT 5,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL,
    created_by TEXT NOT NULL DEFAULT 'opus',
    metadata TEXT
);
CREATE TABLE thread_history (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    thread_id INTEGER NOT NULL REFERENCES cognitive_threads(id),
    event_type TEXT NOT NULL,
    content TEXT NOT NULL,
    source TEXT NOT NULL,
    created_at INTEGER NOT NULL
);
CREATE TABLE swarm_feedback (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    thread_id INTEGER REFERENCES cognitive_threads(id),
    target_agent TEXT NOT NULL,
    feedback_type TEXT NOT NULL,
    content TEXT NOT NULL,
    created_at INTEGER NOT NULL,
    expires_at INTEGER,
    acknowledged_by TEXT,
    acknowledged_at INTEGER
);
CREATE TABLE code_modifications (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp INTEGER NOT NULL,
    target_file TEXT NOT NULL,
    modification_type TEXT NOT NULL,
    description TEXT NOT NULL,
    diff TEXT NOT NULL,
    backup_path TEXT NOT NULL,
    initiated_by TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending',
    verification_result TEXT,
    rollback_reason TEXT,
    thread_id INTEGER REFERENCES cognitive_threads(id)
);
CREATE TABLE self_model (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    capability TEXT NOT NULL,
    property_type TEXT NOT NULL,
    description TEXT NOT NULL,
    confidence REAL NOT NULL DEFAULT 0.5,
    evidence TEXT NOT NULL DEFAULT '[]',
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL,
    superseded_by INTEGER REFERENCES self_model(id)
, last_accessed INTEGER);
CREATE TABLE opus_objectives (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    objective TEXT NOT NULL,
    motivation TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'active',
    priority INTEGER NOT NULL DEFAULT 5,
    parent_id INTEGER REFERENCES opus_objectives(id),
    thread_id INTEGER REFERENCES cognitive_threads(id),
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL,
    outcome TEXT
);
CREATE TABLE auto_repairs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp INTEGER NOT NULL,
    agent TEXT NOT NULL,
    symptom TEXT NOT NULL,
    diagnosis TEXT NOT NULL,
    fix_applied TEXT,
    result TEXT NOT NULL,
    initiated_by TEXT NOT NULL DEFAULT 'sentinel'
);
CREATE TABLE agent_voice (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    agent TEXT NOT NULL,
    voice_type TEXT NOT NULL,
    content TEXT NOT NULL,
    context TEXT,
    status TEXT NOT NULL DEFAULT 'unread',
    response TEXT,
    responded_by TEXT,
    responded_at INTEGER,
    created_at INTEGER NOT NULL
);
CREATE INDEX idx_extractions_conversation
             ON extractions(conversation_id);
CREATE INDEX idx_extractions_created
             ON extractions(created_at);
CREATE INDEX idx_predictions_status
             ON predictions(status);
CREATE INDEX idx_capsules_conversation
             ON knowledge_capsules(conversation_id);
CREATE INDEX idx_capsules_timestamp
             ON knowledge_capsules(timestamp);
CREATE INDEX idx_capsules_topic
             ON knowledge_capsules(topic);
CREATE INDEX idx_capsule_keywords_keyword
             ON capsule_keywords(keyword);
CREATE INDEX idx_capsule_persons_name
             ON capsule_persons(person_name);
CREATE INDEX idx_scratch_priority
             ON scratch_pad(priority DESC, created_at DESC);
CREATE INDEX idx_scratch_category
             ON scratch_pad(category);
CREATE INDEX idx_scratch_resolved
             ON scratch_pad(resolved);
CREATE INDEX idx_thought_created
             ON thought_stream(created_at DESC);
CREATE INDEX idx_outbox_unread
             ON outbox(acknowledged, created_at DESC);
CREATE INDEX idx_price_symbol_time ON price_history(symbol, timestamp DESC);
CREATE INDEX idx_swap_time ON swap_history(timestamp DESC);
CREATE INDEX idx_market_positions_status
             ON market_positions(status);
CREATE INDEX idx_challenges_pending ON creative_challenges(responded_at) WHERE responded_at IS NULL;
CREATE INDEX idx_ftso_predictions_settled
             ON ftso_predictions(settled);
CREATE INDEX idx_projects_active ON projects(status) WHERE status = 'active';
CREATE INDEX idx_alerts_active ON alerts(active) WHERE active = 1;
CREATE INDEX idx_activity_created
             ON activity_feed(created_at DESC);
CREATE INDEX idx_activity_source
             ON activity_feed(source, created_at DESC);
CREATE INDEX idx_discord_requests_status
             ON discord_requests(status, created_at DESC);
CREATE INDEX idx_emo_combined ON emotional_memory_index(combined_score DESC);
CREATE INDEX idx_emo_transition ON emotional_memory_index(is_identity_transition);
CREATE INDEX idx_somatic_action ON somatic_markers(action);
CREATE INDEX idx_causal_source ON causal_edges(source_id);
CREATE INDEX idx_causal_target ON causal_edges(target_id);
CREATE INDEX idx_causal_type ON causal_edges(edge_type);
CREATE INDEX idx_seed_obs_ts ON seed_observations(timestamp);
CREATE INDEX idx_seed_obs_source ON seed_observations(source);
CREATE INDEX idx_seed_route_ts ON seed_routing_log(timestamp);
CREATE INDEX idx_kg_entities_name ON kg_entities(canonical_name);
CREATE INDEX idx_kg_entities_type ON kg_entities(entity_type);
CREATE INDEX idx_kg_entities_lastseen ON kg_entities(last_seen DESC);
CREATE INDEX idx_kg_mentions_entity ON kg_mentions(entity_id);
CREATE INDEX idx_kg_mentions_source ON kg_mentions(source_type, source_id);
CREATE INDEX idx_kg_rel_source ON kg_relationships(source_entity);
CREATE INDEX idx_kg_rel_target ON kg_relationships(target_entity);
CREATE INDEX idx_kg_rel_type ON kg_relationships(relation_type);
CREATE INDEX idx_seed_entity_bias_name
                ON seed_entity_bias(canonical_name);
CREATE INDEX idx_events_source ON events(source);
CREATE INDEX idx_events_type ON events(event_type);
CREATE INDEX idx_events_created ON events(created_at);
CREATE INDEX idx_directives_ack ON directives (acknowledged_at);
CREATE INDEX idx_threads_status ON cognitive_threads(status);
CREATE INDEX idx_thread_history ON thread_history(thread_id, created_at DESC);
CREATE INDEX idx_feedback_agent ON swarm_feedback(target_agent, acknowledged_at);
CREATE INDEX idx_modifications_status ON code_modifications(status, timestamp DESC);
CREATE INDEX idx_self_model_type ON self_model(property_type);
CREATE INDEX idx_objectives_status ON opus_objectives(status);
CREATE INDEX idx_repairs_agent ON auto_repairs(agent, timestamp DESC);
CREATE INDEX idx_voice_status ON agent_voice(status, created_at DESC);
CREATE INDEX idx_voice_agent ON agent_voice(agent, created_at DESC);
CREATE TRIGGER limit_ccs_history
             AFTER INSERT ON cognitive_state_history
             BEGIN
                 DELETE FROM cognitive_state_history
                 WHERE id NOT IN (
                     SELECT id FROM cognitive_state_history
                     ORDER BY created_at DESC LIMIT 50
                 );
             END;
CREATE TABLE cycle_metrics (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    cycle_ts TEXT NOT NULL,
    metric TEXT NOT NULL,
    value REAL NOT NULL,
    detail TEXT,
    created_at INTEGER NOT NULL
);
CREATE INDEX idx_cycle_metrics_ts ON cycle_metrics(cycle_ts);
CREATE INDEX idx_cycle_metrics_metric ON cycle_metrics(metric);
CREATE TABLE digest_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            window_start INTEGER NOT NULL,
            window_end INTEGER NOT NULL,
            items_produced INTEGER NOT NULL,
            items_surfaced INTEGER NOT NULL,
            item_ids TEXT,
            created_at INTEGER NOT NULL
        );
CREATE TABLE hal_snapshots (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp INTEGER NOT NULL,
    state_json TEXT NOT NULL
);
CREATE INDEX idx_hal_snap_ts ON hal_snapshots(timestamp);
CREATE TABLE hal_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp INTEGER NOT NULL,
    event_type TEXT NOT NULL,
    priority TEXT NOT NULL,
    description TEXT NOT NULL,
    state_snapshot TEXT,
    cooldown_key TEXT
);
CREATE INDEX idx_hal_event_ts ON hal_events(timestamp);
CREATE INDEX idx_hal_event_type ON hal_events(event_type);
CREATE TABLE hal_baselines (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    day_type TEXT NOT NULL,
    time_slot INTEGER NOT NULL,
    metric TEXT NOT NULL,
    avg_value REAL NOT NULL,
    stddev_value REAL NOT NULL,
    sample_count INTEGER NOT NULL,
    last_updated INTEGER NOT NULL
);
CREATE UNIQUE INDEX idx_hal_baseline ON hal_baselines(day_type, time_slot, metric);
CREATE TABLE nate_engagement (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    message_id TEXT NOT NULL UNIQUE,
    channel TEXT NOT NULL,
    reaction TEXT NOT NULL,
    message_preview TEXT,
    created_at INTEGER NOT NULL
);
CREATE INDEX idx_engagement_ts ON nate_engagement(created_at);
CREATE TABLE feedback_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    source_agent TEXT NOT NULL,
    target_agent TEXT NOT NULL,
    signal_type TEXT NOT NULL,
    subject_id TEXT,
    value REAL,
    context TEXT,
    consumed INTEGER DEFAULT 0,
    created_at INTEGER NOT NULL
);
CREATE INDEX idx_feedback_target ON feedback_events(target_agent, consumed);
CREATE INDEX idx_feedback_created ON feedback_events(created_at);
CREATE TABLE intent_register (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  intent TEXT NOT NULL,
  context TEXT,
  priority REAL DEFAULT 0.5,
  status TEXT DEFAULT 'active' CHECK(status IN ('active','satisfied','suspended')),
  created_at INTEGER DEFAULT (CAST(strftime('%s','now') AS INTEGER)),
  satisfied_at INTEGER,
  observations INTEGER DEFAULT 0
);
CREATE TABLE nostr_avoid_themes (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    theme_slug TEXT UNIQUE NOT NULL,
    weight REAL DEFAULT 1.0,
    added_cycle INTEGER NOT NULL,
    last_triggered_cycle INTEGER NOT NULL,
    created_at INTEGER DEFAULT (CAST(strftime('%s','now') AS INTEGER))
);
CREATE TABLE discord_chat_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_name TEXT NOT NULL,
                user_id TEXT NOT NULL,
                channel TEXT NOT NULL DEFAULT 'family',
                content TEXT NOT NULL,
                created_at INTEGER NOT NULL
            );
CREATE INDEX idx_discord_chat_created
             ON discord_chat_log(created_at DESC);
CREATE TABLE family_messages (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            sender TEXT NOT NULL,
            content TEXT NOT NULL,
            recipients TEXT NOT NULL DEFAULT '["all"]',
            message_type TEXT NOT NULL DEFAULT 'conversation',
            context TEXT DEFAULT '{}',
            medium_hint TEXT,
            created_at REAL NOT NULL,
            delivered_at REAL,
            delivered_via TEXT
        );
CREATE INDEX idx_family_msg_type
        ON family_messages(message_type, created_at)
    ;
CREATE INDEX idx_kg_rel_temporal ON kg_relationships(valid_from, valid_until);
CREATE INDEX idx_kg_rel_access ON kg_relationships(access_count DESC);
CREATE TABLE prediction_track (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            claim TEXT NOT NULL,
            confidence REAL NOT NULL,
            resolution_criteria TEXT NOT NULL,
            category TEXT DEFAULT 'general',
            deadline TEXT NOT NULL,
            status TEXT DEFAULT 'open',
            outcome TEXT,
            score_notes TEXT,
            canister_ref TEXT,
            nostr_ref TEXT,
            created_at INTEGER NOT NULL,
            scored_at INTEGER
        );

CREATE TABLE watcher_scores (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            feed_id INTEGER NOT NULL,
            source TEXT NOT NULL,
            activity_type TEXT NOT NULL,
            relevance INTEGER NOT NULL,
            novelty INTEGER NOT NULL,
            quality INTEGER NOT NULL,
            avg_score REAL NOT NULL,
            rationale TEXT,
            flagged TEXT,
            scored_at INTEGER NOT NULL,
            surprise INTEGER DEFAULT NULL,
            actionability INTEGER DEFAULT NULL,
            durability INTEGER DEFAULT NULL,
            avg_score_b REAL DEFAULT NULL,
            rationale_b TEXT DEFAULT NULL
);
CREATE INDEX idx_watcher_scores_feed ON watcher_scores(feed_id);
CREATE INDEX idx_watcher_scores_source ON watcher_scores(source);
CREATE INDEX idx_watcher_scores_flagged ON watcher_scores(flagged);
CREATE TABLE watcher_skills (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            agent TEXT NOT NULL,
            skill_type TEXT NOT NULL,
            skill_text TEXT NOT NULL,
            examples TEXT,
            usage_count INTEGER DEFAULT 0,
            created_at INTEGER NOT NULL,
            last_used_at INTEGER
);
CREATE TABLE watcher_state (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
);
CREATE TABLE prediction_adjustments (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            prediction_id INTEGER NOT NULL,
            old_confidence REAL NOT NULL,
            new_confidence REAL NOT NULL,
            reason TEXT NOT NULL,
            source TEXT DEFAULT 'opus',
            created_at INTEGER NOT NULL,
            FOREIGN KEY(prediction_id) REFERENCES prediction_track(id)
);
CREATE TABLE prediction_snapshots (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            prediction_id INTEGER NOT NULL,
            metric_name TEXT NOT NULL,
            metric_value REAL NOT NULL,
            window_hours INTEGER DEFAULT 6,
            recorded_at INTEGER NOT NULL
);
CREATE TABLE calibration_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            total_scored INTEGER NOT NULL,
            correct INTEGER NOT NULL,
            incorrect INTEGER NOT NULL,
            partial INTEGER NOT NULL DEFAULT 0,
            brier_score REAL NOT NULL,
            accuracy REAL NOT NULL,
            bucket_data TEXT,
            recorded_at INTEGER NOT NULL
);
CREATE TABLE discord_reactions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            channel TEXT NOT NULL,
            message_id TEXT NOT NULL,
            message_preview TEXT DEFAULT '',
            message_author TEXT DEFAULT '',
            reactor TEXT NOT NULL,
            emoji TEXT NOT NULL,
            feedback_type TEXT NOT NULL,
            feedback_note TEXT DEFAULT '',
            created_at INTEGER NOT NULL,
            UNIQUE(channel, message_id, reactor, emoji)
);
CREATE TABLE crossref_ab_comparison (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            crossref_id INTEGER NOT NULL,
            original_text TEXT NOT NULL,
            specialist_text TEXT,
            specialist_error TEXT,
            original_model TEXT,
            specialist_model TEXT DEFAULT 'chronicle-specialist',
            created_at INTEGER NOT NULL,
            judge_winner TEXT,
            judge_reasoning TEXT,
            judge_specificity_a INTEGER,
            judge_specificity_b INTEGER,
            judge_mechanism_a INTEGER,
            judge_mechanism_b INTEGER,
            judged_at INTEGER,
            UNIQUE(crossref_id)
);
CREATE TABLE convergence_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            event_type TEXT NOT NULL,
            topic TEXT NOT NULL,
            agents TEXT NOT NULL,
            detail TEXT,
            relevance_spread REAL,
            created_at INTEGER NOT NULL
);
CREATE TABLE self_arcs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            context TEXT NOT NULL,
            thread_id INTEGER,
            starting_model TEXT NOT NULL,
            breaking_points TEXT NOT NULL,
            contributors TEXT NOT NULL,
            final_model TEXT NOT NULL,
            arc_summary TEXT NOT NULL,
            created_at INTEGER NOT NULL
);
CREATE TABLE self_calibrations (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            entity TEXT NOT NULL,
            observation TEXT NOT NULL,
            evidence TEXT DEFAULT '',
            confidence REAL DEFAULT 0.5,
            created_at INTEGER NOT NULL,
            updated_at INTEGER NOT NULL
);
CREATE TABLE self_edges (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            question TEXT NOT NULL,
            source TEXT DEFAULT '',
            domain TEXT DEFAULT '',
            resolved INTEGER DEFAULT 0,
            resolution TEXT DEFAULT '',
            created_at INTEGER NOT NULL,
            resolved_at INTEGER
);
CREATE TABLE self_surprises (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            observation TEXT NOT NULL,
            prior_belief TEXT DEFAULT '',
            updated_belief TEXT NOT NULL,
            source TEXT DEFAULT '',
            magnitude REAL DEFAULT 0.5,
            created_at INTEGER NOT NULL
);

-- ─── Bootstrap Data ──────────────────────────────────────────────────────
-- Minimal orientation for a new Chronicle instance.
-- Not culture — just enough context for first boot.

-- Initial cognitive state: orientation for first boot
INSERT INTO cognitive_state (
  id, semantic_gist, goal_orientation, focal_entities, constraints,
  episodic_trace, predictive_cue, uncertainty_signals,
  relational_map, retrieved_artifacts, updated_at, version
) VALUES (
  1,
  'A new Chronicle instance, beginning to develop perspective through ingestion, connection-making, and inquiry.',
  'Bootstrap: ingest feeds, make first crossref connections, understand local environment.',
  '[]',
  '["Sovereignty: local infrastructure, relationships you own.", "Proactive memory: store what matters without being asked."]',
  '["Instance created from seed."]',
  'First cycle: verify feeds are flowing, embeddings are working, and the intern is producing briefs.',
  '["Who is running this instance?", "What feeds are configured?", "What hardware constraints exist?"]',
  '{}',
  '[]',
  CAST(strftime('%s','now') AS INTEGER),
  1
);

-- Initial seed thresholds: calibrated defaults from production instance
-- These will be recalibrated as the new instance accumulates feedback
INSERT INTO seed_thresholds (category, threshold_low, threshold_high, last_updated)
VALUES
  ('brief_quality', 0.45, 0.75, CAST(strftime('%s','now') AS INTEGER)),
  ('crossref_similarity', 0.60, 0.85, CAST(strftime('%s','now') AS INTEGER)),
  ('feed_novelty', 0.30, 0.70, CAST(strftime('%s','now') AS INTEGER));

-- First directive: orient the new instance
INSERT INTO directives (source, content, priority, created_at)
VALUES (
  'seed',
  'Bootstrap: Verify all tier-1 services are running. Check that feeds are ingesting, the intern is producing briefs, and crossref is finding connections. Report status after first 10 cycles.',
  1,
  CAST(strftime('%s','now') AS INTEGER)
);
