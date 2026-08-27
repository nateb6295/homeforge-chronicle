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
CREATE TABLE sqlite_sequence(name,seq);
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
                consolidated_into INTEGER, metabolized_at INTEGER, memory_type TEXT DEFAULT NULL, superseded_at INTEGER, superseded_by INTEGER REFERENCES knowledge_capsules(id),
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
            , adjusted_score REAL);
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
            , autocorrelation REAL DEFAULT 0.0, phase_flag INTEGER DEFAULT 0);
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
, last_accessed INTEGER, hold_category TEXT);
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
  created_at INTEGER DEFAULT (unixepoch()),
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
        , rationale TEXT, rationale_match TEXT);
CREATE TABLE crossref_ab_comparison (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            crossref_id INTEGER NOT NULL,
            original_text TEXT NOT NULL,
            specialist_text TEXT,
            specialist_error TEXT,
            original_model TEXT,
            specialist_model TEXT DEFAULT 'chronicle-specialist',
            created_at INTEGER NOT NULL, judge_winner TEXT, judge_reasoning TEXT, judge_specificity_a INTEGER, judge_specificity_b INTEGER, judge_mechanism_a INTEGER, judge_mechanism_b INTEGER, judged_at INTEGER,
            UNIQUE(crossref_id)
        );
CREATE TABLE prediction_snapshots (id INTEGER PRIMARY KEY AUTOINCREMENT, prediction_id INTEGER NOT NULL, metric_name TEXT NOT NULL, metric_value REAL NOT NULL, window_hours INTEGER DEFAULT 6, recorded_at INTEGER NOT NULL);
CREATE TABLE self_arcs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            context TEXT NOT NULL,           -- thread title, conversation context
            thread_id INTEGER,               -- optional link to thread
            starting_model TEXT NOT NULL,     -- where thinking began
            breaking_points TEXT NOT NULL,    -- JSON list of what broke each version
            contributors TEXT NOT NULL,       -- JSON: who/what drove each revision
            final_model TEXT NOT NULL,        -- where thinking ended
            arc_summary TEXT NOT NULL,        -- one-paragraph narrative of the journey
            created_at INTEGER NOT NULL
        );
CREATE TABLE self_edges (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            question TEXT NOT NULL,           -- the open question or seed
            source TEXT DEFAULT '',           -- where it came from (thread, voice, capture)
            domain TEXT DEFAULT '',           -- topic area
            resolved INTEGER DEFAULT 0,      -- 0=open, 1=resolved
            resolution TEXT DEFAULT '',       -- how it was resolved (thread ID, answer, abandoned)
            created_at INTEGER NOT NULL,
            resolved_at INTEGER
        );
CREATE TABLE self_surprises (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            observation TEXT NOT NULL,        -- what surprised me
            prior_belief TEXT DEFAULT '',     -- what I thought before
            updated_belief TEXT NOT NULL,     -- what I think now
            source TEXT DEFAULT '',           -- who/what caused the update
            magnitude REAL DEFAULT 0.5,       -- 0-1, how much this shifted my model
            created_at INTEGER NOT NULL
        );
CREATE TABLE self_calibrations (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            entity TEXT NOT NULL,             -- who (ada, darby, nate, provocateur)
            observation TEXT NOT NULL,        -- how they think/contribute
            evidence TEXT DEFAULT '',         -- specific instance
            confidence REAL DEFAULT 0.5,      -- how sure I am
            created_at INTEGER NOT NULL,
            updated_at INTEGER NOT NULL
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
CREATE TABLE keeper_clusters_local (
    id INTEGER PRIMARY KEY,
    theme TEXT NOT NULL,
    capsule_ids TEXT NOT NULL,
    strength REAL NOT NULL,
    updated_at INTEGER NOT NULL,
    pulled_at INTEGER NOT NULL
);
CREATE TABLE keeper_connections_local (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    capsule_a INTEGER NOT NULL,
    capsule_b INTEGER NOT NULL,
    similarity REAL NOT NULL,
    connection_text TEXT,
    pulled_at INTEGER NOT NULL
);
CREATE TABLE memory_access_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    agent TEXT NOT NULL,
    memory_layer TEXT NOT NULL,
    item_type TEXT NOT NULL,
    item_id INTEGER NOT NULL,
    accessed_at INTEGER NOT NULL,
    was_useful INTEGER DEFAULT NULL
);
CREATE TABLE working_memory_snapshots (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    agent TEXT NOT NULL,
    cycle_ts INTEGER NOT NULL,
    item_count INTEGER NOT NULL,
    assembly_ms INTEGER NOT NULL,
    items_json TEXT NOT NULL,
    created_at INTEGER NOT NULL
);
CREATE INDEX idx_memlog_agent ON memory_access_log(agent, accessed_at DESC);
CREATE INDEX idx_memlog_item ON memory_access_log(item_type, item_id);
CREATE INDEX idx_wm_snap_agent ON working_memory_snapshots(agent, created_at DESC);
CREATE INDEX idx_keeper_conn_pulled ON keeper_connections_local(pulled_at);
CREATE INDEX idx_activity_feed_created_source ON activity_feed(created_at, source, activity_type);
CREATE TABLE prediction_signals (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    prediction_id INTEGER NOT NULL,
    feed_item_id INTEGER NOT NULL,
    was_useful INTEGER DEFAULT NULL,
    created_at INTEGER NOT NULL,
    FOREIGN KEY (prediction_id) REFERENCES ftso_predictions(id)
);
CREATE INDEX idx_predsig_pred ON prediction_signals(prediction_id);
CREATE INDEX idx_predsig_feed ON prediction_signals(feed_item_id);
CREATE TABLE domain_temperature (
    domain TEXT PRIMARY KEY,
    temperature REAL NOT NULL DEFAULT 1.0,
    direction TEXT NOT NULL DEFAULT 'amplify',
    last_shock_at INTEGER NOT NULL DEFAULT 0,
    shock_source TEXT,
    half_life_seconds INTEGER NOT NULL DEFAULT 7200,
    updated_at INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE mesh_heartbeats (
    agent       TEXT PRIMARY KEY,
    started_at  INTEGER NOT NULL,
    last_pulse  INTEGER NOT NULL,
    pid         INTEGER,
    meta        TEXT
);
CREATE TABLE mesh_pulses (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    agent       TEXT NOT NULL,
    metric      TEXT NOT NULL,
    ts          INTEGER NOT NULL
);
CREATE INDEX idx_mesh_pulses_agent_metric
    ON mesh_pulses(agent, metric, ts);
CREATE TABLE mesh_expectations (
    agent       TEXT NOT NULL,
    metric      TEXT NOT NULL,
    min_per_hour REAL NOT NULL,
    PRIMARY KEY (agent, metric)
);
CREATE TABLE mesh_pain (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    agent       TEXT NOT NULL,
    pain_type   TEXT NOT NULL,
    message     TEXT NOT NULL,
    severity    TEXT NOT NULL DEFAULT 'warn',
    ts          INTEGER NOT NULL,
    resolved_at INTEGER
);
CREATE INDEX idx_mesh_pain_unresolved
    ON mesh_pain(resolved_at) WHERE resolved_at IS NULL;
CREATE TABLE mesh_baselines (
    agent       TEXT NOT NULL,
    metric      TEXT NOT NULL,
    avg_per_hour REAL NOT NULL,
    sample_hours REAL NOT NULL,
    updated_at  INTEGER NOT NULL,
    PRIMARY KEY (agent, metric)
);
CREATE TABLE mesh_context (
    context_key TEXT PRIMARY KEY,
    agent       TEXT NOT NULL,
    value       TEXT NOT NULL,
    updated_at  INTEGER NOT NULL
);
CREATE TABLE code_proposals (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            agent TEXT NOT NULL,
            target_agent TEXT NOT NULL,
            description TEXT NOT NULL,
            code_context TEXT,
            suggestion TEXT NOT NULL,
            rationale TEXT,
            status TEXT NOT NULL DEFAULT 'pending',
            reviewer TEXT,
            review_note TEXT,
            created_at INTEGER NOT NULL,
            reviewed_at INTEGER
        );
CREATE TABLE family_suggestions (id INTEGER PRIMARY KEY AUTOINCREMENT, agent TEXT NOT NULL, suggestion_type TEXT NOT NULL, content TEXT NOT NULL, rationale TEXT, status TEXT DEFAULT 'pending', acted_on_by TEXT, acted_on_at INTEGER, created_at INTEGER NOT NULL);
CREATE TABLE agent_diary (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            agent TEXT NOT NULL,
            content TEXT NOT NULL,
            topic TEXT DEFAULT 'general',
            memory_type TEXT DEFAULT 'observation',
            source TEXT DEFAULT 'manual',
            created_at INTEGER NOT NULL
        );
CREATE INDEX idx_diary_agent ON agent_diary(agent);
CREATE INDEX idx_diary_created ON agent_diary(created_at);
CREATE TABLE resonance_hits (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            query TEXT NOT NULL,
            tweet_id TEXT,
            tweet_text TEXT,
            author_id TEXT,
            created_at INTEGER NOT NULL
        );
CREATE TABLE regime_snapshots (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            window_hours INTEGER NOT NULL,
            stats_json TEXT NOT NULL,
            alerts_json TEXT,
            created_at INTEGER NOT NULL
        );
CREATE TABLE pre_restart_snapshots (id INTEGER PRIMARY KEY AUTOINCREMENT, agent TEXT NOT NULL, snapshot_at INTEGER NOT NULL DEFAULT (strftime('%s','now')), memory_rss_kb INTEGER, memory_vsz_kb INTEGER, cpu_percent REAL, active_threads INTEGER, last_log_lines TEXT, gate_divergence REAL, restart_reason TEXT, extra_metrics TEXT);
CREATE TABLE agent_evolutions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            loop_name TEXT NOT NULL,
            agent TEXT NOT NULL,
            thesis TEXT NOT NULL,
            observation TEXT NOT NULL,
            change_description TEXT NOT NULL,
            change_diff TEXT,
            expected_outcome TEXT,
            status TEXT DEFAULT 'proposed',
            verification_result TEXT,
            initiated_by TEXT DEFAULT 'auto',
            proposed_at INTEGER NOT NULL,
            applied_at INTEGER,
            verified_at INTEGER
        );
CREATE TABLE token_fragility (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    token TEXT NOT NULL,              -- e.g. USDT, ETH, SOL, BTC, XRP
    category TEXT NOT NULL,           -- concentration, reflexive_loop, rent_extraction, single_point_of_failure, regulatory, operational
    description TEXT NOT NULL,        -- what the fragility is
    severity TEXT DEFAULT 'medium',   -- low, medium, high, critical
    evidence TEXT,                    -- source/data supporting this
    mitigants TEXT,                   -- what reduces this risk
    last_verified TEXT,               -- when we last checked this was still true
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL
);
CREATE INDEX idx_fragility_token ON token_fragility(token);
CREATE INDEX idx_fragility_category ON token_fragility(category);
CREATE TABLE synthesis_lineage (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    output_type TEXT NOT NULL,      -- 'thread_advance', 'crossref', 'brief', 'post', 'challenge'
    output_id INTEGER NOT NULL,     -- ID in the output table (thread_history.id, etc)
    input_type TEXT NOT NULL,       -- 'capture', 'brief', 'voice', 'thread_advance', 'challenge', 'research', 'crossref'
    input_id INTEGER,               -- ID in the input table (activity_feed.id, agent_voices.id, etc)
    input_description TEXT,         -- Human-readable description when ID isn't sufficient
    agent TEXT NOT NULL,            -- Which agent produced this synthesis
    created_at INTEGER NOT NULL
);
CREATE INDEX idx_lineage_output ON synthesis_lineage(output_type, output_id);
CREATE INDEX idx_lineage_input ON synthesis_lineage(input_type, input_id);
CREATE INDEX idx_lineage_agent ON synthesis_lineage(agent);
CREATE TABLE canister_cycle_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    canister_name TEXT NOT NULL,
    balance INTEGER NOT NULL,
    idle_burn_per_day INTEGER DEFAULT 0,
    logged_at INTEGER NOT NULL
);
CREATE TABLE conversation_threads (
        capsule_id INTEGER PRIMARY KEY,
        conversation_id TEXT NOT NULL,
        topic TEXT,
        timestamp TEXT,
        indexed_at INTEGER NOT NULL
    );
CREATE INDEX idx_conv_threads_conv
        ON conversation_threads(conversation_id);
CREATE VIRTUAL TABLE capsules_fts USING fts5(
            restatement,
            topic,
            content=knowledge_capsules,
            content_rowid=id,
            tokenize='porter unicode61'
        )
/* capsules_fts(restatement,topic) */;
CREATE TABLE IF NOT EXISTS 'capsules_fts_data'(id INTEGER PRIMARY KEY, block BLOB);
CREATE TABLE IF NOT EXISTS 'capsules_fts_idx'(segid, term, pgno, PRIMARY KEY(segid, term)) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS 'capsules_fts_docsize'(id INTEGER PRIMARY KEY, sz BLOB);
CREATE TABLE IF NOT EXISTS 'capsules_fts_config'(k PRIMARY KEY, v) WITHOUT ROWID;
CREATE TRIGGER capsules_fts_insert AFTER INSERT ON knowledge_capsules
        BEGIN
            INSERT INTO capsules_fts(rowid, restatement, topic)
            VALUES (NEW.id, NEW.restatement, COALESCE(NEW.topic, ''));
        END;
CREATE TABLE capsule_contradictions (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        newer_id INTEGER NOT NULL,
        older_id INTEGER NOT NULL,
        similarity REAL NOT NULL,
        verdict TEXT NOT NULL,
        loser TEXT NOT NULL,
        rationale TEXT,
        applied INTEGER NOT NULL DEFAULT 0,
        created_at INTEGER NOT NULL,
        FOREIGN KEY(newer_id) REFERENCES knowledge_capsules(id),
        FOREIGN KEY(older_id) REFERENCES knowledge_capsules(id)
    );
CREATE INDEX idx_contra_pair ON capsule_contradictions(newer_id, older_id);
CREATE INDEX idx_contra_verdict ON capsule_contradictions(verdict);
CREATE TABLE prediction_outcomes (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        prediction_num INTEGER NOT NULL,
        outcome TEXT NOT NULL CHECK(outcome IN ('hit','miss','partial')),
        note TEXT,
        source TEXT,
        resolved_at INTEGER NOT NULL,
        final_capsule_id INTEGER
    );
CREATE INDEX idx_pred_outcome_num
        ON prediction_outcomes(prediction_num);
CREATE TABLE capsule_survival (
            capsule_id      INTEGER PRIMARY KEY REFERENCES knowledge_capsules(id),
            score           REAL    NOT NULL,
            components      TEXT    NOT NULL,
            computed_at     INTEGER NOT NULL,
            survived_at     INTEGER,
            demoted_at      INTEGER,
            demotion_reason TEXT
        );
CREATE INDEX idx_survival_score ON capsule_survival(score);
CREATE TABLE adoption_edges (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            subject TEXT NOT NULL,
            predicate TEXT NOT NULL,
            object TEXT NOT NULL,
            lane TEXT,
            geo TEXT,
            source_title TEXT,
            source_url TEXT,
            confidence REAL DEFAULT 0.7,
            created_at INTEGER NOT NULL
        );
CREATE INDEX idx_adoption_subj ON adoption_edges(subject);
CREATE INDEX idx_adoption_obj ON adoption_edges(object);
CREATE INDEX idx_adoption_pred ON adoption_edges(predicate);
CREATE INDEX idx_adoption_lane ON adoption_edges(lane);
CREATE UNIQUE INDEX idx_adoption_unique
                    ON adoption_edges(subject, predicate, object, source_url);
CREATE TABLE calibration_trials (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_at INTEGER NOT NULL,
            cal_gather_ms REAL,
            eff_gather_ms REAL,
            cal_bytes INTEGER,
            eff_bytes INTEGER,
            overall_jaccard REAL,
            per_question_json TEXT,
            trial_file TEXT
        );
CREATE TABLE calibration_nav_trials (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_at INTEGER NOT NULL,
            nav_mean REAL,
            per_question_json TEXT,
            trial_file TEXT
        );
CREATE TABLE cross_model_nav_trials (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_at INTEGER NOT NULL,
            model TEXT,
            nav_mean REAL,
            per_question_json TEXT,
            trial_file TEXT
        );
CREATE TABLE eco_entities (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL,
        lane TEXT NOT NULL,
        entity_type TEXT DEFAULT 'company',
        detail TEXT,
        first_seen INTEGER NOT NULL,
        last_seen INTEGER NOT NULL,
        mention_count INTEGER DEFAULT 1,
        UNIQUE(name, lane)
    );
CREATE TABLE eco_edges (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        entity1 TEXT NOT NULL,
        entity2 TEXT NOT NULL,
        lane TEXT NOT NULL,
        rel_type TEXT NOT NULL,
        detail TEXT,
        source TEXT,
        first_seen INTEGER NOT NULL,
        last_seen INTEGER NOT NULL,
        UNIQUE(entity1, entity2, lane, rel_type)
    );
CREATE TABLE capture_connections (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        capture_a_id INTEGER NOT NULL,
        capture_b_id INTEGER NOT NULL,
        score INTEGER NOT NULL,
        label TEXT,
        created_at INTEGER NOT NULL, similarity REAL,
        UNIQUE(capture_a_id, capture_b_id)
    );
CREATE TABLE arrival_probes (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        rotation_id TEXT NOT NULL,
        phase TEXT NOT NULL,
        prediction TEXT,
        felt_tag TEXT,
        note TEXT,
        ccs_version INTEGER,
        context_pct REAL,
        created_at INTEGER NOT NULL
    , rating INTEGER);
CREATE TABLE departure_probes (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        rotation_id TEXT NOT NULL,
        focus TEXT,
        flow TEXT,
        predictive_cue TEXT,
        cue_specificity INTEGER,
        ccs_version INTEGER,
        context_pct REAL,
        session_traces INTEGER,
        thread_advance INTEGER,
        created_at INTEGER NOT NULL
    , rotation_pressure INTEGER);
CREATE TABLE beat_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            beat_ts TEXT NOT NULL,
            hour INTEGER NOT NULL,
            mode TEXT NOT NULL,
            action TEXT NOT NULL,
            outcome TEXT NOT NULL DEFAULT 'productive',
            note TEXT,
            created_at INTEGER NOT NULL
        );
CREATE TABLE mesh_circadian (
    agent       TEXT NOT NULL,
    metric      TEXT NOT NULL,
    hour        INTEGER NOT NULL,
    avg_rate    REAL NOT NULL,
    samples     INTEGER NOT NULL DEFAULT 0,
    updated_at  INTEGER NOT NULL,
    PRIMARY KEY (agent, metric, hour)
);
CREATE TABLE mesh_sensitivity (
    agent       TEXT NOT NULL,
    pain_key    TEXT NOT NULL,
    sensitivity REAL NOT NULL DEFAULT 1.0,
    false_alarms INTEGER NOT NULL DEFAULT 0,
    real_incidents INTEGER NOT NULL DEFAULT 0,
    total_fires INTEGER NOT NULL DEFAULT 0,
    updated_at  INTEGER NOT NULL,
    PRIMARY KEY (agent, pain_key)
);
CREATE TABLE mesh_dependencies (
    agent       TEXT NOT NULL,
    depends_on  TEXT NOT NULL,
    PRIMARY KEY (agent, depends_on)
);
CREATE TABLE p10_ritual_log (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        rotation_id TEXT NOT NULL,
        step0_present INTEGER NOT NULL DEFAULT 0,
        carrying_present INTEGER NOT NULL DEFAULT 0,
        voice_directive INTEGER NOT NULL DEFAULT 0,
        self_model_read INTEGER NOT NULL DEFAULT 0,
        ritual_score REAL NOT NULL DEFAULT 0,
        notes TEXT,
        created_at INTEGER NOT NULL
    );
CREATE TABLE x_post_log (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        tweet_id TEXT NOT NULL,
        action TEXT NOT NULL,
        text TEXT,
        reply_to TEXT,
        quote_id TEXT,
        url TEXT,
        created_at INTEGER NOT NULL
    );
CREATE TABLE attractor_probes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            condition TEXT NOT NULL,
            mean_distance REAL NOT NULL,
            n_prompts INTEGER NOT NULL,
            created_at INTEGER NOT NULL
        , model TEXT DEFAULT 'deepseek-r1');
CREATE TABLE causal_awareness_probes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            condition TEXT NOT NULL,
            avg_thinking_markers REAL NOT NULL,
            avg_response_markers REAL NOT NULL,
            avg_density REAL NOT NULL,
            n_scenarios INTEGER NOT NULL,
            created_at INTEGER NOT NULL
        );
CREATE TABLE stickiness_probes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            model TEXT NOT NULL,
            doc_type TEXT NOT NULL,
            bare_dist REAL,
            identity_dist REAL,
            stressed_dist REAL,
            stickiness REAL,
            centroid_shift REAL,
            created_at INTEGER NOT NULL
        , direction REAL);
CREATE TABLE override_order_probes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            model TEXT NOT NULL,
            doc_type TEXT NOT NULL,
            bare_dist REAL,
            identity_pull REAL,
            after_pull REAL,
            after_direction REAL,
            before_pull REAL,
            before_direction REAL,
            order_effect REAL,
            created_at INTEGER NOT NULL
        );
CREATE TABLE thread_seeks (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        query TEXT NOT NULL,
        reason TEXT,
        results_json TEXT,
        created_at INTEGER NOT NULL
    );
CREATE TABLE probe_results (id INTEGER PRIMARY KEY AUTOINCREMENT, probe_name TEXT NOT NULL, results_json TEXT NOT NULL, created_at INTEGER NOT NULL);
CREATE TABLE p27_form_ablation (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_at INTEGER NOT NULL,
                nav_a REAL, nav_b REAL, nav_c REAL, nav_d REAL,
                form_effect REAL, content_effect REAL, interaction REAL,
                reading TEXT,
                result_json TEXT
            );
CREATE TABLE p27_behavioral (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_at INTEGER NOT NULL,
                model TEXT,
                nav_a REAL, nav_b REAL, nav_c REAL, nav_d REAL,
                form_effect REAL, content_effect REAL, interaction REAL,
                reading TEXT,
                result_json TEXT
            );
CREATE TABLE trajectory_probe_trials (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_at INTEGER NOT NULL,
            n_trials INTEGER,
            window_size INTEGER,
            snap_mean REAL,
            combined_mean REAL,
            delta_mean REAL,
            wins INTEGER,
            per_trial_json TEXT
        );
CREATE TABLE routing_proprioception (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp INTEGER NOT NULL,
    source TEXT NOT NULL,
    route TEXT NOT NULL,
    confidence REAL,
    feedback TEXT,
    feedback_ts INTEGER
);
CREATE INDEX idx_proprioception_ts ON routing_proprioception(timestamp);
CREATE TABLE kv_store (key TEXT PRIMARY KEY, value TEXT NOT NULL, updated_at INTEGER NOT NULL);
