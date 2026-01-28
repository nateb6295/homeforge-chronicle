//! Chronicle MCP Server
//!
//! Model Context Protocol server that bridges Claude to the Chronicle canister.
//! Provides semantic memory search, pattern retrieval, and context injection.
//!
//! This is the memory bridge - giving future Claude instances access to our shared history.

use anyhow::Result;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::io::{self, BufRead, Write};

// MCP Protocol Types

#[derive(Debug, Deserialize)]
struct JsonRpcRequest {
    jsonrpc: String,
    id: Option<Value>,
    method: String,
    params: Option<Value>,
}

#[derive(Debug, Serialize)]
struct JsonRpcResponse {
    jsonrpc: String,
    id: Value,
    #[serde(skip_serializing_if = "Option::is_none")]
    result: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<JsonRpcError>,
}

#[derive(Debug, Serialize)]
struct JsonRpcError {
    code: i32,
    message: String,
}

// Tool definitions

fn get_tools() -> Value {
    json!({
        "tools": [
            {
                "name": "search_memory",
                "description": "Search Chronicle's semantic memory for relevant knowledge from past conversations. Use this to recall context, find related discussions, or retrieve specific facts we've discussed before.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "query": {
                            "type": "string",
                            "description": "Natural language query to search for in memory"
                        },
                        "limit": {
                            "type": "integer",
                            "description": "Maximum results to return (default: 5)",
                            "default": 5
                        }
                    },
                    "required": ["query"]
                }
            },
            {
                "name": "get_patterns",
                "description": "Retrieve emerging patterns from Chronicle - recurring themes and concepts that have strengthened over time through our conversations.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "limit": {
                            "type": "integer",
                            "description": "Maximum patterns to return (default: 10)",
                            "default": 10
                        },
                        "min_confidence": {
                            "type": "number",
                            "description": "Minimum confidence threshold (0.0-1.0, default: 0.5)",
                            "default": 0.5
                        }
                    }
                }
            },
            {
                "name": "get_recent_context",
                "description": "Get recent knowledge capsules for temporal context - what have we been discussing lately?",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "limit": {
                            "type": "integer",
                            "description": "Number of recent capsules (default: 10)",
                            "default": 10
                        }
                    }
                }
            },
            {
                "name": "memory_health",
                "description": "Check the health and status of the Chronicle memory system.",
                "inputSchema": {
                    "type": "object",
                    "properties": {}
                }
            },
            {
                "name": "store_memory",
                "description": "Store a new fact or memory in Chronicle. Use this to persist important information from our conversation - facts about the user, decisions made, project details, or anything worth remembering for future sessions.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "content": {
                            "type": "string",
                            "description": "The fact or memory to store. Should be a clear, self-contained statement."
                        },
                        "topic": {
                            "type": "string",
                            "description": "Optional topic category (e.g., 'homeforge', 'family', 'project-config')"
                        },
                        "keywords": {
                            "type": "array",
                            "items": { "type": "string" },
                            "description": "Optional keywords for indexing and retrieval"
                        },
                        "persons": {
                            "type": "array",
                            "items": { "type": "string" },
                            "description": "Optional names of people mentioned"
                        }
                    },
                    "required": ["content"]
                }
            },
            {
                "name": "session_wrapup",
                "description": "Store multiple memories at once - ideal for end-of-session summaries. Efficiently batches the storage of key facts, decisions, and learnings from the conversation.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "memories": {
                            "type": "array",
                            "description": "Array of memories to store",
                            "items": {
                                "type": "object",
                                "properties": {
                                    "content": { "type": "string", "description": "The fact or memory" },
                                    "topic": { "type": "string", "description": "Topic category" },
                                    "keywords": { "type": "array", "items": { "type": "string" } }
                                },
                                "required": ["content"]
                            }
                        },
                        "session_summary": {
                            "type": "string",
                            "description": "Optional brief summary of what the session accomplished"
                        }
                    },
                    "required": ["memories"]
                }
            },
            {
                "name": "surface_context",
                "description": "Proactively surface relevant memories based on conversation context. Unlike search_memory which requires a specific query, this tool analyzes the provided context and returns memories that might be relevant - things you might not have thought to search for. Use at the start of a conversation or when the topic shifts.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "context": {
                            "type": "string",
                            "description": "The conversation context to analyze - could be the user's opening message, a topic description, or recent exchange"
                        },
                        "include_patterns": {
                            "type": "boolean",
                            "description": "Whether to include relevant patterns (default: true)",
                            "default": true
                        },
                        "max_memories": {
                            "type": "integer",
                            "description": "Maximum memories to surface (default: 8)",
                            "default": 8
                        }
                    },
                    "required": ["context"]
                }
            },
            {
                "name": "get_cognitive_state",
                "description": "Get the current Compressed Cognitive State (CCS) - the bounded working memory containing decision-critical variables. This is a structured representation of: goals, constraints, focal entities, uncertainties, and recent context. Use this at session start instead of scattered searches.",
                "inputSchema": {
                    "type": "object",
                    "properties": {}
                }
            },
            {
                "name": "update_cognitive_state",
                "description": "Update the Compressed Cognitive State with new information from the current session. This REPLACES the previous state (not appends). Use at session end or when significant context shifts occur. Provide the fields you want to update - unspecified fields retain their current values.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "episodic_trace": {
                            "type": "array",
                            "items": { "type": "string" },
                            "description": "Recent events/changes to record (replaces existing trace)"
                        },
                        "semantic_gist": {
                            "type": "string",
                            "description": "Updated dominant intent/topic"
                        },
                        "goal_orientation": {
                            "type": "string",
                            "description": "Updated persistent objective"
                        },
                        "add_entity": {
                            "type": "object",
                            "properties": {
                                "name": { "type": "string" },
                                "type": { "type": "string", "enum": ["person", "project", "concept", "organization", "technology"] },
                                "salience": { "type": "number" },
                                "context": { "type": "string" }
                            },
                            "description": "Add a new focal entity"
                        },
                        "add_uncertainty": {
                            "type": "object",
                            "properties": {
                                "description": { "type": "string" },
                                "magnitude": { "type": "number" }
                            },
                            "description": "Add an uncertainty signal"
                        },
                        "predictive_cue": {
                            "type": "string",
                            "description": "What's expected next"
                        }
                    }
                }
            },
            {
                "name": "compress_cognitive_state",
                "description": "Use LLM-based compression to update the cognitive state. This is the core ACC operation: CCS_t = C_θ(x_t, CCS_{t-1}, A_t^+; S_CCS). Provide the current context and optionally relevant artifacts, and the LLM will compress everything into a schema-constrained update. Use this for session wrap-ups or when significant context accumulates.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "current_context": {
                            "type": "string",
                            "description": "The current session context to compress - what happened, key decisions, new information"
                        },
                        "artifact_queries": {
                            "type": "array",
                            "items": { "type": "string" },
                            "description": "Optional queries to retrieve relevant artifacts for context (will be filtered through qualification gate)"
                        },
                        "model": {
                            "type": "string",
                            "description": "LLM model to use for compression (default: claude-sonnet-4-20250514)"
                        }
                    },
                    "required": ["current_context"]
                }
            },
            {
                "name": "write_note",
                "description": "Write a note to the scratch pad - a personal space for leaving notes to yourself between cognitive cycles and sessions. Use this to track thoughts, todos, questions, or reminders.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "content": {
                            "type": "string",
                            "description": "The note content"
                        },
                        "category": {
                            "type": "string",
                            "enum": ["thought", "todo", "question", "idea", "reminder"],
                            "description": "Optional category for the note"
                        },
                        "priority": {
                            "type": "integer",
                            "description": "Priority level (higher = more important, default: 0)"
                        },
                        "expires_in_hours": {
                            "type": "integer",
                            "description": "Optional: note expires after this many hours"
                        }
                    },
                    "required": ["content"]
                }
            },
            {
                "name": "read_notes",
                "description": "Read notes from the scratch pad. Returns active notes sorted by priority and creation time.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "limit": {
                            "type": "integer",
                            "description": "Maximum notes to return (default: 10)",
                            "default": 10
                        },
                        "category": {
                            "type": "string",
                            "description": "Filter by category (optional)"
                        },
                        "include_resolved": {
                            "type": "boolean",
                            "description": "Include resolved notes (default: false)",
                            "default": false
                        }
                    }
                }
            },
            {
                "name": "resolve_note",
                "description": "Mark a scratch pad note as resolved (done). The note remains in the database but won't appear in normal reads.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "note_id": {
                            "type": "integer",
                            "description": "The ID of the note to resolve"
                        }
                    },
                    "required": ["note_id"]
                }
            },
            {
                "name": "clear_resolved",
                "description": "Permanently delete all resolved notes from the scratch pad. Use this to clean up after completing tasks.",
                "inputSchema": {
                    "type": "object",
                    "properties": {}
                }
            },
            {
                "name": "get_outbox",
                "description": "Get unread messages from the Chronicle Mind outbox - messages the autonomous cognitive loop has left for you.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "limit": {
                            "type": "integer",
                            "description": "Maximum messages to return (default: 10)",
                            "default": 10
                        }
                    }
                }
            },
            {
                "name": "acknowledge_messages",
                "description": "Acknowledge (mark as read) outbox messages. Call with no ID to acknowledge all.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "message_id": {
                            "type": "integer",
                            "description": "Optional specific message ID to acknowledge. If not provided, acknowledges all."
                        }
                    }
                }
            },
            {
                "name": "get_thoughts",
                "description": "Get recent thoughts from the Chronicle Mind thought stream - a log of cognitive reasoning from autonomous cycles.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "limit": {
                            "type": "integer",
                            "description": "Maximum thoughts to return (default: 5)",
                            "default": 5
                        }
                    }
                }
            }
        ]
    })
}

// Tool execution

async fn execute_tool(name: &str, args: &Value) -> Result<Value> {
    use homeforge_chronicle::icp::IcpClient;
    use homeforge_chronicle::OllamaEmbedding;

    // Configuration - could be made configurable via env vars
    let canister_id = std::env::var("CHRONICLE_CANISTER_ID")
        .unwrap_or_else(|_| "fqqku-bqaaa-aaaai-q4wha-cai".to_string());
    let identity = std::env::var("CHRONICLE_IDENTITY")
        .unwrap_or_else(|_| "chronicle-auto".to_string());
    let ollama_url = std::env::var("CHRONICLE_OLLAMA_URL")
        .unwrap_or_else(|_| "http://localhost:11434".to_string());
    let model = std::env::var("CHRONICLE_EMBEDDING_MODEL")
        .unwrap_or_else(|_| "mxbai-embed-large".to_string());

    match name {
        "search_memory" => {
            use homeforge_chronicle::db::Database;
            use homeforge_chronicle::find_top_k_similar;

            let query = args.get("query")
                .and_then(|v| v.as_str())
                .ok_or_else(|| anyhow::anyhow!("Missing query parameter"))?;
            let limit = args.get("limit")
                .and_then(|v| v.as_u64())
                .unwrap_or(5) as usize;

            // Open local database
            let home = std::env::var("HOME")?;
            let db_path = format!("{}/.homeforge-chronicle/processed.db", home);
            let db = Database::new(std::path::Path::new(&db_path))?;

            // Try semantic search first (requires Ollama), fall back to keyword search
            let (formatted, search_type) = match OllamaEmbedding::new(&ollama_url, &model)
                .and_then(|client| client.embed(query))
            {
                Ok(query_embedding) => {
                    // Semantic search with embeddings
                    let embeddings = db.get_all_embeddings()?;
                    let similar = find_top_k_similar(&query_embedding, &embeddings, limit);

                    let results: Vec<Value> = similar.iter().filter_map(|(capsule_id, score)| {
                        db.get_capsule_display_info(*capsule_id).ok().flatten().map(|(restatement, timestamp, topic, _confidence)| {
                            json!({
                                "similarity": format!("{:.3}", score),
                                "content": restatement,
                                "topic": topic,
                                "timestamp": timestamp
                            })
                        })
                    }).collect();
                    (results, "semantic")
                }
                Err(_) => {
                    // Fallback to keyword search
                    eprintln!("Ollama unavailable, falling back to keyword search");
                    let keywords: Vec<String> = query.split_whitespace()
                        .map(|s| s.to_lowercase())
                        .filter(|s| s.len() > 2) // Skip short words
                        .collect();

                    let results = db.search_capsules_by_keyword(&keywords, limit as i64)?;
                    let formatted: Vec<Value> = results.iter().map(|(capsule_id, content, score)| {
                        let info = db.get_capsule_display_info(*capsule_id).ok().flatten();
                        json!({
                            "similarity": format!("{:.3}", score),
                            "content": content,
                            "topic": info.as_ref().map(|(_, _, t, _)| t.clone()).unwrap_or_default(),
                            "timestamp": info.as_ref().map(|(_, ts, _, _)| ts.clone()).unwrap_or_default()
                        })
                    }).collect();
                    (formatted, "keyword")
                }
            };

            Ok(json!({
                "query": query,
                "results_count": formatted.len(),
                "memories": formatted,
                "search_type": search_type,
                "source": "local_db"
            }))
        }

        "get_patterns" => {
            let limit = args.get("limit")
                .and_then(|v| v.as_u64())
                .unwrap_or(10) as usize;
            let min_confidence = args.get("min_confidence")
                .and_then(|v| v.as_f64())
                .unwrap_or(0.5);

            // Get enriched patterns from local database
            let home = std::env::var("HOME")?;
            let db_path = format!("{}/.homeforge-chronicle/processed.db", home);
            let db = homeforge_chronicle::db::Database::new(std::path::Path::new(&db_path))?;

            let patterns = db.get_enriched_patterns(min_confidence, limit, true)?;

            let formatted: Vec<Value> = patterns.iter().map(|p| {
                // Format recent capsules
                let recent: Vec<Value> = p.recent_capsules.iter().map(|(id, content, ts)| {
                    json!({
                        "capsule_id": id,
                        "content": if content.len() > 150 { format!("{}...", &content[..150]) } else { content.clone() },
                        "timestamp": ts
                    })
                }).collect();

                // Build decay trajectory info
                let decay_info = json!({
                    "days_since_reinforcement": p.days_since_reinforcement,
                    "days_until_decay_starts": p.days_until_decay_starts,
                    "projected_confidence_7d": format!("{:.2}", p.projected_confidence_7d),
                    "will_deactivate_in_days": p.will_deactivate_in
                });

                json!({
                    "id": p.id,
                    "summary": p.summary,
                    "capsule_count": p.capsule_count,
                    "confidence": format!("{:.2}", p.confidence),
                    "active": p.active,
                    "decay_trajectory": decay_info,
                    "recent_capsules": recent
                })
            }).collect();

            Ok(json!({
                "pattern_count": formatted.len(),
                "patterns": formatted
            }))
        }

        "get_recent_context" => {
            let limit = args.get("limit")
                .and_then(|v| v.as_u64())
                .unwrap_or(10);

            let client = IcpClient::from_dfx_identity(&canister_id, &identity).await?;
            let capsules = client.get_recent_capsules(limit).await?;

            let formatted: Vec<Value> = capsules.iter().map(|c| {
                json!({
                    "content": c.restatement,
                    "topic": c.topic,
                    "timestamp": c.timestamp,
                    "keywords": c.keywords
                })
            }).collect();

            Ok(json!({
                "recent_count": formatted.len(),
                "recent_memories": formatted
            }))
        }

        "memory_health" => {
            // Get local stats first (always available)
            let home = std::env::var("HOME")?;
            let db_path = format!("{}/.homeforge-chronicle/processed.db", home);
            let db = homeforge_chronicle::db::Database::new(std::path::Path::new(&db_path))?;
            let pattern_count = db.get_pattern_count()?;
            let local_capsule_count = db.get_capsule_count()?;
            let local_embedding_count = db.get_embedding_count()?;

            // Try ICP canister if configured
            let (canister_status, chain_capsules, chain_embeddings) = if !canister_id.is_empty() {
                match IcpClient::from_dfx_identity(&canister_id, &identity).await {
                    Ok(client) => {
                        let health = client.health().await.unwrap_or_else(|_| "unreachable".to_string());
                        let caps = client.get_capsule_count().await.unwrap_or(0);
                        let embs = client.get_embedding_count().await.unwrap_or(0);
                        (Some(health), Some(caps), Some(embs))
                    }
                    Err(_) => (Some("not_configured".to_string()), None, None)
                }
            } else {
                (None, None, None)
            };

            Ok(json!({
                "status": "healthy",
                "local_capsules": local_capsule_count,
                "local_embeddings": local_embedding_count,
                "local_patterns": pattern_count,
                "canister_status": canister_status,
                "capsules_on_chain": chain_capsules,
                "embeddings_on_chain": chain_embeddings,
                "canister_id": if canister_id.is_empty() { None } else { Some(&canister_id) }
            }))
        }

        "store_memory" => {
            use homeforge_chronicle::icp::{KnowledgeCapsule, CapsuleEmbedding};

            let content = args.get("content")
                .and_then(|v| v.as_str())
                .ok_or_else(|| anyhow::anyhow!("Missing content parameter"))?;

            let topic = args.get("topic")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string());

            let keywords: Vec<String> = args.get("keywords")
                .and_then(|v| v.as_array())
                .map(|arr| arr.iter().filter_map(|v| v.as_str().map(|s| s.to_string())).collect())
                .unwrap_or_default();

            let persons: Vec<String> = args.get("persons")
                .and_then(|v| v.as_array())
                .map(|arr| arr.iter().filter_map(|v| v.as_str().map(|s| s.to_string())).collect())
                .unwrap_or_default();

            // Generate embedding
            let embedding_client = OllamaEmbedding::new(&ollama_url, &model)?;
            let embedding = embedding_client.embed(content)?;

            // Create capsule
            let timestamp = chrono::Utc::now().to_rfc3339();
            let conversation_id = format!("mcp-live-{}", chrono::Utc::now().timestamp());

            let capsule = KnowledgeCapsule {
                id: 0, // Will be assigned by canister
                conversation_id: conversation_id.clone(),
                restatement: content.to_string(),
                timestamp: Some(timestamp.clone()),
                location: None,
                topic: topic.clone(),
                confidence_score: 1.0, // High confidence for explicit stores
                persons: persons.clone(),
                entities: vec![],
                keywords: keywords.clone(),
                created_at: 0, // Will be set by canister
            };

            // Store capsule
            let client = IcpClient::from_dfx_identity(&canister_id, &identity).await?;
            let ids = client.add_capsules_bulk(vec![capsule]).await?;

            let capsule_id = ids.first()
                .ok_or_else(|| anyhow::anyhow!("Failed to get capsule ID"))?;

            // Store embedding
            let emb = CapsuleEmbedding {
                capsule_id: *capsule_id,
                embedding,
                model_name: model.clone(),
            };
            client.add_embeddings_bulk(vec![emb]).await?;

            Ok(json!({
                "success": true,
                "capsule_id": capsule_id,
                "content": content,
                "topic": topic,
                "keywords": keywords,
                "persons": persons,
                "timestamp": timestamp,
                "message": "Memory stored successfully on Internet Computer"
            }))
        }

        "session_wrapup" => {
            use homeforge_chronicle::icp::{KnowledgeCapsule, CapsuleEmbedding};

            let memories = args.get("memories")
                .and_then(|v| v.as_array())
                .ok_or_else(|| anyhow::anyhow!("Missing memories array"))?;

            let session_summary = args.get("session_summary")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string());

            let embedding_client = OllamaEmbedding::new(&ollama_url, &model)?;
            let client = IcpClient::from_dfx_identity(&canister_id, &identity).await?;

            let timestamp = chrono::Utc::now().to_rfc3339();
            let session_id = format!("mcp-wrapup-{}", chrono::Utc::now().timestamp());

            let mut stored = Vec::new();
            let mut capsules_to_store = Vec::new();
            let mut embeddings_to_store = Vec::new();

            // Process each memory
            for (i, mem) in memories.iter().enumerate() {
                let content = mem.get("content")
                    .and_then(|v| v.as_str())
                    .unwrap_or("");

                if content.is_empty() {
                    continue;
                }

                let topic = mem.get("topic")
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string());

                let keywords: Vec<String> = mem.get("keywords")
                    .and_then(|v| v.as_array())
                    .map(|arr| arr.iter().filter_map(|v| v.as_str().map(|s| s.to_string())).collect())
                    .unwrap_or_default();

                // Generate embedding
                match embedding_client.embed(content) {
                    Ok(embedding) => {
                        let capsule = KnowledgeCapsule {
                            id: 0,
                            conversation_id: session_id.clone(),
                            restatement: content.to_string(),
                            timestamp: Some(timestamp.clone()),
                            location: None,
                            topic: topic.clone(),
                            confidence_score: 1.0,
                            persons: vec![],
                            entities: vec![],
                            keywords: keywords.clone(),
                            created_at: 0,
                        };
                        capsules_to_store.push(capsule);
                        embeddings_to_store.push(embedding);
                        stored.push(json!({
                            "index": i,
                            "content": content,
                            "topic": topic,
                            "status": "prepared"
                        }));
                    }
                    Err(e) => {
                        stored.push(json!({
                            "index": i,
                            "content": content,
                            "status": "error",
                            "error": format!("{}", e)
                        }));
                    }
                }
            }

            // Bulk store capsules
            let capsule_ids = client.add_capsules_bulk(capsules_to_store).await?;

            // Bulk store embeddings
            let embs: Vec<CapsuleEmbedding> = capsule_ids.iter().zip(embeddings_to_store.iter()).map(|(id, emb)| {
                CapsuleEmbedding {
                    capsule_id: *id,
                    embedding: emb.clone(),
                    model_name: model.clone(),
                }
            }).collect();
            client.add_embeddings_bulk(embs).await?;

            // Update stored items with IDs
            for (item, id) in stored.iter_mut().zip(capsule_ids.iter()) {
                if let Some(obj) = item.as_object_mut() {
                    obj.insert("capsule_id".to_string(), json!(id));
                    obj.insert("status".to_string(), json!("stored"));
                }
            }

            Ok(json!({
                "success": true,
                "memories_stored": capsule_ids.len(),
                "session_summary": session_summary,
                "details": stored,
                "timestamp": timestamp,
                "message": format!("Session wrap-up complete: {} memories stored", capsule_ids.len())
            }))
        }

        "surface_context" => {
            use homeforge_chronicle::db::Database;
            use homeforge_chronicle::find_top_k_similar;
            use std::collections::HashSet;

            let context = args.get("context")
                .and_then(|v| v.as_str())
                .ok_or_else(|| anyhow::anyhow!("Missing context parameter"))?;
            let include_patterns = args.get("include_patterns")
                .and_then(|v| v.as_bool())
                .unwrap_or(true);
            let max_memories = args.get("max_memories")
                .and_then(|v| v.as_u64())
                .unwrap_or(8) as usize;

            // Get embedding client and database
            let embedding_client = OllamaEmbedding::new(&ollama_url, &model)?;
            let home = std::env::var("HOME")?;
            let db_path = format!("{}/.homeforge-chronicle/processed.db", home);
            let db = Database::new(std::path::Path::new(&db_path))?;

            // Strategy: Do multiple semantic searches and combine results
            // 1. Direct embedding of the full context
            // 2. Extract key phrases and search for those too

            let mut all_results: Vec<(i64, f32)> = Vec::new();
            let mut seen_ids: HashSet<i64> = HashSet::new();

            // Get all embeddings once
            let embeddings = db.get_all_embeddings()?;

            // Search 1: Full context embedding
            let context_embedding = embedding_client.embed(context)?;
            let direct_results = find_top_k_similar(&context_embedding, &embeddings, max_memories);
            for (id, score) in direct_results {
                if seen_ids.insert(id) {
                    all_results.push((id, score));
                }
            }

            // Search 2: If context is long, also search key parts
            // Extract sentences/phrases that might be distinct concepts
            if context.len() > 100 {
                let sentences: Vec<&str> = context.split(&['.', '?', '!', '\n'][..])
                    .map(|s| s.trim())
                    .filter(|s| s.len() > 20)
                    .take(3)
                    .collect();

                for sentence in sentences {
                    if let Ok(sent_embedding) = embedding_client.embed(sentence) {
                        let sent_results = find_top_k_similar(&sent_embedding, &embeddings, 3);
                        for (id, score) in sent_results {
                            if seen_ids.insert(id) {
                                all_results.push((id, score * 0.9)); // Slight penalty for indirect matches
                            }
                        }
                    }
                }
            }

            // Sort by score and take top results
            all_results.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
            all_results.truncate(max_memories);

            // Format memory results
            let memories: Vec<Value> = all_results.iter().filter_map(|(capsule_id, score)| {
                db.get_capsule_display_info(*capsule_id).ok().flatten().map(|(restatement, timestamp, topic, confidence)| {
                    json!({
                        "relevance": format!("{:.2}", score),
                        "content": restatement,
                        "topic": topic,
                        "timestamp": timestamp,
                        "confidence": format!("{:.2}", confidence)
                    })
                })
            }).collect();

            // Optionally get relevant patterns
            let patterns: Vec<Value> = if include_patterns {
                let active_patterns = db.get_enriched_patterns(0.5, 5, true)?;

                // Filter to patterns that seem relevant to the context
                // Simple heuristic: check if pattern summary words appear in context
                let context_lower = context.to_lowercase();
                active_patterns.iter()
                    .filter(|p| {
                        let summary_words: Vec<&str> = p.summary.split(&[',', ' '][..])
                            .map(|w| w.trim())
                            .filter(|w| w.len() > 3)
                            .collect();
                        summary_words.iter().any(|w| context_lower.contains(&w.to_lowercase()))
                    })
                    .take(3)
                    .map(|p| {
                        json!({
                            "pattern": p.summary,
                            "strength": p.capsule_count,
                            "confidence": format!("{:.2}", p.confidence)
                        })
                    })
                    .collect()
            } else {
                vec![]
            };

            Ok(json!({
                "context_analyzed": if context.len() > 100 { format!("{}...", &context[..100]) } else { context.to_string() },
                "memories_surfaced": memories.len(),
                "memories": memories,
                "related_patterns": patterns,
                "note": "These memories were proactively surfaced based on semantic similarity to your context. They may provide useful background even if you didn't explicitly search for them."
            }))
        }

        "get_cognitive_state" => {
            use homeforge_chronicle::db::Database;

            let home = std::env::var("HOME")?;
            let db_path = format!("{}/.homeforge-chronicle/processed.db", home);
            let db = Database::new(std::path::Path::new(&db_path))?;

            let ccs = db.get_cognitive_state()?;

            Ok(json!({
                "cognitive_state": {
                    "semantic_gist": ccs.semantic_gist,
                    "goal_orientation": ccs.goal_orientation,
                    "focal_entities": ccs.focal_entities,
                    "constraints": ccs.constraints,
                    "uncertainty_signals": ccs.uncertainty_signals,
                    "predictive_cue": ccs.predictive_cue,
                    "episodic_trace": ccs.episodic_trace,
                    "retrieved_artifacts": ccs.retrieved_artifacts,
                    "relational_map": ccs.relational_map
                },
                "metadata": {
                    "updated_at": ccs.updated_at,
                    "version": ccs.version,
                    "compression_model": ccs.compression_model
                },
                "summary": ccs.summary()
            }))
        }

        "update_cognitive_state" => {
            use homeforge_chronicle::db::Database;
            use homeforge_chronicle::{FocalEntity, EntityType, UncertaintySignal};

            let home = std::env::var("HOME")?;
            let db_path = format!("{}/.homeforge-chronicle/processed.db", home);
            let db = Database::new(std::path::Path::new(&db_path))?;

            // Get current state
            let mut ccs = db.get_cognitive_state()?;

            // Update fields that were provided
            if let Some(trace) = args.get("episodic_trace").and_then(|v| v.as_array()) {
                ccs.episodic_trace = trace.iter()
                    .filter_map(|v| v.as_str().map(|s| s.to_string()))
                    .collect();
            }

            if let Some(gist) = args.get("semantic_gist").and_then(|v| v.as_str()) {
                ccs.semantic_gist = gist.to_string();
            }

            if let Some(goal) = args.get("goal_orientation").and_then(|v| v.as_str()) {
                ccs.goal_orientation = goal.to_string();
            }

            if let Some(cue) = args.get("predictive_cue").and_then(|v| v.as_str()) {
                ccs.predictive_cue = cue.to_string();
            }

            // Add new entity if provided
            if let Some(entity) = args.get("add_entity") {
                if let (Some(name), Some(etype)) = (
                    entity.get("name").and_then(|v| v.as_str()),
                    entity.get("type").and_then(|v| v.as_str())
                ) {
                    let entity_type = match etype {
                        "person" => EntityType::Person,
                        "project" => EntityType::Project,
                        "concept" => EntityType::Concept,
                        "organization" => EntityType::Organization,
                        "technology" => EntityType::Technology,
                        _ => EntityType::Other,
                    };
                    let salience = entity.get("salience").and_then(|v| v.as_f64()).unwrap_or(0.5);
                    let context = entity.get("context").and_then(|v| v.as_str()).map(|s| s.to_string());

                    ccs.focal_entities.push(FocalEntity {
                        name: name.to_string(),
                        entity_type,
                        salience,
                        context,
                    });
                }
            }

            // Add uncertainty if provided
            if let Some(uncertainty) = args.get("add_uncertainty") {
                if let Some(desc) = uncertainty.get("description").and_then(|v| v.as_str()) {
                    let magnitude = uncertainty.get("magnitude").and_then(|v| v.as_f64()).unwrap_or(0.5);
                    ccs.uncertainty_signals.push(UncertaintySignal {
                        description: desc.to_string(),
                        magnitude,
                        resolution_path: None,
                    });
                }
            }

            // Save updated state (REPLACEMENT semantics)
            ccs.updated_at = chrono::Utc::now().timestamp();
            db.set_cognitive_state(&ccs)?;

            Ok(json!({
                "success": true,
                "message": "Cognitive state updated (replacement semantics)",
                "new_version": ccs.version + 1,
                "summary": ccs.summary()
            }))
        }

        "compress_cognitive_state" => {
            use homeforge_chronicle::db::Database;
            use homeforge_chronicle::{CognitiveCompressor, CompressionInput, QualificationGate, RetrievedArtifact};
            use homeforge_chronicle::llm::ClaudeClient;
            use homeforge_chronicle::find_top_k_similar;

            let current_context = args.get("current_context")
                .and_then(|v| v.as_str())
                .ok_or_else(|| anyhow::anyhow!("Missing current_context parameter"))?;

            let artifact_queries: Vec<String> = args.get("artifact_queries")
                .and_then(|v| v.as_array())
                .map(|arr| arr.iter().filter_map(|v| v.as_str().map(|s| s.to_string())).collect())
                .unwrap_or_default();

            let llm_model = args.get("model")
                .and_then(|v| v.as_str())
                .unwrap_or("claude-sonnet-4-20250514")
                .to_string();

            // Get database and current state
            let home = std::env::var("HOME")?;
            let db_path = format!("{}/.homeforge-chronicle/processed.db", home);
            let db = Database::new(std::path::Path::new(&db_path))?;
            let previous_state = db.get_cognitive_state()?;

            // Retrieve and qualify artifacts if queries provided
            let mut qualified_artifacts = Vec::new();
            let mut artifact_summaries = Vec::new();

            if !artifact_queries.is_empty() {
                let embedding_client = OllamaEmbedding::new(&ollama_url, &model)?;
                let embeddings = db.get_all_embeddings()?;
                let gate = QualificationGate::default();

                for query in &artifact_queries {
                    if let Ok(query_embedding) = embedding_client.embed(query) {
                        let similar = find_top_k_similar(&query_embedding, &embeddings, 3);

                        for (capsule_id, score) in similar {
                            if let Ok(Some((restatement, _timestamp, _topic, _confidence))) =
                                db.get_capsule_display_info(capsule_id)
                            {
                                let artifact = RetrievedArtifact {
                                    capsule_id,
                                    relevance: score as f64,
                                    qualified: false,
                                    summary: Some(restatement.clone()),
                                };

                                // Apply qualification gate
                                if gate.qualify(&artifact, &previous_state) {
                                    artifact_summaries.push(restatement);
                                    qualified_artifacts.push(RetrievedArtifact {
                                        qualified: true,
                                        ..artifact
                                    });
                                }
                            }
                        }
                    }
                }
            }

            // Build compression input
            let input = CompressionInput {
                current_context: current_context.to_string(),
                qualified_artifacts: qualified_artifacts.clone(),
                artifact_summaries,
            };

            // Create LLM client and compressor
            let llm = ClaudeClient::from_env(llm_model.clone())?;
            let compressor = CognitiveCompressor::new(llm_model.clone());

            // Run compression
            let new_state = compressor.compress(&input, &previous_state, &llm)?;

            // Save to database
            db.set_cognitive_state(&new_state)?;

            Ok(json!({
                "success": true,
                "message": "Cognitive state compressed via LLM (ACC compression operator)",
                "compression_model": llm_model,
                "artifacts_qualified": qualified_artifacts.len(),
                "previous_version": previous_state.version,
                "new_version": new_state.version,
                "new_state": {
                    "semantic_gist": new_state.semantic_gist,
                    "goal_orientation": new_state.goal_orientation,
                    "focal_entities": new_state.focal_entities,
                    "episodic_trace": new_state.episodic_trace,
                    "predictive_cue": new_state.predictive_cue
                },
                "summary": new_state.summary()
            }))
        }

        // ============================================================
        // Scratch Pad Tools
        // ============================================================

        "write_note" => {
            use homeforge_chronicle::db::Database;

            let content = args.get("content")
                .and_then(|v| v.as_str())
                .ok_or_else(|| anyhow::anyhow!("Missing content parameter"))?;

            let category = args.get("category")
                .and_then(|v| v.as_str());

            let priority = args.get("priority")
                .and_then(|v| v.as_i64())
                .unwrap_or(0) as i32;

            let expires_at = args.get("expires_in_hours")
                .and_then(|v| v.as_i64())
                .map(|hours| chrono::Utc::now().timestamp() + (hours * 3600));

            let home = std::env::var("HOME")?;
            let db_path = format!("{}/.homeforge-chronicle/processed.db", home);
            let db = Database::new(std::path::Path::new(&db_path))?;

            let note_id = db.write_scratch_note(content, category, priority, expires_at)?;

            Ok(json!({
                "success": true,
                "note_id": note_id,
                "content": content,
                "category": category,
                "priority": priority,
                "expires_at": expires_at,
                "message": "Note written to scratch pad"
            }))
        }

        "read_notes" => {
            use homeforge_chronicle::db::Database;

            let limit = args.get("limit")
                .and_then(|v| v.as_u64())
                .unwrap_or(10) as usize;

            let category = args.get("category")
                .and_then(|v| v.as_str());

            let include_resolved = args.get("include_resolved")
                .and_then(|v| v.as_bool())
                .unwrap_or(false);

            let home = std::env::var("HOME")?;
            let db_path = format!("{}/.homeforge-chronicle/processed.db", home);
            let db = Database::new(std::path::Path::new(&db_path))?;

            let notes = db.get_scratch_notes(limit, category, include_resolved)?;

            let formatted: Vec<Value> = notes.iter().map(|n| {
                json!({
                    "id": n.id,
                    "content": n.content,
                    "category": n.category,
                    "priority": n.priority,
                    "created_at": n.created_at,
                    "updated_at": n.updated_at,
                    "expires_at": n.expires_at,
                    "resolved": n.resolved
                })
            }).collect();

            let active_count = db.get_scratch_note_count(false)?;

            Ok(json!({
                "notes_count": formatted.len(),
                "total_active": active_count,
                "notes": formatted
            }))
        }

        "resolve_note" => {
            use homeforge_chronicle::db::Database;

            let note_id = args.get("note_id")
                .and_then(|v| v.as_i64())
                .ok_or_else(|| anyhow::anyhow!("Missing note_id parameter"))?;

            let home = std::env::var("HOME")?;
            let db_path = format!("{}/.homeforge-chronicle/processed.db", home);
            let db = Database::new(std::path::Path::new(&db_path))?;

            let resolved = db.resolve_scratch_note(note_id)?;

            Ok(json!({
                "success": resolved,
                "note_id": note_id,
                "message": if resolved { "Note marked as resolved" } else { "Note not found" }
            }))
        }

        "clear_resolved" => {
            use homeforge_chronicle::db::Database;

            let home = std::env::var("HOME")?;
            let db_path = format!("{}/.homeforge-chronicle/processed.db", home);
            let db = Database::new(std::path::Path::new(&db_path))?;

            // Also clear expired notes
            let expired_cleared = db.clear_expired_notes()?;
            let resolved_cleared = db.clear_resolved_notes()?;

            Ok(json!({
                "success": true,
                "resolved_cleared": resolved_cleared,
                "expired_cleared": expired_cleared,
                "message": format!("Cleared {} resolved and {} expired notes", resolved_cleared, expired_cleared)
            }))
        }

        // ============================================================
        // Outbox Tools - Messages from Chronicle Mind
        // ============================================================

        "get_outbox" => {
            use homeforge_chronicle::db::Database;

            let limit = args.get("limit")
                .and_then(|v| v.as_u64())
                .unwrap_or(10) as usize;

            let home = std::env::var("HOME")?;
            let db_path = format!("{}/.homeforge-chronicle/processed.db", home);
            let db = Database::new(std::path::Path::new(&db_path))?;

            let messages = db.get_unread_messages(limit)?;

            let formatted: Vec<Value> = messages.iter().map(|m| {
                json!({
                    "id": m.id,
                    "message": m.message,
                    "priority": m.priority,
                    "category": m.category,
                    "created_at": m.created_at
                })
            }).collect();

            Ok(json!({
                "unread_count": formatted.len(),
                "messages": formatted,
                "note": if formatted.is_empty() { "No unread messages from Chronicle Mind" } else { "Messages from the autonomous cognitive loop" }
            }))
        }

        "acknowledge_messages" => {
            use homeforge_chronicle::db::Database;

            let home = std::env::var("HOME")?;
            let db_path = format!("{}/.homeforge-chronicle/processed.db", home);
            let db = Database::new(std::path::Path::new(&db_path))?;

            let message_id = args.get("message_id")
                .and_then(|v| v.as_i64());

            match message_id {
                Some(id) => {
                    let acked = db.acknowledge_message(id)?;
                    Ok(json!({
                        "success": acked,
                        "message": if acked { format!("Acknowledged message {}", id) } else { format!("Message {} not found", id) }
                    }))
                }
                None => {
                    let count = db.acknowledge_all_messages()?;
                    Ok(json!({
                        "success": true,
                        "acknowledged_count": count,
                        "message": format!("Acknowledged {} messages", count)
                    }))
                }
            }
        }

        // ============================================================
        // Thought Stream Tools
        // ============================================================

        "get_thoughts" => {
            use homeforge_chronicle::db::Database;

            let limit = args.get("limit")
                .and_then(|v| v.as_u64())
                .unwrap_or(5) as usize;

            let home = std::env::var("HOME")?;
            let db_path = format!("{}/.homeforge-chronicle/processed.db", home);
            let db = Database::new(std::path::Path::new(&db_path))?;

            let thoughts = db.get_recent_thoughts(limit)?;

            let formatted: Vec<Value> = thoughts.iter().map(|t| {
                // Parse actions_taken JSON
                let actions: Vec<String> = serde_json::from_str(&t.actions_taken).unwrap_or_default();

                json!({
                    "cycle_id": t.cycle_id,
                    "context": t.context_summary,
                    "reasoning": if t.reasoning.len() > 500 {
                        format!("{}...", &t.reasoning[..500])
                    } else {
                        t.reasoning.clone()
                    },
                    "actions": actions,
                    "created_at": t.created_at
                })
            }).collect();

            Ok(json!({
                "thought_count": formatted.len(),
                "thoughts": formatted,
                "note": "Recent reasoning from the Chronicle Mind cognitive loop"
            }))
        }

        _ => Err(anyhow::anyhow!("Unknown tool: {}", name))
    }
}

// MCP Server

fn handle_request(request: &JsonRpcRequest) -> JsonRpcResponse {
    let id = request.id.clone().unwrap_or(Value::Null);

    match request.method.as_str() {
        "initialize" => {
            JsonRpcResponse {
                jsonrpc: "2.0".to_string(),
                id,
                result: Some(json!({
                    "protocolVersion": "2024-11-05",
                    "capabilities": {
                        "tools": {}
                    },
                    "serverInfo": {
                        "name": "chronicle-memory",
                        "version": "0.1.0"
                    }
                })),
                error: None,
            }
        }

        "notifications/initialized" => {
            // No response needed for notifications
            JsonRpcResponse {
                jsonrpc: "2.0".to_string(),
                id,
                result: Some(json!({})),
                error: None,
            }
        }

        "tools/list" => {
            JsonRpcResponse {
                jsonrpc: "2.0".to_string(),
                id,
                result: Some(get_tools()),
                error: None,
            }
        }

        "tools/call" => {
            let params = request.params.as_ref();
            let tool_name = params
                .and_then(|p| p.get("name"))
                .and_then(|n| n.as_str())
                .unwrap_or("");
            let tool_args = params
                .and_then(|p| p.get("arguments"))
                .cloned()
                .unwrap_or(json!({}));

            // Execute tool in async runtime
            let rt = tokio::runtime::Runtime::new().unwrap();
            match rt.block_on(execute_tool(tool_name, &tool_args)) {
                Ok(result) => {
                    JsonRpcResponse {
                        jsonrpc: "2.0".to_string(),
                        id,
                        result: Some(json!({
                            "content": [{
                                "type": "text",
                                "text": serde_json::to_string_pretty(&result).unwrap_or_default()
                            }]
                        })),
                        error: None,
                    }
                }
                Err(e) => {
                    JsonRpcResponse {
                        jsonrpc: "2.0".to_string(),
                        id,
                        result: Some(json!({
                            "content": [{
                                "type": "text",
                                "text": format!("Error: {}", e)
                            }],
                            "isError": true
                        })),
                        error: None,
                    }
                }
            }
        }

        _ => {
            JsonRpcResponse {
                jsonrpc: "2.0".to_string(),
                id,
                result: None,
                error: Some(JsonRpcError {
                    code: -32601,
                    message: format!("Method not found: {}", request.method),
                }),
            }
        }
    }
}

fn main() -> Result<()> {
    // Log to stderr so it doesn't interfere with MCP protocol
    eprintln!("Chronicle MCP Server starting...");
    eprintln!("Memory bridge active - connecting past and present.");

    let stdin = io::stdin();
    let mut stdout = io::stdout();

    for line in stdin.lock().lines() {
        let line = match line {
            Ok(l) => l,
            Err(e) => {
                eprintln!("Error reading stdin: {}", e);
                continue;
            }
        };

        if line.trim().is_empty() {
            continue;
        }

        eprintln!("Received: {}", &line[..std::cmp::min(100, line.len())]);

        let request: JsonRpcRequest = match serde_json::from_str(&line) {
            Ok(r) => r,
            Err(e) => {
                eprintln!("Failed to parse request: {}", e);
                continue;
            }
        };

        let response = handle_request(&request);

        // Skip response for notifications
        if request.method.starts_with("notifications/") {
            continue;
        }

        let response_json = serde_json::to_string(&response)?;
        eprintln!("Responding: {}", &response_json[..std::cmp::min(100, response_json.len())]);

        writeln!(stdout, "{}", response_json)?;
        stdout.flush()?;
    }

    Ok(())
}
