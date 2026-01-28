//! Memory Metabolism
//!
//! Autonomous pattern evolution system. As new knowledge capsules arrive,
//! they reinforce existing patterns or seed new ones. Patterns that aren't
//! reinforced gradually decay. Strong patterns can cluster into meta-patterns.
//!
//! This is the "growing" part of the knowledge system - it metabolizes new
//! information rather than just accumulating it.

use anyhow::{Context, Result};
use crate::db::Database;
use crate::embedding::{cosine_similarity, OllamaEmbedding};

/// Configuration for metabolism behavior
#[derive(Debug, Clone)]
pub struct MetabolismConfig {
    /// Minimum similarity to consider a pattern match (0.0-1.0)
    pub similarity_threshold: f32,
    /// Confidence boost when a pattern is reinforced
    pub reinforcement_boost: f64,
    /// Initial confidence for newly seeded patterns
    pub seed_confidence: f64,
    /// Daily decay rate for unreinforced patterns
    pub decay_rate: f64,
    /// Minimum confidence before pattern is deactivated
    pub min_active_confidence: f64,
    /// Days without reinforcement before decay begins
    pub decay_grace_days: i64,
    /// Minimum confidence for meta-pattern clustering
    pub meta_pattern_threshold: f64,
}

impl Default for MetabolismConfig {
    fn default() -> Self {
        Self {
            similarity_threshold: 0.72,
            reinforcement_boost: 0.08,
            seed_confidence: 0.35,
            decay_rate: 0.02,
            min_active_confidence: 0.15,
            decay_grace_days: 14,
            meta_pattern_threshold: 0.75,
        }
    }
}

/// Result of metabolizing a single capsule
#[derive(Debug)]
pub struct MetabolismResult {
    pub capsule_id: i64,
    pub patterns_reinforced: Vec<(i64, f32)>, // (pattern_id, similarity)
    pub pattern_seeded: Option<i64>,
}

/// Metabolize a single capsule - find or create patterns it relates to
pub fn metabolize_capsule(
    db: &Database,
    capsule_id: i64,
    capsule_embedding: &[f32],
    config: &MetabolismConfig,
) -> Result<MetabolismResult> {
    // Get all pattern embeddings
    let pattern_embeddings = db.get_pattern_embeddings()?;

    // Find similar patterns
    let mut matches: Vec<(i64, f32)> = pattern_embeddings
        .iter()
        .map(|(pattern_id, embedding)| {
            let sim = cosine_similarity(capsule_embedding, embedding);
            (*pattern_id, sim)
        })
        .filter(|(_, sim)| *sim >= config.similarity_threshold)
        .collect();

    // Sort by similarity descending
    matches.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));

    let mut result = MetabolismResult {
        capsule_id,
        patterns_reinforced: Vec::new(),
        pattern_seeded: None,
    };

    if matches.is_empty() {
        // No matching patterns - seed a new one from this capsule
        let pattern_id = seed_pattern_from_capsule(db, capsule_id, capsule_embedding, config)?;
        result.pattern_seeded = Some(pattern_id);
    } else {
        // Reinforce matching patterns
        for (pattern_id, similarity) in &matches {
            reinforce_pattern(db, *pattern_id, capsule_id, *similarity, config)?;
            result.patterns_reinforced.push((*pattern_id, *similarity));
        }
    }

    Ok(result)
}

/// Reinforce an existing pattern with a new capsule
fn reinforce_pattern(
    db: &Database,
    pattern_id: i64,
    capsule_id: i64,
    similarity: f32,
    config: &MetabolismConfig,
) -> Result<()> {
    // Boost is proportional to similarity
    let boost = config.reinforcement_boost * (similarity as f64);

    // Update pattern confidence and timestamp
    db.reinforce_pattern(pattern_id, boost)?;

    // Link capsule to pattern
    db.link_capsule_to_pattern(capsule_id, pattern_id)?;

    Ok(())
}

/// Seed a new pattern from a capsule
fn seed_pattern_from_capsule(
    db: &Database,
    capsule_id: i64,
    capsule_embedding: &[f32],
    config: &MetabolismConfig,
) -> Result<i64> {
    // Get capsule info to create pattern summary
    let capsule = db.get_capsule_by_id(capsule_id)?
        .ok_or_else(|| anyhow::anyhow!("Capsule {} not found", capsule_id))?;

    // Use topic if available, otherwise first 100 chars of restatement
    let pattern_summary = capsule.topic
        .unwrap_or_else(|| {
            let s = &capsule.restatement;
            if s.len() > 100 {
                format!("{}...", &s[..100])
            } else {
                s.clone()
            }
        });

    // Create the pattern
    let pattern_id = db.create_pattern(&pattern_summary, config.seed_confidence)?;

    // Store pattern embedding
    db.store_pattern_embedding(pattern_id, capsule_embedding)?;

    // Link capsule to pattern
    db.link_capsule_to_pattern(capsule_id, pattern_id)?;

    Ok(pattern_id)
}

/// Apply decay to patterns that haven't been reinforced recently
pub fn decay_patterns(db: &Database, config: &MetabolismConfig) -> Result<DecayResult> {
    let now = chrono::Utc::now().timestamp();
    let grace_period_seconds = config.decay_grace_days * 24 * 60 * 60;
    let cutoff = now - grace_period_seconds;

    // Get patterns that haven't been reinforced since cutoff
    let stale_patterns = db.get_patterns_older_than(cutoff)?;

    let mut decayed = 0;
    let mut deactivated = 0;

    for (pattern_id, current_confidence) in stale_patterns {
        let new_confidence = current_confidence - config.decay_rate;

        if new_confidence < config.min_active_confidence {
            // Deactivate pattern
            db.deactivate_pattern(pattern_id)?;
            deactivated += 1;
        } else {
            // Apply decay
            db.update_pattern_confidence(pattern_id, new_confidence)?;
            decayed += 1;
        }
    }

    Ok(DecayResult { decayed, deactivated })
}

/// Result of decay operation
#[derive(Debug)]
pub struct DecayResult {
    pub decayed: usize,
    pub deactivated: usize,
}

/// Detect meta-patterns by clustering high-confidence patterns
pub fn detect_meta_patterns(
    db: &Database,
    embedding_client: &OllamaEmbedding,
    config: &MetabolismConfig,
) -> Result<Vec<MetaPattern>> {
    // Get strong patterns
    let strong_patterns = db.get_active_patterns(config.meta_pattern_threshold, 100)?;

    if strong_patterns.len() < 2 {
        return Ok(Vec::new());
    }

    // Get their embeddings
    let pattern_embeddings = db.get_pattern_embeddings()?;
    let pattern_embedding_map: std::collections::HashMap<i64, Vec<f32>> =
        pattern_embeddings.into_iter().collect();

    // Simple clustering: find patterns that are similar to each other
    let mut clusters: Vec<Vec<(i64, String, f64)>> = Vec::new();
    let mut assigned: std::collections::HashSet<i64> = std::collections::HashSet::new();

    for (pattern_id, summary, _count, confidence) in &strong_patterns {
        if assigned.contains(pattern_id) {
            continue;
        }

        let Some(embedding) = pattern_embedding_map.get(pattern_id) else {
            continue;
        };

        // Find other patterns similar to this one
        let mut cluster = vec![(*pattern_id, summary.clone(), *confidence)];
        assigned.insert(*pattern_id);

        for (other_id, other_summary, _other_count, other_confidence) in &strong_patterns {
            if assigned.contains(other_id) {
                continue;
            }

            if let Some(other_embedding) = pattern_embedding_map.get(other_id) {
                let sim = cosine_similarity(embedding, other_embedding);
                if sim >= config.similarity_threshold {
                    cluster.push((*other_id, other_summary.clone(), *other_confidence));
                    assigned.insert(*other_id);
                }
            }
        }

        if cluster.len() >= 2 {
            clusters.push(cluster);
        }
    }

    // Convert clusters to meta-patterns
    let mut meta_patterns = Vec::new();
    for cluster in clusters {
        let pattern_ids: Vec<i64> = cluster.iter().map(|(id, _, _)| *id).collect();
        let summaries: Vec<&str> = cluster.iter().map(|(_, s, _)| s.as_str()).collect();
        let avg_confidence: f64 = cluster.iter().map(|(_, _, c)| c).sum::<f64>() / cluster.len() as f64;

        // Create a combined summary (could use LLM for better summarization)
        let meta_summary = format!(
            "Meta-pattern ({} patterns): {}",
            cluster.len(),
            summaries.join(" | ")
        );

        meta_patterns.push(MetaPattern {
            pattern_ids,
            summary: meta_summary,
            confidence: avg_confidence,
        });
    }

    Ok(meta_patterns)
}

/// A meta-pattern formed from clustering related patterns
#[derive(Debug)]
pub struct MetaPattern {
    pub pattern_ids: Vec<i64>,
    pub summary: String,
    pub confidence: f64,
}

/// Metabolize all unprocessed capsules (batch operation)
pub fn metabolize_all_new(
    db: &Database,
    embedding_client: &OllamaEmbedding,
    config: &MetabolismConfig,
) -> Result<BatchMetabolismResult> {
    // Get capsules that haven't been metabolized yet
    let unprocessed = db.get_unmetabolized_capsules()?;

    let mut results = BatchMetabolismResult {
        processed: 0,
        patterns_reinforced: 0,
        patterns_seeded: 0,
        errors: 0,
    };

    for (capsule_id, restatement) in unprocessed {
        // Get or generate embedding
        let embedding = match db.get_capsule_embedding(capsule_id)? {
            Some(emb) => emb,
            None => {
                // Generate embedding if missing
                match embedding_client.embed(&restatement) {
                    Ok(emb) => {
                        db.store_capsule_embedding(capsule_id, &emb, embedding_client.model_name())?;
                        emb
                    }
                    Err(e) => {
                        eprintln!("Failed to generate embedding for capsule {}: {}", capsule_id, e);
                        results.errors += 1;
                        continue;
                    }
                }
            }
        };

        match metabolize_capsule(db, capsule_id, &embedding, config) {
            Ok(result) => {
                results.processed += 1;
                results.patterns_reinforced += result.patterns_reinforced.len();
                if result.pattern_seeded.is_some() {
                    results.patterns_seeded += 1;
                }

                // Mark capsule as metabolized
                db.mark_capsule_metabolized(capsule_id)?;
            }
            Err(e) => {
                eprintln!("Failed to metabolize capsule {}: {}", capsule_id, e);
                results.errors += 1;
            }
        }
    }

    Ok(results)
}

/// Result of batch metabolism operation
#[derive(Debug)]
pub struct BatchMetabolismResult {
    pub processed: usize,
    pub patterns_reinforced: usize,
    pub patterns_seeded: usize,
    pub errors: usize,
}

/// Capsule info for seeding patterns
pub struct CapsuleInfo {
    pub id: i64,
    pub restatement: String,
    pub topic: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = MetabolismConfig::default();
        assert!(config.similarity_threshold > 0.0);
        assert!(config.similarity_threshold < 1.0);
        assert!(config.seed_confidence < config.meta_pattern_threshold);
    }

    #[test]
    fn test_decay_result() {
        let result = DecayResult {
            decayed: 5,
            deactivated: 2,
        };
        assert_eq!(result.decayed, 5);
        assert_eq!(result.deactivated, 2);
    }
}
