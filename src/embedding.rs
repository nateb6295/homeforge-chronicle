//! Embedding Generation
//!
//! Generates vector embeddings for knowledge capsules using local Ollama.
//! Enables semantic search via cosine similarity.

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

/// Ollama embedding client for local inference
pub struct OllamaEmbedding {
    base_url: String,
    model: String,
    client: reqwest::blocking::Client,
}

#[derive(Debug, Serialize)]
struct EmbeddingRequest {
    model: String,
    prompt: String,
}

#[derive(Debug, Deserialize)]
struct EmbeddingResponse {
    embedding: Vec<f32>,
}

impl OllamaEmbedding {
    /// Create a new Ollama embedding client
    pub fn new(base_url: &str, model: &str) -> Result<Self> {
        let client = reqwest::blocking::Client::builder()
            .timeout(std::time::Duration::from_secs(60))
            .build()
            .context("Failed to create HTTP client")?;

        Ok(Self {
            base_url: base_url.to_string(),
            model: model.to_string(),
            client,
        })
    }

    /// Generate embedding for a single text
    pub fn embed(&self, text: &str) -> Result<Vec<f32>> {
        let request = EmbeddingRequest {
            model: self.model.clone(),
            prompt: text.to_string(),
        };

        let url = format!("{}/api/embeddings", self.base_url);

        let response = self
            .client
            .post(&url)
            .json(&request)
            .send()
            .context("Failed to send embedding request")?;

        if !response.status().is_success() {
            let status = response.status();
            let error_text = response.text().unwrap_or_default();
            anyhow::bail!(
                "Ollama embedding request failed with status {}: {}",
                status,
                error_text
            );
        }

        let embedding_response: EmbeddingResponse = response
            .json()
            .context("Failed to parse embedding response")?;

        Ok(embedding_response.embedding)
    }

    /// Generate embeddings for multiple texts (batched)
    pub fn embed_batch(&self, texts: &[String]) -> Result<Vec<Vec<f32>>> {
        let mut embeddings = Vec::with_capacity(texts.len());

        for text in texts {
            let embedding = self.embed(text)?;
            embeddings.push(embedding);
        }

        Ok(embeddings)
    }

    /// Get the model name
    pub fn model_name(&self) -> &str {
        &self.model
    }
}

/// Calculate cosine similarity between two vectors
pub fn cosine_similarity(a: &[f32], b: &[f32]) -> f32 {
    if a.len() != b.len() {
        return 0.0;
    }

    let dot_product: f32 = a.iter().zip(b.iter()).map(|(x, y)| x * y).sum();
    let norm_a: f32 = a.iter().map(|x| x * x).sum::<f32>().sqrt();
    let norm_b: f32 = b.iter().map(|x| x * x).sum::<f32>().sqrt();

    if norm_a == 0.0 || norm_b == 0.0 {
        return 0.0;
    }

    dot_product / (norm_a * norm_b)
}

/// Find top-k most similar embeddings
pub fn find_top_k_similar(
    query_embedding: &[f32],
    embeddings: &[(i64, Vec<f32>)], // (capsule_id, embedding)
    k: usize,
) -> Vec<(i64, f32)> {
    let mut similarities: Vec<(i64, f32)> = embeddings
        .iter()
        .map(|(id, emb)| (*id, cosine_similarity(query_embedding, emb)))
        .collect();

    // Sort by similarity descending
    similarities.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));

    similarities.into_iter().take(k).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cosine_similarity_identical() {
        let a = vec![1.0, 2.0, 3.0];
        let b = vec![1.0, 2.0, 3.0];
        let sim = cosine_similarity(&a, &b);
        assert!((sim - 1.0).abs() < 0.0001);
    }

    #[test]
    fn test_cosine_similarity_orthogonal() {
        let a = vec![1.0, 0.0, 0.0];
        let b = vec![0.0, 1.0, 0.0];
        let sim = cosine_similarity(&a, &b);
        assert!(sim.abs() < 0.0001);
    }

    #[test]
    fn test_cosine_similarity_opposite() {
        let a = vec![1.0, 2.0, 3.0];
        let b = vec![-1.0, -2.0, -3.0];
        let sim = cosine_similarity(&a, &b);
        assert!((sim + 1.0).abs() < 0.0001);
    }

    #[test]
    fn test_find_top_k() {
        let query = vec![1.0, 0.0, 0.0];
        let embeddings = vec![
            (1, vec![1.0, 0.0, 0.0]),    // Perfect match
            (2, vec![0.5, 0.5, 0.0]),    // Partial match
            (3, vec![0.0, 1.0, 0.0]),    // Orthogonal
            (4, vec![-1.0, 0.0, 0.0]),   // Opposite
        ];

        let top_2 = find_top_k_similar(&query, &embeddings, 2);
        assert_eq!(top_2.len(), 2);
        assert_eq!(top_2[0].0, 1); // Best match first
        assert_eq!(top_2[1].0, 2); // Second best
    }
}
