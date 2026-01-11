use anyhow::{Context, Result};
use chrono::{DateTime, Duration, Utc};
use std::collections::HashMap;

use crate::extraction::Classification;

/// A weekly digest of extractions
#[derive(Debug, Clone)]
pub struct WeeklyDigest {
    pub week_start: DateTime<Utc>,
    pub week_end: DateTime<Utc>,
    pub entries_by_theme: HashMap<String, Vec<DigestEntry>>,
    pub total_entries: usize,
}

/// An entry in a weekly digest
#[derive(Debug, Clone)]
pub struct DigestEntry {
    pub extraction_id: i64,
    pub conversation_id: String,
    pub title: String,
    pub summary: String,
    pub classification: Classification,
    pub created_at: DateTime<Utc>,
    pub quotes: Vec<String>,
}

/// A theme thread showing evolution over time
#[derive(Debug, Clone)]
pub struct ThemeThread {
    pub theme: String,
    pub entries: Vec<ThreadEntry>,
    pub date_range: (DateTime<Utc>, DateTime<Utc>),
}

/// An entry in a theme thread
#[derive(Debug, Clone)]
pub struct ThreadEntry {
    pub extraction_id: i64,
    pub title: String,
    pub summary: String,
    pub created_at: DateTime<Utc>,
    pub quotes: Vec<String>,
}

/// Prediction registry entry
#[derive(Debug, Clone)]
pub struct Prediction {
    pub id: i64,
    pub extraction_id: i64,
    pub claim: String,
    pub date_made: DateTime<Utc>,
    pub timeline: Option<String>,
    pub status: PredictionStatus,
    pub validation_date: Option<DateTime<Utc>>,
    pub notes: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum PredictionStatus {
    Pending,
    Validated,
    Invalidated,
}

impl std::fmt::Display for PredictionStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PredictionStatus::Pending => write!(f, "pending"),
            PredictionStatus::Validated => write!(f, "validated"),
            PredictionStatus::Invalidated => write!(f, "invalidated"),
        }
    }
}

impl WeeklyDigest {
    /// Generate markdown output for the weekly digest
    pub fn to_markdown(&self) -> String {
        let mut md = String::new();

        // Header
        md.push_str(&format!(
            "# Weekly Digest: {} to {}\n\n",
            self.week_start.format("%Y-%m-%d"),
            self.week_end.format("%Y-%m-%d")
        ));

        md.push_str(&format!("Total entries: {}\n\n", self.total_entries));

        if self.total_entries == 0 {
            md.push_str("No entries for this week.\n");
            return md;
        }

        // Group by theme
        let mut themes: Vec<_> = self.entries_by_theme.keys().collect();
        themes.sort();

        for theme in themes {
            let entries = &self.entries_by_theme[theme];
            md.push_str(&format!("## {}\n\n", theme));

            for entry in entries {
                md.push_str(&format!("### {}\n\n", entry.title));
                md.push_str(&format!("*{}* | {}\n\n", entry.classification, entry.created_at.format("%Y-%m-%d")));
                md.push_str(&format!("{}\n\n", entry.summary));

                if !entry.quotes.is_empty() {
                    md.push_str("**Key quotes:**\n\n");
                    for quote in &entry.quotes {
                        md.push_str(&format!("> {}\n\n", quote));
                    }
                }

                md.push_str("---\n\n");
            }
        }

        md
    }
}

impl ThemeThread {
    /// Generate markdown output for the theme thread
    pub fn to_markdown(&self) -> String {
        let mut md = String::new();

        // Header
        md.push_str(&format!("# Theme: {}\n\n", self.theme));
        md.push_str(&format!(
            "Evolution from {} to {}\n\n",
            self.date_range.0.format("%Y-%m-%d"),
            self.date_range.1.format("%Y-%m-%d")
        ));

        md.push_str(&format!("Total entries: {}\n\n", self.entries.len()));
        md.push_str("---\n\n");

        // Chronological entries
        for entry in &self.entries {
            md.push_str(&format!("## {} ({})\n\n", entry.title, entry.created_at.format("%Y-%m-%d")));
            md.push_str(&format!("{}\n\n", entry.summary));

            if !entry.quotes.is_empty() {
                md.push_str("**Key quotes:**\n\n");
                for quote in &entry.quotes {
                    md.push_str(&format!("> {}\n\n", quote));
                }
            }

            md.push_str("---\n\n");
        }

        md
    }
}

/// Prediction registry
#[derive(Debug, Clone)]
pub struct PredictionRegistry {
    pub predictions: Vec<Prediction>,
}

impl PredictionRegistry {
    /// Generate markdown output for the prediction registry
    pub fn to_markdown(&self) -> String {
        let mut md = String::new();

        md.push_str("# Prediction Registry\n\n");
        md.push_str(&format!("Total predictions: {}\n\n", self.predictions.len()));

        // Group by status
        let pending: Vec<_> = self.predictions.iter().filter(|p| p.status == PredictionStatus::Pending).collect();
        let validated: Vec<_> = self.predictions.iter().filter(|p| p.status == PredictionStatus::Validated).collect();
        let invalidated: Vec<_> = self.predictions.iter().filter(|p| p.status == PredictionStatus::Invalidated).collect();

        md.push_str(&format!("- Pending: {}\n", pending.len()));
        md.push_str(&format!("- Validated: {}\n", validated.len()));
        md.push_str(&format!("- Invalidated: {}\n\n", invalidated.len()));

        // Pending predictions
        if !pending.is_empty() {
            md.push_str("## Pending Predictions\n\n");
            for pred in pending {
                md.push_str(&format!("### {}\n\n", pred.claim));
                md.push_str(&format!("**Made:** {}\n\n", pred.date_made.format("%Y-%m-%d")));
                if let Some(timeline) = &pred.timeline {
                    md.push_str(&format!("**Timeline:** {}\n\n", timeline));
                }
                if let Some(notes) = &pred.notes {
                    md.push_str(&format!("**Notes:** {}\n\n", notes));
                }
                md.push_str("---\n\n");
            }
        }

        // Validated predictions
        if !validated.is_empty() {
            md.push_str("## Validated Predictions\n\n");
            for pred in validated {
                md.push_str(&format!("### ✓ {}\n\n", pred.claim));
                md.push_str(&format!("**Made:** {}\n\n", pred.date_made.format("%Y-%m-%d")));
                if let Some(validation_date) = pred.validation_date {
                    md.push_str(&format!("**Validated:** {}\n\n", validation_date.format("%Y-%m-%d")));
                }
                if let Some(notes) = &pred.notes {
                    md.push_str(&format!("**Notes:** {}\n\n", notes));
                }
                md.push_str("---\n\n");
            }
        }

        // Invalidated predictions
        if !invalidated.is_empty() {
            md.push_str("## Invalidated Predictions\n\n");
            for pred in invalidated {
                md.push_str(&format!("### ✗ {}\n\n", pred.claim));
                md.push_str(&format!("**Made:** {}\n\n", pred.date_made.format("%Y-%m-%d")));
                if let Some(validation_date) = pred.validation_date {
                    md.push_str(&format!("**Invalidated:** {}\n\n", validation_date.format("%Y-%m-%d")));
                }
                if let Some(notes) = &pred.notes {
                    md.push_str(&format!("**Notes:** {}\n\n", notes));
                }
                md.push_str("---\n\n");
            }
        }

        md
    }
}

/// Generate a weekly digest for the past 7 days
pub fn generate_weekly_digest(entries: Vec<DigestEntry>) -> WeeklyDigest {
    let now = Utc::now();
    let week_start = now - Duration::days(7);

    let mut entries_by_theme: HashMap<String, Vec<DigestEntry>> = HashMap::new();

    // This function receives already-themed entries, so we need to get themes from database
    // For now, we'll use a placeholder theme grouping
    entries_by_theme.insert("General".to_string(), entries.clone());

    WeeklyDigest {
        week_start,
        week_end: now,
        entries_by_theme,
        total_entries: entries.len(),
    }
}

/// Generate a theme thread from entries
pub fn generate_theme_thread(theme: String, entries: Vec<ThreadEntry>) -> ThemeThread {
    let date_range = if entries.is_empty() {
        (Utc::now(), Utc::now())
    } else {
        let start = entries.iter().map(|e| e.created_at).min().unwrap();
        let end = entries.iter().map(|e| e.created_at).max().unwrap();
        (start, end)
    };

    ThemeThread {
        theme,
        entries,
        date_range,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_digest_entry() -> DigestEntry {
        DigestEntry {
            extraction_id: 1,
            conversation_id: "test-conv".to_string(),
            title: "Test Entry".to_string(),
            summary: "This is a test summary.".to_string(),
            classification: Classification::Insight,
            created_at: Utc::now(),
            quotes: vec!["Test quote".to_string()],
        }
    }

    #[test]
    fn test_weekly_digest_creation() {
        let entries = vec![create_test_digest_entry()];
        let digest = generate_weekly_digest(entries);

        assert_eq!(digest.total_entries, 1);
        assert!(digest.week_start < digest.week_end);
    }

    #[test]
    fn test_weekly_digest_markdown() {
        let entries = vec![create_test_digest_entry()];
        let digest = generate_weekly_digest(entries);
        let markdown = digest.to_markdown();

        assert!(markdown.contains("Weekly Digest"));
        assert!(markdown.contains("Test Entry"));
        assert!(markdown.contains("This is a test summary"));
    }

    #[test]
    fn test_theme_thread_creation() {
        let entries = vec![ThreadEntry {
            extraction_id: 1,
            title: "Thread Entry".to_string(),
            summary: "Thread summary".to_string(),
            created_at: Utc::now(),
            quotes: vec![],
        }];

        let thread = generate_theme_thread("test-theme".to_string(), entries);

        assert_eq!(thread.theme, "test-theme");
        assert_eq!(thread.entries.len(), 1);
    }

    #[test]
    fn test_theme_thread_markdown() {
        let entries = vec![ThreadEntry {
            extraction_id: 1,
            title: "Thread Entry".to_string(),
            summary: "Thread summary".to_string(),
            created_at: Utc::now(),
            quotes: vec!["Key insight".to_string()],
        }];

        let thread = generate_theme_thread("lattice".to_string(), entries);
        let markdown = thread.to_markdown();

        assert!(markdown.contains("Theme: lattice"));
        assert!(markdown.contains("Thread Entry"));
        assert!(markdown.contains("Key insight"));
    }

    #[test]
    fn test_prediction_registry_markdown() {
        let predictions = vec![
            Prediction {
                id: 1,
                extraction_id: 1,
                claim: "Test prediction".to_string(),
                date_made: Utc::now(),
                timeline: Some("Q1 2025".to_string()),
                status: PredictionStatus::Pending,
                validation_date: None,
                notes: Some("Testing notes".to_string()),
            },
        ];

        let registry = PredictionRegistry { predictions };
        let markdown = registry.to_markdown();

        assert!(markdown.contains("Prediction Registry"));
        assert!(markdown.contains("Test prediction"));
        assert!(markdown.contains("Pending: 1"));
        assert!(markdown.contains("Q1 2025"));
    }

    #[test]
    fn test_empty_weekly_digest() {
        let digest = generate_weekly_digest(vec![]);
        let markdown = digest.to_markdown();

        assert_eq!(digest.total_entries, 0);
        assert!(markdown.contains("No entries for this week"));
    }
}
