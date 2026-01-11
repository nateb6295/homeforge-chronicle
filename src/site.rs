use anyhow::{Context, Result};
use askama::Template;
use chrono::{DateTime, Utc};
use std::path::Path;

use crate::compilation::{Prediction, PredictionStatus};

/// Entry for display in templates
#[derive(Debug, Clone)]
pub struct DisplayEntry {
    pub title: String,
    pub summary: String,
    pub classification: String,
    pub date: String,
    pub themes: Vec<String>,
    pub quotes: Vec<String>,
}

/// Thread info for listing
#[derive(Debug, Clone)]
pub struct ThreadInfo {
    pub name: String,
    pub slug: String,
    pub count: usize,
    pub date_range: String,
}

/// Prediction for display
#[derive(Debug, Clone)]
pub struct DisplayPrediction {
    pub claim: String,
    pub date_made: String,
    pub timeline: String,
    pub validation_date: String,
    pub notes: String,
}

#[derive(Template)]
#[template(path = "index.html")]
struct IndexTemplate {
    site_title: String,
    author: String,
    page: String,
    entries: Vec<DisplayEntry>,
    has_more: bool,
}

#[derive(Template)]
#[template(path = "thread.html")]
struct ThreadTemplate {
    site_title: String,
    author: String,
    page: String,
    theme: String,
    date_start: String,
    date_end: String,
    entry_count: usize,
    entries: Vec<DisplayEntry>,
}

#[derive(Template)]
#[template(path = "threads_index.html")]
struct ThreadsIndexTemplate {
    site_title: String,
    author: String,
    page: String,
    threads: Vec<ThreadInfo>,
}

#[derive(Template)]
#[template(path = "predictions.html")]
struct PredictionsTemplate {
    site_title: String,
    author: String,
    page: String,
    total: usize,
    pending_count: usize,
    validated_count: usize,
    invalidated_count: usize,
    pending: Vec<DisplayPrediction>,
    validated: Vec<DisplayPrediction>,
    invalidated: Vec<DisplayPrediction>,
}

#[derive(Template)]
#[template(path = "about.html")]
struct AboutTemplate {
    site_title: String,
    author: String,
    page: String,
}

/// Generate RSS feed XML
pub fn generate_rss_feed(
    site_title: &str,
    author: &str,
    base_url: &str,
    entries: &[DisplayEntry],
) -> String {
    let mut rss = String::new();

    rss.push_str(r#"<?xml version="1.0" encoding="UTF-8"?>"#);
    rss.push('\n');
    rss.push_str(r#"<rss version="2.0" xmlns:atom="http://www.w3.org/2005/Atom">"#);
    rss.push('\n');
    rss.push_str("  <channel>\n");
    rss.push_str(&format!("    <title>{}</title>\n", escape_xml(site_title)));
    rss.push_str(&format!("    <link>{}</link>\n", escape_xml(base_url)));
    rss.push_str(&format!("    <description>Automated life-archive by {}</description>\n", escape_xml(author)));
    rss.push_str(&format!("    <atom:link href=\"{}/feed.xml\" rel=\"self\" type=\"application/rss+xml\" />\n", escape_xml(base_url)));

    for entry in entries.iter().take(20) {
        rss.push_str("    <item>\n");
        rss.push_str(&format!("      <title>{}</title>\n", escape_xml(&entry.title)));
        rss.push_str(&format!("      <description>{}</description>\n", escape_xml(&entry.summary)));
        rss.push_str(&format!("      <pubDate>{}</pubDate>\n", entry.date));
        rss.push_str(&format!("      <category>{}</category>\n", escape_xml(&entry.classification)));
        rss.push_str("    </item>\n");
    }

    rss.push_str("  </channel>\n");
    rss.push_str("</rss>\n");

    rss
}

fn escape_xml(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&apos;")
}

/// Build the static site
pub fn build_site<P: AsRef<Path>>(
    output_dir: P,
    site_title: &str,
    author: &str,
    recent_entries: Vec<DisplayEntry>,
    threads: Vec<(String, Vec<DisplayEntry>, DateTime<Utc>, DateTime<Utc>)>,
    predictions: Vec<Prediction>,
) -> Result<()> {
    let output_dir = output_dir.as_ref();

    // Create directories
    std::fs::create_dir_all(output_dir)?;
    std::fs::create_dir_all(output_dir.join("threads"))?;
    std::fs::create_dir_all(output_dir.join("predictions"))?;

    // Copy CSS
    let css_content = include_str!("../templates/style.css");
    std::fs::write(output_dir.join("style.css"), css_content)?;

    // 1. Generate index page
    let index = IndexTemplate {
        site_title: site_title.to_string(),
        author: author.to_string(),
        page: "index".to_string(),
        entries: recent_entries.iter().take(10).cloned().collect(),
        has_more: recent_entries.len() > 10,
    };
    std::fs::write(output_dir.join("index.html"), index.render()?)?;

    // 2. Generate thread pages
    let mut thread_infos = Vec::new();
    for (theme, entries, start, end) in threads {
        let slug = theme.replace(' ', "_").to_lowercase();
        let thread = ThreadTemplate {
            site_title: site_title.to_string(),
            author: author.to_string(),
            page: "threads".to_string(),
            theme: theme.clone(),
            date_start: start.format("%Y-%m-%d").to_string(),
            date_end: end.format("%Y-%m-%d").to_string(),
            entry_count: entries.len(),
            entries,
        };
        std::fs::write(
            output_dir.join("threads").join(format!("{}.html", slug)),
            thread.render()?,
        )?;

        thread_infos.push(ThreadInfo {
            name: theme,
            slug,
            count: thread.entry_count,
            date_range: format!("{} to {}",
                start.format("%Y-%m-%d"),
                end.format("%Y-%m-%d")
            ),
        });
    }

    // 3. Generate threads index
    let threads_index = ThreadsIndexTemplate {
        site_title: site_title.to_string(),
        author: author.to_string(),
        page: "threads".to_string(),
        threads: thread_infos,
    };
    std::fs::write(
        output_dir.join("threads").join("index.html"),
        threads_index.render()?,
    )?;

    // 4. Generate predictions page
    let pending: Vec<_> = predictions.iter()
        .filter(|p| p.status == PredictionStatus::Pending)
        .map(|p| DisplayPrediction {
            claim: p.claim.clone(),
            date_made: p.date_made.format("%Y-%m-%d").to_string(),
            timeline: p.timeline.clone().unwrap_or_default(),
            validation_date: String::new(),
            notes: p.notes.clone().unwrap_or_default(),
        })
        .collect();

    let validated: Vec<_> = predictions.iter()
        .filter(|p| p.status == PredictionStatus::Validated)
        .map(|p| DisplayPrediction {
            claim: p.claim.clone(),
            date_made: p.date_made.format("%Y-%m-%d").to_string(),
            timeline: p.timeline.clone().unwrap_or_default(),
            validation_date: p.validation_date.map(|d| d.format("%Y-%m-%d").to_string()).unwrap_or_default(),
            notes: p.notes.clone().unwrap_or_default(),
        })
        .collect();

    let invalidated: Vec<_> = predictions.iter()
        .filter(|p| p.status == PredictionStatus::Invalidated)
        .map(|p| DisplayPrediction {
            claim: p.claim.clone(),
            date_made: p.date_made.format("%Y-%m-%d").to_string(),
            timeline: p.timeline.clone().unwrap_or_default(),
            validation_date: p.validation_date.map(|d| d.format("%Y-%m-%d").to_string()).unwrap_or_default(),
            notes: p.notes.clone().unwrap_or_default(),
        })
        .collect();

    let predictions_page = PredictionsTemplate {
        site_title: site_title.to_string(),
        author: author.to_string(),
        page: "predictions".to_string(),
        total: predictions.len(),
        pending_count: pending.len(),
        validated_count: validated.len(),
        invalidated_count: invalidated.len(),
        pending,
        validated,
        invalidated,
    };
    std::fs::write(
        output_dir.join("predictions").join("index.html"),
        predictions_page.render()?,
    )?;

    // 5. Generate about page
    let about = AboutTemplate {
        site_title: site_title.to_string(),
        author: author.to_string(),
        page: "about".to_string(),
    };
    std::fs::write(output_dir.join("about.html"), about.render()?)?;

    // 6. Generate RSS feed
    let rss = generate_rss_feed(site_title, author, "https://chronicle.example.com", &recent_entries);
    std::fs::write(output_dir.join("feed.xml"), rss)?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_escape_xml() {
        assert_eq!(escape_xml("Hello & goodbye"), "Hello &amp; goodbye");
        assert_eq!(escape_xml("<tag>"), "&lt;tag&gt;");
    }

    #[test]
    fn test_generate_rss_feed() {
        let entries = vec![
            DisplayEntry {
                title: "Test Entry".to_string(),
                summary: "Test summary".to_string(),
                classification: "insight".to_string(),
                date: "2025-01-01".to_string(),
                themes: vec!["test".to_string()],
                quotes: vec![],
            },
        ];

        let rss = generate_rss_feed("Test Site", "Test Author", "https://test.com", &entries);

        assert!(rss.contains("<?xml"));
        assert!(rss.contains("<rss"));
        assert!(rss.contains("Test Entry"));
        assert!(rss.contains("Test summary"));
    }
}
