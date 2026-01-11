# SPEC.md - Homeforge Chronicle

## Overview

An automated life-archive system that transforms Claude conversation exports into a structured, timestamped public record hosted on the Internet Computer Protocol. The system prioritizes provenance, minimal maintenance, and raw authenticity over polish.

## Design Philosophy

- **Exhaust, not effort**: The user generates raw material through normal AI conversations. The system processes this exhaust into structured archive without requiring dedicated "writing time."
- **Timestamped provenance**: Every entry carries verifiable timestamps. The archive serves as proof-of-thinking if predictions or pattern recognition prove prescient.
- **Raw over polished**: Preserve the texture of thinking-in-progress. Do not over-summarize or flatten into corporate blog voice.
- **Zero daily maintenance**: After initial setup, the system runs on scheduled triggers. User involvement limited to periodic conversation exports.

## Core Components

### 1. Ingestion Layer

**Input**: Claude conversation exports (JSON format from claude.ai export)

**Processing**:
- Parse conversation structure (human/assistant turns, timestamps)
- Extract metadata (conversation ID, date range, approximate token count)
- Deduplicate against previously processed conversations

**Storage**: Local SQLite database for processing state and deduplication

### 2. Extraction Engine

**Theme Detection**:
- Identify recurring topics across conversations (e.g., "lattice", "homeforge", "ICP", "XRP", "AI agency")
- Tag conversations with primary and secondary themes
- Track theme evolution over time

**Content Classification**:
- `prediction`: Explicit claims about future events with implied or explicit timeline
- `insight`: Novel connection or pattern recognition
- `milestone`: Project completion, infrastructure change, significant decision
- `reflection`: Personal processing, emotional content, life context
- `technical`: Build logs, configuration details, troubleshooting

**Extraction Outputs**:
- Title (generated, <10 words)
- Summary (2-3 sentences, preserves original voice)
- Key quotes (verbatim excerpts that capture essential thinking)
- Themes (array of tags)
- Classification (primary type from above)
- Timestamp (original conversation date)
- Confidence score (how substantive vs casual the conversation was)

### 3. Compilation Layer

**Weekly Digest**:
- Aggregate extractions from past 7 days
- Group by theme
- Generate narrative thread connecting entries
- Output as single markdown file

**Thread Documents**:
- Long-form compilation of single theme across time
- Auto-generated when theme reaches threshold (e.g., 10+ entries)
- Includes chronological evolution showing how thinking developed

**Prediction Registry**:
- Separate index of all predictions
- Fields: claim, date made, timeline (if specified), status (pending/validated/invalidated), validation date, notes
- Designed for retrospective scoring

### 4. Publishing Layer

**Target**: Internet Computer Protocol canister (static asset hosting)

**Structure**:
```
/
├── index.html (recent entries, navigation)
├── archive/
│   └── YYYY/
│       └── MM/
│           └── DD-[slug].html
├── threads/
│   └── [theme-slug].html
├── predictions/
│   └── index.html
├── about.html (static, manually authored)
└── feed.xml (RSS for those who want it)
```

**Styling**:
- Minimal CSS, fast loading
- Dark mode default
- Monospace or near-monospace typography
- No JavaScript required for reading (progressive enhancement OK)

**Deployment**:
- Use `dfx` CLI for ICP deployment
- Single command deploy from compiled output directory

### 5. Automation Layer

**Trigger Options** (implement simplest first):
- Manual: User runs CLI command after exporting conversations
- Scheduled: Cron job / systemd timer checks export directory
- Future: API integration if Anthropic exposes conversation access

**Pipeline**:
```
[Export dropped in watched directory]
    ↓
[Ingestion: parse, dedupe, store]
    ↓
[Extraction: theme, classify, summarize]
    ↓
[Compilation: weekly digest, thread updates]
    ↓
[Build: generate static HTML]
    ↓
[Deploy: push to ICP canister]
    ↓
[Notify: optional webhook/email confirmation]
```

## Technical Requirements

### Language
Rust (for ICP compatibility and performance)

### Dependencies (suggested, not mandated)
- `serde` / `serde_json`: Conversation export parsing
- `rusqlite`: Local state management
- `reqwest`: HTTP if needed for future integrations
- `tera` or `askama`: HTML templating
- `dfx` SDK: ICP deployment
- LLM integration: API calls to Claude for extraction/summarization (or local model if preferred)

### LLM Integration for Extraction

The extraction engine requires LLM capabilities for:
- Theme detection
- Summarization
- Title generation
- Classification

**Options**:
1. **Claude API**: Most consistent with source material, requires API key and usage costs
2. **Local model via Ollama**: Runs on homeforge hardware (Jetson, etc.), no ongoing costs, potentially lower quality
3. **Hybrid**: Local for simple classification, API for summarization

Implement as swappable backend. Start with Claude API for quality baseline.

### Configuration

```toml
# config.toml

[input]
watch_directory = "~/claude-exports"
processed_db = "~/.homeforge-chronicle/processed.db"

[extraction]
llm_backend = "claude-api"  # or "ollama"
llm_model = "claude-sonnet-4-20250514"  # or local model name
themes = ["lattice", "homeforge", "icp", "xrp", "ai-agency", "construction", "family"]

[output]
build_directory = "~/.homeforge-chronicle/build"
site_title = "Homeforge Chronicle"
author = "Nate"

[deployment]
canister_id = "xxxxx-xxxxx-xxxxx-xxxxx-cai"
network = "ic"  # or "local" for testing

[schedule]
auto_deploy = true
notify_webhook = ""  # optional
```

## Development Phases

### Phase 1: Foundation
- [ ] Project structure, Cargo.toml, basic CLI skeleton
- [ ] Configuration parsing
- [ ] SQLite schema for processed conversations and extractions

### Phase 2: Ingestion
- [ ] Claude export JSON parser
- [ ] Conversation deduplication logic
- [ ] Basic CLI command: `chronicle ingest <file>`

### Phase 3: Extraction (LLM Integration)
- [ ] Claude API client (or Ollama client)
- [ ] Prompt engineering for theme detection
- [ ] Prompt engineering for summarization
- [ ] Prompt engineering for classification
- [ ] Store extractions in SQLite
- [ ] CLI command: `chronicle extract`

### Phase 4: Compilation
- [ ] Weekly digest generator
- [ ] Theme thread compiler
- [ ] Prediction registry builder
- [ ] CLI command: `chronicle compile`

### Phase 5: Static Site Generation
- [ ] HTML templates (index, archive, thread, prediction, about)
- [ ] Minimal CSS
- [ ] RSS feed generation
- [ ] CLI command: `chronicle build`

### Phase 6: ICP Deployment
- [ ] dfx integration
- [ ] Asset canister deployment
- [ ] CLI command: `chronicle deploy`

### Phase 7: Automation
- [ ] File watcher for export directory
- [ ] Full pipeline orchestration
- [ ] Optional notification on deploy
- [ ] CLI command: `chronicle watch` (daemon mode)

### Phase 8: Refinement
- [ ] Theme evolution visualization
- [ ] Prediction tracking and scoring
- [ ] Search functionality (static, client-side OK)
- [ ] Performance optimization

## Testing Strategy

- Unit tests for parsing, deduplication, compilation logic
- Integration tests for LLM extraction (mock responses for CI, real API for local validation)
- End-to-end test: sample conversation export → deployed site
- Manual review of extraction quality for first N conversations

## Success Criteria

1. User exports Claude conversations to a directory
2. System processes new conversations without manual intervention
3. Public archive updates with new entries within 24 hours of export
4. Archive is readable, navigable, and loads fast
5. Predictions are trackable over time
6. Total ongoing effort for user: <5 minutes per week (just the export action)

## Future Considerations (Out of Scope for V1)

- Direct Anthropic API integration (when/if available)
- Voice note ingestion
- Homeforge system logs integration
- Multi-user support
- ActivityPub integration for federation
- AI-generated "annual review" documents

## Notes for Implementation

- Preserve original conversation timestamps, not processing timestamps
- When in doubt, keep more context rather than over-summarizing
- The archive should feel like reading someone's thinking, not a corporate blog
- Errors should be logged but not block the pipeline—partial updates are acceptable
- The user has Jetson Orin Nano, Raspberry Pi5, iMac, HP Elitebook available for local processing if needed
