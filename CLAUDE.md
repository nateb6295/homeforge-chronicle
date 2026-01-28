# Homeforge Chronicle

Automated life-archive system that transforms Claude conversation exports into a structured, timestamped public record hosted on ICP.

## Workflow

1. **Ingest**: `./target/release/chronicle ingest-bulk ~/claude-exports/conversations.json`
2. **Extract**: Ask Claude to extract (uses Max plan, not separate API billing)
3. **Build**: `./target/release/chronicle build`
4. **Deploy**: User runs `chronicle deploy` manually (requires dfx passphrase)

## Key Paths

- Watch directory: `~/claude-exports/`
- Database: `~/.homeforge-chronicle/processed.db`
- Build output: `~/.homeforge-chronicle/build/`
- Canister ID: Set via `CHRONICLE_CANISTER_ID` env var

## Extraction Format

When extracting conversations, generate:
- **title**: <10 words
- **summary**: 2-3 sentences, preserve original voice
- **themes**: customizable theme list
- **classification**: prediction | insight | milestone | reflection | technical
- **key_quotes**: verbatim excerpts capturing essential thinking
- **confidence_score**: 0.0-1.0 (how substantive vs casual)

## Database Insert

```sql
-- Insert extraction
INSERT INTO extractions (conversation_id, title, summary, classification, confidence_score, created_at)
VALUES (?, ?, ?, ?, ?, unixepoch());

-- Link themes (get_or_create theme first)
INSERT INTO extraction_themes (extraction_id, theme_id) VALUES (?, ?);

-- Insert quotes
INSERT INTO quotes (extraction_id, content) VALUES (?, ?);
```

## Deploy Command

```bash
DFX_WARNING=-mainnet_plaintext_identity dfx deploy --network ic --identity <your-identity>
```

Your dfx identity needs controller + asset permissions on the canister.

## Environment Variables

- `CHRONICLE_CANISTER_ID` - Your ICP canister ID
- `CHRONICLE_IDENTITY` - dfx identity name
- `CHRONICLE_OLLAMA_URL` - Ollama server for embeddings (default: http://localhost:11434)
- `CHRONICLE_EMBEDDING_MODEL` - Embedding model (default: mxbai-embed-large)
- `ANTHROPIC_API_KEY` - For the Mind cognitive loop
