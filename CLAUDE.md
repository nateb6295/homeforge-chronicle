# Homeforge Chronicle

Automated life-archive system that transforms Claude conversation exports into a structured, timestamped public record hosted on ICP.

## Workflow

1. **Ingest**: `./target/release/chronicle ingest-bulk /home/bradf/claude-exports/conversations.json`
2. **Extract**: Ask Claude to extract (uses Max plan, not separate API billing)
3. **Build**: `./target/release/chronicle build`
4. **Deploy**: User runs `chronicle deploy` manually (requires dfx passphrase)

## Key Paths

- Watch directory: `/home/bradf/claude-exports/`
- Database: `/home/bradf/.homeforge-chronicle/processed.db`
- Build output: `/home/bradf/.homeforge-chronicle/build/`
- Canister ID: `nbt4b-giaaa-aaaai-q33lq-cai`

## Extraction Format

When extracting conversations, generate:
- **title**: <10 words
- **summary**: 2-3 sentences, preserve original voice
- **themes**: from [lattice, homeforge, icp, xrp, ai-agency, construction, family] or new relevant ones
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
DFX_WARNING=-mainnet_plaintext_identity dfx deploy --network ic --identity chronicle-auto
```

Identity `chronicle-auto` (principal: `kalce-s3e7q-ob55s-ttoe7-z2x5y-x3tof-onliz-2gaad-zsh3w-etvve-rqe`) has controller + asset permissions.
