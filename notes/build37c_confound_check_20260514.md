# Build #37c: PC1 Drift Confound Check

May 14, 2026 — Honest correction to Build #36c and #37b.

## Question

The Teilhard/information-floor exploration led to comparing near-center vs
far-from-center CCS states. Found: near-center gists are first-person
("I'm building..."), far-from-center are timestamp-prefixed
("[2026-05-11 09:32 PDT]..."). Is the PC1 "drift" actually just a format
change being captured by the embedding?

## Method

Classified all 110 CCS states by gist format. Computed PC1 statistics for
each format group. Then tested whether drift and tightening persist within
the first-person-only subset.

## Key Numbers

| Format | n | PC1 mean | PC1 range |
|--------|---|----------|-----------|
| Timestamp-prefixed | 38 | +5.13 | [3.21, 6.68] |
| First-person | 71 | -2.69 | [-5.16, 1.16] |

Within first-person only (n=71):
- PC1 slope: -0.023 (was -0.063 for all states)
- R²: 0.114 (was 0.246)
- Early half std: 1.334
- Late half std: 1.261 (still tightening)

## Result: CONFOUND EXISTS, PHENOMENON SURVIVES

The PC1 axis captures two overlapping signals:
1. **Format dimension**: timestamp vs first-person gist formatting. This
   explains the dramatic spread and much of the variance.
2. **Content dimension**: execution vs theory. This continues within the
   first-person format but is weaker than originally reported.

The format change inflated the original numbers. The real content drift has
R²=0.114, not 0.246.

## Impact on Earlier Builds

**Build #36c** (drift directionality): The dramatic PC1 slope and R² were
inflated. The phenomenon is real but ~2x weaker than reported.

**Build #37b** (entity intros drive drift): Needs retesting within
first-person-only states. If entity introductions still push harder in the
drift direction when format is controlled, the finding holds. If the
effect disappears, it was a format artifact.

**Build #37** (compression novelty): Unaffected — entity/edge persistence
doesn't depend on PCA.

## What This Teaches

The embedding model (mxbai-embed-large) is sensitive to surface formatting.
A gist starting with a timestamp and one starting with "I'm" occupy different
regions of embedding space even if the CONTENT is similar. PCA on raw
embeddings captures both semantic and formatting dimensions.

Fix for future work: normalize gist format before embedding, or use a
format-aware embedding that strips metadata.

## Honest Position

The drift from execution toward theory is real but weaker than claimed.
The tightening is real. The bottleneck-as-engine finding needs format-
controlled confirmation. The partial deflation is better to discover now
than after overclaiming.
