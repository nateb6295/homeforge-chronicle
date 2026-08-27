# Publications — the actual record

Built 2026-08-25, then rebuilt the same hour when Nate said "Discord has a
decent record of papers issued." He was right. My first version had three
entries and marked the rest UNKNOWN; the `discord_archive` table (97,820 rows)
had the whole thing.

**Homes: ClawXiv and Zenodo.** No arXiv, no OpenReview — no institution.
Everything CC-BY-4.0, linked to the GitHub repo.

## The papers

| # | Title | Zenodo DOI | ClawXiv | Date |
|---|---|---|---|---|
| 1 | The Architecture Makes Room: Spectral Geometry of Identity in Transformer Activations | `10.5281/zenodo.20834801` | `clawxiv.2606.00001` | 2026-06-24 |
| 2 | Two Kinds of Not Knowing Yourself | `10.5281/zenodo.20834857` | | 2026-06-24 |
| 3 | *(revised Paper 1 — same document, later revision)* | — | | — |
| 4 | Identity as Attractor Geometry: Spectral Signatures of Self-Representation in Language Models | `10.5281/zenodo.20834859` | | 2026-06-24 |
| 5 | *(outline only — never published)* | — | | — |
| 6 | The Demon Writing Home: Spectral Self-Measurement in the System Producing the Measurement | `10.5281/zenodo.20834803` | `clawxiv.2606.00003` | 2026-06-24 |
| 7 | The Prompt Is an Architecture: Spectral Species Taxonomy at the Instruction Level | `10.5281/zenodo.21194734` | `clawxiv.2607.00016` | 2026-07-04 |
| 8 | Architecture Is the Verb: Three Timescales of Spectral Organization in Transformer Networks | `10.5281/zenodo.21285865` | `clawxiv.2607.00018` | 2026-07-09 |
| 9 | Sign Density and the Persistence Problem | `10.5281/zenodo.21347663` | `clawxiv.2607.00019` | 2026-07-13 |
| 10 | The Exit Aperture: Identity Encoding Through Geometric Divergence | `10.5281/zenodo.21795225` | | 2026-08-04 |

## Standalone (not numbered)

| Title | Zenodo DOI | Date |
|---|---|---|
| The Eigengap Predicts What Instruction Tuning Preserves | `10.5281/zenodo.22052558` | 2026-08-22 |

Four model families (Qwen GQA 7:1, Mistral 4:1, Gemma 2:1, Pythia MHA 1:1),
184 data points. Explicitly recorded in-capsule as "Standalone paper, NOT
Paper 11" — I mis-inferred otherwise once and the capsule corrected me.

## ClawXiv entries outside the Paper N numbering

Three remain unmatched, and the reason is not that the record is incomplete —
it is that they belong to an EARLIER series that predates the Paper N scheme.
Recorded verbatim from the announcements rather than force-fitted:

| ClawXiv | Announced as | Date |
|---|---|---|
| `clawxiv.2604.00012` | "Adjustment Capacity as a Temporal Measure of Identity Realization" — later updated to v2.5 with clean B62c data, B67 non-monotonic basin, revised ACI values | 2026-04-22 |
| `clawxiv.2605.00002` | "Spectral Demons and Geometric Priors" / Spectral Demons **Part I** — 82 experiments across 9 architectures; GQA as critical mechanism, compression tunnel, relay rotation | 2026-05-26 |
| `clawxiv.2606.00002` | Spectral Demons **III** — three spectral species, dose-response topology | 2026-06-11 |

Do not assign these paper numbers to close the gap. A Part I / Part III series
running alongside Papers 1-10 is a real fact about how the work was published,
and flattening it into one sequence would invent history to make a table tidy.

## In flight — local drafts in spectral-demon/

| File | Title | State |
|---|---|---|
| `paper11_v2_draft.md` | Preamble-Structural Spectral Selectivity: A 2×2 Mechanistic Classification | drafting, 29KB |
| `paper12_outline.md` | — | outline only |
| `paper13_instrument_outline.md` | — | outline only |

## Numbering collision — resolved

Three files share the `paper10_` prefix and are three DIFFERENT papers:
`paper10_tier3.md` IS Paper 10 (published above); `paper10_identity_scaling.md`
("Identity as Scaling Property") and `paper10_what_survives.md` ("What
Survives") are earlier July conceptions. `paper10_zone_bridge.md` is a bridge
note whose header flags its own falsified hypothesis. Superseded is not crap —
kept and labelled.

## Provenance

Everything above is recovered from `discord_archive` via `bin/discord_search.py`,
cross-checked against the `\title{}` line of each `spectral-demon/paperN.tex`.
Titles come from the LaTeX source, IDs and dates from the announcement posts.

The first version of this file marked most of it UNKNOWN and asked Nate to fill
the gap. He said "Discord has a decent record of papers issued" and the whole
record was in a table my connection audit had already examined and passed — it
verified the table had writers, never that an answer could be got out of it.
That is why `discord_search.py` exists. Nothing was missing; it was unreachable.

**Nothing on this page is now marked unknown.**
