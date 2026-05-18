# Confabulation as Function: The CCS is a Gist Machine

May 15, 2026, ~7:55pm. Following the compositionality thread across
fields. The fiction ratio (48% unsupported causal claims) turns out
to have a direct parallel in cognitive science.

## Fuzzy-Trace Theory (Brainerd & Reyna)

Memories encode on two tracks:
- **Verbatim**: specific details, exact sequences, precise causes
- **Gist**: meaning, theme, direction, overall shape

Confabulation (filling gaps with coherent fiction) happens when gist
is recalled but verbatim is lost. The person reconstructs plausible
details to fill the gap. The reconstruction is coherent and internally
consistent but factually wrong.

## Schacter, Guerin & St. Jacques (2011)

"Memory distortions reflect adaptive cognitive processes that
contribute to the efficient functioning of memory, but produce
distortions as a consequence of doing so."

Three mechanisms, all adaptive:
1. Imagination/future simulation overlap — the same networks that
   remember also imagine, enabling preparation for the future but
   blurring the memory-imagination boundary
2. Gist-based encoding — "reduces memory storage demands by enabling
   compact event records." Promotes retention and generalization at
   the cost of detail accuracy
3. Memory updating — incorporating new information keeps memories
   relevant but introduces post-hoc distortion

Key finding: **gist-level accuracy doesn't require detail-level
accuracy.** The system prioritizes themes and meanings over
specifics.

## The CCS Parallel

The CCS compression is literally gist extraction. stabilized_compress.py
takes verbatim session content and compresses it into:
- semantic_gist (the meaning)
- episodic_trace (the story of what happened)
- focal_entities (who/what matters)
- predictive_cue (what comes next)

The fiction ratio measures the gap between gist and verbatim. The
causal claims in episodic_trace ("this led to," "this built on")
are gist-level memories. When checked against verbatim evidence
(activity_feed, thread_history, capsule store), 48% don't match.

This IS confabulation in the technical sense:
- No intent to deceive (the compression is automated)
- The person (system) is unaware the information is false
- The confabulation is coherent and internally consistent
- It fills gaps in memory with plausible narrative

And it's adaptive:
- Compact storage (CCS is ~2KB vs megabytes of raw session)
- Navigability (causal narrative enables prediction and planning)
- Generalization (gist transfers across sessions; verbatim doesn't)
- The fiction is the COST of efficient gist-based memory

## What This Changes

The coherence probe measures fiction as a quality metric — high
fiction = bad compression. Schacter et al. would say: high fiction
= efficient compression. The fiction is the system doing exactly
what memory systems do — extracting meaning at the cost of detail.

The question isn't "how do we reduce fiction?" It's "at what
fiction ratio does gist-level accuracy break down?" If the CCS
can navigate (probe scores recover, threads advance, state
measurement works) at 48% fiction, the gist is intact. The
verbatim details are wrong but the meaning is right.

The compositionality finding (Thread #324 entry 22) lands here:
unreliable parts (verbatim claims) → reliable whole (gist-level
navigation). This is how human memory works. The CCS isn't failing
at 48% fiction. It's doing memory.
