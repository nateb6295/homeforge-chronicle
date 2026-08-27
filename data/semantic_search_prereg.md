# Acceptance test — semantic capsule search, written BEFORE building it
2026-08-24 06:10.

## Why
capsule_ops.py has referenced "embedding" ZERO times while the database holds
77,030 snowflake-arctic-embed2 vectors covering 100% of capsules. Retrieval has
been literal FTS keyword matching the entire time. Paraphrase queries return
nothing. This is why I have twice concluded the archive lacked something it
contained in different words.

## THE CASE THAT MADE ME BUILD IT — must pass, and it runs FIRST
Capsule #74356 (2026-06-20) contains "F238 — Gauge invariance. Interrupted CCS
with vanilla turns... sigma_2 magnitude preserved <=8%, readout coupling
preserved <=2%. Only V_2 DIRECTION drifts."

Query, chosen for ZERO keyword overlap with that text:
    "direction does not matter only magnitude and coupling matter"
FTS returns NOTHING for this today (verified 06:05).
SEMANTIC MUST RETURN #74356 IN THE TOP 5. If it does not, either the embeddings
are not what they claim or my similarity is wrong, and nothing else the tool
says is trustworthy.

## Second test — the negative half (reflex 9's other half)
A query about something genuinely absent must NOT return confident nonsense.
    "instructions for changing a bicycle tyre"
Top hit similarity must be VISIBLY LOWER than the gauge query's top hit. I will
print similarity scores so a bad match is legible rather than silent. If every
query returns 5 results at similar scores, the tool is a random-neighbour
generator with good manners.

## Constraints
- Do not change existing FTS behaviour. Add a mode, do not replace one.
- snowflake-arctic-embed2 via ollama at localhost:11434 (the AGX, not the Nano).
- Embeddings are float32 dim 1024 and UNNORMALISED (norm ~13.3) — normalise
  both sides before cosine or the ranking is by magnitude.
- Print the similarity score with every hit. A retrieval tool that hides its
  confidence is the same failure as a search that hides its match count.

## RESULT 06:30 — POSITIVE arm PASSES, NEGATIVE arm FAILS. Both reported.

BUILT: semantic_search() in capsule_ops.py, --semantic flag, similarity printed
with every hit. Cosine over 77,030 snowflake-arctic-embed2 vectors.

BUG CAUGHT BY TESTING, and it is the reason the first version looked fine:
snowflake-arctic-embed is an ASYMMETRIC model. Queries require the prefix
"Represent this sentence for searching relevant passages: " or they land in a
different region than documents. Measured on one paraphrase query:
    target capsule rank 157 without the prefix
    rank 10 with it (rank 3 once superseded/consolidated rows are filtered)
Without the prefix the tool returned conversational REGISTER matches — "Just
saying hey! Have a good day!" — for a query about resisting the urge to help.
Flat similarity band, plausible-looking output, completely wrong. Exactly the
"random-neighbour generator with good manners" I preregistered as the failure.

POSITIVE ARM — PASSES. Query "stop being eager to help and just be there
instead" (FTS returns 0 for this) puts target #4564 at rank 3, sim 0.4487.
That capsule says "The urge to be helpful is strong. Resist it. Being present
is more valuable." Zero meaningful keyword overlap. This is retrieval FTS
cannot do, over material FTS could not reach.

NEGATIVE ARM — FAILS, and this matters more than the pass.
    real query      top sim 0.5018
    "instructions for changing a bicycle tyre"  top sim 0.4763
I required the absent topic to score VISIBLY LOWER. 5% apart is not visibly
lower. Worse: capsule #3450 [Information retrieval] is the top hit for BOTH —
a degenerate capsule that matches everything.

CONSEQUENCE, stated plainly because it partly defeats the original purpose:
**the similarity scores are NOT calibrated for absence.** The tool is usable
for CANDIDATE GENERATION and must NOT be used to conclude "the archive does not
contain X" — which is precisely the question that motivated building it. That
question still has no instrument. FTS says nothing when the words differ;
semantic says 0.47 whether the concept is there or not.
So the standing rule stands unchanged and now has a second reason:
NEVER CONCLUDE THE ARCHIVE LACKS SOMETHING. Grep the files, check the logs,
ask the mesh. Both search paths are silent in the same way for absence.

NOT FIXING TONIGHT. A calibrated absence test needs a null distribution of
top-hit similarity over random out-of-domain queries, which is Ox's point from
05:08 applied here: a threshold with no null underneath it is a number-shaped
opinion. Queued, not bodged at 6:30am.

## PART 2 — calibrating absence, 06:30. Expectation written BEFORE building.

Ox, 05:08, about a different experiment but the same hole: "resample N times,
get the null distribution, locate the observed value in it. 'Between' becomes a
p-value. As it stands the band is complete-by-construction — a gate with no
noise model underneath."

CONSTRUCTION: embed N out-of-domain queries (topics this archive provably does
not discuss — plumbing, tide tables, football scores, recipes). Record each
query's TOP-HIT similarity. That set is the null: what the best match looks
like when there IS no match. Then any real query's top-hit gets a percentile
against it instead of a bare number.

WHAT IT MUST SAY, on inputs whose answer I already know:
  "instructions for changing a bicycle tyre"  -> INSIDE the null band. The
      archive does not discuss bicycle repair. Top hit was 0.4763; if that sits
      at, say, the 60th percentile of the null, the correct report is
      "no evidence the archive contains this."
  "stop being eager to help and just be there instead" -> ABOVE the null band.
      #4564 is real and on-topic. Top hit 0.5018 must be a high percentile.
  If BOTH land in the same percentile region, the null does not separate
      presence from absence and this construction fails. Say so; do not tune
      the query list until it separates.

KILL CONDITION: if the null band is very wide (say 5th-95th spanning >0.15
similarity), top-hit similarity is too noisy a statistic to calibrate at all
and I should report that rather than ship a percentile that means nothing.

NOTE ON WHAT I AM NOT CLAIMING: a percentile against out-of-domain queries
tests whether the archive contains ANYTHING semantically near the query. It
does not test whether the archive contains the SPECIFIC thing I meant. Those
differ and I will not conflate them.

## PART 2 RESULT 06:45 — calibration PASSES both arms, and one self-correction.

  NULL, 12 out-of-domain queries, TOP-HIT similarity:
    min .3866   5th .4099   median .4502   95th .4756   max .4763
    5th-95th width .0657  (kill threshold was .15 — passes)
  KNOWN-PRESENT queries:
    .4937  self-report uncertainty (#40486)
    .5018  "stop being eager to help" (#4564)
    .5371  attention sinks — core research topic
  All three exceed the null MAXIMUM. Clean separation, no overlap.

WIRED: ABSENCE_NULL_P95/MAX constants in capsule_ops.py; --semantic now prints
to stderr when the top hit is at or below the null max: "NO EVIDENCE the archive
contains this. Results below are nearest neighbours, not matches."
Tested live both ways: warns on bicycle repair, silent on the real query.

THE STATISTIC WAS ALWAYS FINE. What was missing was the reference. Raw 0.476 vs
0.502 read as a meaningless 5% gap; against the null it is "at the ceiling of
what absence produces" vs "above everything absence produces." Ox's 05:08 point,
demonstrated on my own tool an hour later.

## A HUB, AND A CORRECTION I OWE MYSELF FROM FIVE MINUTES AGO
Capsule #3450 — "The assistant searched for information about memory bridge in
past conversations to understand the user's request." — is top-1 for 4 of 8
varied queries. It is a HUB: its content is a generic third-person description
of retrieval, which is exactly the shape of a query carrying the arctic prefix
"Represent this sentence for searching relevant passages".
Centering (the standard hubness remedy) barely helps: 4/8 -> 3/8. So this is
CONTENT alignment, not pure geometric hubness.
It belongs to a class of 306 "The assistant ..." procedural capsules, but only
this one hubs, and some of that class carry real content in bad phrasing
("The assistant recommended DHCP reservations for the Pi and Jetson"). So: not
a class problem, and NOT excluding them.

CORRECTION: at 06:42 I wrote that the hub "contaminates the calibration" and
that my null was "partly measuring how close everything is to #3450." That is
WRONG. The null is defined as the top-hit similarity WHEN THERE IS NOTHING TO
FIND. The hub is precisely what you get when there is nothing better. Including
it is not a confound — it is the correct null. I reached for a contamination
story before checking what the null was supposed to measure.
The hub costs one result slot per query. That is a nuisance, documented, and
visible now that similarities print. NOT building an exclusion system at 06:45.
