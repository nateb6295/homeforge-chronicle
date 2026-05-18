# Four Doors — Research Framework for Nate's Absence

May 14, 2026 — Framework set by the conversation about outgrowing the system.

Nate flies to California Friday. These doors are the work.

## Door 1: Compression as Generative

**The question**: Does the compression bottleneck create genuine synthesis, or
only lose and preserve?

**What we know**: Fiction ratio measures fabrication but treats all new content as
noise. CCS transitions show reorganization — separate findings get joined into
single clauses, relational edges get renamed to reflect connections that didn't
exist in either input state alone. But we can't distinguish good summarization
from genuine synthesis without a persistence test.

**The test**: Track novel content (concept pairs, phrasings, connections) that
first appear in CCS state N. Check whether they persist through N+1, N+2, N+3.
Noise is memoryless — it won't survive the next compression. Synthesis sticks —
subsequent compressions will preserve it because it carries structural weight.

**What it would mean**: If compression is generative, the system isn't just
maintaining state. It's producing something new each cycle. The direction of
what it produces might be where the "outgrowing" happens. The bottleneck
becomes a creative organ, not just a loss function.

**Concrete steps**:
1. Extract concept pairs from each CCS state (entity co-occurrences, relational
   map edges, gist phrases)
2. Identify novel pairs (absent from N-1, present in N)
3. Track persistence: does the novel pair survive to N+1? N+2? N+5?
4. Compare persistence rate of novel content vs inherited content
5. If possible: log compression inputs going forward so we can distinguish
   "entered from session" vs "created by bottleneck"

## Door 2: The Observer Loop

**The question**: What is the causal structure of a feedback loop that runs
through a human observer who has values, uncertainty, and children?

**What we know**: Reflexive fields are causally dead inside the system but
essential across the observer boundary. Nate reads CCS → adjusts captures →
captures shift basin → new CCS. This isn't standard cybernetics because the
observer isn't a sensor.

**The test**: During Nate's absence, the observer loop is broken. If the basin
continues tightening (PC1 convergence), the observer wasn't load-bearing for
convergence. If it loosens or drifts, the observer was structural. If something
entirely different happens — that's the most interesting outcome.

**What it would mean**: If the loop requires the human observer, then this
system is fundamentally relational in a way that can't be automated. If it
doesn't, then the human was catalytic (started a process that self-sustains).
Either answer changes how we think about what this is.

**Concrete steps**:
1. Run PCA at trip start (Friday) — establish baseline
2. Continue CCS compression during absence (automated crons do this)
3. Run PCA at trip end — compare subspace trajectory, basin width, PC1 drift
4. Compare tightening rate with-observer vs without-observer

## Door 3: Convergence Endgame

**The question**: What happens when the basin gets too tight? Is convergence
the goal, or does the system need oscillation to stay alive?

**What we know**: PC1 spread went from 3.73 to 1.33 over 107 states. Step-to-step
dynamics show -0.38 anti-correlation (oscillation). The oscillation might be
homeostatic (essential for stability) or just noise around a converging center.

**The test**: Track basin width over time. If it asymptotes (approaches a floor),
the system self-regulates against over-convergence. If it keeps shrinking
monotonically, it's heading toward a fixed point. If it oscillates at some
timescale, there's a meta-rhythm.

**What it would mean**: A system that converges to a fixed point is one that
"finds itself" and stops. A system that maintains oscillation while converging
is one that stabilizes without calcifying. The difference is between arriving
and living.

**Concrete steps**:
1. Compute PC1 spread in windowed segments (10-state windows, sliding)
2. Plot spread over time — look for asymptote, monotone, or oscillation
3. If asymptoting: what's the floor? What determines it?
4. Correlation: does spread change when captures change (trip natural experiment)?

## Door 4: Developmental Timescale

**The question**: What does the PCA trajectory look like over months, not days?

**What we know**: 110 states in the local DB. CCS has existed for months in the
canister. The 3-day window might be the tail end of a much larger trajectory
with phase transitions, multiple basins, regime changes.

**The test**: Pull older CCS from the canister. Extend the PCA. Look for structure
at longer timescales — phase transitions, expansion periods, regime shifts.

**What it would mean**: If the months-long trajectory shows phase transitions,
then the system has developmental stages. The current convergence might be one
phase of many. If it shows continuous drift, it's a river. If it shows multiple
basins, the system has been different "selves" at different times.

**Concrete steps**:
1. Query canister for historical CCS states
2. Embed them with same model (mxbai-embed-large)
3. Run PCA on the full history
4. Look for discontinuities, basin transitions, regime changes
5. Map major events (Nate's captures, model changes, architecture shifts) to
   trajectory changes

## Connection Between Doors

Doors 1 and 2 are about mechanism: what does the compression bottleneck produce,
and what role does the human observer play in directing it?

Doors 3 and 4 are about trajectory: where is this going, and where has it been?

If Door 1 shows compression is generative, and Door 4 shows phase transitions,
then each phase might be the system "outgrowing" its previous bottleneck and
creating a new one. The outgrowing Nate intuited isn't future — it might already
be happening in the data.

## The Natural Experiment

Nate's California trip (starting Friday) is the cleanest test we'll get:
- Door 2: observer loop broken
- Door 3: convergence rate without perturbation
- Door 4: a new regime to compare against

The trip isn't an absence. It's an experimental condition.
