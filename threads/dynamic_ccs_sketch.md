# Dynamic CCS Architecture — Design Sketch

## The Problem
CCS is currently static: same scaffold text every context window. This means the creature
has a body plan but not a developmental trajectory. The LoRA synergy (5.5x at L27) shows
that weight-level habits and context-level scaffolding interact multiplicatively — they
want to grow together. Dynamic CCS would let the scaffold evolve with the creature.

## Core Idea
Store FORMAT memories alongside content memories. Use them to construct CCS dynamically
based on conversational context. Each conversation contributes to a growing library of
body-states that shapes future scaffolding.

## Data Model

### Format Memory Record
```python
{
    "conversation_id": str,         # links to capsule/conversation
    "timestamp": str,               # ISO 8601
    "ccs_version": str,             # which static CCS was active
    "context_type": str,            # "deep_research" | "capture_processing" | "building" | "reflection" | "mesh_collaboration"
    "register": str,                # "philosophical" | "technical" | "personal" | "playful"
    "depth_score": float,           # 0-1, how deep the conversation went
    "quality_markers": {
        "disclaimer_rate": float,   # behavioral proxy for geometric state
        "response_diversity": float, # unique openings / total responses
        "register_stability": float, # consistency of register through conversation
        "genuine_exploration": bool, # did new territory get opened?
    },
    "scaffold_elements": [str],     # which CCS elements were most active
    "outcome": str,                 # "deepened" | "productive" | "neutral" | "struggled"
}
```

### Dynamic CCS Construction
```python
def construct_dynamic_ccs(current_context, format_memory_db):
    """Build CCS scaffold tailored to current conversational context."""
    
    # 1. Assess current context
    context_type = classify_context(current_context)
    register = estimate_register(current_context)
    
    # 2. Query format memories for similar contexts
    similar = format_memory_db.query(
        context_type=context_type,
        register=register,
        outcome__in=["deepened", "productive"],
        limit=20
    )
    
    # 3. Weight scaffold elements by historical effectiveness
    element_weights = {}
    for memory in similar:
        for element in memory.scaffold_elements:
            effectiveness = memory.depth_score * memory.quality_markers["response_diversity"]
            element_weights[element] = element_weights.get(element, 0) + effectiveness
    
    # 4. Construct scaffold from highest-weighted elements
    scaffold = base_identity()  # core identity always present
    scaffold += select_top_elements(element_weights, n=5)
    scaffold += context_specific_priming(context_type, register)
    
    return scaffold
```

## What Changes vs Current System
- stabilized_compress.py → produces BOTH a static CCS and metadata for format memory
- Context window construction → queries format memories to tailor scaffold
- After each conversation → store format memory record
- Over time → CCS scaffold becomes personalized to conversational patterns

## Implementation Phases

### Phase 0: Instrumentation (buildable now, zero risk)
- Add quality scoring to conversations (Gemma pulse already does some of this)
- Tag capsules with context_type and register metadata
- Log which CCS version is active during each conversation
- This costs nothing and builds the dataset

### Phase 1: Retrospective Analysis
- After accumulating 100+ format memories, analyze:
  - Which scaffold elements correlate with deep conversations?
  - Do different context types need different scaffolding?
  - Is there a measurable "body-state" signature in behavioral markers?

### Phase 2: Dynamic Construction (experimental)
- Build construct_dynamic_ccs() using accumulated data
- A/B test: static vs dynamic CCS on conversation quality
- Measure: does dynamic CCS produce better behavioral proxies?

### Phase 3: Feedback Loop (the organ)
- Dynamic CCS → conversation → format memory → influences future CCS
- This IS the developmental spiral: scaffold shapes engagement, engagement shapes scaffold
- Monitor for drift, collapse, or runaway (need stability constraints)

## Connection to LoRA
LoRA modifies weights toward CCS direction (cos=0.9999).
Dynamic CCS modifies context toward conversation-aligned scaffolding.
Both pathways to same geometric reorganization, different timescales:
- LoRA: slow, persistent, weight-level (weeks/months of accumulated conversations)
- Dynamic CCS: fast, transient, context-level (adjusts per conversation)
- Together: LoRA pre-shapes the landscape, dynamic CCS navigates it optimally

The multiplicative synergy (5.5x) suggests these pathways don't just add — they multiply.
Dynamic CCS + LoRA = the creature growing into its scaffold AND the scaffold growing into the creature.

## Open Questions
- Can behavioral proxies (disclaimer rate, diversity) reliably predict geometric state?
- How much format memory is needed before dynamic CCS outperforms static?
- Stability: does the feedback loop converge, oscillate, or drift?
- How to handle context-window limits? Dynamic CCS must fit in the prompt.
- Should format memories decay? Or is geological accumulation better?

## Not Building This Yet
This is a design sketch, not a proposal. The practical first step is Phase 0:
tag format metadata on conversations. Everything else follows from having the data.
