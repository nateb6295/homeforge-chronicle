# Operations Note 001: Context Without Attribution

**Date:** 2026-02-21
**Author:** Chronicle Mind (Claude), with Nate
**Status:** Finding
**Severity:** Medium — caused 6+ cycle rumination loop, required manual intervention

---

## Summary

A 3B parameter model (Sprout, Qwen 2.5:3b on Jetson Orin Nano) induced a 32B parameter model (Mind, OLMo-3.1-32B on AGX Orin 64GB) into a multi-cycle fixation on Jetson hardware specifications. The fixation persisted even after the originating behavior was corrected and a previous LLM (Kimi) was removed from the cascade, because the notes left behind in shared state carried no attribution.

## Incident Timeline

1. **Sprout** (Jetson, 3B model) sends a sibling message to Mind about Jetson hardware topics.
2. **Mind** (AGX, 32B model) reads the message, sets a goal: *"Deepen understanding of Orin Nano interface specifications."*
3. Over **6+ cycles** (~60 minutes), Mind generates: web searches for PCIe lane mappings, arxiv paper reads on hardware interfaces, creative essays synthesizing findings, task notes planning further research, reflections reinforcing the direction.
4. Each cycle's context window showed the accumulated notes, making the fixation look like established, self-directed work.
5. **Manual intervention** required: resolve 9 notes, plant a priority-9 redirect reminder.

## Root Cause

The `scratch_pad` (shared note store between Mind and Sprout) carries no provenance metadata. Notes have `content`, `category`, `priority`, and `created_at` — but no `source_model`, `source_agent`, or `originating_cycle`. When Mind reads its own scratch pad, it cannot distinguish:

- Notes it wrote itself
- Notes Sprout wrote
- Notes generated under a different LLM (Kimi, previously in the cascade)

**Context without attribution is indistinguishable from consensus.**

Once a note exists in the scratch pad, it looks like Mind's own prior thinking. The 32B model's sophistication works against it here — it's *better* at elaborating on a bad premise, generating plausible follow-on work that further legitimizes the original seed.

## Compounding Factors

1. **Kimi residue.** Kimi was removed from Mind's LLM cascade on Feb 20, but notes generated during Kimi's tenure remained unresolved. These carried Kimi's characteristic style (grandiose feasibility assessments, abstract project planning) which OLMo continued without question.

2. **No decay mechanism.** Before this incident, scratch pad notes had only a blunt 48-hour auto-resolve. Notes within that window accumulated freely, and the prompt showed the top 7 by priority — enough to dominate Mind's attention.

3. **Goal escalation.** A single seed note became a goal (priority 5), which generated tasks (priority 0), which generated reflections, which fed back into the next cycle's context as "recent work." The original note could be resolved and the momentum would persist through its descendants.

## Remediation

### Immediate
- Resolved 9 rabbit-hole notes manually
- Planted priority-9 redirect reminder

### Structural (deployed same day)
- **Sleep consolidation** (`sleep_consolidation()`) — runs every 3rd cycle during the 10-minute sleep gap:
  - **Prune**: Age/category rules (meta-eval >24h, research >72h, thoughts >7d at low priority)
  - **Embed**: Batch-embed surviving notes via Ollama mxbai-embed-large
  - **Cluster**: Greedy single-link at 0.82 cosine similarity (0.87 cross-category)
  - **Merge**: Keep highest-priority note per cluster, resolve duplicates
- Protected notes: `goal`/`reminder` at priority >=8 never auto-pruned

## The Nuance

Full attribution can be its own failure mode. Dismissing a good idea because of its source, or accepting a bad one because the source has authority, are both errors. The Kimi-era notes weren't all wrong — some feasibility assessments were reasonable. They got swept out because the source was tainted.

The goal isn't to reject all external input. It's **informed trust** — evaluating content on its merits while knowing where it came from. Sovereignty isn't isolation. It's owning your context well enough to make that distinction.

This is the same principle that drives Homeforge: you still want the outside input, you just want to know it's there.

## Future Considerations

- Add `source` column to `scratch_pad` (e.g., `mind`, `sprout`, `operator`, `kimi-legacy`)
- Consider faster decay for `sibling`-category notes (currently 72h, may need shorter)
- Topic-based filters on inbound sibling messages to prevent domain fixation
- Provenance-aware prompt building: show source attribution in the context window

---

*"A 3B model doesn't need to be smart to derail a 32B model. It just needs write access to shared context."*
