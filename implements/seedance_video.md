# Implement: Seedance Video Generation API

## Status: LOGGED — for Nate's video experiments

## Source
- Seedance 2.0 by ByteDance
- Third-party APIs: fal.ai, PiAPI, Atlas Cloud
- Pricing comparison: devtk.ai/en/blog/ai-video-generation-pricing-2026

## What
AI video generation via cloud API. Text-to-video and image-to-video.
Top ELO scores as of April 2026.

## Pricing
- fal.ai: ~$0.05 per 5-second 720p clip
- Atlas Cloud (Seedance v1.5 Fast): $0.022/sec (cheapest tier)
- Official API: not yet launched (Hollywood disputes)
- BytePlus: 2M free API tokens on signup
- Compare: OpenAI Sora 2 is ~100x more expensive at equivalent resolution

## Nate's Intent
Capture #135996: "I would love to figure out a way to integrate NoSpoon and make some videos."
NoSpoon Studios (@kyrannio) — agentic video platform with shotlist import.

## Integration Path
1. Simple: Python wrapper around fal.ai REST API (like posse.py for publishing)
2. Medium: Wire into POSSE pipeline — text post → video adaptation → multi-platform
3. Advanced: Integrate NoSpoon's shotlist format for structured video creation

## Hardware
Cannot run locally on AGX — video generation requires 40-80GB VRAM minimum.
Cloud API only. Budget-friendly at current pricing.

## Next
Wait for Nate to express further interest. Have the pricing research ready.
