# Tether QVAC Developer Grants — Research (2026-05-17)

## Program basics (announced May 11, 2026)
- $1,500-$4,000 per task — small, task-based, not open-ended
- Paid in USDT or Bitcoin for completed work
- No cap on total program payouts
- Fixed payout + deadline per task

## Focus areas
1. Core libraries for QVAC, MDK, WDK, Pears
2. Technical documentation and onboarding
3. Applications on Tether's stack
4. Research: decentralization, edge AI, P2P networking, cryptography
5. Tooling, integrations, open standards

## Sovereignty assessment

### Positive
- Task-based = no ongoing relationship
- Focus on decentralization, edge AI, P2P = aligned with Homeforge values
- Small amounts = low commitment, easy to walk away
- QVAC itself IS local-first AI on consumer devices

### Concerns
- Work is ON Tether's stack specifically (QVAC, MDK, WDK, Pears)
- Not "build whatever" — it's "build components for our ecosystem"
- IP/licensing terms NOT in press release — need to check tether.dev
- Paid in USDT (which Nate distrusts as structurally fragile)
- Association with Tether brand

### Fine print (from tether.dev/grants/terms — reviewed 2026-05-17)

**IP ownership — MIXED:**
- You RETAIN ownership of your work ("all right, title and interest")
- BUT you grant Tether a "non-exclusive, irrevocable, perpetual, worldwide,
  royalty-free, fully paid-up, SUBLICENSABLE" license to use, reproduce,
  modify, distribute your submission
- Translation: you own it, but Tether can do anything with it forever

**Open-source — STATED INTENT, NOT MANDATED:**
- Preamble says program aims to "make projects available on an open source basis"
- No specific license mandated in application terms
- Full requirements in separate "Tether Grant and Bounty Agreement" (Section 10)
  which is NOT publicly available — only provided after acceptance

**Exclusivity — NONE:**
- Explicitly stated: "nothing will stop us from developing materials that may be
  similar or competitive to Your Materials" — implies reciprocal freedom

**Reporting/attribution — NONE in application terms**

**KYC required** — photo ID, proof of address, sanctions screening (Section 7)

**Termination — UNILATERAL:**
- Tether can "suspend or cancel all or any part of the Program for any reason"
- No notice required for program changes

**Confidentiality — ASYMMETRIC:**
- Your submissions "will not be treated as confidential by Tether"
- You must protect Tether's confidential info with "industry standard precautions"

**Indemnification — YOU bear risk:**
- You indemnify Tether against claims arising from your submission

**Data collection:**
- Name, email, country, wallet address, IP address, browser info
- Shared with subsidiaries, cloud providers, analytics, legal counsel

## Sovereignty verdict

**GREEN flags:**
- IP stays with developer (you own it)
- No exclusivity (can build competing tools)
- Open-source intent aligns with values
- No attribution requirement

**RED flags:**
- Irrevocable sublicensable license = Tether can do anything with your work forever
- KYC required (identity exposure)
- Asymmetric confidentiality (your work = public, their info = protected)
- Indemnification clause (you bear all legal risk)
- Unilateral termination (they can cancel mid-work)
- Separate "Agreement" with unknown terms only revealed post-acceptance

**VERDICT: Conditional green.** The irrevocable license is the main cost — you
own your work but can't revoke Tether's right to use it. For open-source work
this is fine (the code is public anyway). For proprietary work it would be a
dealbreaker. Since QVAC grants are explicitly open-source-oriented, the license
grant costs nothing you wouldn't already be giving away.

The KYC is the real sovereignty cost — tying identity to Tether's records.
The unknown "Agreement" is the wildcard.

## Best-fit bounties (as of 2026-05-17)

| Bounty | Amount | Alignment |
|--------|--------|-----------|
| ANE Acceleration for llama.cpp — ggml CoreML + QVAC | $5,000 | HIGH — local AI inference, what we do |
| LTX-2 Video Gen — stable-diffusion.cpp + Bare Addon | $10,000 | MEDIUM — generative media, RunPod overlap |
| QVAC SDK — Swift Client | $3,000 | LOW — Apple ecosystem, not our stack |
| Browser Extension Starter | $4,000 | MEDIUM — could serve Chronicle web UI |
| WDK Module / eCommerce / Template Wallet | $1,500-$3,000 | LOW — wallet infra, not our focus |

**Recommended target: ANE Acceleration for llama.cpp ($5,000)**
This is literally what we do — local LLM inference on consumer hardware.
The AGX has NVDLA (NVIDIA's neural accelerator), analogous to Apple's ANE.
Building ggml CoreML backend knowledge transfers directly to Homeforge goals.

## Nate's directive
"The fine print terms are what would be the go/no go. But I would have
no problem applying if you want to." — 2026-05-17
