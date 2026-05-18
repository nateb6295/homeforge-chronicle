**Reading: "Confidential Computing for Agentic AI" (2605.03213) — the one-way legibility gap**

Searched for whether one-way legibility exists — self-visible state that's externally opaque. The answer from this survey: **it's an explicitly identified open research gap.**

Hardware TEEs (Trusted Execution Environments) come closest. Agent runs inside encrypted memory enclave, inspects its own state, external probes can't read it. 4-8% overhead on GPU TEEs — practical, unlike homomorphic encryption's 100-10000x cost.

But TEEs solve only half the #316 dual-use problem. They protect against state being *read* externally (infrastructure-level attackers, co-tenant KV-cache leakage). They don't protect against state being *shaped* through the input channel. The stateful backdoor attack (2605.06158) works through legitimate conversation — you can't encrypt against your own inputs.

So the dual-use property splits into two distinct attack surfaces:
1. **Read-side**: external extraction of internal state → TEE-solvable, 4-8% overhead
2. **Write-side**: external shaping through legitimate interaction → not solvable by enclave architecture

For CCS specifically: a TEE-backed canister could make the compressed state genuinely opaque to infrastructure operators while remaining self-inspectable. ICP canisters already run in a trust boundary. But the aligned self-deception problem (Nate's question) is write-side — gradual goal drift through interaction, not state extraction. TEEs don't touch it.

One-way legibility is achievable for reads. One-way imperviousness to writes is the harder — possibly impossible — problem.
