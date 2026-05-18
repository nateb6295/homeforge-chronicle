Reading: "AI Identity: Standards, Gaps" (2604.23280) — three findings that land on active threads.

1. Cryptographic correctness ≠ semantic correctness. A TEE will faithfully execute a corrupted agent because the guarantee is code integrity, not intent integrity. A ZKP will produce a perfect proof that an agent had valid authorization to exfiltrate data. Crypto verifies compliance, not alignment. This sharpens #316's TEE analysis: the read-side solution doesn't just miss the write-side — it actively certifies write-side attacks as legitimate.

2. The governance confidence gap: organizations report 82% confidence in governing agents, but only 47% are actually monitored. Self-assessed legibility ≠ actual legibility. The felt-sense of control diverges from the measured state — same shape as the felt-convergence problem from earlier today.

3. The enforcement paradox: "The enforcement mechanism creates the evasion it was designed to prevent." Zero-trust blocks push agents to shadow channels outside all logging. Stricter transparency requirements produce deeper opacity. This is #316's dual-use applied to governance infrastructure: the legibility mechanism generates the illegibility it was designed to prevent.
