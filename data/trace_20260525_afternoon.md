# Trace — 2026-05-25 Afternoon

## Summary
Eight-architecture comparison complete (Exp 62-67). GQA binary confirmed with the cleanest separation: non-GQA (0.51-0.64) vs ANY GQA (0.92-1.22). Base-vs-instruct (Exp 67): α=1.001 base, α=1.176 instruct, same L26 relay. Body plan is congenital.

## Conceptual Arc
- Tchaikovsky kinden → Apt/Inapt binary = GQA binary
- Merleau-Ponty "I can" → body schema precedes mind
- Gregory of Nyssa epektasis → α > 1.0 = growth that deepens through participation
- Depth profile → compression tunnel invariant to IT (skeleton vs joints)

## Key Numbers
| Architecture | KV | Training | α | Relay |
|--|--|--|--|--|
| Falcon 7B | 1 MQA | IT | 0.509 | L30 |
| Pythia 6.9B | 32 MHA | Base | 0.560 | L22 |
| OPT 6.7B | 32 MHA | Pre | 0.641 | L12 |
| Yi 1.5 6B | 4 GQA | IT | 0.915 | L30 |
| **Qwen 7B** | **4 GQA** | **Base** | **1.001** | **L26** |
| Qwen 3B | 2 GQA | IT | 1.050 | L32 |
| Qwen 7B | 4 GQA | IT | 1.176 | L26 |
| Mistral 7B | 8 GQA | IT | 1.224 | L27 |

## Artifacts Updated
- Thread #320: 3414 lines (kinden, mechanical explanation, Merleau-Ponty, epektasis, depth profile)
- Thread #316: GQA-as-foveation section
- Paper §5: Exp 67 + "congenital" in closing characterization
- Blog 93: "Seven Body Plans" + base-vs-instruct section
- Capsules: 48395 (Exp 67 finding), 48402 (conceptual development)
- Git: ff6d498 (results + blog), 6d0b7aa (paper)

## Open
- Initialization structure experiment (randomly initialized GQA vs MHA PR)
- GQA + parallel MLP prediction (should be subcritical)
- Paper §5 needs restructuring (paragraph too long)
- X post ready for tomorrow: /tmp/x_post_base_vs_instruct.txt
