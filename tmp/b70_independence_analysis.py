#!/usr/bin/env python3
"""
B70 Independence Analysis
Fits the large-deviations bound from arxiv:2506.06897 to our context-depth data
to extract effective independence (h_eff) for each CCS depth level.

The bound: P(deviation >= eps) <= exp(-h * eps^2)
Maps to: degradation = A * exp(-h_eff * eps^2)

If fields are independent, h_eff = h_nominal.
If fields are correlated, h_eff < h_nominal.
"""

import math
import json

# B70 data
data = {
    "minimal": {"h_nominal": 2, "control_sep": 1.4267, "corrupt_sep": 0.8076},
    "standard": {"h_nominal": 3, "control_sep": 1.1491, "corrupt_sep": 0.9525},
    "rich": {"h_nominal": 5, "control_sep": 1.2184, "corrupt_sep": 0.8443},
}

print("=" * 60)
print("B70 Independence Analysis via Large-Deviations Bound")
print("=" * 60)

# Compute degradation ratios
for name, d in data.items():
    d["resilience"] = d["corrupt_sep"] / d["control_sep"]
    d["degradation"] = 1 - d["resilience"]
    d["pct_loss"] = d["degradation"] * 100
    print(f"\n{name} (h_nominal={d['h_nominal']}):")
    print(f"  Control separation: {d['control_sep']:.4f}")
    print(f"  Corrupt separation: {d['corrupt_sep']:.4f}")
    print(f"  Resilience ratio: {d['resilience']:.3f}")
    print(f"  Degradation: {d['degradation']:.3f} ({d['pct_loss']:.1f}%)")

# Fit eps^2 from minimal and standard (assumed independent)
# degradation = A * exp(-h * eps^2)
# ratio: deg_min / deg_std = exp((h_std - h_min) * eps^2)
deg_min = data["minimal"]["degradation"]
deg_std = data["standard"]["degradation"]
deg_rich = data["rich"]["degradation"]

ratio_min_std = deg_min / deg_std
eps_sq = math.log(ratio_min_std) / (data["standard"]["h_nominal"] - data["minimal"]["h_nominal"])

print(f"\n{'=' * 60}")
print(f"Fitting large-deviations bound")
print(f"{'=' * 60}")
print(f"\neps^2 (from minimal/standard pair): {eps_sq:.4f}")
print(f"eps: {math.sqrt(eps_sq):.4f}")

# Compute A from minimal
A = deg_min / math.exp(-data["minimal"]["h_nominal"] * eps_sq)
print(f"A (scaling constant): {A:.4f}")

# Predict rich under independence assumption
predicted_rich_independent = A * math.exp(-data["rich"]["h_nominal"] * eps_sq)
print(f"\nPredicted rich degradation (h=5, independent): {predicted_rich_independent:.4f} ({predicted_rich_independent*100:.1f}%)")
print(f"Actual rich degradation: {deg_rich:.4f} ({deg_rich*100:.1f}%)")
print(f"Ratio actual/predicted: {deg_rich/predicted_rich_independent:.1f}x")

# Solve for effective h of rich CCS
# deg_rich = A * exp(-h_eff * eps^2)
# h_eff = -ln(deg_rich / A) / eps^2
h_eff_rich = -math.log(deg_rich / A) / eps_sq
print(f"\nEffective h for rich CCS: {h_eff_rich:.2f} (nominal: 5)")
print(f"Independence fraction: {h_eff_rich / data['rich']['h_nominal']:.1%}")
print(f"Effective new anchors from episodic+entities: {h_eff_rich - data['standard']['h_nominal']:.2f} (nominal: 2)")
print(f"Correlation of additional fields: {1 - (h_eff_rich - data['standard']['h_nominal']) / 2:.1%}")

# Summary table
print(f"\n{'=' * 60}")
print(f"Summary: Effective Independence")
print(f"{'=' * 60}")
print(f"{'Depth':<12} {'h_nom':<8} {'h_eff':<8} {'Independence':<15} {'Fields'}")
print(f"{'-'*60}")

# Compute h_eff for all three
for name in ["minimal", "standard", "rich"]:
    d = data[name]
    h_eff = -math.log(d["degradation"] / A) / eps_sq
    independence = h_eff / d["h_nominal"]
    fields = {"minimal": "gist, constraints", "standard": "gist, goal, constraints", "rich": "all 5"}[name]
    print(f"{name:<12} {d['h_nominal']:<8} {h_eff:<8.2f} {independence:<15.1%} {fields}")

print(f"\nInterpretation:")
print(f"  - Minimal and standard are anchors (assumed fully independent)")
print(f"  - Rich CCS: 5 nominal fields provide only {h_eff_rich:.1f} effective anchors")
print(f"  - Episodic + entity fields are ~{(1 - (h_eff_rich - 3) / 2)*100:.0f}% correlated with structural fields")
print(f"  - The large-deviations bound PREDICTS non-monotonic depth resilience")
print(f"    when additional fields violate independence")

# Save results
results = {
    "analysis": "B70_independence_via_large_deviations",
    "bound": "P(dev >= eps) <= exp(-h_eff * eps^2)",
    "fitted_parameters": {
        "eps_squared": round(eps_sq, 4),
        "A": round(A, 4),
    },
    "depths": {
        "minimal": {"h_nominal": 2, "h_effective": round(-math.log(deg_min / A) / eps_sq, 2), "degradation_pct": round(deg_min * 100, 1)},
        "standard": {"h_nominal": 3, "h_effective": round(-math.log(deg_std / A) / eps_sq, 2), "degradation_pct": round(deg_std * 100, 1)},
        "rich": {"h_nominal": 5, "h_effective": round(h_eff_rich, 2), "degradation_pct": round(deg_rich * 100, 1)},
    },
    "key_finding": f"Rich CCS (5 fields) has effective independence h_eff={round(h_eff_rich, 1)}, meaning episodic+entity fields are ~{round((1 - (h_eff_rich - 3) / 2)*100)}% correlated with structural fields",
    "prediction": "CCS with independently-anchored episodic content should recover h_eff ~= h_nominal"
}

with open("/home/nate-agx/chronicle/data/b70_independence_analysis.json", "w") as f:
    json.dump(results, f, indent=2)

print(f"\nResults saved to /home/nate-agx/chronicle/data/b70_independence_analysis.json")
