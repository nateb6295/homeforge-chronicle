#!/bin/bash
# B74 Pod Setup — install dependencies and run probe
set -e

echo "=== B74 Pod Setup ==="
echo "Installing TransformerLens and dependencies..."

pip install transformer-lens torch scikit-learn numpy --quiet

echo "Dependencies installed."
echo ""
echo "Running B74 probe..."

python3 /workspace/layerwise_identity_probe.py run

echo ""
echo "=== B74 Complete ==="
echo "Results saved to ~/chronicle/data/layerwise_identity_probe.json"
