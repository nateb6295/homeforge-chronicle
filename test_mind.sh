#!/bin/bash
# Test script for chronicle-mind

# Load environment
set -a
source ~/.config/chronicle-mind.env
set +a

echo "Testing chronicle-mind..."
echo "ANTHROPIC_API_KEY is ${#ANTHROPIC_API_KEY} chars"

timeout 90 /home/bradf/projects/homeforge-chronicle/target/release/chronicle-mind
