#!/bin/bash
# CI gate: fail if Python code directly writes to budget
set -e
VIOLATIONS=$(grep -rn "budget\.record\|cost_ledger\.add\|budget\.deduct\|budget\.spend" \
  /root/.config/opencode/redis-memory/ --include="*.py" | grep -v "test_" | grep -v "budget_client" || true)
if [ -n "$VIOLATIONS" ]; then
  echo "❌ Budget write violations found:"
  echo "$VIOLATIONS"
  exit 1
else
  echo "✅ No budget write violations"
fi
