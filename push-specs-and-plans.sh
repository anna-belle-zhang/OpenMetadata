#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")"

echo "=== Staging all changes ==="
git add -A

echo ""
echo "=== Status ==="
git status --short | head -30
TOTAL=$(git status --short | wc -l)
echo "... ($TOTAL files total)"

echo ""
echo "=== Committing ==="
git commit -m "chore: archive 3d-lineage-ingestion specs and plans, add living specs"

echo ""
echo "=== Pushing to origin/main ==="
git push origin main

echo ""
echo "Done."
