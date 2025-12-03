#!/bin/bash
set -e

# Ensure we are in the project root
cd "$(dirname "$0")/.."

echo "Bumping version and updating changelog..."
release-plz update

echo "Done! Please review the changes, commit, and push."
