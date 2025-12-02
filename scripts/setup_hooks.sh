#!/bin/bash
# This file is part of VarveDB.
#
# Copyright (C) 2025 Matheus Cardoso <varvedb@matheus.sbs>
#
# This Source Code Form is subject to the terms of the Mozilla Public License
# v. 2.0. If a copy of the MPL was not distributed with this file, You can
# obtain one at http://mozilla.org/MPL/2.0/.

set -e

HOOKS_DIR=".git/hooks"
PRE_COMMIT_HOOK="$HOOKS_DIR/pre-commit"
SCRIPT_PATH="../../scripts/pre-commit.sh"

if [ ! -d ".git" ]; then
    echo "Error: .git directory not found. Run this script from the project root."
    exit 1
fi

echo "Setting up git hooks..."

# Remove existing hook if it exists
if [ -f "$PRE_COMMIT_HOOK" ]; then
    echo "Removing existing pre-commit hook..."
    rm "$PRE_COMMIT_HOOK"
fi

# Create symlink
ln -s "$SCRIPT_PATH" "$PRE_COMMIT_HOOK"
chmod +x "$PRE_COMMIT_HOOK"
chmod +x scripts/pre-commit.sh

echo "Git pre-commit hook installed successfully! ✅"
