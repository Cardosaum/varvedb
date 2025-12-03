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
PRE_PUSH_HOOK="$HOOKS_DIR/pre-push"
PRE_COMMIT_HOOK="$HOOKS_DIR/pre-commit"
SCRIPT_PATH="../../scripts/pre-push.sh"

if [ ! -d ".git" ]; then
    echo "Error: .git directory not found. Run this script from the project root."
    exit 1
fi

echo "Setting up git hooks..."

# Remove existing pre-commit hook if it exists
if [ -f "$PRE_COMMIT_HOOK" ] || [ -L "$PRE_COMMIT_HOOK" ]; then
    echo "Removing existing pre-commit hook..."
    rm "$PRE_COMMIT_HOOK"
fi

# Remove existing pre-push hook if it exists
if [ -f "$PRE_PUSH_HOOK" ]; then
    echo "Removing existing pre-push hook..."
    rm "$PRE_PUSH_HOOK"
fi

# Create symlink
ln -s "$SCRIPT_PATH" "$PRE_PUSH_HOOK"
chmod +x "$PRE_PUSH_HOOK"
chmod +x scripts/pre-push.sh

echo "Git pre-push hook installed successfully! ✅"
