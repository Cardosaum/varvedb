#!/bin/bash
# This file is part of VarveDB.
#
# Copyright (C) 2025 Matheus Cardoso <varvedb@matheus.sbs>
#
# This Source Code Form is subject to the terms of the Mozilla Public License
# v. 2.0. If a copy of the MPL was not distributed with this file, You can
# obtain one at http://mozilla.org/MPL/2.0/.

set -e

echo "Running pre-commit checks..."

# 1. License Check
echo "Checking licenses..."
python3 scripts/license_check.py

# 2. Formatting
echo "Checking formatting..."
cargo fmt -- --check

# 3. Clippy
echo "Running Clippy..."
cargo clippy -- -D warnings

# 4. Tests
echo "Running tests..."
cargo test

echo "All checks passed! 🚀"
