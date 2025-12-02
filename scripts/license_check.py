#!/usr/bin/env python3
import os
import sys
import argparse

LICENSE_HEADER = """// This file is part of VarveDB.
//
// Copyright (C) 2025 Matheus Cardoso <varvedb@matheus.sbs>
//
// This Source Code Form is subject to the terms of the Mozilla Public License
// v. 2.0. If a copy of the MPL was not distributed with this file, You can
// obtain one at http://mozilla.org/MPL/2.0/.
"""

def check_file(filepath, fix=False):
    with open(filepath, 'r') as f:
        content = f.read()

    if LICENSE_HEADER.strip() in content:
        return True

    if fix:
        print(f"Adding license header to {filepath}")
        # Preserve shebang if present
        if content.startswith("#!"):
            lines = content.splitlines(keepends=True)
            new_content = lines[0] + "\n" + LICENSE_HEADER + "".join(lines[1:])
        else:
            new_content = LICENSE_HEADER + "\n" + content
        
        with open(filepath, 'w') as f:
            f.write(new_content)
        return True
    else:
        print(f"Missing license header: {filepath}")
        return False

def main():
    parser = argparse.ArgumentParser(description="Check or fix license headers.")
    parser.add_argument("--fix", action="store_true", help="Apply missing license headers")
    args = parser.parse_args()

    target_dirs = ["src", "tests", "examples", "benches"]
    missing_headers = False

    for d in target_dirs:
        if not os.path.exists(d):
            continue
        for root, _, files in os.walk(d):
            for file in files:
                if file.endswith(".rs"):
                    filepath = os.path.join(root, file)
                    if not check_file(filepath, fix=args.fix):
                        missing_headers = True

    if missing_headers and not args.fix:
        print("\nSome files are missing license headers. Run with --fix to add them.")
        sys.exit(1)
    
    if not missing_headers:
        print("All files have license headers.")

if __name__ == "__main__":
    main()
