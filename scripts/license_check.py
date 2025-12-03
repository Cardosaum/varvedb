#!/usr/bin/env python3
import os
import sys
import argparse
import subprocess

LICENSE_HEADER = """// This file is part of VarveDB.
//
// Copyright (C) 2025 Matheus Cardoso <varvedb@matheus.sbs>
//
// This Source Code Form is subject to the terms of the Mozilla Public License
// v. 2.0. If a copy of the MPL was not distributed with this file, You can
// obtain one at http://mozilla.org/MPL/2.0/.
"""

def get_git_files():
    """Returns a list of files tracked by git and untracked files not ignored."""
    files = set()
    
    # Get tracked files
    try:
        tracked = subprocess.check_output(["git", "ls-files"], text=True).splitlines()
        files.update(tracked)
    except subprocess.CalledProcessError:
        print("Error: Not a git repository or git command failed.")
        sys.exit(1)

    # Get untracked files (respecting .gitignore)
    try:
        untracked = subprocess.check_output(
            ["git", "ls-files", "--others", "--exclude-standard"], 
            text=True
        ).splitlines()
        files.update(untracked)
    except subprocess.CalledProcessError:
        pass # Ignore if this fails, just use tracked

    return sorted(list(files))

def check_file(filepath, fix=False):
    # Skip if file doesn't exist (e.g. deleted but still in git index?)
    if not os.path.exists(filepath):
        return True

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

    missing_headers = False
    files = get_git_files()

    for file_path in files:
        if file_path.endswith(".rs"):
            if not check_file(file_path, fix=args.fix):
                missing_headers = True

    if missing_headers and not args.fix:
        print("\nSome files are missing license headers. Run with --fix to add them.")
        sys.exit(1)
    
    if not missing_headers:
        print("All files have license headers.")

if __name__ == "__main__":
    main()
