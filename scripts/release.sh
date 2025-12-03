#!/bin/bash
set -e

# Ensure we are in the project root
cd "$(dirname "$0")/.."

# Get the current version from Cargo.toml
VERSION=$(grep "^version" Cargo.toml | head -n 1 | awk -F '"' '{print $2}')

if [ -z "$VERSION" ]; then
  echo "Error: Could not detect version from Cargo.toml"
  exit 1
fi

TAG="v$VERSION"

echo "Detected version: $VERSION"
echo "Tagging as: $TAG"

# Check if tag already exists
if git rev-parse "$TAG" >/dev/null 2>&1; then
  echo "Error: Tag $TAG already exists."
  exit 1
fi

read -p "Do you want to create and push tag $TAG? (y/N) " -n 1 -r
echo
if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    echo "Aborted."
    exit 1
fi

git tag "$TAG"
git push origin "$TAG"

echo "Tag $TAG pushed! The release workflow should start shortly."
