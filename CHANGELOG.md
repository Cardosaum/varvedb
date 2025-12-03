# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.3] - 2025-12-03

### Features

- Display `.links` element on small screens by removing its hidden style.

## [0.1.2] - 2025-12-03

### Documentation

- Update logo and license links to use absolute URLs.

### Features

- Add release scripts and refine GitHub Actions workflow to separate version bumping from tag-based publishing.
- Add initial website project files, dependencies, and build output.
- Revamp homepage hero, features, and architecture sections with updated content, an interactive install command, and enhanced diagram descriptions.
- Add responsive mobile navigation menu with hamburger toggle
- Add max-width and responsive padding to navbar

### Miscellaneous Tasks

- Add GitHub Actions workflow to deploy the website to Cloudflare Pages.
- Only run main ci on rust file changes
- Migrate Cloudflare Pages deployment from pages-action to wrangler-action.

## [0.1.1] - 2025-12-03

### Documentation

- Add logo and center header content in README
- Update installation instructions, rename usage section, add supported Rust versions and contributing sections, and clarify license statement

### Features

- Add logo and update README with a development warning and an enhanced architecture diagram.
- Redesign and relocate `logo.svg` to `assets/logo.svg`
- Introduce new project logo for the book and README, and add a development status warning to the introduction.
- Introduce constants for encryption and serialization parameters and replace hardcoded values with them.

### Miscellaneous Tasks

- Update minimum supported Rust version to 1.81.0 in Cargo.toml and README.md
- Set rust toolchain

## [0.1.0] - 2024-05-22

### Added
- Initial release of VarveDB.
- Core storage engine using LMDB.
- Zero-copy deserialization with rkyv.
- Writer and Reader abstractions.
- Processor for reactive event handling.
- Authenticated encryption support.
