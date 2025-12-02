# Issue: Implement CI/CD Security Checks

## Context
To ensure production readiness, the CI pipeline should include security audits and license checks.

## Goal
Add `cargo audit` and `cargo deny` to the CI workflow.

## Implementation Details
- Create or update `.github/workflows/ci.yml`.
- Add steps to run `cargo audit` (checks for vulnerabilities in dependencies).
- Add steps to run `cargo deny` (checks for license compliance and bans).

## Acceptance Criteria
- [ ] CI fails if dependencies have known vulnerabilities.
- [ ] CI fails if incompatible licenses are used.
