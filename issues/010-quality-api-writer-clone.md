# Issue: Review Writer Clone Semantics

## Context
`Writer` holds `Storage` (which is `Clone`) but `Writer` itself does not derive `Clone`.

## Goal
Determine if `Writer` should be `Clone` and implement it if beneficial.

## Implementation Details
- If `Writer` is stateless (other than `Storage` and `metrics`), deriving `Clone` makes it easier to pass around.
- If `Writer` holds the `watch::Sender`, cloning it might need care (though `Sender` is not cloneable, so it might need to be wrapped in `Arc`).

## Acceptance Criteria
- [ ] `Writer` implements `Clone` if feasible and useful.
