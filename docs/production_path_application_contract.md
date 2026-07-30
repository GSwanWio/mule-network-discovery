# Production-Path Application Contract

## Purpose

Define one resumable daily entry point for seed-led mule-network discovery.
Synthetic and Databricks execution must use the same application path.
Only the physical source provider may change.

## Public entry point

`run_daily_network_discovery(...)` is the only production-path entry point.

It receives:

- a `DiscoverySourceProvider`
- a `SourceLoadRequest`
- a state directory or state-store implementation
- AI settings and call budgets
- graph and recursion guardrails
- a live decision-adapter factory

It returns one immutable run result containing source identity, stage results,
graph outputs, AI usage, frontier state, termination status, and resilience status.

## Required execution order

1. Load the provider-neutral source bundle.
2. Validate all nine source datasets.
3. Verify the source snapshot hash.
4. Initialize or resume persisted run state.
5. Run deterministic EID and first-layer counterparty discovery.
6. Complete counterparty AI before exposing linked customers.
7. Complete customer AI before allowing recursive expansion.
8. Repeat bounded discovery and AI phases until termination.
9. Persist final state and run the operational-resilience gate.

## Phase barriers

- Only suspicious or known-suspicious counterparties may expose customers.
- Only mule-like customer decisions may create recursive expansion sources.
- Legitimate, common, public, low-concern, and insufficient-evidence branches do not expand.

## Fail-closed behavior

AI failure, invalid output, disabled live AI, exhausted budget, or invalid evidence
must block expansion. Failed work remains persisted and visible.

## Restart and idempotency

The state namespace, dataset ID, run date, and source snapshot hash identify the run.
Restarts resume persisted work. Unchanged evidence must not repeat AI calls or expansion.
Changed evidence may reopen only the affected subject and branch.

## Completion criteria

A run is complete only when source validation passes, persisted state is valid,
no actionable or failed-closed frontier work remains, groups are terminated,
and the operational-resilience gate passes.

## Exclusions

Scenario-specific runners, scenario runtime directories, direct CSV reads in business logic,
and deterministic demo decision adapters are not part of the production path.
