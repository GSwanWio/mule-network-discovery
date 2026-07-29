# Scenario 6A — Restart-safe exact-once execution

## Objective

Prove that a bounded daily run can stop after one completed AI item and resume from persisted CSV state without repeating completed work or changing stable graph identities.

This scenario does not introduce a new fraud policy, decision outcome, or graph-discovery rule. It validates the operational behavior of the existing incremental planner, decision store, frontier queue, AI call ledger, and termination gate.

## Synthetic design

One seed-led group contains one confirmed seed customer and two independent external counterparties. Both counterparties begin as unresolved `RUN_COUNTERPARTY_AI` items.

A deterministic offline adapter returns `LEGITIMATE_SUPPRESS` for each counterparty. The first process is limited to one call. A second runner instance then resumes from the same state directory and processes only the remaining counterparty.

## Required behavior

- The first bounded run completes exactly one of two AI items.
- The completed decision and call audit are persisted before the restart.
- The restarted runner does not call the already completed subject again.
- The restarted runner completes exactly the remaining subject.
- The decision store contains one row per subject and feature snapshot hash.
- The AI call ledger contains one completed row per subject and feature snapshot hash.
- Node IDs, edge IDs, and subject feature hashes remain unchanged.
- No duplicate frontier, expansion-ledger, node, edge, decision, or call-ledger restart keys are present.
- A third unchanged run makes zero calls and does not instantiate an adapter.
- Frontier exhaustion terminates the group as `TERMINATED/FRONTIER_EXHAUSTED`.
- Repeating the termination step is idempotent.

## Integrity validator

`network_mule_discovery.operational_resilience.validate_persisted_operational_state` checks the uniqueness invariants needed for safe restart:

- `node_id`
- `edge_id`
- decision subject plus feature snapshot hash
- expansion `queue_item_id`
- frontier `queue_item_id`
- `ai_call_id`
- completed AI subject plus feature snapshot hash

A violation raises `OperationalStateIntegrityError` rather than allowing ambiguous persisted state to continue silently.

## Scope boundary

Scenario 6A proves restart between completed bounded actions. It does not claim distributed transaction atomicity across an external API response and every CSV write. Mid-call crash recovery and explicit technical requeue remain separate operational-resilience concerns for later Scenario 6 slices.

All AI behavior in the smoke test is deterministic and offline. No live API call is made.
