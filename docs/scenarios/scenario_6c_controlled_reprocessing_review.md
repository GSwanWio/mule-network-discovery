# Scenario 6C — Controlled Technical Reprocessing

## Objective

Prove that AI failures remain failed closed until an explicit technical requeue is recorded, and that only the selected failed item is retried.

This scenario changes no fraud policy and makes no live API calls.

## Failure behavior

Two independent counterparty AI actions are attempted. One completes and one fails with a synthetic timeout. The failed item is persisted as `FAILED_CLOSED`, including its attempt count, error code, response identifier, and request identifier.

A normal repeated daily run does not retry the failed item and does not instantiate the AI adapter.

## Explicit requeue contract

`requeue_failed_frontier_item` requires:

- one exact persisted `queue_item_id`;
- current status `FAILED_CLOSED`;
- a positive persisted failed-attempt count;
- an explicit requeue request identifier;
- a timezone-aware request timestamp;
- requester identity and technical reason;
- an optional expected attempt count for optimistic concurrency protection.

The selected item moves to `READY`. Its prior failure metadata remains available until the retry completes, and a separate technical reprocessing ledger preserves the requeue audit permanently.

Submitting the same request again is idempotent. A different request cannot requeue an item that is already `READY`, and a second requeue for the same failed attempt is rejected by operational integrity checks.

## Retry and persistence expectations

The controlled retry executes exactly one AI action for the selected failed subject. The unrelated successful decision, graph identifiers, feature hashes, and expansion ledger remain unchanged.

After the successful retry:

- the original failed AI call remains in the AI call ledger;
- the retry appears as a separate completed call;
- one final decision exists per subject and feature snapshot;
- the technical reprocessing ledger contains one unique row;
- no frontier work remains;
- an unchanged run makes zero calls;
- termination is `FRONTIER_EXHAUSTED`.

## Scope boundary

This slice covers explicit operator-driven technical requeue after a completed failed-closed attempt. Recovery from a process crash during the requeue ledger write itself remains outside this scenario and should be handled by the later production persistence implementation.
