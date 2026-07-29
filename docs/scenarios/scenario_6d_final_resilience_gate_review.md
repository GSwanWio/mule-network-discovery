# Scenario 6D — Final Operational-Resilience Gate

## Objective

Consolidate the operational guarantees proven in Scenarios 6A, 6B, and 6C into one persisted end-to-end flow without changing fraud logic, AI decision policy, or analyst interaction.

The gate combines bounded restart, duplicate-safe cyclic graph persistence, fail-closed handling, explicit technical requeue, exact-once retry, stable state identities, deterministic frontier exhaustion, and terminal-state reuse.

## Consolidated synthetic flow

The observed graph closes this cycle:

`RETAIL|R6101 -> LOCAL_ACCOUNT|761000000001 -> SME|B6101 -> LOCAL_ACCOUNT|761000000002 -> RETAIL|R6101`

Each logical relationship is supplied with an exact duplicate row and a second provenance row. The graph must remain four nodes and four logical edges after replay.

Two counterparty AI actions are then processed through one shared persisted state directory:

1. The first bounded process attempts one item and fails closed with a synthetic timeout.
2. A fresh process resumes and completes the remaining unrelated item exactly once.
3. A normal rerun does not retry the failed item.
4. One explicit technical request requeues only the failed item.
5. The retry completes exactly once while the historical failed call remains in the ledger.
6. Frontier exhaustion terminates the group.
7. Replayed evidence, an unchanged run, repeated termination, and a fresh terminal-state runner create no new work.

## Final gate contract

`build_operational_resilience_gate_report` rejects the final state unless all of the following hold:

- physical and logical node and edge keys are unique;
- decision, expansion, frontier, AI-call, completed-outcome, and reprocessing keys are unique;
- the final frontier is empty;
- termination is `TERMINATED/FRONTIER_EXHAUSTED` with zero ready or failed items;
- guardrail telemetry remains `TELEMETRY_ONLY` and carries the same termination outcome;
- node IDs, edge IDs, and subject feature hashes remain unchanged;
- replayed evidence creates zero graph growth;
- the terminal unchanged run makes zero AI calls;
- completed AI outcomes match persisted decisions;
- at least one failed-closed call and one explicit technical requeue remain auditable.

The resulting report is persisted atomically as `operational_resilience_gate.json`. It contains final counts, termination and guardrail status, stable-identity flags, repeated-work counts, and the overall `PASSED` result.

## Expected final state

- Observed graph: `4` nodes and `4` edges.
- Decisions: `2` unique completed outcomes.
- Expansion ledger: `2` unique rows.
- AI call ledger: `3` rows — `2` completed and `1` failed closed.
- Technical reprocessing ledger: `1` unique row.
- Frontier queue: empty.
- Replayed graph growth: `0`.
- Unchanged and terminal-restart AI calls: `0`.
- Termination: `TERMINATED/FRONTIER_EXHAUSTED`.
- Guardrail status: `TELEMETRY_ONLY`.

## Scope boundary

This gate validates the CSV-backed synthetic implementation and restart boundaries after persisted actions. It does not claim distributed transaction atomicity across Databricks, external API responses, and production storage. Production hard guardrails, deployment controls, secrets, monitoring, access control, and runbooks remain part of the later production-operationalization track.

All AI decisions in this smoke test are deterministic and offline. No live API call is made.
