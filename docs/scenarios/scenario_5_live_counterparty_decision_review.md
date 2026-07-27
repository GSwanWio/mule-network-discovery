# Scenario 5 live counterparty decision review

## Purpose

Validate the controlled live-AI lifecycle for a small shared counterparty whose evidence is insufficient to justify exposing linked customers.

## Starting evidence

- One confirmed mule seed.
- One shared local counterparty.
- Two linked non-seed customers.
- Three low-value payment events across three customers.
- No rapid-drain pattern, known-mule beneficiary, concentrated flow, or common/public operating pattern.

## Controlled behavior

1. The observed graph remains complete at four nodes and three edges.
2. Exactly one counterparty assessment is eligible for execution.
3. The runner is live-disabled unless both the environment gate and `--execute-live-ai` are present.
4. Suppression decisions block downstream customer assessment while preserving observed relationships.
5. A failed call creates no decision, exposes no customers, and prevents termination.
6. An unchanged completed decision is reused without another call.

## Intended synthetic interpretation

The intended interpretation is `INSUFFICIENT_EVIDENCE_SUPPRESS`. This outcome is deterministic only in the offline smoke test. The live runner persists and applies the model's actual final decision without overriding it.

## Successful insufficient-evidence outcome

- Applied decision: `INSUFFICIENT_EVIDENCE_SUPPRESS`.
- Suppressed non-seed relationships: 2.
- Customer AI actions: 0.
- Recursive sources: 0.
- Failed frontier items: 0.
- Termination: `FRONTIER_EXHAUSTED`.
- Observed graph: 4 nodes and 3 edges.

## Audit and persistence

The review output contains the projected graph, decision store, frontier queue, AI call ledger, response/request identifiers, token usage, termination status, guardrail telemetry, and a compact JSON telemetry record. Runtime outputs remain excluded from Git.
