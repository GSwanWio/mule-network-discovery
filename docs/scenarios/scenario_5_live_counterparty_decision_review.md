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

## Live policy-v2 validation outcome

The first live assessment under the original counterparty policy produced:

- Decision: `SUSPICIOUS_EXPAND`
- Confidence: `HIGH`
- Reason code: `MULTIPLE_ONE_OFF_CUSTOMERS_GOODS_SERVICES`
- Feature snapshot hash: `a1eb74b5715dbfe88462bdbe5f6d8768cabd8dc9555ff1855d96f8f7049cbd26`

This result exposed a policy-calibration gap: sparse shared usage and the
absence of recurrence were being interpreted as positive suspicious
evidence.

The counterparty policy was then versioned as
`counterparty-assessment-policy-v2`. The updated policy requires
independent behavioral corroboration beyond shared-counterparty topology.

The policy change generated a new feature snapshot hash:

`b079fde12af1a1e7e24027a3564afc8af3e190bd5d09890104bd608cf57d6384`

The original decision remained preserved as historical audit but was no
longer applied.

The controlled live reassessment under policy v2 produced:

- Decision: `INSUFFICIENT_EVIDENCE_SUPPRESS`
- Confidence: `HIGH`
- Reason code: `ONE_OFF_SHARED_PAYMENTS_ONLY`
- Suppressed non-seed relationships: `2`
- Customer AI actions queued: `0`
- Recursive sources queued: `0`
- Observed graph nodes/edges preserved: `4/3`
- Termination: `TERMINATED/FRONTIER_EXHAUSTED`

The subsequent no-call rerun reused the persisted policy-v2 decision and
made zero additional AI calls.

The model made both live decisions. No analyst approval, intervention, or
manual outcome override was used.

