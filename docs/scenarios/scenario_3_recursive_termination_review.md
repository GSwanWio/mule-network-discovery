# Scenario 3 recursive termination review

## Purpose

Prove that a customer assessed as `MULE_LIKE` through a deterministic beneficiary-to-confirmed-mule relationship is consumed exactly once as a recursive source, without reopening the already represented known-mule beneficiary relationship.

## Expected execution

1. `RETAIL|R3002` is the only `DISCOVER_CUSTOMER_RELATIONSHIPS` source.
2. Its completed outward payments point to the confirmed mule account already represented by the `BENEFICIARY_ADDED_MULE_ACCOUNT` graph edge.
3. No other customer shares that destination in the synthetic source pack.
4. Recursive discovery therefore produces zero new relationships and zero graph growth.
5. A completed zero-row expansion-ledger record consumes the source.
6. The remaining frontier is empty and the group is marked `TERMINATED/FRONTIER_EXHAUSTED`.
7. An unchanged rerun performs no discovery, appends no ledger row, and makes no AI calls.

## Review expectations

- Observed graph: 3 nodes and 2 edges.
- Existing beneficiary-to-known-mule edge involving `RETAIL|R3002`: 1.
- New counterparties: 0.
- New nodes and edges: 0/0.
- Expansion ledger: one completed zero-row round for `RETAIL|R3002`.
- Ready frontier: 0.
- Failed-closed frontier: 0.
- Termination reason: `FRONTIER_EXHAUSTED`.
- Guardrail mode: `TELEMETRY_ONLY`.
- Live AI calls: 0.

The synthetic account may appear in recursive discovery as an unshared account-level counterparty because no other customer pays it. This does not create a new branch: the internal known-mule relationship is already represented deterministically in the graph and the zero-row round closes the frontier.
