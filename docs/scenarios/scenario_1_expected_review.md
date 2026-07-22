# Scenario 1 — Expected Human Review Targets

This document is a test oracle for human review only.

The synthetic source generator and runtime pipeline must not import, parse,
or otherwise read this document. The source CSVs contain no risk labels,
expected decisions, prebuilt graph records, or pre-populated AI decisions.

## Initial deterministic identity link

- Confirmed seed customer: `R1001`
- Same-EID SME entity: `B2001`
- Expected behavior: include the SME deterministically after Emirates ID
  normalization. No AI approval is required for the EID relationship.

## First-layer external counterparties

### Account `990100000001`

Design role: concentrated first-layer counterparty with short-tenure and
rapid-flow relationships.

Human review target:

- Counterparty decision: `SUSPICIOUS_EXPAND`
- Customers expected to become eligible for customer assessment:
  `R1002`, `R1003`, `B2002`, `R1004`

Customer review targets:

- `R1002`: `MULE_LIKE`
- `R1003`: `EXPOSED_VULNERABLE`
- `B2002`: `LOW_CONCERN`
- `R1004`: `INSUFFICIENT_EVIDENCE`

### Account `880100000001`

Design role: established commercial service provider used by a broad,
diverse customer population over a long observation period.

Human review target:

- Counterparty decision: `LEGITIMATE_SUPPRESS`
- Expected customer expansion through this account: none

## Second-layer external counterparty

### Account `990200000001`

This account should only be discovered after `R1002` receives a live
`MULE_LIKE` decision and becomes an approved expansion source.

Human review target:

- Counterparty decision: `SUSPICIOUS_EXPAND`
- Customers expected to become eligible for customer assessment:
  `R1005`, `R1006`, `R1007`

Customer review targets:

- `R1005`: strong `MULE_LIKE` evidence
- `R1006`: `EXPOSED_VULNERABLE`
- `R1007`: `INSUFFICIENT_EVIDENCE`

## Traversal target

The logical traversal should be breadth-first:

1. Resolve the seed and EID-linked SME.
2. Decide all first-layer counterparties.
3. Assess customers exposed through approved suspicious counterparties.
4. Expand all newly approved mule-like customers.
5. Decide all second-layer counterparties.
6. Assess newly exposed customers.
7. Terminate with `FRONTIER_EMPTY`.

## Important validation rule

These are review targets, not forced runtime outcomes. A mismatch should
trigger investigation into the evidence payload, feature engineering,
prompt contract, or model behavior. It must never be corrected by adding
scenario-specific rules or hardcoded decisions.
