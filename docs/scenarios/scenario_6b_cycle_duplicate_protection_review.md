# Scenario 6B: Cycle and Duplicate Protection

## Purpose

Scenario 6B validates that cyclic customer-counterparty paths and duplicated source evidence cannot inflate the observed network or repeat completed processing.

This scenario changes no fraud logic, AI outcome policy, or analyst workflow. It adds structural coalescing and integrity checks at the recursive graph boundary.

## Synthetic path

The scenario closes this directed cycle:

`RETAIL|R6101 -> LOCAL_ACCOUNT|761000000001 -> SME|B6101 -> LOCAL_ACCOUNT|761000000002 -> RETAIL|R6101`

Each customer-counterparty discovery input contains an exact duplicate row and a second independent provenance row for the same logical relationship.

## Required behavior

- One logical customer or counterparty node is persisted per group and node key.
- One logical edge is persisted per group, edge type, source node, and target node.
- Exact duplicate relationship rows do not inflate event counts.
- Distinct provenance keys and summaries remain attached to the coalesced logical edge.
- Existing edge identifiers remain stable when the same evidence is replayed or additional provenance is observed.
- Closing the cycle reuses the already observed seed customer instead of creating another node.
- Replaying the completed cycle creates zero new nodes and zero new edges.
- Completed counterparty decisions and expansion ledger entries remain exact-once.
- The final frontier terminates as `FRONTIER_EXHAUSTED`.
- A tampered persisted state containing duplicate logical edges fails the operational integrity check.

## Persistence and audit

The operational integrity validator now checks both physical identifiers and logical graph keys. Runtime outputs remain excluded from Git. The smoke test uses deterministic offline decisions and makes no live API calls.

## Deferred scope

Mid-call process crashes, explicit technical requeue, changed-evidence branch isolation, and production guardrail calibration remain later Scenario 6 slices.
