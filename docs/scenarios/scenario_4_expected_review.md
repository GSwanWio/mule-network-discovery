# Scenario 4 — Emirates-ID-only groups

## Purpose

Prove that deterministic Emirates-ID relationships can form stable, seed-anchored groups without payment, beneficiary, counterparty, or AI dependencies.

## Synthetic structure

- Group A: retail seed `R4001` linked to SMEs `B4001` and `B4002` through one normalized Emirates ID.
- Group B: SME seed `B4101` linked to retail customer `R4101` through a second normalized Emirates ID.
- Two seeds, five customer entities, two normalized Emirates IDs.
- No inward payments, outward payments, or beneficiary records.
- No prebuilt groups, nodes, edges, risk flags, or decisions.

## Expected deterministic result

- Two stable groups with sizes three and two.
- Five observed customer nodes and three `SAME_EMIRATES_ID` edges.
- All identity relationships remain visible.
- The EID link itself requires no AI approval.
- EID-only linked customers are not queued for customer AI when the caller disables EID-only assessment.
- Zero counterparty AI actions and zero recursive expansion sources.
- Both groups terminate as `FRONTIER_EXHAUSTED`.
- An unchanged rerun preserves group IDs and performs no additional work.

## Policy boundary

`build_unified_seed_groups` retains its existing default behavior of assessing EID-linked customers. Scenario 4 explicitly uses `assess_eid_linked_customers=False` because the groups contain identity evidence only and no behavioral or transactional evidence. The decision engine also honors the edge-level customer-discovery flag, so deterministic identity visibility can be separated from customer risk assessment.

## Guardrails

Guardrail status remains `TELEMETRY_ONLY`. Scenario 4 is intentionally small and is not used to calibrate production breadth or depth limits.
