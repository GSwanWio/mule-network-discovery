# Step 8 Model-Behavior Acceptance Checkpoint

Checkpoint date: 2026-08-07

## Scenario 1 — Accepted

- Run: RUN_8e031e47202ffeb152b5
- Fresh live AI calls: 11
- Result: TERMINATED / FRONTIER_EXHAUSTED
- Graph: 1 group / 108 nodes / 109 edges
- Deterministic same-Emirates-ID mule behavior verified
- B2001 deterministic expansion verified
- Visual analyst review accepted
- Full zero-call regression gate: 83/83 passed
- Accepted implementation/test commit: 93b229f

## Scenario 2 — Accepted

- Run: RUN_72f09961f877a0e39675
- Fresh live AI calls: 1
- Decision: COMMON_PUBLIC_SUPPRESS
- Result: TERMINATED / FRONTIER_EXHAUSTED
- Graph: 1 group / 502 nodes / 501 edges
- 500-customer common/public branch correctly collapsed
- Visual analyst review accepted

## Scenario 3 — Accepted

- Run: RUN_4d3e73354bdcae623dec
- Fresh live AI calls: 2
- RETAIL|R3002: MULE_LIKE
- SME|B3001: EXPOSED_VULNERABLE
- Result: TERMINATED / FRONTIER_EXHAUSTED
- Graph: 1 group / 3 nodes / 2 edges
- R3002 mule behavior visually accepted
- B3001 potentially exposed behavior visually accepted

## Remaining Step 8 work

1. Run fresh Scenario 4 deterministic EID-only acceptance.
2. Visually review Scenario 4.
3. Run fresh Scenario 5 live model-behavior acceptance.
4. Visually review Scenario 5.
5. Perform cross-scenario calibration.
6. Standardize analyst-facing "Potential victim" terminology.
7. Clean up temporary tests/reports/scripts.
8. Complete final Step 8 acceptance.

Scenario 4 has not been executed yet. The last attempted command failed with a Python syntax error before execution.
