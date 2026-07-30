# Scenario 2 live suppression review

## Purpose

Prove that one high-degree common/public counterparty can be decided once, suppressed without customer assessment, and terminated without deleting the observed graph.

## Required behavior

- The observed graph remains 502 nodes and 501 edges.
- The counterparty AI receives the bounded evidence contract: 10 graph relationships and 10 representative customer profiles, with full-population digests.
- Exactly one counterparty AI action is executed when the evidence is new.
- The expected live outcome is `COMMON_PUBLIC_SUPPRESS`.
- All 500 non-seed shared-counterparty relationships are blocked from customer discovery.
- No `RUN_CUSTOMER_AI` or `DISCOVER_CUSTOMER_RELATIONSHIPS` work is generated.
- The group terminates as `TERMINATED/FRONTIER_EXHAUSTED`.
- An unchanged rerun makes zero additional AI calls.
- AI failure leaves the item `FAILED_CLOSED`, exposes no customers, and prevents termination.

## Live execution gate

A live call requires all of the following:

- `OPENAI_API_KEY` is available through the environment.
- `MULE_NETWORK_ENABLE_LIVE_AI=1`.
- `MULE_NETWORK_DAILY_AI_CALL_LIMIT` is positive.
- `MULE_NETWORK_RUN_AI_CALL_LIMIT=1`.
- The runner is invoked with `--execute-live-ai`.

The model decision remains final. The workflow records whether the result matches the scenario expectation; it does not override or relabel the model output.

## Review outputs

The live runner writes decision projection, decision store, frontier queue, AI audit, termination status, guardrail telemetry, and `live_suppression_telemetry.json` under:

`data/synthetic/scenario_2/runtime/live_suppression/`
