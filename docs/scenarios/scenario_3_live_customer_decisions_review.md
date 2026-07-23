# Scenario 3 live customer decisions

## Objective

Validate the direct customer-assessment path created when a customer adds or pays an account already owned by a confirmed mule. The confirmed mule ownership is deterministic, so no counterparty AI decision is required.

## Inputs

The observed graph contains three customer nodes and two `BENEFICIARY_ADDED_SEED_ACCOUNT` edges:

- `RETAIL|R3001`: confirmed mule seed.
- `RETAIL|R3002`: payment-backed beneficiary relationship with rapid flow-through behavior.
- `SME|B3001`: add-only beneficiary relationship with no transaction history.

The customer frontier receives neutral behavioral and deterministic relationship evidence. Scenario labels and intended decisions are not included in model payloads.

## Required execution behavior

- Queue exactly two `RUN_CUSTOMER_AI` actions.
- Execute no counterparty AI action.
- Persist each customer decision and AI audit metadata.
- Queue `DISCOVER_CUSTOMER_RELATIONSHIPS` only for a final `MULE_LIKE` decision.
- Keep all other customer outcomes visible without allowing recursive expansion.
- Preserve the complete observed graph.
- Reuse unchanged completed decisions without further AI calls.
- Mark an individual failed action `FAILED_CLOSED` without expanding that customer or blocking independent customer decisions.

## Intended synthetic review outcome

The intended evidence interpretation is:

- `RETAIL|R3002`: `MULE_LIKE` because repeated inward funding is followed rapidly by transfers to a confirmed mule account with a high flow-through ratio.
- `SME|B3001`: `INSUFFICIENT_EVIDENCE` because only an add-only beneficiary relationship is observed and there is no transaction behavior.

These outcomes are not hard-coded into the live runner. The model's valid final decisions are persisted and surfaced as returned.

## Safety gates

Live execution requires all of the following:

- `OPENAI_API_KEY` is available.
- `MULE_NETWORK_ENABLE_LIVE_AI=1`.
- Positive daily and per-run call limits.
- The runner is invoked with `--execute-live-ai`.

The scenario contains two customer subjects. A normal first live execution should therefore use a per-run limit of two. A longer client timeout may be configured with `OPENAI_TIMEOUT_SECONDS` when needed.

## Review outputs

The runner writes the following under `data/synthetic/scenario_3/runtime/live_customer_decisions/`:

- Decision-projected groups, nodes, and edges.
- Current subject snapshots.
- Remaining frontier queue.
- Decision store.
- AI call ledger.
- `live_customer_decision_telemetry.json`.

Runtime state and model response identifiers are operational artifacts and must not be committed to Git.
