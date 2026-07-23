# Scenario 3 — Beneficiary to Confirmed Mule

## Purpose

Prove the deterministic path where a customer adds an account already owned by a confirmed mule as a beneficiary. This path does not require counterparty AI because the destination account ownership is already known from the confirmed seed pool.

## Source design

- Confirmed retail mule `R3001` and its internal account.
- Retail customer `R3002` adds that account as a beneficiary and makes eight rapid payments after eight separate inward transfers.
- SME customer `B3001` adds the same account but makes no payment.
- All Emirates IDs are unique.
- Beneficiary IDs remain customer-scoped.
- No prebuilt graph, risk labels, or AI decisions are included.

## Expected deterministic result

- Two `BENEFICIARY_ADDED_SEED_ACCOUNT` relationships.
- The beneficiary discovery output classifies `R3002` as `PAYMENT_BACKED` with eight matching transfer events.
- The beneficiary discovery output classifies `B3001` as `ADD_ONLY` with zero matching transfer events.
- The unified graph keeps the stable generic beneficiary edge contract; payment evidence remains in the discovery and customer behavioral evidence rather than changing the base graph hash.
- The seed account is resolved through normalized local account number even though the seed record also contains an IBAN.
- No external counterparty node is created.
- No counterparty AI action is queued.
- `R3002` and `B3001` are queued directly for customer AI.
- No customer becomes a recursive source until a final `MULE_LIKE` customer decision is persisted.

## Evidence expectations

`R3002` should show high recent flow-through, eight distinct recent funding sources, and all eight outward payments occurring within two hours of an inward payment.

`B3001` should show a deterministic beneficiary relationship but no observed inward or outward transaction history.

## Safety expectations

The data pack supplies no expected outcome to runtime code. Planning makes zero live API calls. A customer assessment failure must remain failed closed and must not create a recursive expansion source.
