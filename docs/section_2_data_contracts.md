# Section 2 Counterparty Data Contracts

## Purpose

Section 2 identifies counterparty relationships that may justify later
network expansion. It does not approve or expand through counterparties.

## Seed mule events

File:

`data/demo/seed_mule_events.csv`

Future Databricks dataset:

`customer_network_seed_mule_events_daily`

Grain:

- one resolved FRC event per seed customer and FRC transaction reference

The dataset contains the seed customer's receiving account details obtained
from the inward payment matched to the FRC reference.

## Counterparty events

File:

`data/demo/counterparty_events.csv`

Future Databricks dataset:

`customer_network_counterparty_events_daily`

Grain:

- one Wio customer-to-counterparty event per source event

Supported event types:

- `TRANSFER_RECEIVED`
- `TRANSFER_SENT`
- `BENEFICIARY_ADDED`

Transfer events represent executed payment activity.

Beneficiary events represent a saved beneficiary relationship and do not
imply that a payment occurred.

## Customer resolution

`customer_id` resolves through the existing identity snapshot:

- SME `business_id`
- retail `customer_id`

The resolved `entity_key` remains the graph identifier.

## Counterparty key priority

Counterparty identity will use the strongest available key:

1. normalized IBAN
2. normalized SWIFT/BIC plus account number
3. normalized local account number

`beneficiary_id` is retained for source traceability and transaction
enrichment but is not the cross-customer counterparty key.

## Seed transaction cutoff

Seed transfer activity is eligible only when:

`event_timestamp <= date_reported`

This cutoff applies to transfer events used to discover seed
counterparties.

Beneficiary-to-known-mule matching is evaluated independently by comparing
saved beneficiary account details with seed account details.

## Section 2 output status

All discovered counterparty relationships remain:

`CANDIDATE_NOT_EXPANDED`

No counterparty becomes an approved graph-expansion path in Section 2.

## Incremental-readiness principle

Event IDs and future counterparty keys must remain stable across daily runs.
Later phases will persist first-seen and last-seen state and run AI only for
new or materially changed evidence.
