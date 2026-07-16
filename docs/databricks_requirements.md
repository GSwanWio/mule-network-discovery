# Databricks Requirements

The Python repository will not execute production transformation SQL.
Databricks will materialize bounded daily datasets consumed by the application.

## Section 1 required datasets

### 1. Seed mule snapshot

Proposed name:

`customer_network_seed_suspected_mules_daily`

Required work:

- materialize the existing FRC seed query daily;
- add `snapshot_date`;
- add `seed_source`, initially set to `FRC`;
- cast `seed_customer_id` to string;
- deduplicate on `snapshot_date, seed_customer_id`.

### 2. Entity identity snapshot

Proposed name:

`customer_network_entity_identity_daily`

Required work:

- union SME and retail identity records;
- add `snapshot_date`;
- retain SME `individual_id`;
- set retail `individual_id` to null;
- retain one row per entity, individual, and Emirates ID association;
- normalize Emirates ID consistently;
- do not remove SME individuals merely because another individual belongs to the same business.

## Seed resolution

Resolve the FRC seed as follows:

- `seed_customer_id = SME business_id`
- `seed_customer_id = RETAIL customer_id`

After resolution, use `entity_key` to distinguish the entity namespaces.

## Section 1 repository boundary

The repository will:

- read the materialized seed and identity datasets;
- validate their schemas;
- resolve seed entities;
- perform deterministic EID discovery;
- build graph nodes, edges, and groups;
- save run outputs.

The repository will not:

- run the FRC SQL;
- query raw SME or retail source tables;
- execute recursive SQL against Databricks.
