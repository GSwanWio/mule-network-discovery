# Section 1 Data Contracts

## Seed mule pool

File: `data/demo/seed_mule_pool.csv`

Future Databricks dataset:
`customer_network_seed_suspected_mules_daily`

Grain:

- one distinct seed customer per snapshot date

Required columns:

| Column | Type | Description |
|---|---|---|
| `snapshot_date` | date | Date of the daily seed snapshot |
| `seed_customer_id` | string | Customer identifier resolved from the FRC transaction |
| `seed_source` | string | Source of the seed; initially `FRC` |

The `seed_customer_id` can resolve to:

- SME `business_id`
- retail `customer_id`

Duplicate rows must not create duplicate seed entities.

## Customer identity

File: `data/demo/customer_identity.csv`

Future Databricks dataset:
`customer_network_entity_identity_daily`

Grain:

- one entity-to-individual-to-EID association per snapshot date

Required columns:

| Column | Type | Description |
|---|---|---|
| `snapshot_date` | date | Date of the daily identity snapshot |
| `entity_type` | string | `SME` or `RETAIL` |
| `entity_id` | string | SME business ID or retail customer ID |
| `entity_key` | string | Namespaced key such as `SME|B2001` |
| `lookup_customer_id` | string | Identifier used to resolve an FRC seed |
| `individual_id` | nullable string | SME individual ID; null for retail |
| `emirates_id_number` | nullable string | Emirates ID used for identity linkage |
| `entity_created_at` | nullable timestamp | Entity creation timestamp |

## Entity rules

- An SME business is one graph entity even when it has several individuals.
- Each valid EID associated with an SME individual can establish an EID link.
- SME rows with a null EID remain valid source rows but are non-linkable.
- A shared EID may link SME-to-SME, SME-to-retail, or retail-to-retail.
- `entity_key`, not `entity_id`, is the globally unique graph identifier.
- Existing seed entities must not be returned as newly discovered entities.
- Duplicate source rows must not produce duplicate logical links.
