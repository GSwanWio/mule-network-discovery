"""Production-shaped CSV contracts for synthetic source scenarios."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class CsvSourceContract:
    """Column contract for one source-shaped CSV."""

    filename: str
    columns: tuple[str, ...]
    required_nonblank: tuple[str, ...]
    unique_keys: tuple[str, ...]


SEED_MULE_POOL_CONTRACT = CsvSourceContract(
    filename="seed_mule_pool.csv",
    columns=(
        "snapshot_date",
        "seed_event_id",
        "seed_customer_id",
        "seed_account_id",
        "seed_account_number",
        "seed_iban",
        "seed_entity_type",
        "date_reported",
        "seed_source",
        "source_event_type",
        "source_transaction_reference",
    ),
    required_nonblank=(
        "snapshot_date",
        "seed_event_id",
        "seed_customer_id",
        "seed_account_id",
        "seed_account_number",
        "seed_iban",
        "seed_entity_type",
        "date_reported",
        "seed_source",
        "source_event_type",
    ),
    unique_keys=("seed_event_id",),
)

CUSTOMER_IDENTITY_CONTRACT = CsvSourceContract(
    filename="customer_identity.csv",
    columns=(
        "entity_type",
        "entity_id",
        "customer_id",
        "business_id",
        "individual_id",
        "emirates_id_number",
        "customer_segment",
        "customer_status",
        "customer_created_date",
    ),
    required_nonblank=(
        "entity_type",
        "entity_id",
        "customer_id",
        "emirates_id_number",
        "customer_segment",
        "customer_status",
        "customer_created_date",
    ),
    unique_keys=("entity_type", "entity_id"),
)

CUSTOMER_ACCOUNT_MASTER_CONTRACT = CsvSourceContract(
    filename="customer_account_master.csv",
    columns=(
        "account_id",
        "customer_id",
        "entity_type",
        "account_number",
        "iban",
        "account_currency",
        "account_status",
        "account_opened_date",
        "account_closed_date",
    ),
    required_nonblank=(
        "account_id",
        "customer_id",
        "entity_type",
        "account_number",
        "iban",
        "account_currency",
        "account_status",
        "account_opened_date",
    ),
    unique_keys=("account_id",),
)

LOCAL_INWARD_PAYMENTS_CONTRACT = CsvSourceContract(
    filename="local_inward_payments.csv",
    columns=(
        "transfer_id",
        "quote_id",
        "status",
        "reference_number",
        "source_account_id",
        "direction",
        "customer_id",
        "transaction_timestamp",
        "beneficiary_id",
        "beneficiary_account_number",
        "beneficiary_iban",
        "source_amount",
        "target_amount",
        "payment_purpose_key",
        "payment_purpose_name",
        "service_type",
        "source_iban",
        "total_fees",
        "fee_details",
    ),
    required_nonblank=(
        "transfer_id",
        "status",
        "reference_number",
        "source_account_id",
        "direction",
        "customer_id",
        "transaction_timestamp",
        "beneficiary_account_number",
        "beneficiary_iban",
        "source_amount",
        "target_amount",
        "payment_purpose_key",
        "payment_purpose_name",
        "service_type",
        "source_iban",
    ),
    unique_keys=("transfer_id",),
)

LOCAL_OUTWARD_PAYMENTS_CONTRACT = CsvSourceContract(
    filename="local_outward_payments.csv",
    columns=(
        "transfer_id",
        "quote_id",
        "status",
        "reference_number",
        "source_account_id",
        "direction",
        "customer_id",
        "transaction_timestamp",
        "beneficiary_id",
        "beneficiary_account_number",
        "source_amount",
        "target_amount",
        "payment_purpose_key",
        "payment_purpose_name",
        "service_type",
        "source_iban",
        "total_fees",
        "fee_details",
    ),
    required_nonblank=(
        "transfer_id",
        "status",
        "reference_number",
        "source_account_id",
        "direction",
        "customer_id",
        "transaction_timestamp",
        "beneficiary_id",
        "beneficiary_account_number",
        "source_amount",
        "target_amount",
        "payment_purpose_key",
        "payment_purpose_name",
        "service_type",
        "source_iban",
    ),
    unique_keys=("transfer_id",),
)

RETAIL_BENEFICIARY_MASTER_CONTRACT = CsvSourceContract(
    filename="retail_beneficiary_master.csv",
    columns=(
        "customer_id",
        "beneficiary_account_number",
        "source",
        "beneficiary_id",
        "customer_type",
        "beneficiary_type",
        "customer_creation_date",
        "beneficiary_account_holder_name",
        "nick_name",
        "is_active",
        "country_of_beneficiary",
        "currency",
        "bank_name",
        "swift_code",
        "beneficiary_created_date",
        "beneficiary_updated_date",
        "legal_type",
        "two_factor_auth_status",
        "cooldown",
        "beneficiary_address_first_line",
        "beneficiary_address_city",
        "beneficiary_address_state",
        "beneficiary_address_country",
        "beneficiary_address_post_code",
    ),
    required_nonblank=(
        "customer_id",
        "beneficiary_account_number",
        "source",
        "beneficiary_id",
        "customer_type",
        "beneficiary_type",
        "customer_creation_date",
        "beneficiary_account_holder_name",
        "is_active",
        "country_of_beneficiary",
        "currency",
        "bank_name",
        "beneficiary_created_date",
        "legal_type",
    ),
    unique_keys=("beneficiary_id",),
)

SME_BENEFICIARY_MASTER_CONTRACT = CsvSourceContract(
    filename="sme_beneficiary_master.csv",
    columns=(
        "business_id",
        "beneficiary_account_number",
        "source",
        "beneficiary_id",
        "customer_type",
        "beneficiary_type",
        "customer_creation_date",
        "beneficiary_account_holder_name",
        "nick_name",
        "is_active",
        "country_of_beneficiary",
        "currency",
        "bank_name",
        "swift_code",
        "beneficiary_created_date",
        "beneficiary_updated_date",
        "legal_type",
        "two_factor_auth_status",
        "cooldown",
        "beneficiary_address_first_line",
        "beneficiary_address_city",
        "beneficiary_address_state",
        "beneficiary_address_country",
        "beneficiary_address_post_code",
    ),
    required_nonblank=(
        "business_id",
        "beneficiary_account_number",
        "source",
        "beneficiary_id",
        "customer_type",
        "beneficiary_type",
        "customer_creation_date",
        "beneficiary_account_holder_name",
        "is_active",
        "country_of_beneficiary",
        "currency",
        "bank_name",
        "beneficiary_created_date",
        "legal_type",
    ),
    unique_keys=("beneficiary_id",),
)

SCENARIO_1_SOURCE_CONTRACTS = (
    SEED_MULE_POOL_CONTRACT,
    CUSTOMER_IDENTITY_CONTRACT,
    CUSTOMER_ACCOUNT_MASTER_CONTRACT,
    LOCAL_INWARD_PAYMENTS_CONTRACT,
    LOCAL_OUTWARD_PAYMENTS_CONTRACT,
    RETAIL_BENEFICIARY_MASTER_CONTRACT,
    SME_BENEFICIARY_MASTER_CONTRACT,
)

SCENARIO_1_SOURCE_FILENAMES = tuple(
    contract.filename
    for contract in SCENARIO_1_SOURCE_CONTRACTS
)
