{{ config(
    materialized = 'incremental',
    unique_key = 'fact_transaction_key_registration_id',
    incremental_strategy = 'merge'
) }}

WITH base AS (

    SELECT
        fact_transaction_id,
        b.block_id,
        intra,
        C.tx_type,
        A._inserted_timestamp
    FROM
        {{ ref('core__fact_transaction') }} A
        JOIN {{ ref('core__dim_block') }}
        b
        ON A.dim_block_id = b.dim_block_id
        JOIN {{ ref('core__dim_transaction_type') }} C
        ON A.dim_transaction_type_id = C.dim_transaction_type_id
    WHERE
        tx_type = 'keyreg'

{% if is_incremental() %}
AND A._inserted_timestamp >= (
    SELECT
        MAX(
            _inserted_timestamp
        )
    FROM
        {{ this }}
)
{% endif %}
),
keyreg AS (
    SELECT
        block_id,
        intra,
        tx_message :txn :votekey :: text AS participation_key,
        tx_message :txn :selkey :: text AS vrf_public_key,
        tx_message :txn :votefst AS vote_first,
        tx_message :txn :votelst AS vote_last,
        tx_message :txn :votekd AS vote_keydilution,
        _inserted_timestamp
    FROM
        {{ ref('silver__transaction') }}
    WHERE
        tx_type = 'keyreg'

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(
            _inserted_timestamp
        )
    FROM
        {{ this }}
)
{% endif %}
)
SELECT
    {{ dbt_utils.surrogate_key(
        ['a.block_id','a.intra','a.tx_type']
    ) }} AS fact_transaction_key_registration_id,
    A.fact_transaction_id,
    participation_key,
    vrf_public_key,
    vote_first,
    vote_last,
    vote_keydilution,
    A._inserted_timestamp
FROM
    base A
    JOIN keyreg b
    ON A.block_id = b.block_id
    AND A.intra = b.intra
