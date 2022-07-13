{{ config(
    materialized = 'incremental',
    unique_key = 'fact_transaction_application_call_id',
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
        tx_type = 'appl'

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
app_ids AS (
    SELECT
        block_id,
        intra,
        COALESCE(
            tx_message :txn :apid,
            tx_message :apid,
            tx_message :"dt" :"gd" :"aWQ=" :"ui"
        ) app_id,
        _inserted_timestamp
    FROM
        {{ ref('silver__transaction') }}
    WHERE
        tx_type = 'appl'

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
    ) }} AS fact_transaction_application_call_id,
    A.fact_transaction_id,
    COALESCE(
        C.dim_application_id,
        {{ dbt_utils.surrogate_key(
            ['null']
        ) }}
    ) dim_application_id,
    A._inserted_timestamp
FROM
    base A
    LEFT JOIN app_ids b
    ON A.block_id = b.block_id
    AND A.intra = b.intra
    LEFT JOIN {{ ref('core__dim_application') }} C
    ON b.app_id = C.app_id
