{{ config(
    materialized = 'incremental',
    unique_key = 'fact_transaction_id',
    incremental_strategy = 'merge'
) }}

WITH base AS (

    SELECT
        intra,
        block_id,
        tx_group_id,
        tx_id,
        inner_tx,
        asset_id,
        sender,
        fee,
        tx_type,
        tx_message,
        extra,
        _inserted_timestamp
    FROM
        {{ ref('silver__transaction') }}

{% if is_incremental() %}
WHERE
    _inserted_timestamp >= (
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
        ['a.block_id','a.intra']
    ) }} AS fact_transaction_id,
    COALESCE(
        b.dim_block_id,
        {{ dbt_utils.surrogate_key(
            ['null']
        ) }}
    ) AS dim_block_id,
    intra,
    tx_group_id,
    tx_id,
    inner_tx,
    COALESCE(
        da.dim_account_id,
        {{ dbt_utils.surrogate_key(
            ['null']
        ) }}
    ) AS dim_account_id__tx_sender,
    COALESCE(
        dim_asset_id,
        {{ dbt_utils.surrogate_key(
            ['null']
        ) }}
    ) AS dim_asset_id,
    fee,
    COALESCE(
        dim_transaction_type_id,
        {{ dbt_utils.surrogate_key(
            ['null']
        ) }}
    ) AS dim_transaction_type_id,
    tx_message,
    extra,
    A._inserted_timestamp
FROM
    base A
    LEFT JOIN {{ ref('core__dim_block') }}
    b
    ON A.block_id = b.block_id
    LEFT JOIN {{ ref('core__dim_account') }}
    da
    ON A.sender = da.address
    LEFT JOIN {{ ref('core__dim_asset') }}
    das
    ON A.asset_id = das.asset_id
    LEFT JOIN {{ ref('core__dim_transaction_type') }}
    dtt
    ON A.tx_type = dtt.tx_type
