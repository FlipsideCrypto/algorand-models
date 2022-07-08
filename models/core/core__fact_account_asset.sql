{{ config(
    materialized = 'incremental',
    unique_key = 'fact_account_asset_id',
    incremental_strategy = 'merge'
) }}

WITH base AS (

    SELECT
        algorand_decode_hex_addr(
            addr :: text
        ) AS address,
        assetid AS asset_id,
        amount :: NUMBER AS amount,
        closed_at,
        created_at,
        deleted AS asset_closed,
        frozen,
        _inserted_timestamp
    FROM
        {{ ref('bronze__account_asset') }}

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
        ['a.address','a.asset_id']
    ) }} AS fact_account_asset_id,
    COALESCE(
        da.dim_account_id,
        {{ dbt_utils.surrogate_key(
            ['null']
        ) }}
    ) AS dim_account_id,
    COALESCE(
        dim_asset_id,
        {{ dbt_utils.surrogate_key(
            ['null']
        ) }}
    ) AS dim_asset_id,
    COALESCE(
        C.dim_block_id,
        {{ dbt_utils.surrogate_key(
            ['null']
        ) }}
    ) AS dim_block_id__asset_added_at,
    COALESCE(
        b.dim_block_id,
        {{ dbt_utils.surrogate_key(
            ['null']
        ) }}
    ) AS dim_block_id__asset_last_removed,
    asset_closed,
    frozen,
    A._inserted_timestamp
FROM
    base A
    LEFT JOIN {{ ref('core__dim_block') }}
    b
    ON A.closed_at = b.block_id
    LEFT JOIN {{ ref('core__dim_block') }} C
    ON A.created_at = C.block_id
    LEFT JOIN {{ ref('core__dim_account') }}
    da
    ON A.address = da.address
    LEFT JOIN {{ ref('core__dim_asset') }}
    das
    ON A.asset_id = das.asset_id
