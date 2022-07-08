{{ config(
    materialized = 'incremental',
    unique_key = 'fact_account_application_id',
    incremental_strategy = 'merge'
) }}

WITH base AS (

    SELECT
        algorand_decode_hex_addr(
            addr :: text
        ) AS address,
        app :: INT AS app_id,
        closed_at AS closed_at,
        created_at AS created_at,
        localstate AS app_info,
        _inserted_timestamp
    FROM
        {{ ref('bronze__account_application') }}

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
        ['a.address','a.app_id']
    ) }} AS fact_account_application_id,
    COALESCE(
        da.dim_account_id,
        {{ dbt_utils.surrogate_key(
            ['null']
        ) }}
    ) AS dim_account_id,
    COALESCE(
        dim_application_id,
        {{ dbt_utils.surrogate_key(
            ['null']
        ) }}
    ) AS dim_application_id,
    app_info,
    COALESCE(
        b.dim_block_id,
        {{ dbt_utils.surrogate_key(
            ['null']
        ) }}
    ) AS dim_block_id__closed_at,
    COALESCE(
        C.dim_block_id,
        {{ dbt_utils.surrogate_key(
            ['null']
        ) }}
    ) AS dim_block_id__created_at,
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
    LEFT JOIN {{ ref('core__dim_application') }}
    dap
    ON A.app_id = dap.app_id
