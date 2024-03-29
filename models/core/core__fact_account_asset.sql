{{ config(
    materialized = 'incremental',
    unique_key = 'fact_account_asset_id',
    incremental_strategy = 'merge',
    cluster_by = ['asset_added_at::DATE']
) }}

WITH base AS (

    SELECT
        address,
        asset_id,
        amount,
        closed_at,
        created_at,
        asset_closed,
        frozen,
        _inserted_timestamp
    FROM
        {{ ref('silver__account_asset') }}

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
    OR address || '--' || asset_id IN (
        SELECT
            address || '--' || asset_id
        FROM
            {{ this }}
        WHERE
            dim_account_id = '-1'
            OR dim_asset_id = '-1'
            OR dim_block_id__asset_added_at = '-1'
    )
{% endif %}
),
add_algo AS (
    SELECT
        address,
        0 AS asset_id,
        microalgos :: INT / pow(
            10,
            6
        ) AS amount,
        closed_at,
        created_at,
        FALSE AS asset_closed,
        FALSE AS frozen,
        _inserted_timestamp
    FROM
        {{ ref('silver__account') }}

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
    OR address || '--' || asset_id IN (
        SELECT
            address || '--' || asset_id
        FROM
            {{ this }}
        WHERE
            dim_account_id = '-1'
            OR dim_asset_id = '-1'
            OR dim_block_id__asset_added_at = '-1'
    )
{% endif %}
),
BOTH AS (
    SELECT
        address,
        asset_id,
        amount,
        closed_at,
        created_at,
        asset_closed,
        frozen,
        _inserted_timestamp
    FROM
        base
    UNION ALL
    SELECT
        address,
        asset_id,
        amount,
        closed_at,
        created_at,
        asset_closed,
        frozen,
        _inserted_timestamp
    FROM
        add_algo
)
SELECT
    {{ dbt_utils.surrogate_key(
        ['a.address','a.asset_id']
    ) }} AS fact_account_asset_id,
    COALESCE(
        da.dim_account_id,
        '-1'
    ) AS dim_account_id,
    A.address,
    COALESCE(
        dim_asset_id,
        '-1'
    ) AS dim_asset_id,
    A.asset_id,
    amount,
    COALESCE(
        C.dim_block_id,
        '-1'
    ) AS dim_block_id__asset_added_at,
    C.block_timestamp AS asset_added_at,
    COALESCE(
        b.dim_block_id,
        '-2'
    ) AS dim_block_id__asset_last_removed,
    b.block_timestamp AS asset_last_removed,
    asset_closed,
    frozen,
    A._inserted_timestamp,
    '{{ env_var("DBT_CLOUD_RUN_ID", "manual") }}' AS _audit_run_id
FROM
    BOTH A
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
