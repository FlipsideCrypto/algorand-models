{{ config(
    materialized = 'view'
) }}

SELECT
    {{ dbt_utils.surrogate_key(
        ['a.asset_id','a.governance_period_slug']
    ) }} AS dim_accepted_asset_id,
    A.asset_id,
    A.algo_ratio,
    A.organization,
    COALESCE(
        b.dim_period_id,
        '-1'
    ) AS dim_period_id,
    COALESCE(
        C.dim_asset_id,
        '-1'
    ) AS dim_asset_id,
    A._INSERTED_TIMESTAMP,
    '{{ env_var("DBT_CLOUD_RUN_ID", "manual") }}' AS _audit_run_id
FROM
    {{ ref('silver__governance_accepted_asset') }} A
    LEFT JOIN {{ ref('governance__dim_period') }}
    b
    ON A.governance_period_slug = b.slug
    LEFT JOIN {{ ref('core__dim_asset') }} C
    ON A.asset_id = C.asset_id
