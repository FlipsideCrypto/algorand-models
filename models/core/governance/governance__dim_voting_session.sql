{{ config(
    materialized = 'view'
) }}

SELECT
    {{ dbt_utils.surrogate_key(
        ['a.id']
    ) }} AS dim_voting_session_id,
    A.id,
    A.slug,
    A.title,
    A.short_description,
    A.voting_start_datetime,
    A.voting_end_datetime,
    A.cooldown_end_datetime,
    COALESCE(
        b.dim_period_id,
        '-1'
    ) AS dim_period_id,
    A._INSERTED_TIMESTAMP,
    '{{ env_var("DBT_CLOUD_RUN_ID", "manual") }}' AS _audit_run_id
FROM
    {{ ref('silver__governance_voting_session') }} A
    LEFT JOIN {{ ref('governance__dim_period') }}
    b
    ON A.governance_period_slug = b.slug
