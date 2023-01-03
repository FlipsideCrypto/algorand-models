{{ config(
    materialized = 'view'
) }}

SELECT
    {{ dbt_utils.surrogate_key(
        ['id']
    ) }} AS dim_period_id,
    id,
    slug,
    title,
    start_datetime,
    registration_end_datetime,
    active_state_end_datetime,
    end_datetime,
    sign_up_address,
    is_active,
    _INSERTED_TIMESTAMP,
    '{{ env_var("DBT_CLOUD_RUN_ID", "manual") }}' AS _audit_run_id
FROM
    {{ ref('silver__governance_period_base') }}
