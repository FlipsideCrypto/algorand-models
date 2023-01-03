{{ config(
    materialized = 'view'
) }}

SELECT
    {{ dbt_utils.surrogate_key(
        ['a.id']
    ) }} AS dim_topic_option_id,
    A.id,
    A.title,
    A.indicator,
    A.is_foundation_choice,
    COALESCE(
        b.dim_topic_id,
        '-1'
    ) AS dim_topic_id,
    A._INSERTED_TIMESTAMP,
    '{{ env_var("DBT_CLOUD_RUN_ID", "manual") }}' AS _audit_run_id
FROM
    {{ ref('silver__governance_topic_option') }} A
    LEFT JOIN {{ ref('governance__dim_topic') }}
    b
    ON A.topic_id = b.id
