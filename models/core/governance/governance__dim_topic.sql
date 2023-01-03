{{ config(
    materialized = 'view'
) }}

SELECT
    {{ dbt_utils.surrogate_key(
        ['a.id']
    ) }} AS dim_topic_id,
    A.id,
    A.title,
    A.description_html,
    COALESCE(
        b.dim_voting_session_id,
        '-1'
    ) AS dim_voting_session_id,
    A._INSERTED_TIMESTAMP,
    '{{ env_var("DBT_CLOUD_RUN_ID", "manual") }}' AS _audit_run_id
FROM
    {{ ref('silver__governance_topic') }} A
    LEFT JOIN {{ ref('governance__dim_voting_session') }}
    b
    ON A.voting_session_slug = b.slug
