{{ config(
    materialized = 'incremental',
    unique_key = 'id',
    incremental_strategy = 'merge'
) }}

SELECT
    slug AS governance_period_slug,
    VALUE :cooldown_end_datetime :: datetime AS cooldown_end_datetime,
    VALUE :id :: STRING AS id,
    VALUE :short_description :: STRING AS short_description,
    VALUE :slug :: STRING AS slug,
    VALUE :title :: STRING AS title,
    VALUE :topic_count :: INT AS topic_count,
    VALUE :voting_end_datetime :: datetime AS voting_end_datetime,
    VALUE :voting_start_datetime :: datetime AS voting_start_datetime,
    _inserted_timestamp
FROM
    {{ source(
        'bronze_api',
        'governance_period'
    ) }},
    LATERAL FLATTEN(
        DATA :data :voting_sessions
    )

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
