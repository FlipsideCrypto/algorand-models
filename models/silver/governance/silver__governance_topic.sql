{{ config(
    materialized = 'incremental',
    unique_key = 'id',
    incremental_strategy = 'merge'
) }}

SELECT
    slug AS voting_session_slug,
    VALUE :description_html :: STRING AS description_html,
    VALUE :id :: STRING AS id,
    VALUE :title :: STRING AS title,
    VALUE :topic_options AS topic_options,
    VALUE :total_vote_count :: INT AS total_vote_count,
    _inserted_timestamp
FROM
    {{ source(
        'bronze_api',
        'governance_voting_session'
    ) }},
    LATERAL FLATTEN(
        DATA :data :topics
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
