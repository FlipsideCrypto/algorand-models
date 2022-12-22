{{ config(
    materialized = 'incremental',
    unique_key = 'id',
    incremental_strategy = 'merge'
) }}

SELECT
    id AS topic_id,
    VALUE :id :: STRING AS id,
    VALUE :indicator :: STRING AS INDICATOR,
    VALUE :is_foundation_choice :: BOOLEAN AS is_foundation_choice,
    VALUE :title :: STRING AS title,
    VALUE :vote_percentage :: FLOAT AS vote_percentage,
    _inserted_timestamp
FROM
    {{ ref('silver__governance_topic') }},
    LATERAL FLATTEN(
        topic_options
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
