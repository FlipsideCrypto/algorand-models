{{ config(
    materialized = 'table'
) }}

SELECT
    slug,
    ethereum.streamline.udf_api(
        'GET',
        CONCAT(
            'https://governance.algorand.foundation/api/voting-sessions/',
            slug
        ),{},{}
    ) AS DATA,
    SYSDATE() AS _inserted_timestamp
FROM
    (
        SELECT
            slug
        FROM
            {{ ref('silver__governance_voting_session') }}

{% if is_incremental() %}
WHERE
    CURRENT_DATE() >=
    AND DATEADD(
        'day',
        3,
        voting_end_datetime
    )
{% endif %}
)
