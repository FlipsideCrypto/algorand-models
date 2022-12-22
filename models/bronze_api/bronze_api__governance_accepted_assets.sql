{{ config(
    materialized = 'table'
) }}

SELECT
    slug,
    ethereum.streamline.udf_api(
        'GET',
        CONCAT(
            'https://governance.algorand.foundation/api/periods/',
            slug,
            '/accepted-assets'
        ),{},{}
    ) AS DATA,
    SYSDATE() AS _inserted_timestamp
FROM
    (
        SELECT
            slug
        FROM
            {{ ref('silver__governance_period_base') }}

{% if is_incremental() %}
WHERE
    CURRENT_DATE() BETWEEN start_time
    AND DATEADD(
        'day',
        3,
        end_time
    )
{% endif %}
)
