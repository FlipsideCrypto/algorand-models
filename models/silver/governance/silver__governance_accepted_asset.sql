{{ config(
    materialized = 'incremental',
    unique_key = ['governance_period_slug'],
    incremental_strategy = 'delete+insert'
) }}

SELECT
    slug AS governance_period_slug,
    VALUE :algo_ratio :: FLOAT AS algo_ratio,
    VALUE :asset_id :: INT AS asset_id,
    VALUE :logo :: STRING AS logo,
    VALUE :name :: STRING AS NAME,
    VALUE :organization :: STRING AS ORGANIZATION,
    _inserted_timestamp
FROM
    {{ source(
        'bronze_api',
        'governance_accepted_assets'
    ) }},
    LATERAL FLATTEN(
        DATA :data :results
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
