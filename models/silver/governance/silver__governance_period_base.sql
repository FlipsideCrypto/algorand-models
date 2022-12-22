{{ config(
    materialized = 'incremental',
    unique_key = 'id',
    incremental_strategy = 'merge'
) }}

SELECT
    VALUE :active_state_end_datetime :: datetime AS active_state_end_datetime,
    VALUE :end_datetime :: datetime AS end_datetime,
    VALUE :id :: STRING AS id,
    VALUE :is_active :: BOOLEAN AS is_active,
    VALUE :registration_end_datetime :: datetime AS registration_end_datetime,
    VALUE :sign_up_address :: STRING AS sign_up_address,
    VALUE :slug :: STRING AS slug,
    VALUE :start_datetime :: datetime AS start_datetime,
    VALUE :title :: STRING AS title,
    _inserted_timestamp
FROM
    {{ source(
        'bronze_api',
        'governance_periods_list'
    ) }},
    LATERAL FLATTEN(DATA :data :results)

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
