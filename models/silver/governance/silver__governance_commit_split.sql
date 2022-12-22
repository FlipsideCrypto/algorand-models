{{ config(
    materialized = 'incremental',
    unique_key = ['block_id','intra'],
    incremental_strategy = 'delete+insert',
    cluster_by = ['_inserted_timestamp::DATE']
) }}

SELECT
    block_id,
    intra,
    tx_id,
    sender,
    receiver,
    INDEX,
    TRIM(
        REPLACE(
            CASE
                WHEN VALUE LIKE '%:%' THEN SPLIT_PART(
                    VALUE,
                    ':',
                    1
                )
                ELSE '0'
            END,
            '"'
        ) :: STRING
    ) AS key,
    TRIM(
        REPLACE(
            CASE
                WHEN VALUE LIKE '%:%' THEN SPLIT_PART(
                    VALUE,
                    ':',
                    2
                )
                ELSE VALUE
            END,
            '"'
        ) :: STRING
    ) AS VALUE,
    _inserted_timestamp
FROM
    {{ ref('silver__governance_actions') }} A
    LEFT JOIN LATERAL SPLIT_TO_TABLE(REPLACE(REPLACE(note, 'af/gov1:j{"com":'), '}'), ',')
WHERE
    invalid_txn = FALSE
    AND commitment = TRUE

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(
            _inserted_timestamp
        )
    FROM
        {{ this }}
)
{% endif %}
