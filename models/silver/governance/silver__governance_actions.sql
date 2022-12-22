{{ config(
    materialized = 'incremental',
    unique_key = ['block_id','intra'],
    incremental_strategy = 'merge',
    cluster_by = ['_inserted_timestamp::DATE']
) }}

SELECT
    block_id,
    intra,
    tx_id,
    sender,
    receiver,
    amount,
    TRY_BASE64_DECODE_STRING(
        tx_message :txn :note :: STRING
    ) AS note,
    CASE
        WHEN note LIKE '%com%' THEN TRUE
        ELSE FALSE
    END commitment,
    CASE
        WHEN REPLACE(
            note,
            ',"bnf"'
        ) LIKE '%com%,%' THEN TRUE
        ELSE FALSE
    END non_algo_commit,
    CASE
        WHEN note ILIKE '%reward%' THEN TRUE
        ELSE FALSE
    END reward,
    CASE
        WHEN commitment = FALSE
        AND reward = FALSE THEN TRUE
        ELSE FALSE
    END vote,
    CASE
        WHEN note LIKE '%bnf%' THEN TRUE
        ELSE FALSE
    END benificiary_defined,
    CASE
        WHEN note LIKE '%com%[%'
        OR note LIKE '%COM%'
        OR note LIKE '%covm%'
        OR note LIKE '%NaN}'
        OR REGEXP_LIKE(
            note,
            'af\/gov1:j\{"[0-9].*'
        ) = TRUE
        OR REGEXP_LIKE(
            note,
            'af\/gov1:j\["[0-9][0-9][0-9].*'
        ) = TRUE
        OR REGEXP_LIKE(
            note,
            'af\/gov1:j \["[0-9].*'
        ) = TRUE THEN TRUE
        WHEN REGEXP_LIKE(
            note,
            'af.*[A-Z0-9]{58}.*'
        )
        AND note NOT LIKE '%"bnf":%'
        AND note NOT LIKE '%"gov":%' THEN TRUE
        WHEN TRY_PARSE_JSON(REGEXP_REPLACE(note, 'af\/gov1:j')) IS NULL
        OR TRY_PARSE_JSON(REGEXP_REPLACE(note, 'af\/gov1:j')) = '{}' THEN TRUE
        WHEN note NOT LIKE '%]'
        AND note NOT LIKE '%}' THEN TRUE
        WHEN note LIKE '%com%'
        AND (REGEXP_LIKE(note, 'af\/gov1:j\{"com":[0-9].*') = FALSE
        AND REGEXP_LIKE(note, 'af\/gov1:j\{"com": [0-9].*') = FALSE
        AND REGEXP_LIKE(note, 'af\/gov1:j\{"com":"[0-9].*') = FALSE) THEN TRUE
        ELSE FALSE
    END AS invalid_txn,
    _inserted_timestamp
FROM
    {{ ref('silver__transaction') }}
WHERE
    tx_type = 'pay'
    AND note LIKE 'af/gov1:%'

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
