{{ config(
    materialized = 'incremental',
    unique_key = ['block_id','intra'],
    incremental_strategy = 'merge',
    cluster_by = ['_inserted_timestamp::DATE']
) }}

WITH base AS (

    SELECT
        block_id,
        intra,
        tx_id,
        sender,
        receiver,
        TRY_PARSE_JSON(REPLACE(note, 'af/gov1:j')) :rewardsPrd :: STRING AS reward_period,
        TRY_PARSE_JSON(REPLACE(note, 'af/gov1:j')) :idx :: INT AS INDEX,
        TRY_PARSE_JSON(REPLACE(note, 'af/gov1:j')) :gov :: STRING AS govenor,
        amount :: INT / pow(
            10,
            6
        ) AS amount,
        _inserted_timestamp
    FROM
        {{ ref('silver__governance_actions') }} A
    WHERE
        invalid_txn = FALSE
        AND reward = TRUE

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
)
SELECT
    A.block_id,
    A.intra,
    A.tx_id,
    A.sender,
    A.receiver,
    A.reward_period,
    A.index,
    govenor,
    A.amount,
    C.governance_period_id,
    A._inserted_timestamp
FROM
    base A
    JOIN {{ ref('silver__block') }}
    b
    ON A.block_id = b.block_id
    JOIN (
        SELECT
            id AS governance_period_id,
            ROW_NUMBER() over(
                ORDER BY
                    start_datetime
            ) AS reward_period
        FROM
            {{ ref('silver__governance_period_base') }}
    ) C
    ON A.reward_period = C.reward_period
