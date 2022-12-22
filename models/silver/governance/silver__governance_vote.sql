{{ config(
    materialized = 'incremental',
    unique_key = ['block_id','intra'],
    incremental_strategy = 'delete+insert',
    cluster_by = ['_inserted_timestamp::DATE']
) }}

WITH base AS (

    SELECT
        block_id,
        intra,
        tx_id,
        sender,
        receiver,
        SPLIT_PART(REPLACE(REPLACE(note, 'af/gov1:j['), ']'), ',', 1) AS voting_session_id,
        b.index AS topic_no,
        COALESCE(
            C.key,
            b.value
        ) AS vote_value,
        C.value AS vote_algo_amount,
        _inserted_timestamp
    FROM
        {{ ref('silver__governance_actions') }} A
        LEFT JOIN LATERAL FLATTEN(input => PARSE_JSON(REPLACE(note, 'af/gov1:j')), outer => TRUE) b
        LEFT JOIN LATERAL FLATTEN(input => TRY_PARSE_JSON(b.value), outer => TRUE) C
    WHERE
        invalid_txn = FALSE
        AND vote = TRUE

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
    A.voting_session_id,
    d.id AS topic_id,
    A.topic_no,
    A.vote_value,
    A.vote_algo_amount,
    A._inserted_timestamp
FROM
    base A
    JOIN {{ ref('silver__block') }}
    b
    ON A.block_id = b.block_id
    JOIN {{ ref('silver__governance_voting_session') }} C
    ON b.block_timestamp BETWEEN C.voting_start_datetime
    AND C.voting_end_datetime
    JOIN (
        SELECT
            voting_session_slug,
            id,
            ROW_NUMBER() over(
                PARTITION BY voting_session_slug
                ORDER BY
                    id
            ) AS topic_no
        FROM
            {{ ref('silver__governance_topic') }}
    ) d
    ON C.slug = d.voting_session_slug
    AND A.topic_no = d.topic_no
