{{ config(
    materialized = 'incremental',
    unique_key = ['governance_period_slug','sender'],
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
        voting_session_id,
        topic_id,
        topic_no,
        vote_value,
        vote_algo_amount,
        _inserted_timestamp
    FROM
        {{ ref('silver__governance_vote') }} A

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
)
SELECT
    block_id,
    intra,
    tx_id,
    sender,
    receiver,
    voting_session_id,
    topic_id,
    topic_no,
    vote_value,
    vote_algo_amount,
    A._INSERTED_TIMESTAMP
FROM
    base A qualify(ROW_NUMBER() over(PARTITION BY voting_session_id, A.sender
ORDER BY
    A.block_id DESC) = 1)
