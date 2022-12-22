{{ config(
    materialized = 'incremental',
    unique_key = ['governance_period_slug','sender'],
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
        commit_algo,
        non_algo_assets,
        non_algo_commit_algo_equivalent,
        total_commit_in_algo,
        benificiary_address,
        governance_period_id,
        _INSERTED_TIMESTAMP
    FROM
        {{ ref('silver__governance_commit') }} A

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
    A.block_id,
    intra,
    tx_id,
    sender,
    receiver,
    commit_algo,
    non_algo_assets,
    non_algo_commit_algo_equivalent,
    total_commit_in_algo,
    benificiary_address,
    A.governance_period_id,
    A._INSERTED_TIMESTAMP
FROM
    base A qualify(ROW_NUMBER() over(PARTITION BY A.governance_period_id, A.sender
ORDER BY
    A.block_id DESC) = 1)
