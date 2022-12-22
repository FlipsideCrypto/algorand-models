{{ config(
    materialized = 'incremental',
    unique_key = ['block_id','intra'],
    incremental_strategy = 'merge',
    cluster_by = ['_inserted_timestamp::DATE']
) }}

WITH algo_ben AS (

    SELECT
        block_id,
        intra,
        tx_id,
        sender,
        receiver,
        OBJECT_AGG(
            key :: STRING,
            VALUE :: variant
        ) AS j,
        j :"0" AS algo_amount,
        j :"bnf" AS benificiary_address,
        _inserted_timestamp
    FROM
        {{ ref('silver__governance_commit_split') }} A
    WHERE
        key IN ('0', 'bnf')

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
GROUP BY
    block_id,
    intra,
    tx_id,
    sender,
    receiver,
    _inserted_timestamp
),
non_algo AS (
    SELECT
        A.block_id,
        intra,
        OBJECT_AGG(
            key :: STRING,
            VALUE :: variant
        ) AS non_algo_assets,
        SUM(A.value :: INT * COALESCE(d.algo_ratio, 0)) AS non_algo_commit_algo_equivalent
    FROM
        {{ ref('silver__governance_commit_split') }} A
        JOIN {{ ref('silver__block') }}
        b
        ON A.block_id = b.block_id
        LEFT JOIN {{ ref('silver__governance_period_base') }} C
        ON b.block_timestamp BETWEEN C.start_datetime
        AND C.registration_end_datetime
        LEFT JOIN {{ ref('silver__governance_accepted_asset') }}
        d
        ON C.slug = d.governance_period_slug
        AND A.key = d.asset_id
    WHERE
        key NOT IN ('0', 'bnf')

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
GROUP BY
    A.block_id,
    intra
)
SELECT
    A.block_id,
    A.intra,
    A.tx_id,
    A.sender,
    A.receiver,
    A.algo_amount :: INT / pow(
        10,
        6
    ) AS commit_algo,
    b.non_algo_assets,
    b.non_algo_commit_algo_equivalent / pow(
        10,
        6
    ) AS non_algo_commit_algo_equivalent,
    A.algo_amount / pow(
        10,
        6
    ) + COALESCE(
        b.non_algo_commit_algo_equivalent,
        0
    ) / pow(
        10,
        6
    ) AS total_commit_in_algo,
    A.benificiary_address :: STRING AS benificiary_address,
    d.id AS governance_period_id,
    A._inserted_timestamp
FROM
    algo_ben A
    LEFT JOIN non_algo b
    ON A.block_id = b.block_id
    AND A.intra = b.intra
    JOIN {{ ref('silver__block') }} C
    ON A.block_id = C.block_id
    LEFT JOIN {{ ref('silver__governance_period_base') }}
    d
    ON C.block_timestamp BETWEEN d.start_datetime
    AND d.registration_end_datetime
