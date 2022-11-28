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
        asset_id,
        asset_amount AS amount,
        asset_receiver,
        sender,
        _inserted_timestamp
    FROM
        {{ ref('silver__transaction') }}
    WHERE
        tx_type = 'axfer'
        AND asset_amount IS NOT NULL
        AND (
            sender = 'ETGSQKACKC56JWGMDAEP5S2JVQWRKTQUVKCZTMPNUGZLDVCWPY63LSI3H4'
            OR asset_receiver = 'ETGSQKACKC56JWGMDAEP5S2JVQWRKTQUVKCZTMPNUGZLDVCWPY63LSI3H4'
        )
        AND NOT (
            sender = 'ETGSQKACKC56JWGMDAEP5S2JVQWRKTQUVKCZTMPNUGZLDVCWPY63LSI3H4'
            AND asset_receiver = 'ETGSQKACKC56JWGMDAEP5S2JVQWRKTQUVKCZTMPNUGZLDVCWPY63LSI3H4'
        )

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
),
algomint AS (
    SELECT
        block_id,
        intra,
        tx_id,
        asset_id,
        amount,
        asset_receiver bridger,
        sender AS bridge_address,
        'inbound' AS action,
        _inserted_timestamp
    FROM
        base
    WHERE
        sender = 'ETGSQKACKC56JWGMDAEP5S2JVQWRKTQUVKCZTMPNUGZLDVCWPY63LSI3H4'
    UNION ALL
    SELECT
        block_id,
        intra,
        tx_id,
        asset_id,
        amount,
        sender bridger,
        asset_receiver AS bridge_address,
        'outbound' AS action,
        _inserted_timestamp
    FROM
        base
    WHERE
        asset_receiver = 'ETGSQKACKC56JWGMDAEP5S2JVQWRKTQUVKCZTMPNUGZLDVCWPY63LSI3H4'
)
SELECT
    block_id,
    intra,
    tx_id,
    A.asset_id,
    CASE
        WHEN A.asset_id = 0 THEN A.amount / pow(
            10,
            6
        )
        WHEN sa.decimals > 0 THEN A.amount / pow(
            10,
            sa.decimals
        )
        ELSE A.amount
    END :: FLOAT AS amount,
    bridger,
    bridge_address,
    action,
    A._inserted_timestamp
FROM
    algomint A
    LEFT JOIN {{ ref('silver__asset') }}
    sa
    ON A.asset_id = sa.asset_id
