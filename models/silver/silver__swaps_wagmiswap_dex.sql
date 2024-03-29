{{ config(
    materialized = 'incremental',
    unique_key = '_unique_key',
    incremental_strategy = 'merge',
    cluster_by = ['_inserted_timestamp::DATE']
) }}

WITH wagmi_app_ids AS (

    SELECT
        DISTINCT app_id
    FROM
        {{ ref('silver__application') }}
    WHERE
        creator_address = 'DKUK6HUCW4USCSMWJQN5JL2GII52QPRGGNJZG6F2TLLWKWJ4XDV2YYOBKA'

{% if is_incremental() %}
AND _INSERTED_TIMESTAMP >= (
    SELECT
        MAX(
            _INSERTED_TIMESTAMP
        )
    FROM
        {{ this }}
) - INTERVAL '4 HOURS'
{% endif %}
),
tx_app_call AS (
    SELECT
        *
    FROM
        {{ ref('silver__transaction') }}
    WHERE
        tx_type = 'appl'

{% if is_incremental() %}
AND _INSERTED_TIMESTAMP >= (
    SELECT
        MAX(
            _INSERTED_TIMESTAMP
        )
    FROM
        {{ this }}
) - INTERVAL '4 HOURS'
{% endif %}
),
tx_pay AS (
    SELECT
        *
    FROM
        {{ ref('silver__transaction') }}
    WHERE
        tx_type = 'pay'

{% if is_incremental() %}
AND _INSERTED_TIMESTAMP >= (
    SELECT
        MAX(
            _INSERTED_TIMESTAMP
        )
    FROM
        {{ this }}
) - INTERVAL '4 HOURS'
{% endif %}
),
tx_a_tfer AS (
    SELECT
        pt.*,
        A.asset_name,
        A.decimals
    FROM
        {{ ref('silver__transaction') }}
        pt
        JOIN {{ ref('silver__asset') }} A
        ON pt.asset_id = A.asset_id
    WHERE
        tx_type = 'axfer'

{% if is_incremental() %}
AND pt._INSERTED_TIMESTAMP >= (
    SELECT
        MAX(
            _INSERTED_TIMESTAMP
        )
    FROM
        {{ this }}
) - INTERVAL '4 HOURS'
{% endif %}
),
wagmi_app AS(
    SELECT
        block_id,
        intra,
        tx_group_id,
        act._INSERTED_TIMESTAMP,
        sender AS swapper,
        app_id,
        CASE
            WHEN tx_message :dt :itx [0] :txn :type :: STRING = 'axfer' THEN asset_name
            WHEN tx_message :dt :itx [0] :txn :type :: STRING = 'pay' THEN 'ALGO'
        END AS to_asset_name,
        CASE
            WHEN tx_message :dt :itx [0] :txn :type :: STRING = 'axfer' THEN tx_message :dt :itx [0] :txn :xaid :: NUMBER
            WHEN tx_message :dt :itx [0] :txn :type :: STRING = 'pay' THEN 0
        END AS to_asset_id,
        CASE
            WHEN tx_message :dt :itx [0] :txn :type :: STRING = 'axfer'
            AND decimals > 0 THEN tx_message :dt :itx [0] :txn :aamt :: FLOAT / pow(
                10,
                decimals
            )
            WHEN tx_message :dt :itx [0] :txn :type :: STRING = 'axfer'
            AND decimals = 0 THEN tx_message :dt :itx [0] :txn :aamt :: FLOAT
            WHEN tx_message :dt :itx [0] :txn :type :: STRING = 'pay' THEN tx_message :dt :itx [0] :txn :amt :: FLOAT / pow(
                10,
                6
            )
        END AS swap_to_amount,
        algorand_decode_b64_addr(
            tx_message :dt :itx [0] :txn :snd :: STRING
        ) AS pool_address
    FROM
        tx_app_call act
        LEFT JOIN {{ ref('silver__asset') }}
        asa
        ON act.tx_message :dt :itx [0] :txn :xaid :: NUMBER = asa.asset_id
    WHERE
        app_id IN (
            SELECT
                app_id
            FROM
                wagmi_app_ids
        )
        AND TRY_BASE64_DECODE_STRING(
            tx_message :txn :apaa [0] :: STRING
        ) ILIKE 'swap'
        AND inner_tx = 'FALSE'
),
from_pay_swaps AS(
    SELECT
        wa.tx_group_id AS tx_group_id,
        pt.intra,
        pt.sender AS swapper,
        'ALGO' AS from_asset_name,
        amount :: FLOAT / pow(
            10,
            6
        ) AS swap_from_amount,
        0 AS from_asset_id
    FROM
        wagmi_app wa
        JOIN tx_pay pt
        ON wa.tx_group_id = pt.tx_group_id
        AND wa.swapper = pt.sender
        AND wa.intra -1 = pt.intra
    WHERE
        pt.inner_tx = 'FALSE'
),
from_axfer_swaps AS(
    SELECT
        wa.tx_group_id AS tx_group_id,
        pt.intra,
        pt.sender AS swapper,
        asset_name AS from_asset_name,
        CASE
            WHEN decimals > 0 THEN asset_amount / pow(
                10,
                decimals
            )
            ELSE asset_amount
        END AS from_amount,
        pt.asset_id AS from_asset_id
    FROM
        wagmi_app wa
        JOIN tx_a_tfer pt
        ON wa.tx_group_id = pt.tx_group_id
        AND wa.intra -1 = pt.intra
        AND wa.swapper = pt.sender
    WHERE
        pt.inner_tx = 'FALSE'
),
from_swaps AS(
    SELECT
        *
    FROM
        from_pay_swaps
    UNION
    SELECT
        *
    FROM
        from_axfer_swaps
)
SELECT
    wa.block_id AS block_id,
    wa.intra AS intra,
    wa.tx_group_id AS tx_group_id,
    wa.app_id,
    fs.swapper,
    fs.from_asset_id AS swap_from_asset_id,
    fs.swap_from_amount :: FLOAT AS swap_from_amount,
    wa.pool_address AS pool_address,
    wa.to_asset_id AS swap_to_asset_id,
    wa.swap_to_amount :: FLOAT AS swap_to_amount,
    concat_ws(
        '-',
        wa.block_id :: STRING,
        wa.intra :: STRING
    ) AS _unique_key,
    wa._INSERTED_TIMESTAMP
FROM
    wagmi_app wa
    LEFT JOIN from_swaps fs
    ON wa.tx_group_id = fs.tx_group_id
    AND wa.intra -1 = fs.intra
