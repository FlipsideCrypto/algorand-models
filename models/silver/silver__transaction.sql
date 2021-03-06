{{ config(
    materialized = 'incremental',
    unique_key = '_unique_key',
    incremental_strategy = 'merge',
    cluster_by = ['_inserted_timestamp::DATE'],
) }}

WITH allTXN AS (

    SELECT
        intra,
        ROUND AS block_id,
        txn :txn :grp :: STRING AS tx_group_id,
        b.txid :: text AS tx_id,
        'false' AS inner_tx,
        CASE
            WHEN txn :txn :type :: STRING = 'appl' THEN NULL
            WHEN txn :txn :type :: STRING = 'pay' THEN 0
            ELSE asset
        END AS asset_id,
        txn :txn :snd :: text AS sender,
        txn :txn :fee / pow(
            10,
            6
        ) AS fee,
        txn :txn :type :: STRING AS tx_type,
        txn :txn :gh :: STRING AS genesis_hash,
        txn AS tx_message,
        extra,
        __HEVO__LOADED_AT,
        DATEADD(
            ms,
            __HEVO__LOADED_AT,
            '1970-01-01'
        ) AS _inserted_timestamp
    FROM
        {{ ref('bronze__transaction') }}
        b
    WHERE
        txid IS NOT NULL
),
innertx AS (
    SELECT
        b.intra + INDEX + 1,
        b.round AS block_id,
        txn :txn :grp :: STRING AS tx_group_id,
        b.txid :: text AS tx_id,
        'TRUE' AS inner_tx,
        CASE
            WHEN VALUE :txn :type :: STRING = 'appl' THEN NULL
            WHEN VALUE :txn :type :: STRING = 'pay' THEN 0
            WHEN VALUE :txn :type :: STRING = 'afrz' THEN VALUE :txn :faid :: NUMBER
            WHEN VALUE :txn :type :: STRING = 'acfg' THEN COALESCE(
                VALUE :txn :caid :: NUMBER,
                VALUE :caid :: NUMBER
            )
            ELSE VALUE :txn :xaid :: STRING
        END AS asset_id,
        VALUE :txn :snd :: text AS sender,
        CASE
            WHEN VALUE :txn :fee IS NULL THEN 0
            ELSE VALUE :txn :fee / pow(
                10,
                6
            )
        END AS fee,
        VALUE :txn :type :: STRING AS tx_type,
        txn :txn :gh :: STRING AS genesis_hash,
        VALUE AS tx_message,
        extra,
        __HEVO__LOADED_AT,
        DATEADD(
            ms,
            __HEVO__LOADED_AT,
            '1970-01-01'
        ) AS _inserted_timestamp
    FROM
        {{ ref('bronze__transaction') }}
        b,
        LATERAL FLATTEN(
            input => txn :dt :itx
        ) f
    WHERE
        txn :dt :itx IS NOT NULL
        AND txid IS NOT NULL
),
uniontxn AS(
    SELECT
        *
    FROM
        allTXN
    UNION
    SELECT
        *
    FROM
        innertx
)
SELECT
    b.intra,
    b.block_id,
    tx_group_id,
    HEX_DECODE_STRING(
        tx_id
    ) AS tx_id,
    TO_BOOLEAN(inner_tx) AS inner_tx,
    asset_id :: NUMBER AS asset_id,
    algorand_decode_b64_addr(
        sender
    ) AS sender,
    fee,
    b.tx_type,
    tx_message,
    extra,
    concat_ws(
        '-',
        b.block_id :: STRING,
        b.intra :: STRING
    ) AS _unique_key,
    _inserted_timestamp
FROM
    uniontxn b

{% if is_incremental() %}
WHERE
    b._inserted_timestamp >= (
        (
            SELECT
                MAX(
                    _inserted_timestamp
                )
            FROM
                {{ this }}
        )
    )
{% endif %}
