{{ config(
    materialized = 'incremental',
    unique_key = '_unique_key',
    incremental_strategy = 'merge',
    cluster_by = ['_inserted_timestamp::DATE']
) }}

WITH nft_trans AS (

    SELECT
        tx_group_id,
        fee,
        amount,
        sender,
        _INSERTED_TIMESTAMP
    FROM
        {{ ref('silver__transaction') }}
    WHERE
        tx_type = 'pay'
        AND receiver = 'XNFT36FUCFRR6CK675FW4BEBCCCOJ4HOSMGCN6J2W6ZMB34KM2ENTNQCP4'
        AND algorand_decode_b64_addr(
            tx_message :txn :close :: STRING
        ) IS NULL
        AND amount > 0

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
all_nft_txn AS (
    SELECT
        block_id,
        A.tx_group_id,
        SUM(
            A.amount
        ) total_sales_amount
    FROM
        {{ ref('silver__transaction') }} A
        JOIN (
            SELECT
                DISTINCT sender,
                tx_group_id
            FROM
                nft_trans
        ) b
        ON A.tx_group_id = b.tx_group_id
        AND A.sender = b.sender
    WHERE
        tx_type = 'pay'

{% if is_incremental() %}
AND A._INSERTED_TIMESTAMP >= (
    SELECT
        MAX(
            _INSERTED_TIMESTAMP
        )
    FROM
        {{ this }}
) - INTERVAL '4 HOURS'
{% endif %}
GROUP BY
    block_id,
    A.tx_group_id
),
buynow AS(
    SELECT
        sale.block_id,
        sale.tx_group_id,
        nft.asset_receiver AS purchaser,
        asset_id AS nft_asset_id,
        sale.total_sales_amount,
        SUM(
            nft.asset_amount
        ) AS number_of_nfts,
        MAX(
            nft._INSERTED_TIMESTAMP
        ) AS _INSERTED_TIMESTAMP
    FROM
        all_nft_txn sale
        JOIN {{ ref('silver__transaction') }}
        nft
        ON sale.tx_group_id = nft.tx_group_id
    WHERE
        tx_type = 'axfer'
        AND asset_amount > 0
    GROUP BY
        sale.block_id,
        sale.tx_group_id,
        nft.asset_receiver,
        asset_id,
        sale.total_sales_amount
),
nft_trans_auc AS (
    SELECT
        tx_message :ca :: DECIMAL / pow(
            10,
            6
        ) AS close_amount,
        tx_group_id
    FROM
        {{ ref('silver__transaction') }}
        pt
    WHERE
        tx_type = 'pay'
        AND algorand_decode_b64_addr(
            tx_message :txn :close :: STRING
        ) = 'XNFT36FUCFRR6CK675FW4BEBCCCOJ4HOSMGCN6J2W6ZMB34KM2ENTNQCP4'
        AND amount IS NOT NULL

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
all_nft_txn_auc AS (
    SELECT
        block_id,
        A.tx_group_id,
        amount,
        _INSERTED_TIMESTAMP
    FROM
        {{ ref('silver__transaction') }} A
    WHERE
        tx_type = 'pay'
        AND tx_group_id IN (
            SELECT
                DISTINCT tx_group_id
            FROM
                nft_trans_auc
        )
        AND receiver != 'XNFT36FUCFRR6CK675FW4BEBCCCOJ4HOSMGCN6J2W6ZMB34KM2ENTNQCP4'

{% if is_incremental() %}
AND A._INSERTED_TIMESTAMP >= (
    SELECT
        MAX(
            _INSERTED_TIMESTAMP
        )
    FROM
        {{ this }}
) - INTERVAL '4 HOURS'
{% endif %}
),
sales_auc AS (
    SELECT
        sales.block_id,
        sales.tx_group_id,
        SUM(
            sales.amount
        ) + SUM(
            close_a.close_amount
        ) / COUNT(
            close_a.close_amount
        ) AS sales,
        MAX(_INSERTED_TIMESTAMP) AS _INSERTED_TIMESTAMP
    FROM
        all_nft_txn_auc sales
        JOIN nft_trans_auc close_a
        ON sales.tx_group_id = close_a.tx_group_id
    GROUP BY
        sales.block_id,
        sales.tx_group_id
),
auc_sales AS(
    SELECT
        sales.block_id,
        sales.tx_group_id,
        nft.asset_receiver AS purchaser,
        ast.asset_id AS nft_asset_id,
        sales.sales total_sales_amount,
        SUM(
            nft.asset_amount
        ) AS number_of_nfts,
        MAX(
            sales._INSERTED_TIMESTAMP
        ) AS _INSERTED_TIMESTAMP,
        decimals
    FROM
        sales_auc sales
        JOIN {{ ref('silver__transaction') }}
        nft
        ON sales.tx_group_id = nft.tx_group_id
        JOIN {{ ref('silver__asset') }}
        ast
        ON nft.asset_id = ast.asset_id
    WHERE
        tx_type = 'axfer'
        AND sales IS NOT NULL
        AND asset_amount IS NOT NULL
    GROUP BY
        sales.block_id,
        sales.tx_group_id,
        nft.asset_receiver,
        ast.asset_id,
        sales.sales,
        decimals
)
SELECT
    block_id,
    tx_group_id,
    'buy now' event_type,
    purchaser,
    nft.nft_asset_id,
    total_sales_amount :: FLOAT / pow(
        10,
        6
    ) AS total_sales_amount,
    CASE
        WHEN ast.decimals > 0 THEN number_of_nfts :: FLOAT / pow(
            10,
            ast.decimals
        )
        WHEN NULLIF(
            ast.decimals,
            0
        ) IS NULL THEN number_of_nfts :: FLOAT
    END AS number_of_nfts,
    concat_ws(
        '-',
        block_id :: STRING,
        tx_group_id :: STRING,
        nft.nft_asset_id :: STRING
    ) AS _unique_key,
    nft._INSERTED_TIMESTAMP
FROM
    buynow nft
    JOIN {{ ref('silver__asset') }}
    ast
    ON nft.nft_asset_id = ast.asset_id
WHERE
    is_nft = TRUE
UNION ALL
SELECT
    block_id,
    tx_group_id,
    'auction' event_type,
    purchaser,
    nft.nft_asset_id,
    total_sales_amount :: FLOAT / pow(
        10,
        6
    ) AS total_sales_amount,
    CASE
        WHEN decimals > 0 THEN number_of_nfts :: FLOAT / pow(
            10,
            decimals
        )
        WHEN NULLIF(
            decimals,
            0
        ) IS NULL THEN number_of_nfts :: FLOAT
    END AS number_of_nfts,
    concat_ws(
        '-',
        block_id :: STRING,
        tx_group_id :: STRING,
        nft.nft_asset_id :: STRING
    ) AS _unique_key,
    nft._INSERTED_TIMESTAMP
FROM
    auc_sales nft
