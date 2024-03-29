{{ config(
    materialized = 'incremental',
    unique_key = '_unique_key',
    incremental_strategy = 'merge',
    cluster_by = ['_inserted_timestamp::DATE']
) }}

WITH gen1 AS (

    SELECT
        asset_id,
        decimals,
        'gen1' AS gen
    FROM
        {{ ref('silver__asset') }}
    WHERE
        asset_name LIKE '%ctorand%'
        AND creator_address = 'X5YPUJ2HTFBY66WKWZOAA75WST5V7HWAGS2346SQFK622VNIRQ5ASXHTGA'

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
gen2 AS (
    SELECT
        asset_id,
        decimals,
        'gen2' AS gen
    FROM
        {{ ref('silver__asset') }}
    WHERE
        asset_name LIKE '%cto Prime%'
        AND creator_address IN (
            'XIUCOQPPZO2UNFD2TXQAEW7W5MPGZROVD2YUOGME22GNORYCJVMEYK3P5U',
            'UFFXUBZ5DFRLOQOB4LOC7GA3HTWMEEE54U3DJRTL27RKKV4UWOIID3I4FU',
            '6DGJ4FUQP623YFFIZXXOJ7OK63VILGT2FDGYCYI62VW2767DRBZFDTRMI4',
            'AB4T4VD7LRGHH75Z3KISVPNDENGY4W227RPAJEBYUDVKVNF2PWDKMHTO4A',
            'KPCXKFGBLR3WZN74BHG3RTKVOK6PW3UP53BHAYK7BLYDUCOTXJYKJU7JUY',
            'VOKX5CEPHTY6WJNZU4SQGCHCBK5MWNYXXIBUFQAMVTOCVP6VS6MFEEAFLM',
            'VVCR4Q2GYOQO3ENWQDQEFFGTNDJRA56QIYHUQ3RCZT36I6WXBAUU2FS7QE',
            'ZI35SDCVSLRTKUQWCA6SXYX2VUKDJ5JJEWDMDH6ZYMXTQBQDAE6GWUEU6I'
        )

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
raw_data AS (
    SELECT
        DISTINCT x.block_id,
        x.tx_group_id,
        x.asset_receiver AS purchaser,
        nft.asset_id AS nft_asset_id,
        decimals,
        x.asset_amount AS number_of_nfts,
        gen AS generation,
        SUM(
            y.amount
        ) AS total_sales_amount,
        MAX(
            x._INSERTED_TIMESTAMP
        ) AS _INSERTED_TIMESTAMP
    FROM
        {{ ref('silver__transaction') }}
        x
        JOIN(
            SELECT
                asset_id,
                decimals,
                gen
            FROM
                gen1
            UNION ALL
            SELECT
                asset_id,
                decimals,
                gen
            FROM
                gen2
        ) nft
        ON x.asset_id = nft.asset_id
        JOIN {{ ref('silver__transaction') }}
        y
        ON x.tx_group_id = y.tx_group_id
        JOIN (
            SELECT
                DISTINCT tx_group_id
            FROM
                {{ ref('silver__transaction') }}
            WHERE
                tx_type = 'appl'
        ) app_call
        ON x.tx_group_id = app_call.tx_group_id
    WHERE
        x.tx_type = 'axfer'
        AND y.tx_type = 'pay'
        AND x.asset_amount > 0
        AND y.tx_message :txn :amt IS NOT NULL

{% if is_incremental() %}
AND x._INSERTED_TIMESTAMP >= (
    SELECT
        MAX(
            _INSERTED_TIMESTAMP
        )
    FROM
        {{ this }}
) - INTERVAL '4 HOURS'
{% endif %}
GROUP BY
    x.block_id,
    x.tx_group_id,
    x.asset_receiver,
    nft.asset_id,
    decimals,
    number_of_nfts,
    generation
)
SELECT
    rd.block_id,
    rd.tx_group_id,
    rd.purchaser,
    rd.nft_asset_id,
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
    rd.generation,
    rd.total_sales_amount :: FLOAT / pow(
        10,
        6
    ) / COUNT(1) over(
        PARTITION BY rd.tx_group_id
    ) AS total_sales_amount,
    concat_ws(
        '-',
        block_id :: STRING,
        tx_group_id :: STRING,
        rd.nft_asset_id :: STRING
    ) AS _unique_key,
    rd._INSERTED_TIMESTAMP
FROM
    raw_data rd
