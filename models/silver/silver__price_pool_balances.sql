{{ config(
    materialized = 'incremental',
    unique_key = "_unique_key",
    incremental_strategy = 'merge',
    cluster_by = ['block_hour']
) }}

WITH lps AS (

    SELECT
        address
    FROM
        {{ ref('silver__pool_addresses') }} C
    WHERE
        C.label = 'tinyman'
        AND label_subtype = 'pool'
        AND (
            C.address_name LIKE '%-ALGO %'
            OR C.address_name LIKE '%ALGO-%'
        )
        AND C.address_name NOT ILIKE '%algo fam%'
        AND C.address_name NOT ILIKE '%smart algo%'
        AND C.address_name NOT ILIKE '%down%'
        AND C.address_name NOT ILIKE '%UP%'
        AND C.address_name NOT LIKE '%S-ALGO%'
        AND C.address_name NOT LIKE '%S-Put%'
        AND C.address <> 'CJKYTA2V2G3OIBUQYV34VKSCNSMAGTOMYNGJJUZY2AFORCPUIHCJC423QY'
),
base_price AS (
    SELECT
        recorded_hour AS HOUR,
        OPEN price,
        1 RANK
    FROM
        {{ source(
            'crosschain_silver',
            'hourly_prices_coin_gecko'
        ) }}
        p
    WHERE
        id = 'algorand'
        AND recorded_hour >= '2022-08-24 01:00:00.000'
    UNION ALL
    SELECT
        HOUR,
        AVG(price) price,
        2 RANK
    FROM
        (
            SELECT
                p.symbol,
                DATE_TRUNC(
                    'hour',
                    recorded_at
                ) AS HOUR,
                price
            FROM
                {{ source(
                    'shared',
                    'prices_v2'
                ) }}
                p
            WHERE
                asset_id IN (
                    'algorand',
                    '4030'
                )
                AND recorded_at >= '2022-01-01' qualify(ROW_NUMBER() over(PARTITION BY DATE_TRUNC('hour', recorded_at), provider
            ORDER BY
                recorded_at DESC)) = 1
        ) x
    GROUP BY
        HOUR
),
hourly_prices_with_gaps AS (
    SELECT
        HOUR,
        price
    FROM
        base_price qualify(ROW_NUMBER() over (PARTITION BY HOUR
    ORDER BY
        RANK) = 1)
),
hourly_prices AS (
    SELECT
        DATE AS HOUR,
        LAST_VALUE(
            price ignore nulls
        ) over(
            ORDER BY
                DATE ASC rows unbounded preceding
        ) AS price
    FROM
        (
            SELECT
                DISTINCT DATE
            FROM
                {{ ref('silver__hourly_pool_balances') }}

{% if is_incremental() %}
WHERE
    DATE :: DATE >= (
        SELECT
            MAX(
                block_hour
            )
        FROM
            {{ this }}
    ) :: DATE - 7
{% endif %}
) A
LEFT JOIN hourly_prices_with_gaps b
ON A.date = b.hour
),
exclude AS (
    SELECT
        DISTINCT address
    FROM
        {{ ref('silver__hourly_pool_balances') }} A
        JOIN {{ ref('silver__asset') }}
        d
        ON A.asset_id = d.asset_ID
    WHERE
        COALESCE(
            asset_url,
            ''
        ) = 'https://app.silodefi.com'
),
balances AS (
    SELECT
        A.address,
        A.date,
        OBJECT_AGG(
            CASE
                WHEN A.asset_id = 0 THEN 'algo'
                ELSE 'other'
            END,
            balance :: variant
        ) AS j,
        OBJECT_AGG(
            CASE
                WHEN A.asset_id = 0 THEN 'algo'
                ELSE 'other'
            END,
            A.asset_id :: variant
        ) AS j_2,
        OBJECT_AGG(
            CASE
                WHEN A.asset_id = 0 THEN 'algo'
                ELSE 'other'
            END,
            d.asset_name :: variant
        ) AS j_3,
        j :algo AS algo_bal,
        j :other AS other_bal,
        j_2 :other AS other_asset_id,
        j_3 :other :: STRING AS other_asset_name
    FROM
        {{ ref('silver__hourly_pool_balances') }} A
        JOIN lps b
        ON A.address = b.address
        LEFT JOIN {{ ref('silver__asset') }}
        d
        ON A.asset_id = d.asset_ID
        LEFT JOIN exclude ex
        ON A.address = ex.address
    WHERE
        (
            (LOWER(asset_name) NOT LIKE '%pool%'
            AND LOWER(asset_name) NOT LIKE '%lp%')
            OR A.asset_Id = 0)
            AND ex.address IS NULL

{% if is_incremental() %}
AND DATE :: DATE >= (
    SELECT
        MAX(
            block_hour
        )
    FROM
        {{ this }}
) :: DATE - 7
{% endif %}
GROUP BY
    A.address,
    A.date
)
SELECT
    A.date AS block_hour,
    other_asset_ID :: INT AS asset_id,
    other_asset_name AS asset_name,
    CASE
        WHEN ROUND(
            other_bal,
            0
        ) = 0 THEN 0
        ELSE (
            algo_bal * price
        ) / other_bal
    END AS price_usd,
    algo_bal :: FLOAT AS algo_balance,
    ROUND(
        other_bal,
        3
    ) :: FLOAT AS non_algo_balance,
    e.address_name pool_name,
    A.address pool_address,
    concat_ws(
        '-',
        block_hour,
        asset_id
    ) AS _unique_key,
    price AS _algo_price
FROM
    balances A
    JOIN hourly_prices C
    ON A.date = C.hour
    LEFT JOIN {{ ref('silver__pool_addresses') }}
    e
    ON A.address = e.address
WHERE
    other_asset_ID IS NOT NULL
    AND other_bal > 0
    AND algo_bal > 0
UNION ALL
SELECT
    HOUR AS block_hour,
    0 :: INT AS asset_id,
    'ALGO' AS asset_name,
    price AS price_usd,
    NULL algo_balance,
    NULL non_algo_balance,
    NULL pool_name,
    NULL pool_address,
    concat_ws(
        '-',
        HOUR,
        0
    ) unique_key,
    price AS _algo_price
FROM
    hourly_prices
