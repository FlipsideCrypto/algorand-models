{{ config(
    materialized = 'incremental',
    unique_key = 'fact_commitment_all_id',
    incremental_strategy = 'merge',
    cluster_by = ['block_timestamp::DATE']
) }}

WITH base AS (

    SELECT
        block_id,
        intra,
        tx_id,
        sender,
        commit_algo,
        non_algo_assets,
        non_algo_commit_algo_equivalent,
        total_commit_in_algo,
        benificiary_address,
        governance_period_id,
        _INSERTED_TIMESTAMP
    FROM
        {{ ref('silver__governance_commit') }}

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
    OR tx_id IN (
        SELECT
            tx_id
        FROM
            {{ this }}
        WHERE
            dim_account_id__govenor = '-1'
            OR dim_period_id = '-1'
            OR dim_block_id = '-1'
    )
{% endif %}
)
SELECT
    {{ dbt_utils.surrogate_key(
        ['a.block_id','a.intra']
    ) }} AS fact_commitment_all_id,
    intra,
    tx_id,
    COALESCE(
        gp.dim_period_id,
        '-2'
    ) AS dim_period_id,
    COALESCE(
        da.dim_account_id,
        '-1'
    ) AS dim_account_id_govenor,
    A.sender AS govenor_address,
    commit_algo,
    non_algo_assets,
    non_algo_commit_algo_equivalent,
    total_commit_in_algo,
    COALESCE(
        dab.dim_account_id,
        CASE
            WHEN A.benificiary_address IS NULL THEN '-2'
            ELSE '-1'
        END
    ) AS dim_account_id_benificiary,
    A.benificiary_address,
    COALESCE(
        b.dim_block_id,
        '-2'
    ) AS dim_block_id,
    b.block_timestamp,
    A._inserted_timestamp,
    '{{ env_var("DBT_CLOUD_RUN_ID", "manual") }}' AS _audit_run_id
FROM
    base A
    LEFT JOIN {{ ref('core__dim_block') }}
    b
    ON A.block_id = b.block_id
    LEFT JOIN {{ ref('core__dim_account') }}
    da
    ON A.sender = da.address
    LEFT JOIN {{ ref('core__dim_account') }}
    dab
    ON A.benificiary_address = dab.address
    LEFT JOIN {{ ref('governance__dim_period') }}
    gp
    ON A.governance_period_id = gp.id
