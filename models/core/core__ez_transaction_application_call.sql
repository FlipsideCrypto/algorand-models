{{ config(
    materialized = 'view'
) }}

SELECT
    b.block_timestamp,
    block_date,
    block_id,
    intra,
    tx_group_id,
    tx_id,
    inner_tx,
    b.tx_sender,
    fee,
    app_id,
    tx_type,
    tx_type_name,
    tx_message,
    extra,
    A._inserted_timestamp,
    '{{ env_var("DBT_CLOUD_RUN_ID", "manual") }}' AS _audit_run_id
FROM
    {{ ref('core__fact_transaction') }}
    b
    JOIN {{ ref('core__dim_block') }} C
    ON b.dim_block_id = C.dim_block_id
    JOIN {{ ref('core__dim_application') }}
    d
    ON A.dim_application_id = d.dim_application_id
    JOIN {{ ref('core__dim_transaction_type') }}
    f
    ON b.dim_transaction_type_id = f.dim_transaction_type_id
WHERE
    f.tx_type = 'appl'
