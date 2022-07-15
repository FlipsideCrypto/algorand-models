{{ config(
    materialized = 'view'
) }}

SELECT
    block_timestamp,
    block_date,
    block_id,
    intra,
    tx_group_id,
    tx_id,
    inner_tx,
    e.address AS tx_sender,
    fee,
    app_id,
    tx_type,
    tx_type_name,
    tx_message,
    extra,
    A._inserted_timestamp
FROM
    {{ ref('core__fact_transaction_application_call') }} A
    JOIN {{ ref('core__fact_transaction') }}
    b
    ON A.fact_transaction_id = b.fact_transaction_id
    JOIN {{ ref('core__dim_block') }} C
    ON b.dim_block_id = C.dim_block_id
    JOIN {{ ref('core__dim_application') }}
    d
    ON A.dim_application_id = d.dim_application_id
    JOIN {{ ref('core__dim_account') }}
    e
    ON b.dim_account_id__tx_sender = e.dim_account_id
    JOIN {{ ref('core__dim_transaction_type') }}
    f
    ON b.dim_transaction_type_id = f.dim_transaction_type_id
