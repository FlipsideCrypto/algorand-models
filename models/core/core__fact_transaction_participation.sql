{{ config(
  materialized = 'incremental',
  unique_key = 'fact_transaction_participation_id',
  incremental_strategy = 'merge',
  cluster_by = ['block_timestamp::DATE']
) }}

WITH base AS(

  SELECT
    block_id,
    intra,
    address,
    _INSERTED_TIMESTAMP
  FROM
    {{ ref('silver__transaction_participation') }}

{% if is_incremental() %}
WHERE
  _INSERTED_TIMESTAMP >= (
    SELECT
      MAX(
        _INSERTED_TIMESTAMP
      )
    FROM
      {{ this }}
  ) - INTERVAL '4 HOURS'
  OR block_id || '--' || address IN (
    SELECT
      block_id || '--' || address
    FROM
      {{ this }}
    WHERE
      dim_block_id = '-1'
      OR dim_account_id = '-1'
  )
{% endif %}
)
SELECT
  {{ dbt_utils.surrogate_key(
    ['a.block_id','a.intra','a.address']
  ) }} AS fact_transaction_participation_id,
  COALESCE(
    ab.block_timestamp,
    '1900-01-01' :: DATE
  ) AS block_timestamp,
  A.block_id,
  COALESCE(
    ab.dim_block_id,
    '-1'
  ) AS dim_block_id,
  A.intra,
  COALESCE(
    ad.dim_account_id,
    '-1'
  ) AS dim_account_id,
  A.address,
  A._INSERTED_TIMESTAMP,
  '{{ env_var("DBT_CLOUD_RUN_ID", "manual") }}' AS _audit_run_id
FROM
  base A
  LEFT JOIN {{ ref('core__dim_block') }}
  ab
  ON A.block_id = ab.block_id
  LEFT JOIN {{ ref('core__dim_account') }}
  ad
  ON A.address = ad.address
