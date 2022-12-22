{{ config(
    materialized = 'table'
) }}

SELECT
    ethereum.streamline.udf_api(
        'GET',
        'https://governance.algorand.foundation/api/periods/',{},{}
    ) AS DATA,
    SYSDATE() AS _inserted_timestamp
