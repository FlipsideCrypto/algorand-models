{{ config(
    materialized = 'incremental',
    unique_key = 'id',
    incremental_strategy = 'merge'
) }}

SELECT
    DATA :data :active_state_end_datetime :: datetime AS active_state_end_datetime,
    DATA :data :algo_amount_in_reward_pool :: INT AS algo_amount_in_reward_pool,
    DATA :data :algo_rewards_reserved_for_defi_participants :: INT AS algo_rewards_reserved_for_defi_participants,
    DATA :data :algo_rewards_reserved_for_non_defi_participants :: INT AS algo_rewards_reserved_for_non_defi_participants,
    DATA :data :governor_count :: INT AS governor_count,
    DATA :data :end_datetime :: datetime AS end_datetime,
    DATA :data :id :: STRING AS id,
    DATA :data :is_active :: BOOLEAN AS is_active,
    DATA :data :registration_end_datetime :: datetime AS registration_end_datetime,
    DATA :data :sign_up_address :: STRING AS sign_up_address,
    DATA :data :slug :: STRING AS slug,
    DATA :data :start_datetime :: datetime AS start_datetime,
    DATA :data :title :: STRING AS title,
    DATA :data :total_committed_algo :: INT AS total_committed_algo,
    DATA :data :total_committed_lp_tokens_in_algo :: INT AS total_committed_lp_tokens_in_algo,
    DATA :data :total_committed_stake :: INT AS total_committed_stake,
    DATA :data :total_committed_stake_for_extra_rewards :: INT AS total_committed_stake_for_extra_rewards,
    _inserted_timestamp
FROM
    {{ source(
        'bronze_api',
        'governance_period'
    ) }}

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
{% endif %}
