{{ config(
  materialized='incremental',
  alias='engagement_events',
  partition_by = {'field': 'event_date', 'data_type': 'date'},
  incremental_strategy = 'insert_overwrite',
  unique_key = ['event_date','event_name'],
  tags=['incremental', 'daily'],
  re_data_monitored=true,
  re_data_time_filter='event_date',
) }}

WITH dimensions AS (
SELECT 
  event_timestamp,
  event_date,
  event_name,
  user_pseudo_id,
  session_id,
  session_engaged
FROM {{ ref('parameters_query') }}
),
metrics AS (
SELECT 
  event_date,
  event_name,
  count(*) as event_count,
  count(distinct user_pseudo_id) as total_users
FROM dimensions
GROUP BY event_date, event_name
)

SELECT 
  event_date,
  event_name,
  event_count,
  total_users,
  event_count/total_users as event_count_per_user
FROM metrics