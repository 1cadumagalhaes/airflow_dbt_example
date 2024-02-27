{{ config(
  materialized='incremental',
  alias='traffic_acquisition',
  partition_by = {'field': 'event_date', 'data_type': 'date'},
  incremental_strategy = 'insert_overwrite',
  unique_key = ['event_date','session_source_medium'],
  tags=['incremental', 'daily'],
  re_data_monitored=true,
  re_data_time_filter='event_date',
) }}

WITH cleanup_traffic AS (
SELECT 
  event_timestamp,
  event_date,
  event_name,
  user_pseudo_id,
  session_id,
  session_engaged,
  page_referrer,
  entrances,
  event_source,
  event_medium,
  IF(entrances=1,event_source,null) session_source,
  IF(entrances=1,event_medium,null) session_medium,
FROM {{ ref('parameters_query') }}
),
session_traffic AS (
SELECT 
  event_timestamp,
  event_date,
  event_name,
  user_pseudo_id,
  session_id,
  session_engaged,
  page_referrer,
  event_source,
  event_medium,
  FIRST_VALUE(session_source IGNORE NULLS) OVER user_session AS session_source,
  FIRST_VALUE(session_medium IGNORE NULLS) OVER user_session AS session_medium
FROM 
  cleanup_traffic
WINDOW user_session AS (PARTITION BY event_date, user_pseudo_id, session_id ORDER BY event_timestamp ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
)
SELECT 
  event_date,
  CONCAT(session_source, " / ", session_medium) as session_source_medium,
  COUNT(DISTINCT user_pseudo_id) as total_users,
  COUNT(DISTINCT session_id) as sessions,
  COUNT(DISTINCT IF(session_engaged=1,session_id||user_pseudo_id,null)) as engaged_sessions
FROM cleanup_traffic
GROUP BY event_date, session_source_medium