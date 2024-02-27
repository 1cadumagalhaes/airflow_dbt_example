{{ config(
  materialized='incremental',
  alias='engagement_pages',
  partition_by = {'field': 'event_date', 'data_type': 'date'},
  incremental_strategy = 'insert_overwrite',
  unique_key = ['event_date','page_path'],
  tags=['incremental', 'daily'],
  re_data_monitored=true,
  re_data_time_filter='event_date',
) }}

WITH dimensions AS (
SELECT 
  event_date,
  event_name,
  page_path,
  page_location,
  user_pseudo_id,
  is_active_user,
FROM {{ ref('parameters_query') }}
WHERE event_name="page_view"
),
metrics AS (
SELECT 
  event_date,
  page_location,
  page_path,
  count(*) as views,
  count(distinct user_pseudo_id) as total_users,
  count(distinct IF(is_active_user,user_pseudo_id,null)) as active_users
FROM dimensions
GROUP BY event_date, page_location,page_path
)

SELECT 
  event_date,
  page_location,
  page_path,
  views,
  total_users,
  active_users,
  SAFE_DIVIDE(views,active_users) as view_per_user
FROM metrics