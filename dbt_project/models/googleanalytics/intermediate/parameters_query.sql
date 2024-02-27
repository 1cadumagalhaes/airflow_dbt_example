{{ config(
  materialized='incremental',
  alias='parameters_query',
  partition_by= {"field": 'event_date', "data_type": 'date'},
  incremental_strategy = 'insert_overwrite',
  unique_key = ['event_timestamp','event_name','user_pseudo_id','session_id'],
  tags=['incremental', 'daily'],
  re_data_monitored=true,
  re_data_time_filter='event_date',
) }}
WITH raw_data AS (
  SELECT
    event_timestamp,
    PARSE_DATE('%Y%m%d', event_date) event_date,
    event_name,
    user_pseudo_id,
    user_id,
    is_active_user,
    ( {{ event_param('page', 'string_value') }} ) as page_path,
    ( {{ event_param('page_location', 'string_value') }} ) as page_location,
    ( {{ event_param('ga_session_id') }} ) as session_id,
    ( {{ event_param('session_engaged') }} ) session_engaged,
    ( {{ event_param('page_referrer', 'string_value') }} ) page_referrer,
    ( {{ event_param('source', 'string_value') }} ) event_source,
    ( {{ event_param('medium', 'string_value') }} ) event_medium,
    ( {{ event_param('entrances') }} ) entrances
  FROM
    {{ source('ga4_cadu_blog', 'events') }} 
  WHERE 
    {% if var('start_date') != 'notset' and var('end_date') != 'notset' %}
      _table_suffix BETWEEN '{{ var('start_date') }}' AND '{{ var('end_date') }}'
    {% elif var('execution_date') != 'notset' %}
      _table_suffix = '{{ var('execution_date') }}'
    {% else %}
      _table_suffix = format_date('%Y%m%d', date_sub(current_date(), interval 1 day))
    {% endif %}
),
set_referral AS (
  SELECT 
    * EXCEPT (event_source, event_medium),
    COALESCE(
      event_source,   
      CASE
          WHEN page_referrer LIKE "%google%" THEN "google" 
          WHEN page_referrer LIKE "%bing%" THEN "bing"
      END
    ) as event_source,
    COALESCE(
      event_medium, 
      CASE
        WHEN page_referrer LIKE "%google%" THEN "organic" 
        WHEN page_referrer LIKE "%bing%" THEN "organic"
        -- WHEN referrer IS NOT NULL THEN "referral"
      END
    ) as event_medium
  FROM
    raw_data
),
cleanup_traffic AS (
  SELECT
    event_timestamp,
    event_date,
    event_name,
    page_path,
    page_location,
    is_active_user,
    user_pseudo_id,
    session_id,
    session_engaged,
    page_referrer,
    entrances,
    event_source,
    event_medium,
    IF(entrances = 1, event_source, null) session_source,
    IF(entrances = 1, event_medium, null) session_medium
  FROM
    set_referral
)
  SELECT * FROM cleanup_traffic