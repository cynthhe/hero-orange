/*
This query retrieves the raw data from the dataset, extracts the User ID and Session ID as a unique key to match user journeys to user data in a later query.
It also is used for the initial scoping of data, such as data range, or parameter restrictions. If you want to reduce bigquery costs, add filters to this query
Include all sessions.
*/

With FilteredDataSet as (
  SELECT
        *,
        user_pseudo_id as pseudo_ID,
        (SELECT value.string_value FROM UNNEST(user_properties) properties WHERE properties.key= "user_type") as user_type,
        (SELECT value.string_value FROM UNNEST(user_properties) properties WHERE properties.key = "awx_app_version") AS awx_app_version,
        (SELECT params.value.int_value FROM UNNEST(event_params) params WHERE params.key= "ga_session_id") as ga_session_id,
        (SELECT params.value.int_value FROM UNNEST(event_params) params WHERE params.key= "ga_session_number") AS ga_session_number
	FROM `phoenix-production-apps.analytics_227444337.events_*`
  WHERE _TABLE_SUFFIX = FORMAT_DATE("%Y%m%d", DATE_SUB(CURRENT_DATE("EST"), INTERVAL 1 DAY)) 
        #_TABLE_SUFFIX BETWEEN "20200701" AND "20200710" 
  ORDER BY pseudo_ID, ga_session_number, event_timestamp
),


/*
This query extracts the session-level metrics from the filtered table.
*/

SessionMetrics as (
   SELECT event_date AS Date,
          pseudo_id,
          ga_session_id,
          ga_session_number,
          #min(CASE WHEN event_name = "session_start" THEN event_timestamp ELSE null END) as session_start_timestamp,
          min(event_timestamp) AS session_start, 
          max(event_timestamp) AS session_end,
          (max(event_timestamp) - min(event_timestamp)) / 1000000 AS session_duration,
          min(DATETIME(TIMESTAMP_MICROS(event_timestamp), "America/New_York")) as session_start_time,
          max(DATETIME(TIMESTAMP_MICROS(event_timestamp), "America/New_York")) as session_end_time,
   
   #FROM `phoenix-production-apps.Phoenix_Launch_Testing.Temp_FilteredDataSet_20200706`
   FROM FilteredDataSet
   #WHERE ga_session_id IS NOT NULL
   GROUP BY Date, pseudo_id, ga_session_id, ga_session_number
   ORDER BY pseudo_id, ga_session_number
),


/*
This query extracts the major dimensions and all screen-level metrics from the filtered table.
The screen_id field is currently incorrect. This is currently triggered by the default firebase screen_view event, but not the new screenview event. (7/8/2020)
*/

ScreenMetrics as (
    SELECT
      event_date AS Date,
      platform,
      device.mobile_brand_name,
      device.mobile_marketing_name,
      device.operating_system_version,
      geo.country,
      geo.region, 
      geo.city,
      app_info.version as app_version,
      device.language,
      awx_app_version,
      user_type,
      pseudo_ID as user_ID,
      ga_session_id,
      ga_session_number,
      #COUNT(1) AS total_steps, 
      event_timestamp,
      DATETIME(TIMESTAMP_MICROS(event_timestamp), "America/New_York") as event_time,
      event_name,
      (SELECT params.value.string_value FROM UNNEST(event_params) params WHERE event_name="screenview" and params.key= "screen_name") as screen_name,
      (SELECT params.value.int_value FROM UNNEST(event_params) params WHERE event_name="screenview" and params.key= "firebase_screen_id") as screen_id,
      ROW_NUMBER() OVER(PARTITION BY pseudo_id, ga_session_id ORDER BY pseudo_id, event_timestamp) AS screen_number,
      (lead(event_timestamp,1) OVER(PARTITION BY pseudo_id, ga_session_id ORDER BY pseudo_id, ga_session_id, event_timestamp) 
       - lead(event_timestamp,0) OVER(PARTITION BY pseudo_id, ga_session_id ORDER BY pseudo_id, ga_session_id, event_timestamp)) / 1000000 
      AS time_on_screen
    
    #FROM `phoenix-production-apps.Phoenix_Launch_Testing.Temp_FilteredDataSet_20200706`
    FROM FilteredDataSet
    
    -- AND event_name="screenview"
    WHERE event_name = "screenview"
    GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20
    ORDER BY user_ID, ga_session_number, screen_number #event_timestamp
)


/*
This query joins the session-level metrics and screen-level metrics.
*/

SELECT  #s1.Date AS Screen_Date,
        s2.Date,
        s1.platform,
        s1.mobile_brand_name,
        s1.mobile_marketing_name,
        s1.operating_system_version,
        s1.country,
        s1.region, 
        s1.city,
        s1.app_version,
        s1.language,
        s1.awx_app_version,
        s1.user_type,
        #s1.user_ID as Screen_user_ID,
        s2.pseudo_ID as user_ID,
        #s1.ga_session_id as Screen_session_id,
        s2.ga_session_id,
        #s1.ga_session_number as Screen_session_number,
        s2.ga_session_number,
        s2.session_duration,
        s2.session_start,
        s2.session_end,
        s2.session_start_time,
        s2.session_end_time,
        s1.event_timestamp,
        s1.event_time,
        s1.event_name,
        s1.screen_name,
        s1.screen_id,
        s1.screen_number,
        s1.time_on_screen
FROM ScreenMetrics s1
FULL OUTER JOIN SessionMetrics s2
#LEFT JOIN SessionMetrics s2
ON s1.user_ID = s2.pseudo_id AND 
   s1.ga_session_id = s2.ga_session_id AND
   s1.ga_session_number = s2.ga_session_number
ORDER BY s1.user_ID, s1.ga_session_number, s1.screen_number
