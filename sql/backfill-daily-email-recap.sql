# This backfills Phoenix app data for the Daily Email Recaps
SELECT
event_date AS date,
 device.operating_system AS device_OS,
 CASE WHEN geo.country = "United States" THEN "US"
      ELSE "ROW"
        END as country,
COUNT(CASE
     WHEN event_name = 'session_start' THEN 1
   ELSE
   NULL
 END
   ) AS session,
 COUNT(CASE
     WHEN event_name = 'screenview' THEN 1
   ELSE
   NULL
 END
   ) AS screenView,     
count(distinct user_pseudo_id) as Users,
FROM
 `phoenix-production-apps.analytics_227444337.events_20220102` #Change the data in 'YYYYMMDD' into actual date like 20210331
GROUP BY
 1,
 2,3
ORDER BY date DESC 
