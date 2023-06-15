-- AWX Alerts Upsell Entry | SVs
SELECT DISTINCT
       params.value.string_value as selection, 
       COUNT (DISTINCT user_pseudo_id) as users

FROM `phoenix-production-apps.analytics_227444337.events_*` t CROSS JOIN UNNEST (event_params) AS params
CROSS JOIN UNNEST (user_properties) AS user_properties

WHERE _TABLE_SUFFIX BETWEEN "20230601" AND FORMAT_DATE("%Y%m%d", DATE_SUB(CURRENT_DATE("EST"), INTERVAL 1 DAY)) 
-- WHERE _TABLE_SUFFIX BETWEEN "20230510" AND "20230531"
AND event_name = "screenview"
AND params.key = "screen_name"
AND params.value.string_value = "settings_notifications"
AND user_properties.key = "subscriber"
AND user_properties.value.string_value = "Free"
AND app_info.version LIKE "%.11%"
GROUP BY 1
ORDER BY users DESC;

-- AWX Alerts Upsell Entry | Users
SELECT DISTINCT
       params.value.string_value as selection, 
       COUNT (DISTINCT user_pseudo_id) as users

FROM `phoenix-production-apps.analytics_227444337.events_*` t CROSS JOIN UNNEST (event_params) AS params
CROSS JOIN UNNEST (user_properties) AS user_properties

WHERE _TABLE_SUFFIX BETWEEN "20230601" AND FORMAT_DATE("%Y%m%d", DATE_SUB(CURRENT_DATE("EST"), INTERVAL 1 DAY))
-- WHERE _TABLE_SUFFIX BETWEEN "20230510" AND "20230531"
AND event_name = "upsell_entry"
AND params.key = "screen_name"
AND params.value.string_value = "settings_notifications"
AND user_properties.key = "subscriber"
AND user_properties.value.string_value = "Free"
AND app_info.version LIKE "%.11%"
GROUP BY 1
ORDER BY users DESC;

-- Notification Settings On/Off Toggle | Users
SELECT DISTINCT
       params.value.string_value as selection, 
       COUNT (DISTINCT user_pseudo_id) as users

FROM `phoenix-production-apps.analytics_227444337.events_*` t CROSS JOIN UNNEST (event_params) AS params

WHERE _TABLE_SUFFIX BETWEEN "20230601" AND FORMAT_DATE("%Y%m%d", DATE_SUB(CURRENT_DATE("EST"), INTERVAL 1 DAY))
-- WHERE _TABLE_SUFFIX BETWEEN "20230510" AND "20230531"
AND event_name = "settings"
AND params.key = "settings_notifications"
AND app_info.version LIKE "%.11%"
GROUP BY 1
ORDER BY users DESC;

-- Notification Settings On/Off Toggle | Screenviews
SELECT DISTINCT
       params.value.string_value as selection, 
       COUNT (DISTINCT user_pseudo_id) as users

FROM `phoenix-production-apps.analytics_227444337.events_*` t CROSS JOIN UNNEST (event_params) AS params

-- WHERE _TABLE_SUFFIX BETWEEN "20230601" AND FORMAT_DATE("%Y%m%d", DATE_SUB(CURRENT_DATE("EST"), INTERVAL 1 DAY))
WHERE _TABLE_SUFFIX BETWEEN "20230510" AND "20230531"
AND event_name = "screenview"
AND params.key = "screen_name"
AND params.value.string_value IN("settings_notifications","settings_notif_location","settings_notif_news")
AND app_info.version LIKE "%.11%"
GROUP BY 1
ORDER BY users DESC;

-- Total Active Users
SELECT
  COUNT(DISTINCT user_pseudo_id) AS users
  
FROM `phoenix-production-apps.analytics_227444337.events_*`
WHERE _TABLE_SUFFIX BETWEEN "20230601" AND FORMAT_DATE("%Y%m%d", DATE_SUB(CURRENT_DATE("EST"), INTERVAL 1 DAY))
-- WHERE _TABLE_SUFFIX BETWEEN "20230510" AND "20230531"
AND app_info.version LIKE "%.11%";

-- Top 10 Notification Preferences
SELECT DISTINCT
       user_properties.value.string_value as selection, 
       COUNT (DISTINCT user_pseudo_id) as users

FROM `phoenix-production-apps.analytics_227444337.events_*` t CROSS JOIN UNNEST (user_properties) AS user_properties

WHERE _TABLE_SUFFIX BETWEEN "20230601" AND FORMAT_DATE("%Y%m%d", DATE_SUB(CURRENT_DATE("EST"), INTERVAL 1 DAY))
-- WHERE _TABLE_SUFFIX BETWEEN "20230510" AND "20230531"
AND user_properties.key = "user_notification_pref"
AND user_properties.value.string_value NOT LIKE "%govt%"
AND app_info.version LIKE "%.11%"
GROUP BY 1
ORDER BY users DESC
LIMIT 10;
