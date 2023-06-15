-- Toggle Current Conditions On/Off | Users
SELECT DISTINCT
       params.value.string_value as selection, 
       COUNT (DISTINCT user_pseudo_id) as users

FROM `phoenix-production-apps.analytics_227444337.events_*` t CROSS JOIN UNNEST (event_params) AS params

-- WHERE _TABLE_SUFFIX BETWEEN "20230601" AND FORMAT_DATE("%Y%m%d", DATE_SUB(CURRENT_DATE("EST"), INTERVAL 1 DAY))
WHERE _TABLE_SUFFIX BETWEEN "20230510" AND "20230531"
AND event_name = "settings"
AND params.key = "settings_current_conditions"
AND app_info.version LIKE "%.11%"
GROUP BY 1
ORDER BY users DESC;

-- Toggle Current Conditions On/Off | Settings SVs
SELECT DISTINCT
       params.value.string_value as selection, 
       COUNT (DISTINCT user_pseudo_id) as users

FROM `phoenix-production-apps.analytics_227444337.events_*` t CROSS JOIN UNNEST (event_params) AS params

WHERE _TABLE_SUFFIX BETWEEN "20230601" AND FORMAT_DATE("%Y%m%d", DATE_SUB(CURRENT_DATE("EST"), INTERVAL 1 DAY))
-- WHERE _TABLE_SUFFIX BETWEEN "20230510" AND "20230531"
AND event_name = "screenview"
AND params.key = "screen_name"
AND params.value.string_value = "settings"
AND app_info.version LIKE "%.11%"
GROUP BY 1
ORDER BY users DESC;

-- Current Conditions Current Status | Users
SELECT DISTINCT
       user_properties.value.string_value as selection, 
       COUNT (DISTINCT user_pseudo_id) as users

FROM `phoenix-production-apps.analytics_227444337.events_*` t CROSS JOIN UNNEST (user_properties) AS user_properties

WHERE _TABLE_SUFFIX BETWEEN "20230601" AND FORMAT_DATE("%Y%m%d", DATE_SUB(CURRENT_DATE("EST"), INTERVAL 1 DAY))
-- WHERE _TABLE_SUFFIX BETWEEN "20230510" AND "20230531"
AND user_properties.key = "user_current_conditions"
AND app_info.version LIKE "%.11%"
GROUP BY 1
ORDER BY users DESC;
