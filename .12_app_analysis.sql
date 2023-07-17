-- Video 5th Nav Entry Point | Users
SELECT DISTINCT
params.value.string_value as selection,
COUNT (DISTINCT user_pseudo_id) as users

FROM `phoenix-production-apps.analytics_227444337.events_*` t CROSS JOIN UNNEST (event_params) AS params
 
-- WHERE _TABLE_SUFFIX BETWEEN "20230706" AND "20230712"
WHERE _TABLE_SUFFIX BETWEEN "20230706" AND FORMAT_DATE("%Y%m%d", DATE_SUB(CURRENT_DATE("EST"), INTERVAL 1 DAY))
AND event_name = "main_nav_bottom"
AND params.key = "menu_action"
AND params.value.string_value = "video"
AND app_info.version LIKE "%.12%"
GROUP BY 1
ORDER BY 1;

-- Video Flyout Menu Entry Points | Users
SELECT DISTINCT
-- params.value.string_value as selection,
COUNT (DISTINCT user_pseudo_id) as users

FROM `phoenix-production-apps.analytics_227444337.events_*` t CROSS JOIN UNNEST (event_params) AS params
 
-- WHERE _TABLE_SUFFIX BETWEEN "20230706" AND "20230712"
WHERE _TABLE_SUFFIX BETWEEN "20230706" AND FORMAT_DATE("%Y%m%d", DATE_SUB(CURRENT_DATE("EST"), INTERVAL 1 DAY))
AND event_name = "main_nav_side_slider"
AND params.key = "menu_action"
AND (params.value.string_value = "video"
OR params.value.string_value = "video_live"
)
AND app_info.version LIKE "%.12%";
-- GROUP BY 1
-- ORDER BY 1;

-- Video Module in the Today Screen | Users
SELECT DISTINCT
params.value.string_value as selection,
COUNT (DISTINCT user_pseudo_id) as users

FROM `phoenix-production-apps.analytics_227444337.events_*` t CROSS JOIN UNNEST (event_params) AS params
 
-- WHERE _TABLE_SUFFIX BETWEEN "20230706" AND "20230712"
WHERE _TABLE_SUFFIX BETWEEN "20230706" AND FORMAT_DATE("%Y%m%d", DATE_SUB(CURRENT_DATE("EST"), INTERVAL 1 DAY))
AND event_name = "screenview"
AND params.key = "screen_name"
AND params.value.string_value = "video_detail"
AND app_info.version LIKE "%.12%"
AND traffic_source.medium != "push"
GROUP BY 1
ORDER BY 1;

-- Video Wall SVs by Date
SELECT DISTINCT
PARSE_DATE("%Y%m%d", event_date) AS Date,
params.value.string_value as selection,
COUNT (DISTINCT user_pseudo_id) as users

FROM `phoenix-production-apps.analytics_227444337.events_*` t CROSS JOIN UNNEST (event_params) AS params
 
WHERE _TABLE_SUFFIX BETWEEN "20230701" AND "20230705"
-- WHERE _TABLE_SUFFIX BETWEEN "20230706" AND FORMAT_DATE("%Y%m%d", DATE_SUB(CURRENT_DATE("EST"), INTERVAL 1 DAY))
AND event_name = "screenview"
AND params.key = "screen_name"
AND params.value.string_value = "video_detail"
AND app_info.version LIKE "%.12%"
AND traffic_source.medium != "push"
GROUP BY 1,2
ORDER BY 1,2;

-- Radar & Maps Module
-- Map Shortcuts Users
SELECT DISTINCT
params.value.string_value as selection,
COUNT (DISTINCT user_pseudo_id) as users

FROM `phoenix-production-apps.analytics_227444337.events_*` t CROSS JOIN UNNEST (event_params) AS params
 
-- WHERE _TABLE_SUFFIX BETWEEN "20230706" AND "20230712"
WHERE _TABLE_SUFFIX BETWEEN "20230706" AND FORMAT_DATE("%Y%m%d", DATE_SUB(CURRENT_DATE("EST"), INTERVAL 1 DAY))
AND event_name = "clicks"
AND params.key = "click_button"
AND params.value.string_value IN("map_radar_shortcut","map_temp_shortcut","map_clouds_shortcut","map_precip_shortcut")
AND app_info.version LIKE "%.12%"
GROUP BY 1
ORDER BY 1;

-- Map Shortcut Users by Date
SELECT DISTINCT
PARSE_DATE("%Y%m%d", event_date) AS Date,
params.value.string_value as selection,
COUNT (DISTINCT user_pseudo_id) as users

FROM `phoenix-production-apps.analytics_227444337.events_*` t CROSS JOIN UNNEST (event_params) AS params
 
-- WHERE _TABLE_SUFFIX BETWEEN "20230701" AND "20230705"
WHERE _TABLE_SUFFIX BETWEEN "20230706" AND FORMAT_DATE("%Y%m%d", DATE_SUB(CURRENT_DATE("EST"), INTERVAL 1 DAY))
AND event_name = "clicks"
AND params.key = "click_button"
AND params.value.string_value IN("map_radar_shortcut","map_temp_shortcut","map_clouds_shortcut","map_precip_shortcut")
AND app_info.version LIKE "%.12%"
GROUP BY 1,2
ORDER BY 1,2;

-- Local Hurricane Tracker (Today Screen)
SELECT DISTINCT
params.value.string_value as selection,
COUNT (DISTINCT user_pseudo_id) as users

FROM `phoenix-production-apps.analytics_227444337.events_*` t CROSS JOIN UNNEST (event_params) AS params
 
-- WHERE _TABLE_SUFFIX BETWEEN "20230701" AND "20230705"
WHERE _TABLE_SUFFIX BETWEEN "20230706" AND FORMAT_DATE("%Y%m%d", DATE_SUB(CURRENT_DATE("EST"), INTERVAL 1 DAY))
AND event_name = "alerts_nav"
AND params.key = "alert_type"
AND params.value.string_value = "hurricane"
AND app_info.version LIKE "%.12%"
GROUP BY 1
ORDER BY 1;

-- Alert Clicks
SELECT DISTINCT
params.value.string_value as selection,
COUNT (DISTINCT user_pseudo_id) as users

FROM `phoenix-production-apps.analytics_227444337.events_*` t CROSS JOIN UNNEST (event_params) AS params
 
-- WHERE _TABLE_SUFFIX BETWEEN "20230701" AND "20230705"
WHERE _TABLE_SUFFIX BETWEEN "20230706" AND FORMAT_DATE("%Y%m%d", DATE_SUB(CURRENT_DATE("EST"), INTERVAL 1 DAY))
AND event_name = "alert_clicks"
AND params.key = "alert_button"
-- AND params.value.string_value = "alert_refresh"
AND app_info.version LIKE "%.12%"
GROUP BY 1
ORDER BY 1;

-- Alert Screens
SELECT DISTINCT
params.value.string_value as selection,
COUNT (DISTINCT user_pseudo_id) as users

FROM `phoenix-production-apps.analytics_227444337.events_*` t CROSS JOIN UNNEST (event_params) AS params
 
-- WHERE _TABLE_SUFFIX BETWEEN "20230701" AND "20230705"
WHERE _TABLE_SUFFIX BETWEEN "20230706" AND FORMAT_DATE("%Y%m%d", DATE_SUB(CURRENT_DATE("EST"), INTERVAL 1 DAY))
AND event_name = "screenview"
AND params.key = "screen_name"
AND params.value.string_value LIKE '%alert%'
AND app_info.version LIKE "%.12%"
GROUP BY 1
ORDER BY 1;

-- Alert Screens by Date
SELECT DISTINCT
PARSE_DATE("%Y%m%d", event_date) AS Date,
params.value.string_value as selection,
COUNT (DISTINCT user_pseudo_id) as users

FROM `phoenix-production-apps.analytics_227444337.events_*` t CROSS JOIN UNNEST (event_params) AS params
 
-- WHERE _TABLE_SUFFIX BETWEEN "20230701" AND "20230705"
WHERE _TABLE_SUFFIX BETWEEN "20230706" AND FORMAT_DATE("%Y%m%d", DATE_SUB(CURRENT_DATE("EST"), INTERVAL 1 DAY))
AND event_name = "screenview"
AND params.key = "screen_name"
AND params.value.string_value LIKE '%alert%'
AND app_info.version LIKE "%.12%"
GROUP BY 1,2
ORDER BY 1,2;

-- .12 Map Improvements | Radar Maps 5th Nav
SELECT DISTINCT
PARSE_DATE("%Y%m%d", event_date) AS Date,
-- params.value.string_value as selection,
COUNT (DISTINCT user_pseudo_id) as users

FROM `phoenix-production-apps.analytics_227444337.events_*` t CROSS JOIN UNNEST (event_params) AS params
 
-- WHERE _TABLE_SUFFIX BETWEEN "20230701" AND "20230705"
WHERE _TABLE_SUFFIX BETWEEN "20230701" AND FORMAT_DATE("%Y%m%d", DATE_SUB(CURRENT_DATE("EST"), INTERVAL 1 DAY))
AND event_name = "main_nav_bottom"
AND params.key = "menu_action"
-- AND params.value.string_value = "radar_maps"
AND params.value.string_value IN ("maps","radar","radar_maps")
-- AND app_info.version LIKE "%.12%"
AND app_info.version LIKE "%.11%"
-- AND app_info.version NOT LIKE "%.12%"
GROUP BY 1
ORDER BY 1;

-- .12 Map Improvements | Map Shortcuts
SELECT
-- (SELECT DISTINCT params.value.string_value FROM UNNEST(event_params) WHERE event_name = "clicks" AND params.key = "click_button" AND params.value.string_value IN("map_temp_shortcut","map_clouds_shortcut","map_precip_shortcut","map_radar_shortcut")) AS click_button,
-- (SELECT DISTINCT params.value.string_value FROM UNNEST(event_params) WHERE event_name = "clicks" AND params.key = "screen_name" AND params.value.string_value IN("now","maps_radar")) AS screen_name,
-- params.value.string_value AS selection,
COUNT(DISTINCT user_pseudo_id) as users

FROM `phoenix-production-apps.analytics_227444337.events_*` t CROSS JOIN UNNEST (event_params) AS params
 
-- WHERE _TABLE_SUFFIX BETWEEN "20230701" AND "20230705"
WHERE _TABLE_SUFFIX BETWEEN "20230701" AND FORMAT_DATE("%Y%m%d", DATE_SUB(CURRENT_DATE("EST"), INTERVAL 1 DAY))
AND event_name = "screenview"
-- AND (params.key = "click_button" OR params.key = "screen_name")
AND params.key = "screen_name"
-- AND params.value.string_value IN ("map_temp_shortcut","map_clouds_shortcut","map_precip_shortcut")
AND params.value.string_value = "now"
AND app_info.version LIKE "%.12%";
-- GROUP BY 1
-- ORDER BY 1;

-- All Click Buttons
SELECT DISTINCT
-- params.value.string_value AS selection,
COUNT(DISTINCT user_pseudo_id) as users

FROM `phoenix-production-apps.analytics_227444337.events_*` t CROSS JOIN UNNEST (event_params) AS params
 
-- WHERE _TABLE_SUFFIX BETWEEN "20230701" AND "20230705"
WHERE _TABLE_SUFFIX BETWEEN "20230701" AND FORMAT_DATE("%Y%m%d", DATE_SUB(CURRENT_DATE("EST"), INTERVAL 1 DAY))
AND event_name = "clicks"
AND params.key = "screen_name"
-- AND params.value.string_value IN ("map_temp_shortcut","map_clouds_shortcut","map_precip_shortcut")
AND params.value.string_value LIKE "%maps_%"
AND app_info.version LIKE "%.12%";
-- GROUP BY 1
-- ORDER BY 1;

-- Map Engagement
SELECT DISTINCT
-- PARSE_DATE("%Y%m%d", event_date) AS Date,
-- params.value.string_value AS selection,
COUNT(DISTINCT user_pseudo_id) as users

FROM `phoenix-production-apps.analytics_227444337.events_*` t CROSS JOIN UNNEST (event_params) AS params
 
-- WHERE _TABLE_SUFFIX BETWEEN "20230701" AND "20230705"
WHERE _TABLE_SUFFIX BETWEEN "20230701" AND FORMAT_DATE("%Y%m%d", DATE_SUB(CURRENT_DATE("EST"), INTERVAL 1 DAY))
AND event_name = "screenview"
AND params.key = "screen_name"
-- AND params.value.string_value = "arrow_tap_details_sheet"
AND params.value.string_value IN ("hurricane_tracker_detail")
AND app_info.version LIKE "%.12%";
-- GROUP BY 1
-- ORDER BY 1;
