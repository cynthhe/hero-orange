-- Upsell Entry Buttons 2023
SELECT DISTINCT
event_name,
params.key as param_key, 
params.value.string_value as selection,
COUNT(DISTINCT user_pseudo_id) as users

FROM `phoenix-production-apps.analytics_227444337.events_*` t CROSS JOIN UNNEST (event_params) AS params
 
-- WHERE _TABLE_SUFFIX BETWEEN "20230601" AND FORMAT_DATE("%Y%m%d", DATE_SUB(CURRENT_DATE("EST"), INTERVAL 1 DAY))
WHERE _TABLE_SUFFIX BETWEEN "20230601" AND "20230630"
AND params.key = "upsell_cta"
AND (app_info.version LIKE "%.9%"
OR app_info.version LIKE "%.10%"
OR app_info.version LIKE "%.11%"
OR app_info.version LIKE "%.12%")
GROUP BY 1,2,3
ORDER BY 1,2,3;

-- Upsell Entry Buttons | Total Users 2023
SELECT DISTINCT
event_name,
params.key as param_key, 
COUNT(DISTINCT user_pseudo_id) as users

FROM `phoenix-production-apps.analytics_227444337.events_*` t CROSS JOIN UNNEST (event_params) AS params
 
-- WHERE _TABLE_SUFFIX BETWEEN "20230601" AND FORMAT_DATE("%Y%m%d", DATE_SUB(CURRENT_DATE("EST"), INTERVAL 1 DAY))
WHERE _TABLE_SUFFIX BETWEEN "20230601" AND "20230630"
AND event_name = "upsell_entry"
AND params.key = "upsell_cta"
AND (app_info.version LIKE "%.9%"
OR app_info.version LIKE "%.10%"
OR app_info.version LIKE "%.11%"
OR app_info.version LIKE "%.12%")
GROUP BY 1,2
ORDER BY 1,2;

-- Upsell Entry Buttons 2022
SELECT DISTINCT
event_name,
params.key as param_key, 
params.value.string_value as selection,
COUNT(DISTINCT user_pseudo_id) as users

FROM `phoenix-production-apps.analytics_227444337.events_*` t CROSS JOIN UNNEST (event_params) AS params
 
-- WHERE _TABLE_SUFFIX BETWEEN "20220601" AND FORMAT_DATE("%Y%m%d", DATE_SUB(CURRENT_DATE("EST"), INTERVAL 366 DAY))
WHERE _TABLE_SUFFIX BETWEEN "20220601" AND "20220630"
AND event_name = "upsell_entry"
AND params.key = "upsell_cta"
GROUP BY 1,2,3
ORDER BY 1,2,3;

-- Upsell Entry Buttons | Total Users 2022
SELECT DISTINCT
event_name,
params.key as param_key, 
COUNT(DISTINCT user_pseudo_id) as users

FROM `phoenix-production-apps.analytics_227444337.events_*` t CROSS JOIN UNNEST (event_params) AS params
 
-- WHERE _TABLE_SUFFIX BETWEEN "20220601" AND FORMAT_DATE("%Y%m%d", DATE_SUB(CURRENT_DATE("EST"), INTERVAL 366 DAY))
WHERE _TABLE_SUFFIX BETWEEN "20220601" AND "20220630"
AND event_name = "upsell_entry"
AND params.key = "upsell_cta"
GROUP BY 1,2
ORDER BY 1,2;

-- Upsell Entry Screens
SELECT DISTINCT
event_name,
params.key as param_key, 
params.value.string_value as selection,
COUNT(DISTINCT user_pseudo_id) as users

FROM `phoenix-production-apps.analytics_227444337.events_*` t CROSS JOIN UNNEST (event_params) AS params
 
-- WHERE _TABLE_SUFFIX BETWEEN "20230601" AND FORMAT_DATE("%Y%m%d", DATE_SUB(CURRENT_DATE("EST"), INTERVAL 1 DAY))
WHERE _TABLE_SUFFIX BETWEEN "20230601" AND "20230630"
AND event_name = "upsell_entry"
AND params.key = "screen_name"
AND (app_info.version LIKE "%.9%"
OR app_info.version LIKE "%.10%"
OR app_info.version LIKE "%.11%"
OR app_info.version LIKE "%.12%")
GROUP BY 1,2,3
ORDER BY 1,2,3;

-- Upsell Entry | Total Users 2023
SELECT DISTINCT
event_name,
params.key as param_key,
COUNT(DISTINCT user_pseudo_id) as users

FROM `phoenix-production-apps.analytics_227444337.events_*` t CROSS JOIN UNNEST (event_params) AS params
 
-- WHERE _TABLE_SUFFIX BETWEEN "20230601" AND FORMAT_DATE("%Y%m%d", DATE_SUB(CURRENT_DATE("EST"), INTERVAL 1 DAY))
WHERE _TABLE_SUFFIX BETWEEN "20230601" AND "20230630"
AND event_name = "upsell_entry"
AND params.key = "screen_name"
AND (app_info.version LIKE "%.9%"
OR app_info.version LIKE "%.10%"
OR app_info.version LIKE "%.11%"
OR app_info.version LIKE "%.12%")
GROUP BY 1,2
ORDER BY 1,2;

-- Upsell Entry Screens 2022
SELECT DISTINCT
event_name,
params.key as param_key, 
params.value.string_value as selection,
COUNT(DISTINCT user_pseudo_id) as users

FROM `phoenix-production-apps.analytics_227444337.events_*` t CROSS JOIN UNNEST (event_params) AS params
 
-- WHERE _TABLE_SUFFIX BETWEEN "20220601" AND FORMAT_DATE("%Y%m%d", DATE_SUB(CURRENT_DATE("EST"), INTERVAL 366 DAY))
WHERE _TABLE_SUFFIX BETWEEN "20220601" AND "20220630"
AND event_name = "upsell_entry"
AND params.key = "screen_name"
GROUP BY 1,2,3
ORDER BY 1,2,3;

-- Upsell Entry Screens | Total Users 2022
SELECT DISTINCT
event_name,
params.key as param_key,
COUNT(DISTINCT user_pseudo_id) as users

FROM `phoenix-production-apps.analytics_227444337.events_*` t CROSS JOIN UNNEST (event_params) AS params
 
-- WHERE _TABLE_SUFFIX BETWEEN "20220601" AND FORMAT_DATE("%Y%m%d", DATE_SUB(CURRENT_DATE("EST"), INTERVAL 366 DAY))
WHERE _TABLE_SUFFIX BETWEEN "20220601" AND "20220630"
AND event_name = "upsell_entry"
AND params.key = "screen_name"
GROUP BY 1,2
ORDER BY 1,2;

-- Subscription Experience 2023
SELECT DISTINCT
event_name,
params.key as param_key, 
params.value.string_value as selection,
COUNT(DISTINCT user_pseudo_id) as users

FROM `phoenix-production-apps.analytics_227444337.events_*` t CROSS JOIN UNNEST (event_params) AS params
 
-- WHERE _TABLE_SUFFIX BETWEEN "20230601" AND FORMAT_DATE("%Y%m%d", DATE_SUB(CURRENT_DATE("EST"), INTERVAL 1 DAY))
WHERE _TABLE_SUFFIX BETWEEN "20230601" AND "20230630"
AND event_name = "upsell_accept"
AND params.key = "experience"
AND params.value.string_value NOT LIKE "%onboarding%"
GROUP BY 1,2,3
ORDER BY 1,2,3;

-- Subscription Experience | Total Users 2023
SELECT DISTINCT
event_name,
params.key as param_key, 
COUNT(DISTINCT user_pseudo_id) as users

FROM `phoenix-production-apps.analytics_227444337.events_*` t CROSS JOIN UNNEST (event_params) AS params
 
-- WHERE _TABLE_SUFFIX BETWEEN "20230601" AND FORMAT_DATE("%Y%m%d", DATE_SUB(CURRENT_DATE("EST"), INTERVAL 1 DAY))
WHERE _TABLE_SUFFIX BETWEEN "20230601" AND "20230630"
AND event_name = "upsell_accept"
AND params.key = "experience"
AND params.value.string_value NOT LIKE "%onboarding%"
GROUP BY 1,2
ORDER BY 1,2;

-- Subscription Type 2023
SELECT DISTINCT
event_name,
params.key as param_key, 
params.value.string_value as selection,
COUNT(DISTINCT user_pseudo_id) as users

FROM `phoenix-production-apps.analytics_227444337.events_*` t CROSS JOIN UNNEST (event_params) AS params
 
-- WHERE _TABLE_SUFFIX BETWEEN "20230501" AND FORMAT_DATE("%Y%m%d", DATE_SUB(CURRENT_DATE("EST"), INTERVAL 1 DAY))
WHERE _TABLE_SUFFIX BETWEEN "20230601" AND "20230630"
AND event_name = "upsell_accept"
AND params.key = "upsell_type"
AND (app_info.version LIKE "%.9%"
OR app_info.version LIKE "%.10%"
OR app_info.version LIKE "%.11%"
OR app_info.version LIKE "%.12%")
GROUP BY 1,2,3
ORDER BY 1,2,3;

-- Subscription Type | Total Users 2023
SELECT DISTINCT
event_name,
params.key as param_key, 
COUNT(DISTINCT user_pseudo_id) as users

FROM `phoenix-production-apps.analytics_227444337.events_*` t CROSS JOIN UNNEST (event_params) AS params
 
-- WHERE _TABLE_SUFFIX BETWEEN "20230501" AND FORMAT_DATE("%Y%m%d", DATE_SUB(CURRENT_DATE("EST"), INTERVAL 1 DAY))
WHERE _TABLE_SUFFIX BETWEEN "20230601" AND "20230630"
AND event_name = "upsell_accept"
AND params.key = "upsell_type"
AND (app_info.version LIKE "%.9%"
OR app_info.version LIKE "%.10%"
OR app_info.version LIKE "%.11%"
OR app_info.version LIKE "%.12%")
GROUP BY 1,2
ORDER BY 1,2;

-- Upsell SVs 2023
SELECT DISTINCT
event_name,
params.key as param_key, 
params.value.string_value as selection,
COUNT(DISTINCT user_pseudo_id) as users

FROM `phoenix-production-apps.analytics_227444337.events_*` t CROSS JOIN UNNEST (event_params) AS params
 
-- WHERE _TABLE_SUFFIX BETWEEN "20230501" AND FORMAT_DATE("%Y%m%d", DATE_SUB(CURRENT_DATE("EST"), INTERVAL 1 DAY))
WHERE _TABLE_SUFFIX BETWEEN "20230601" AND "20230630"
AND event_name = "screenview"
AND params.key = "screen_name"
AND params.value.string_value IN("subscription_options","subscription_details","subscription_comparisons")
AND (app_info.version LIKE "%.9%"
OR app_info.version LIKE "%.10%"
OR app_info.version LIKE "%.11%"
OR app_info.version LIKE "%.12%")
GROUP BY 1,2,3
ORDER BY 1,2,3;

-- Upsell Accept Screen Names 2023
SELECT DISTINCT
event_name,
params.key as param_key, 
params.value.string_value as selection,
COUNT(DISTINCT user_pseudo_id) as users

FROM `phoenix-production-apps.analytics_227444337.events_*` t CROSS JOIN UNNEST (event_params) AS params
 
-- WHERE _TABLE_SUFFIX BETWEEN "20230501" AND FORMAT_DATE("%Y%m%d", DATE_SUB(CURRENT_DATE("EST"), INTERVAL 1 DAY))
WHERE _TABLE_SUFFIX BETWEEN "20230601" AND "20230630"
AND event_name = "upsell_accept"
AND params.key = "screen_name"
AND params.value.string_value NOT LIKE "%onboarding%"
AND (app_info.version LIKE "%.9%"
OR app_info.version LIKE "%.10%"
OR app_info.version LIKE "%.11%"
OR app_info.version LIKE "%.12%")
GROUP BY 1,2,3
ORDER BY 1,2,3;
