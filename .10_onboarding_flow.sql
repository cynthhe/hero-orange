-- Experience
SELECT DISTINCT
platform,
event_name,
params.key as param_key, 
params.value.string_value as selection,
COUNT(DISTINCT user_pseudo_id) AS users,

FROM `phoenix-production-apps.analytics_227444337.events_*` t CROSS JOIN UNNEST (event_params) AS params
 
-- WHERE _TABLE_SUFFIX BETWEEN "20230501" AND FORMAT_DATE("%Y%m%d", DATE_SUB(CURRENT_DATE("EST"), INTERVAL 1 DAY))
WHERE _TABLE_SUFFIX BETWEEN "20230601" AND "20230630"
AND event_name = "onboarding_flow"
AND params.key = "experience"
AND (app_info.version LIKE "%.10%" AND
app_info.version != "6.7.10"
OR app_info.version LIKE "%.11%"
OR app_info.version LIKE "%.12%")
GROUP BY 1,2,3,4
ORDER BY 1,2,3,4;

-- Onboarding Type
SELECT DISTINCT
platform,
event_name,
params.key as param_key, 
params.value.string_value as selection,
COUNT(DISTINCT user_pseudo_id) AS users,

FROM `phoenix-production-apps.analytics_227444337.events_*` t CROSS JOIN UNNEST (event_params) AS params
 
-- WHERE _TABLE_SUFFIX BETWEEN "20230501" AND FORMAT_DATE("%Y%m%d", DATE_SUB(CURRENT_DATE("EST"), INTERVAL 1 DAY))
WHERE _TABLE_SUFFIX BETWEEN "20230601" AND "20230630"
AND event_name = "onboarding_flow"
AND params.key = "onboarding_type"
AND (app_info.version LIKE "%.10%" AND
app_info.version != "6.7.10"
OR app_info.version LIKE "%.11%"
OR app_info.version LIKE "%.12%")
GROUP BY 1,2,3,4
ORDER BY 1,2,3,4;

-- Onboarding Type | Android 13
SELECT DISTINCT
platform,
device.operating_system_version,
event_name,
params.key as param_key, 
params.value.string_value as selection,
COUNT(DISTINCT user_pseudo_id) AS users,

FROM `phoenix-production-apps.analytics_227444337.events_*` t CROSS JOIN UNNEST (event_params) AS params
 
-- WHERE _TABLE_SUFFIX BETWEEN "20230501" AND FORMAT_DATE("%Y%m%d", DATE_SUB(CURRENT_DATE("EST"), INTERVAL 1 DAY))
WHERE _TABLE_SUFFIX BETWEEN "20230601" AND "20230630"
AND event_name = "onboarding_flow"
AND params.key = "onboarding_type"
AND (app_info.version LIKE "%.10%" AND
app_info.version != "6.7.10"
OR app_info.version LIKE "%.11%"
OR app_info.version LIKE "%.12%")
AND device.operating_system_version LIKE "%Android 13%"
GROUP BY 1,2,3,4,5
ORDER BY 1,2,3,4,5;

-- Prompt Terms
SELECT DISTINCT
platform,
event_name,
params.key as param_key, 
params.value.string_value as selection,
COUNT(DISTINCT user_pseudo_id) AS users,

FROM `phoenix-production-apps.analytics_227444337.events_*` t CROSS JOIN UNNEST (event_params) AS params
 
-- WHERE _TABLE_SUFFIX BETWEEN "20230501" AND FORMAT_DATE("%Y%m%d", DATE_SUB(CURRENT_DATE("EST"), INTERVAL 1 DAY))
WHERE _TABLE_SUFFIX BETWEEN "20230601" AND "20230630"
AND event_name = "onboarding_flow"
AND params.key = "prompt_terms"
AND (app_info.version LIKE "%.10%" AND
app_info.version != "6.7.10"
OR app_info.version LIKE "%.11%"
OR app_info.version LIKE "%.12%")
GROUP BY 1,2,3,4
ORDER BY 1,2,3,4;

-- Prompt Terms | Android 13
SELECT DISTINCT
platform,
device.operating_system_version,
event_name,
params.key as param_key, 
params.value.string_value as selection,
COUNT(DISTINCT user_pseudo_id) AS users,

FROM `phoenix-production-apps.analytics_227444337.events_*` t CROSS JOIN UNNEST (event_params) AS params
 
-- WHERE _TABLE_SUFFIX BETWEEN "20230601" AND FORMAT_DATE("%Y%m%d", DATE_SUB(CURRENT_DATE("EST"), INTERVAL 1 DAY))
WHERE _TABLE_SUFFIX BETWEEN "20230601" AND "20230630"
AND event_name = "onboarding_flow"
AND params.key = "prompt_terms"
AND (app_info.version LIKE "%.10%" AND
app_info.version != "6.7.10"
OR app_info.version LIKE "%.11%"
OR app_info.version LIKE "%.12%")
AND device.operating_system_version LIKE "%Android 13%"
GROUP BY 1,2,3,4,5
ORDER BY 1,2,3,4,5;

-- Viewed PP/TOU
SELECT DISTINCT
platform,
event_name,
params.key as param_key, 
params.value.string_value as selection,
COUNT(DISTINCT user_pseudo_id) AS users,

FROM `phoenix-production-apps.analytics_227444337.events_*` t CROSS JOIN UNNEST (event_params) AS params
 
-- WHERE _TABLE_SUFFIX BETWEEN "20230501" AND FORMAT_DATE("%Y%m%d", DATE_SUB(CURRENT_DATE("EST"), INTERVAL 1 DAY))
WHERE _TABLE_SUFFIX BETWEEN "20230601" AND "20230630"
AND event_name = "settings"
AND params.key = "menu_action"
AND params.value.string_value IN("view_privacy_statement","view_terms_of_use")
AND (app_info.version LIKE "%.10%" AND
app_info.version != "6.7.10"
OR app_info.version LIKE "%.11%"
OR app_info.version LIKE "%.12%")
GROUP BY 1,2,3,4
ORDER BY 1,2,3,4;

-- Viewed PP/TOU | Android 13
SELECT DISTINCT
platform,
device.operating_system_version,
event_name,
params.key as param_key, 
params.value.string_value as selection,
COUNT(DISTINCT user_pseudo_id) AS users,

FROM `phoenix-production-apps.analytics_227444337.events_*` t CROSS JOIN UNNEST (event_params) AS params
 
-- WHERE _TABLE_SUFFIX BETWEEN "20230501" AND FORMAT_DATE("%Y%m%d", DATE_SUB(CURRENT_DATE("EST"), INTERVAL 1 DAY))
WHERE _TABLE_SUFFIX BETWEEN "20230601" AND "20230630"
AND event_name = "settings"
AND params.key = "menu_action"
AND params.value.string_value IN("view_privacy_statement","view_terms_of_use")
AND (app_info.version LIKE "%.10%" AND
app_info.version != "6.7.10"
OR app_info.version LIKE "%.11%"
OR app_info.version LIKE "%.12%")
AND device.operating_system_version LIKE "%Android 13%"
GROUP BY 1,2,3,4,5
ORDER BY 1,2,3,4,5;

-- Prompt Location
SELECT DISTINCT
platform,
event_name,
params.key as param_key, 
params.value.string_value as selection,
COUNT(DISTINCT user_pseudo_id) AS users,

FROM `phoenix-production-apps.analytics_227444337.events_*` t CROSS JOIN UNNEST (event_params) AS params
 
-- WHERE _TABLE_SUFFIX BETWEEN "20230501" AND FORMAT_DATE("%Y%m%d", DATE_SUB(CURRENT_DATE("EST"), INTERVAL 1 DAY))
WHERE _TABLE_SUFFIX BETWEEN "20230601" AND "20230630"
AND event_name = "onboarding_flow"
AND params.key = "prompt_loc_os"
AND (app_info.version LIKE "%.10%" AND
app_info.version != "6.7.10"
OR app_info.version LIKE "%.11%"
OR app_info.version LIKE "%.12%")
GROUP BY 1,2,3,4
ORDER BY 1,2,3,4;

-- Prompt Location | Android 13
SELECT DISTINCT
platform,
device.operating_system_version,
event_name,
params.key as param_key, 
params.value.string_value as selection,
COUNT(DISTINCT user_pseudo_id) AS users,

FROM `phoenix-production-apps.analytics_227444337.events_*` t CROSS JOIN UNNEST (event_params) AS params
 
-- WHERE _TABLE_SUFFIX BETWEEN "20230501" AND FORMAT_DATE("%Y%m%d", DATE_SUB(CURRENT_DATE("EST"), INTERVAL 1 DAY))
WHERE _TABLE_SUFFIX BETWEEN "20230601" AND "20230630"
AND event_name = "onboarding_flow"
AND params.key = "prompt_loc_os"
AND (app_info.version LIKE "%.10%" AND
app_info.version != "6.7.10"
OR app_info.version LIKE "%.11%"
OR app_info.version LIKE "%.12%")
AND device.operating_system_version LIKE "%Android 13%"
GROUP BY 1,2,3,4,5
ORDER BY 1,2,3,4,5;

-- Prompt ATT Allow
SELECT DISTINCT
platform,
event_name,
#params.key as param_key, 
#params.value.string_value as selection,
COUNT(DISTINCT user_pseudo_id) AS users,

FROM `phoenix-production-apps.analytics_227444337.events_*` t CROSS JOIN UNNEST (event_params) AS params
 
-- WHERE _TABLE_SUFFIX BETWEEN "20230601" AND FORMAT_DATE("%Y%m%d", DATE_SUB(CURRENT_DATE("EST"), INTERVAL 1 DAY))
WHERE _TABLE_SUFFIX BETWEEN "20230601" AND "20230630"
AND event_name = "prompt_att_allow"
AND params.key = "att_user_type"
AND (app_info.version LIKE "%.10%" AND
app_info.version != "6.7.10"
OR app_info.version LIKE "%.11%"
OR app_info.version LIKE "%.12%")
GROUP BY 1,2
ORDER BY 1,2;

-- Prompt ATT Don't Allow
SELECT DISTINCT
platform,
event_name,
#params.key as param_key, 
#params.value.string_value as selection,
COUNT(DISTINCT user_pseudo_id) AS users,

FROM `phoenix-production-apps.analytics_227444337.events_*` t CROSS JOIN UNNEST (event_params) AS params
 
-- WHERE _TABLE_SUFFIX BETWEEN "20230501" AND FORMAT_DATE("%Y%m%d", DATE_SUB(CURRENT_DATE("EST"), INTERVAL 1 DAY))
WHERE _TABLE_SUFFIX BETWEEN "20230601" AND "20230630"
AND event_name = "prompt_att_dont_allow"
AND params.key = "att_user_type"
AND (app_info.version LIKE "%.10%" AND
app_info.version != "6.7.10"
OR app_info.version LIKE "%.11%"
OR app_info.version LIKE "%.12%")
GROUP BY 1,2
ORDER BY 1,2;

-- Prompt GDPR | Android Total
SELECT DISTINCT
platform,
event_name,
params.key as param_key, 
params.value.string_value as selection,
COUNT(DISTINCT user_pseudo_id) AS users,

FROM `phoenix-production-apps.analytics_227444337.events_*` t CROSS JOIN UNNEST (event_params) AS params
 
-- WHERE _TABLE_SUFFIX BETWEEN "20230501" AND FORMAT_DATE("%Y%m%d", DATE_SUB(CURRENT_DATE("EST"), INTERVAL 1 DAY))
WHERE _TABLE_SUFFIX BETWEEN "20230601" AND "20230630"
AND event_name = "onboarding_flow"
AND params.key = "prompt_gdpr"
AND (app_info.version LIKE "%.10%" AND
app_info.version != "6.7.10"
OR app_info.version LIKE "%.11%"
OR app_info.version LIKE "%.12%")
AND platform = "ANDROID"
GROUP BY 1,2,3,4
ORDER BY 1,2,3,4;

-- Prompt GDPR | Android 13
SELECT DISTINCT
platform,
device.operating_system_version,
event_name,
params.key as param_key, 
params.value.string_value as selection,
COUNT(DISTINCT user_pseudo_id) AS users,

FROM `phoenix-production-apps.analytics_227444337.events_*` t CROSS JOIN UNNEST (event_params) AS params
 
-- WHERE _TABLE_SUFFIX BETWEEN "20230501" AND FORMAT_DATE("%Y%m%d", DATE_SUB(CURRENT_DATE("EST"), INTERVAL 1 DAY))
WHERE _TABLE_SUFFIX BETWEEN "20230601" AND "20230630"
AND event_name = "onboarding_flow"
AND params.key = "prompt_gdpr"
AND (app_info.version LIKE "%.10%" AND
app_info.version != "6.7.10"
OR app_info.version LIKE "%.11%"
OR app_info.version LIKE "%.12%")
AND device.operating_system_version LIKE "%Android 13%"
GROUP BY 1,2,3,4,5
ORDER BY 1,2,3,4,5;

-- Prompt Notifications
SELECT DISTINCT
platform,
event_name,
params.key as param_key, 
params.value.string_value as selection,
COUNT(DISTINCT user_pseudo_id) AS users,

FROM `phoenix-production-apps.analytics_227444337.events_*` t CROSS JOIN UNNEST (event_params) AS params
 
-- WHERE _TABLE_SUFFIX BETWEEN "20230501" AND FORMAT_DATE("%Y%m%d", DATE_SUB(CURRENT_DATE("EST"), INTERVAL 1 DAY))
WHERE _TABLE_SUFFIX BETWEEN "20230601" AND "20230630"
AND event_name = "onboarding_flow"
AND params.key = "prompt_notifications_os"
AND (app_info.version LIKE "%.10%" AND
app_info.version != "6.7.10"
OR app_info.version LIKE "%.11%"
OR app_info.version LIKE "%.12%")
GROUP BY 1,2,3,4
ORDER BY 1,2,3,4;

-- Prompt Notifications | Android 13
SELECT DISTINCT
platform,
device.operating_system_version,
event_name,
params.key as param_key, 
params.value.string_value as selection,
COUNT(DISTINCT user_pseudo_id) AS users,

FROM `phoenix-production-apps.analytics_227444337.events_*` t CROSS JOIN UNNEST (event_params) AS params
 
-- WHERE _TABLE_SUFFIX BETWEEN "20230501" AND FORMAT_DATE("%Y%m%d", DATE_SUB(CURRENT_DATE("EST"), INTERVAL 1 DAY))
WHERE _TABLE_SUFFIX BETWEEN "20230601" AND "20230630"
AND event_name = "onboarding_flow"
AND params.key = "prompt_notifications_os"
AND (app_info.version LIKE "%.10%" AND
app_info.version != "6.7.10"
OR app_info.version LIKE "%.11%"
OR app_info.version LIKE "%.12%")
AND device.operating_system_version LIKE "%Android 13%"
AND geo.continent != "Europe"
GROUP BY 1,2,3,4,5
ORDER BY 1,2,3,4,5;
