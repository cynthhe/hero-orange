SELECT #PARSE_DATE("%Y%m%d", event_date) AS Date, 
       #event_name, 
       #params.key as param_key, 
       #params.value.string_value as screen_name, 
       geo.country,
       geo.region,
       count(*) as screenviews,
       count(distinct user_pseudo_id) as users

FROM `phoenix-production-apps.analytics_227444337.events_*` t CROSS JOIN UNNEST (event_params) AS params
 
WHERE _TABLE_SUFFIX BETWEEN "20211101" AND "20211118"    -- Adjust date range
     AND event_name = "screenview"
     AND params.key = "screen_name"
     AND params.value.string_value IN ("hurricane_tracker_detail","hurricane_tracker_list","maps_tropical_storm_path")
GROUP BY 1,2#,3#,4
ORDER BY users desc
