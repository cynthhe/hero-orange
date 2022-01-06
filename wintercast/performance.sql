SELECT #PARSE_DATE("%Y%m%d", event_date) AS Date, 
       #event_name, 
       #params.key as param_key, 
       #params.value.string_value as screen_name, 
       count(*) as screenviews,
       count(distinct user_pseudo_id) as users

FROM `phoenix-production-apps.analytics_227444337.events_*` t CROSS JOIN UNNEST (event_params) AS params
 
WHERE _TABLE_SUFFIX BETWEEN "20211104" AND "20211212"    -- Adjust date range
     AND event_name = "screenview"
     AND params.key = "screen_name"
     AND params.value.string_value IN ("wintercast_5day","wintercast_list")
     #AND geo.country = "United States"
#GROUP BY 1
#ORDER BY 1;
