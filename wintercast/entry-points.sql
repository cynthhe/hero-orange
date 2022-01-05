-- Wintercast Entry Points
SELECT DISTINCT
       #event_date,
       params.value.string_value,
       #platform,
       COUNT(*) AS clicks,
       COUNT(DISTINCT user_pseudo_id) AS users,
       #COUNT(*) as screenviews
FROM `phoenix-production-apps.analytics_227444337.events_*` t CROSS JOIN UNNEST (event_params) AS params
WHERE _TABLE_SUFFIX BETWEEN "20211110" AND "20220103"

# Alerts Banner in Today
#AND event_name = "alerts_nav"
#AND params.key = "alert_type"
#AND params.value.string_value = "WINTER"

#5th Bottom Nav
#AND event_name = "main_nav_bottom"
#AND params.key = "menu_action"
#AND params.value.string_value IN ("wintercast")

# Entry from Hourly and Daily Details
#AND event_name = "clicks"
#AND params.key = "click_button"
#AND params.value.string_value IN ("wintercast_entry")

# See More in Today
AND event_name = "clicks"
AND params.key = "click_button"
AND params.value.string_value IN ("wintercast_see_more")

#AND device.operating_system = "iOS"
#AND device.operating_system = "Android"

#AND event_name = "screenview"
#AND params.key = "screen_name"
#AND params.value.string_value IN ("now","wintercast_list","wintercast_5day")

#AND geo.country = "United States"
GROUP BY 1
#ORDER BY 1;
