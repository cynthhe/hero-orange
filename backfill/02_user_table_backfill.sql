select
Date
, user_pseudo_id
, platform
, device_brand
, device_model
, device_OS
, country
, region
, city
, app_version
, install_source_app_info
, language
, stream_id
, campaign
, medium
, source
, after_interstitial_ad
, carrier_name
, install_source_user
, user_type
, content_tags
, screen_name
, app_subscription_type
, COUNT(CASE WHEN event_name = 'session_start' THEN 1 ELSE NULL END) AS sessions
, COUNT(CASE WHEN event_name = 'screenview' THEN 1 ELSE NULL END) AS custom_screenviews
, COUNT(CASE WHEN event_name = 'screen_view' THEN 1 ELSE NULL END) AS default_screen_views
, COUNT(CASE WHEN event_name = 'app_open' THEN 1 ELSE NULL END) AS app_opens

from `phoenix-production-apps.staging_pipeline.01_master_events` 

where date = @run_date

group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23
