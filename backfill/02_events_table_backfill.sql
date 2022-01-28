select
Date
, event_name
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
, pro_tip_title
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
, COUNT(CASE WHEN protip_action_type = 'click' THEN 1 ELSE NULL END) as pro_tip_click
, COUNT(CASE WHEN protip_action_type = 'view' THEN 1 ELSE NULL END) as pro_tip_view
, COUNT(CASE WHEN protip_action_type = 'dismiss' THEN 1 ELSE NULL END) as pro_tip_dismiss
, COUNT(CASE WHEN event_name = 'interstitial_ad_imp' THEN 1 ELSE NULL END) AS count_interstital_ad
, COUNT(CASE WHEN event_name = 'after_interstitial_ad' THEN 1 ELSE NULL END) AS count_after_interstital_ad
, COUNT(CASE WHEN event_name = 'install' THEN 1 ELSE NULL END) AS install
, COUNT(CASE WHEN app_subscription_type = 'free_trial' THEN 1 ELSE NULL END) as free_trials
, COUNT(CASE WHEN app_subscription_type = 'subscription' THEN 1 ELSE NULL END) as subscription
, SUM(Purchase_value_currency) as Purchase_value_currency
, SUM(purchase_value_USD) as purchase_value_USD
, COUNT(CASE WHEN event_name = 'location_favorite' THEN 1 ELSE NULL END) AS location_favorite

from `phoenix-production-apps.staging_pipeline.01_master_events` 

where date = @run_date

group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23
