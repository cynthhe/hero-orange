select
Date
, session_id
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
, install_campaign
, install_medium
, install_source
, campaign
, medium
, source
, carrier_name
, install_source_user
, user_pseudo_id
, user_type
, app_subscription_type
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

from
(select
Date
, session_id
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
, CASE WHEN event_name="install" THEN LAST_VALUE(campaign ignore nulls) over (PARTITION BY session_id order by date_time desc rows between unbounded preceding and unbounded following) END as install_campaign
,CASE WHEN event_name="app_open" THEN LAST_VALUE(campaign ignore nulls) over (PARTITION BY session_id order by date_time desc rows between unbounded preceding and unbounded following) END as campaign
, CASE WHEN event_name="install" THEN LAST_VALUE(medium ignore nulls) over (PARTITION BY session_id order by date_time desc rows between unbounded preceding and unbounded following) END as install_medium
,CASE WHEN event_name="app_open" THEN LAST_VALUE(medium ignore nulls) over (PARTITION BY session_id order by date_time desc rows between unbounded preceding and unbounded following) END as medium
, CASE WHEN event_name="install" THEN LAST_VALUE(source ignore nulls) over (PARTITION BY session_id order by date_time desc rows between unbounded preceding and unbounded following) END as install_source
,CASE WHEN event_name="app_open" THEN LAST_VALUE(source ignore nulls) over (PARTITION BY session_id order by date_time desc rows between unbounded preceding and unbounded following) END as source
, carrier_name
, install_source_user
, user_pseudo_id
, user_type
, app_subscription_type
, event_name
, protip_action_type
, Purchase_value_currency
, purchase_value_USD

from
`phoenix-production-apps.staging_pipeline.01_master_events` 
where date = @run_date

)

group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24
