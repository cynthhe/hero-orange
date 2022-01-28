SELECT  PARSE_DATE("%Y%m%d", event_date) AS Date,
        timestamp_micros(event_timestamp) as date_time,
        event_name,
        user_pseudo_id,
        (user_pseudo_id || (SELECT params.value.int_value FROM UNNEST(event_params) params WHERE params.key= "ga_session_id")) as session_id,
        platform,
        device.mobile_brand_name as device_brand,
        device.mobile_model_name as device_model,
        device.mobile_marketing_name as device_marketing_name,
        device.operating_system as device_OS,
        device.operating_system_version as device_OS_version,
        geo.country as country,
        geo.region as region, 
        geo.city as city,
        app_info.version as app_version,
        app_info.install_source as install_source_app_info,
        device.language as `language`,
        stream_id,
        
        case when event_name = 'install' then (SELECT params.value.string_value FROM UNNEST(event_params) params WHERE event_name="install" and params.key= "campaign") 
        when event_name = 'app_open' then (SELECT params.value.string_value FROM UNNEST(event_params) params WHERE event_name="app_open" and params.key= "awx_campaign")
        when event_name = 'firebase_campaign' then (SELECT params.value.string_value FROM UNNEST(event_params) params WHERE event_name="firebase_campaign" and params.key= "campaign")
        when event_name = 'campaign_details' then (SELECT params.value.string_value FROM UNNEST(event_params) params WHERE event_name="campaign_details" and params.key= "campaign")
        else null
        end as campaign,
        
        case when event_name = 'app_open' then (SELECT params.value.string_value FROM UNNEST(event_params) params WHERE event_name="app_open" and params.key= "awx_medium")
        when event_name = 'firebase_campaign' then (SELECT params.value.string_value FROM UNNEST(event_params) params WHERE event_name="firebase_campaign" and params.key= "medium")
        when event_name = 'campaign_details' then (SELECT params.value.string_value FROM UNNEST(event_params) params WHERE event_name="campaign_details" and params.key= "medium")
        else null
        end as medium,	
        
        case when event_name = 'install' then (SELECT params.value.string_value FROM UNNEST(event_params) params WHERE event_name="install" and params.key= "media_source") 
        when event_name = 'app_open' then (SELECT params.value.string_value FROM UNNEST(event_params) params WHERE event_name="app_open" and params.key= "awx_source")
        when event_name = 'firebase_campaign' then (SELECT params.value.string_value FROM UNNEST(event_params) params WHERE event_name="firebase_campaign" and params.key= "source")
        when event_name = 'campaign_details' then (SELECT params.value.string_value FROM UNNEST(event_params) params WHERE event_name="campaign_details" and params.key= "source")
        else null
        end as `source`,

        (CASE WHEN event_name = 'after_interstitial_ad' THEN TRUE ELSE FALSE END) AS after_interstitial_ad,
        (SELECT params.value.string_value FROM UNNEST(event_params) params WHERE event_name="pro_tip" and params.key= "protip_title") as pro_tip_title,
        (SELECT params.value.string_value FROM UNNEST(event_params) params WHERE event_name="pro_tip" and params.key= "protip_action") as protip_action_type,
        (SELECT user.value.string_value	FROM UNNEST(user_properties) user WHERE user.key = 'carrier_name') as carrier_name,
        (SELECT user.value.string_value	FROM UNNEST(user_properties) user WHERE user.key = 'install_source') as install_source_user,
        (SELECT user.value.string_value	FROM UNNEST(user_properties) user WHERE user.key = 'user_type') as user_type,
        (SELECT params.value.string_value FROM UNNEST(event_params) params WHERE event_name="screenview" and params.key= "content_tags") as content_tags,
        
         coalesce((SELECT params.value.string_value FROM UNNEST(event_params) params WHERE event_name in ("screenview") and params.key like "screen_name")
         ,(SELECT params.value.string_value FROM UNNEST(event_params) params WHERE event_name in ("screen_view") and params.key like "firebase_screen")) as screen_name,
        
        (SELECT CASE WHEN (SELECT params.value.int_value FROM UNNEST(event_params) params WHERE event_name="in_app_purchase" and params.key= "free_trial") = 1 THEN "free_trial"
         WHEN (SELECT params.value.int_value FROM UNNEST(event_params) params WHERE event_name="in_app_purchase" and params.key= "subscription") = 1 THEN 'subscription'
         else null
         end) as app_subscription_type,
        
        (SELECT params.value.string_value	 FROM UNNEST(event_params) params WHERE event_name="in_app_purchase" and params.key= "currency") as currency,
        
        SUM((SELECT SAFE_DIVIDE(ifnull(params.value.int_value,0),1000000) FROM UNNEST(event_params) params WHERE event_name="in_app_purchase" and params.key= "value")) as Purchase_value_currency,
        SUM((ifnull(event_value_in_usd,0))) as purchase_value_USD
        

FROM `phoenix-production-apps.analytics_227444337.events_20*`
where _TABLE_SUFFIX = format_date('%y%m%d', @run_date)

group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31
