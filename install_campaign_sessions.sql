WITH user_event_list AS
     (
       SELECT 
              B.date AS activity_date,
              B.event_name,
              A.user_pseudo_id, 
              B.platform,
              B.device_brand,
              B.device_model,
              B.device_marketing_name,
              B.device_OS_version,
              B.country,
              B.region,
              B.app_version,
              B.install_source_app_info,
              A.install_date,
              A.install_time,
              A.install_source,
              A.install_campaign,
              A.install_medium
       FROM `phoenix-production-apps.digital_turbine.02_install_source_users` AS A
       LEFT JOIN `phoenix-production-apps.staging_pipeline.01_master_events` AS B
       ON A.user_pseudo_id = B.user_pseudo_id
       WHERE #Date = DATE_SUB(CURRENT_DATE("EST"), INTERVAL 1 DAY) 
             Date BETWEEN "2023-06-01" AND "2023-06-30"
             #AND install_date <= "2022-06-08"
             AND install_source <> "organic"
             AND install_source IN("digitalturbine_int","googleadwords_int","Apple Search Ads")
             AND country = "United States"
             AND event_name IN ("install", "app_open", "session_start", "screenview", "app_remove", "in_app_purchase", "first_open")
       ORDER BY user_pseudo_id
      )
      
# To aggregate the metrics (e.g. sessions, screenviews, etc) from the above staging query by counting the corresponding event names

SELECT 
       -- activity_date, 
       -- user_pseudo_id, 
       -- install_date, 
       -- install_time, 
       -- DATE_DIFF(activity_date, install_date, DAY) AS days_since_acquisition,
       install_source, 
       -- install_campaign, 
       -- install_medium,
       platform,
       -- device_brand,
       -- device_model,
       -- device_marketing_name,
       -- device_OS_version,
       -- country,
       -- region,
       -- app_version,
       -- install_source_app_info AS FB_default_install_source,
       -- COUNT(CASE WHEN event_name = 'app_open' THEN 1 ELSE NULL END) AS app_opens,
       COUNT(CASE WHEN event_name = 'session_start' THEN 1 ELSE NULL END) AS sessions,
       COUNT(DISTINCT user_pseudo_id) AS users
       -- COUNT(CASE WHEN event_name = 'screenview' THEN 1 ELSE NULL END) AS screenviews,
       -- COUNT(CASE WHEN event_name = 'app_remove' THEN 1 ELSE NULL END) AS uninstalls,
       -- COUNT(CASE WHEN event_name = 'in_app_purchase' THEN 1 ELSE NULL END) AS in_app_purchases,
       -- COUNT(CASE WHEN event_name = 'install' THEN 1 ELSE NULL END) AS installs,
       -- COUNT(CASE WHEN event_name = 'first_open' THEN 1 ELSE NULL END) AS first_opens
FROM user_event_list
GROUP BY 1,2
ORDER BY install_source
-- GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17
-- ORDER BY user_pseudo_id, activity_date


#CREATE OR REPLACE TABLE 
 
 /*
 CREATE TABLE digital_turbine.03_all_install_user_KPIs
 PARTITION BY activity_date
 OPTIONS(
   description="Each row represents by user by activity date. This starts on 2/21 and includes both organic & non-organic installs. Partitioned by the activity date"
 ) AS

 SELECT *
 FROM `phoenix-production-apps.digital_turbine.03_non_partitioned`
*/

# Delete Data
/*
DELETE FROM `phoenix-production-apps.digital_turbine.03_all_install_user_KPIs` 
WHERE activity_date=[DATE]
*/

 # Backfill Data
/*
INSERT `phoenix-production-apps.digital_turbine.03_all_install_user_KPIs` --Line 1
(activity_date, user_pseudo_id, install_date, install_time, days_since_acquisition, install_source, install_campaign, install_medium, platform, device_brand, device_model, device_marketing_name, device_OS_version, country, region, app_version, FB_default_install_source, app_opens, sessions, screenviews, uninstalls, in_app_purchases, installs, first_opens)

SELECT *
FROM `phoenix-production-apps.digital_turbine.03_backfill_[DATE]`
*/

# Check Data

/*
SELECT activity_date,
       count(*) as events
FROM `phoenix-production-apps.digital_turbine.03_all_install_user_KPIs` 
WHERE activity_date >= DATE_SUB(CURRENT_DATE("EST"), INTERVAL 14 DAY)
GROUP BY 1
ORDER BY 1
*/
