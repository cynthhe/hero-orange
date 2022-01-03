# Get the monthly by OS by country mobile app (Phoenix) KPIs for the monthly country report

SELECT
      platform as Platform,
      geo.country as Country,
      CAST(SUBSTR(event_date, 1, 4) AS INT64) AS Year, 
      CAST(SUBSTR(event_date, 5, 2) AS INT64) AS Month, 
      COUNT(DISTINCT user_pseudo_id) AS Users,
      COUNT(CASE WHEN event_name = 'session_start' THEN 1 ELSE NULL END) AS Sessions,
      COUNT(CASE WHEN event_name = 'screenview' THEN 1 ELSE NULL END) AS Screenviews
      
FROM `phoenix-production-apps.analytics_227444337.events_*`

WHERE _TABLE_SUFFIX BETWEEN "20211101" AND "20211130"
      #BETWEEN FORMAT_DATE("%Y%m%d", DATE_TRUNC(DATE_SUB(DATE_TRUNC(CURRENT_DATE("EST"), MONTH), INTERVAL 1 DAY), MONTH))
      #AND FORMAT_DATE("%Y%m%d", DATE_SUB(DATE_TRUNC(CURRENT_DATE("EST"), MONTH), INTERVAL 1 DAY))
      AND platform = "ANDROID"
      #AND platform = "IOS"
GROUP BY 1,2,3,4
ORDER BY Sessions DESC;
