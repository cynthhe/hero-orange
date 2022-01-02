# Update on the app side: legacy app data has duplicate users from the Phoenix app, so we need to subtract the overcounting users from legacy

#Android Phoenix Users in Legacy User Base
SELECT
DATE(EXTRACT(YEAR FROM PARSE_DATE("%Y%m%d", date)), EXTRACT(MONTH FROM PARSE_DATE("%Y%m%d", date)),1) AS year_month,
(CASE WHEN geoNetwork.country = "United States" THEN "US" ELSE "ROW" END) AS country,
COUNT(DISTINCT fullvisitorid) AS users
FROM `maximal-chariot-89720.64022857.ga_sessions_*` CROSS JOIN UNNEST (hits) hits
WHERE _TABLE_SUFFIX BETWEEN '20210601' AND '20210630' #[change data range in here]
AND regexp_contains(hits.appInfo.appVersion, r'^7.')
GROUP BY 1,2
ORDER BY 1;

#iOS Phoenix Users in Legacy User Base
SELECT
DATE(EXTRACT(YEAR FROM PARSE_DATE("%Y%m%d", date)), EXTRACT(MONTH FROM PARSE_DATE("%Y%m%d", date)),1) AS year_month,
(CASE WHEN geoNetwork.country = "United States" THEN "US" ELSE "ROW" END) AS country,
COUNT(DISTINCT fullvisitorid) AS users
FROM `maximal-chariot-89720.78208874.ga_sessions_*` CROSS JOIN UNNEST (hits) hits
WHERE _TABLE_SUFFIX BETWEEN '20210601' AND '20210630' #[change data range in here]
AND regexp_contains(hits.appInfo.appVersion, r'^14.')
GROUP BY 1,2
ORDER BY 1,2;
