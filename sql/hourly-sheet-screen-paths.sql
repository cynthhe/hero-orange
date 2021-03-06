WITH all_hourly_screens AS (
  SELECT
    user_pseudo_id,
    (SELECT params.value.int_value FROM UNNEST(event_params) params WHERE event_name = "screenview" and params.key = "ga_session_number") as session_number,
    (SELECT params.value.string_value FROM UNNEST(event_params) params WHERE event_name = "screenview" and params.key = "screen_name") as screen_name,
    event_timestamp
FROM `phoenix-production-apps.analytics_227444337.events_*` t CROSS JOIN UNNEST (event_params) AS params
WHERE _TABLE_SUFFIX BETWEEN '20210101' AND '20210101' --> Adjust date range here
AND platform = "ANDROID"
AND params.value.string_value IN ("hourly_forecast","hourly_details_sheet")
AND (SELECT params.value.int_value FROM UNNEST(event_params) params WHERE event_name="screenview" and params.key= "ga_session_number") IS NOT NULL
ORDER BY user_pseudo_id, session_number, event_timestamp
)

, screen_index AS (
  SELECT
    user_pseudo_id,
    session_number,
    session_id,
    screen_name,
    event_timestamp,
    screen_number
  FROM (
    SELECT
      user_pseudo_id,
      session_number,
      screen_name,
      event_timestamp,
      CONCAT(user_pseudo_id, "-", session_number) AS session_id,
      ROW_NUMBER() OVER (PARTITION BY CONCAT(user_pseudo_id, "-", session_number) ORDER BY event_timestamp ASC) AS screen_number
    FROM all_hourly_screens
    WHERE (user_pseudo_id, "-", session_number) IS NOT NULL
    )
  ORDER BY user_pseudo_id, session_number, event_timestamp
  )

, user_journey AS
  (
    SELECT 
      user_pseudo_id,
      session_number,
      session_id,
      MAX(IF(screen_number = 1, screen_name, NULL)) as screen_1,
      MAX(IF(screen_number = 2, screen_name, NULL)) as screen_2,
      MAX(IF(screen_number = 3, screen_name, NULL)) as screen_3,
      MAX(IF(screen_number = 4, screen_name, NULL)) as screen_4,
      MAX(IF(screen_number = 5, screen_name, NULL)) as screen_5,
      MAX(IF(screen_number = 6, screen_name, NULL)) as screen_6,
      MAX(IF(screen_number = 7, screen_name, NULL)) as screen_7,
      MAX(IF(screen_number = 8, screen_name, NULL)) as screen_8,
      MAX(IF(screen_number = 9, screen_name, NULL)) as screen_9,
      MAX(IF(screen_number = 10, screen_name, NULL)) as screen_10
    FROM screen_index 
    WHERE session_id IS NOT NULL
    GROUP BY 1,2,3
    ORDER BY session_id
  )

, screen_path_journey AS (
SELECT 
  CONCAT(screen_1,
  IF(screen_2 IS NULL,"",">"),IFNULL(screen_2,""),
  IF(screen_3 IS NULL,"",">"),IFNULL(screen_3,""),
  IF(screen_4 IS NULL,"",">"),IFNULL(screen_4,""),
  IF(screen_5 IS NULL,"",">"),IFNULL(screen_5,""),
  IF(screen_6 IS NULL,"",">"),IFNULL(screen_6,""),
  IF(screen_7 IS NULL,"",">"),IFNULL(screen_7,""),
  IF(screen_8 IS NULL,"",">"),IFNULL(screen_8,""),
  IF(screen_9 IS NULL,"",">"),IFNULL(screen_9,""),
  IF(screen_10 IS NULL,"",">"),IFNULL(screen_10,"")
  ) AS screen_path,
  user_pseudo_id AS users,
  session_id AS sessions
FROM user_journey 
WHERE session_id IS NOT NULL
GROUP BY 1,2,3
ORDER BY sessions desc
)

SELECT
  CONTAINS_SUBSTR(screen_path, "hourly_details_sheet>hourly_forecast>hourly_details_sheet") AS close,
  CONTAINS_SUBSTR(screen_path, "hourly_details_sheet>hourly_details_sheet") AS swipe,
  COUNT(DISTINCT users) AS users,
  COUNT(DISTINCT sessions) AS sessions
FROM screen_path_journey
GROUP BY 1,2;
