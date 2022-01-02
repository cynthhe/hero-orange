SELECT 
       (CASE WHEN AWX_Channel = "Partner" THEN "Partner"
            WHEN AWX_Channel = "True Direct" THEN "Direct"
            WHEN AWX_Channel = "Push Notifications" THEN "Pushly"
            ELSE Source
        END) AS AWX_Source,
       sum(sessions) as sessions
FROM `maximal-chariot-89720.AWX_Source.AWX_Channel_Groupings_Partitioned` 
WHERE Date BETWEEN "2021-12-01" AND "2021-12-31" #change date range here
      AND Country = "US"
      #AND Country = "ROW"
GROUP BY 1
ORDER BY sessions DESC
