# Aggregates browser push sessions and pageviews for the month
SELECT Date, 
       Year, 
       Month, 
       Day, 
       #Country, 
       #Device_Category, 
       AWX_Channel, 
       sum(sessions) as sessions, 
       sum(pageviews) as pageviews
       
FROM `AWX_Source.AWX_Channel_Groupings_Partitioned`
WHERE Date 
      BETWEEN "2021-12-01" AND "2021-12-31"
      #AND AWX_Channel IN ("Push Notifications", "Organic Search")
      AND AWX_Channel IN ("Push Notifications")
      
GROUP BY 1,2,3,4,5

ORDER BY Date;
