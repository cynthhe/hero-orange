# Check the event list, passing yes for either string_value or int_value

select 
  event_name
  ,params.key as param_key
  ,(case when params.value.string_value is not null then "yes" else null end) as string_value # to limit number of rows
  ,(case when params.value.int_value is not null then "yes" else null end) as int_value # to limit number of rows
from `phoenix-production-apps.analytics_227444337.events_*` t 
cross join unnest (event_params) AS params
where _table_suffix between "20210401" and "20210401" -- Adjust date range
group by 1,2,3,4
order by event_name, param_key;
