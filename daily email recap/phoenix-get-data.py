from google.oauth2 import service_account
import pandas_gbq
import pandas_gbq
import pandas as pd
from tqdm import tqdm
from datetime import datetime, timedelta

credentials = service_account.Credentials.from_service_account_file(
   'DigitalMedia-dcc7b32fd489.json',
)
project_id = 'phoenix-production-apps' #dont touch this

#getting dates
def yesterday_date():
   yesterday = datetime.strftime(datetime.now() - timedelta(days=1, hours=4), '%Y%m%d')
   #
   #print (yesterday)
   return (yesterday)
def yesterday_date_int():
   yest_date_int = datetime.strftime(datetime.now() - timedelta(days=1, hours=4), '%d')
   return int(yest_date_int)
def yesterday_month_int():
   yest_mo_int = datetime.strftime(datetime.now() - timedelta(days=1, hours=4), '%m')
   return int(yest_mo_int)
def yesterday_year_int():
   yesterday_year_int = datetime.strftime(datetime.now() - timedelta(days=1, hours=4), '%Y')
   return int(yesterday_year_int)
def first_day_of_year():
    yesterday_year_str = datetime.strftime(datetime.now() - timedelta(days=1, hours=4), '%Y')
    bob = (yesterday_year_str + "0101")
    return bob
def first_day_of_month():
    import datetime
    def first_day_of_month(date):
        first_day = datetime.datetime(date.year, date.month, 1)
        return first_day.strftime('%Y%m%d')
    first_day = first_day_of_month(datetime.date(yesterday_year_int(), yesterday_month_int(), yesterday_date_int()))
    #print (first_day)

    return first_day

def last_day_of_last_month():
    today = datetime.date.today()
    first = today.replace(day=1)
    lastMonth = first - datetime.timedelta(days=1)
    print(lastMonth.strftime('%Y%m%d'))

#getting country data
def daily_country_data(): #Daily Country Data
   date_1 = yesterday_date()
   sql = """
SELECT
 event_date AS date,
 geo.country AS country,
 device.operating_system AS device_OS,
COUNT(CASE
     WHEN event_name = 'session_start' THEN 1
   ELSE
   NULL
 END
   ) AS session,
 COUNT(CASE
     WHEN event_name = 'screenview' THEN 1
   ELSE
   NULL
 END
   ) AS screenView,     
count(distinct user_pseudo_id) as Users,
FROM
 `phoenix-production-apps.analytics_227444337.events_{}`
GROUP BY
 1,
 2,
 3
ORDER BY date, Users DESC 
   """.format(date_1)

   df = pandas_gbq.read_gbq(
       sql,
       project_id=project_id,
       dialect="standard"
   )
   df['date'] =  pd.to_datetime(df['date'], format='%Y/%m/%d')
   df["New Date"] = yesterday_date()
   cols = df.columns.tolist()
   cols2 = cols[-1:] + cols[:-1]
   df = df[cols2]
   del df['date']
   #print(df.head)
   return df
def monthly_country_data(): #Monthly Country Data
   yest_date = yesterday_date()
   first_of_mo_date = first_day_of_month()
   first_of_year = first_day_of_year()
   sql = """
SELECT
 CURRENT_DATETIME() AS date,
  geo.country AS country,
 device.operating_system AS device_OS,
COUNT(CASE
     WHEN event_name = 'session_start' THEN 1
   ELSE
   NULL
 END
   ) AS session,
 COUNT(CASE
     WHEN event_name = 'screenview' THEN 1
   ELSE
   NULL
 END
   ) AS screenView,     
count(distinct user_pseudo_id) as Users,
FROM
 `phoenix-production-apps.analytics_227444337.events_*`
WHERE
 _TABLE_SUFFIX BETWEEN '{}' AND '{}'
GROUP BY
 1,
 2,3
ORDER BY session DESC 
   """.format(first_of_mo_date,yest_date)

   df = pandas_gbq.read_gbq(
       sql,
       project_id=project_id,
       dialect="standard"
   )
   df['date'] =  pd.to_datetime(df['date'], format='%Y/%m/%d').dt.date

   #print(df.head)
   df["New Date"] = first_day_of_month() + ' - ' + yesterday_date()
   cols = df.columns.tolist()
   cols2 = cols[-1:] + cols[:-1]
   df = df[cols2]
   del df['date']
   #print(df.head)
   return df
def yearly_country_data(): #yearly Country Data
   yest_date = yesterday_date()
   first_of_mo_date = first_day_of_month()
   first_of_year = first_day_of_year()

   sql = """
SELECT
 CURRENT_DATETIME() AS date,
  geo.country AS country,
 device.operating_system AS device_OS,
COUNT(CASE
     WHEN event_name = 'session_start' THEN 1
   ELSE
   NULL
 END
   ) AS session,
 COUNT(CASE
     WHEN event_name = 'screenview' THEN 1
   ELSE
   NULL
 END
   ) AS screenView,     
count(distinct user_pseudo_id) as Users,
FROM
 `phoenix-production-apps.analytics_227444337.events_*`
WHERE
 _TABLE_SUFFIX BETWEEN '{}' AND '{}'
GROUP BY
 1,
 2,3
ORDER BY session DESC 
   """.format(first_of_year,yest_date)

   df = pandas_gbq.read_gbq(
       sql,
       project_id=project_id,
       dialect="standard"
   )
   df['date'] =  pd.to_datetime(df['date'], format='%Y/%m/%d')

   df["New Date"] = first_day_of_year() + ' - ' + yesterday_date()
   cols = df.columns.tolist()
   cols2 = cols[-1:] + cols[:-1]
   df = df[cols2]
   del df['date']
   #print(df.head)
   return df

#getting normal data
def daily_metrics_data(): #Daily Metrics by Day
   yest_date = yesterday_date()
   #yest_date = "20210313"
   sql = """
SELECT
event_date AS date,
 device.operating_system AS device_OS,
 CASE WHEN geo.country = "United States" THEN "US"
      ELSE "ROW"
        END as country,
COUNT(CASE
     WHEN event_name = 'session_start' THEN 1
   ELSE
   NULL
 END
   ) AS session,
 COUNT(CASE
     WHEN event_name = 'screenview' THEN 1
   ELSE
   NULL
 END
   ) AS screenView,     
count(distinct user_pseudo_id) as Users,
FROM
 `phoenix-production-apps.analytics_227444337.events_{}`
GROUP BY
 1,
 2,3
ORDER BY date DESC 
   """.format(yest_date)

   df = pandas_gbq.read_gbq(
       sql,
       project_id=project_id,
       dialect="standard"
   )
   df['date'] =  pd.to_datetime(df['date'], format='%Y/%m/%d').dt.date
   df["blank1"] = ""
   df["blank2"] = ""
   df["date_str"] = ""
   df['date_str'] = df['date']
   df['date_str'] = df['date_str'].astype(str)
   df[['Year','Month','Day']] = df.date_str.str.split("-", expand=True)
   df = df[['date','device_OS','country','session','screenView','Users','blank1','blank2','date_str','Day','Month','Year']]

   return df

def monthly_metrics_data():
   yest_date = yesterday_date()
   first_of_mo_date = first_day_of_month()
   first_of_year = first_day_of_year()

   sql = """
SELECT
DATE_TRUNC(PARSE_DATE('%Y%m%d',event_date), MONTH) as date,
 platform AS device_OS,
CASE WHEN geo.country = "United States" THEN "US"
      ELSE "ROW"
        END as country,
COUNT(CASE
     WHEN event_name = 'session_start' THEN 1
   ELSE
   NULL
 END
   ) AS session,
 COUNT(CASE
     WHEN event_name = 'screenview' THEN 1
   ELSE
   NULL
 END
   ) AS screenView,     
count(distinct user_pseudo_id) as Users,
FROM
 `phoenix-production-apps.analytics_227444337.events_*`
WHERE
 _TABLE_SUFFIX BETWEEN '{}' AND '{}'
GROUP BY
 1,2,3
ORDER BY date ASC 
   """.format(first_of_mo_date,yest_date)
#yest_date
   df = pandas_gbq.read_gbq(
       sql,
       project_id=project_id,
       dialect="standard"
   )

   df['date'] = pd.to_datetime(df['date'], format='%Y/%m/%d').dt.date
   df["date_str"] = ""
   df['date_str'] = df['date']
   df['date_str'] = df['date_str'].astype(str)
   df[['Year', 'Month', 'Day']] = df.date_str.str.split("-", expand=True)
   df = df[['date', 'device_OS', 'country', 'session', 'screenView', 'Users', 'date_str', 'Day',
            'Month', 'Year']]

   return df
