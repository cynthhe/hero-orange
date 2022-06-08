# -*- coding: utf-8 -*-

#
# Python 3.6, argparse
# Submits query to JW-Player data-retrieval URL
# Dumps data into CSV file
#
# Usage: python <PYTHON_MODULE> --site-id <SITE_ID> --authorization <AUTHORIZATION>
#

import json
import shutil
import sys, os
from apiclient import discovery
from google.oauth2 import service_account
import requests
import pandas as pd
import os
from datetime import datetime, timedelta

__location__ = os.path.realpath(
    os.path.join(os.getcwd(), os.path.dirname(__file__)))

# Report configurations
_REPORT_NAME = 'your-favorite-jw-report' # Report name, will be used as prefix for the CSV file
# Available dimensions: [ 'ad_schedule_id', 'city', 'country_code', 'device_id', 'page_domain',
#                         'eastern_date', 'is_first_play', 'media_id', 'tag', 'player_id', 'playlist_id',
#                         'playlist_type', 'play_reason', 'promotion', 'region', 'platform_id', 'page_url',
#                         'video_duration', 'date'
#                       ]
# Available aggregations: [ 'max', 'min', 'sum'
#                         ]
# Available metrics: [ 'ads_per_viewer', 'ad_clicks', 'ad_completes', 'ad_impressions', 'ad_skips',
#                      'completes', 'complete_rate', 'embeds', 'plays', 'plays_per_viewer', 'play_rate',
#                      '25_percent_completes', '50_percent_completes', '75_percent_completes',
#                      'time_watched', 'time_watched_per_viewer', 'unique_viewers'
#                    ]
# Available sorts: [ 'ASCENDING', 'DESCENDING'
#                  ]

yesterday = datetime.strftime(datetime.now() - timedelta(1), '%Y-%m-%d')
#yesterday = "2020-11-29"
yesterday_year = datetime.strptime(yesterday, '%Y-%m-%d').year
yesterday_month = datetime.strptime(yesterday, '%Y-%m-%d').month
yesterday_day = datetime.strptime(yesterday, '%Y-%m-%d').day


_REPORT_QUERY = { # Query to be executed
	'start_date': yesterday,
	'end_date'  : yesterday,
	'dimensions': ['date'],
	#'dimensions': [
	#	'platform_id'
	#],
	'metrics': [
		{
			'operation': 'sum',
			'field': 'plays'
		}
	],
	'sort': [
		{
			'field': 'plays',
			'order': 'DESCENDING'
		}
	],
	'include_metadata': 1 # 1 to include meta-data, 0 not to
}


secretz = 'OTJkf8ti_w6XLZ8nwjTbx2InTVZaTVNYUnRSemRQZWtKT01uTmtiMHRNVVVkR2NVeFgn'
keyz = 'xvIjuucb'

response = requests.post('https://api.jwplayer.com/v2/sites/' + keyz + '/analytics/queries/?format=csv', # JW-Player data-retrieval URL
						 stream=True,
						 headers={ 'Authorization': secretz }, # Indicates your clearance
						 data=json.dumps(_REPORT_QUERY)) # Passes the query specified above
print('Got response')
if response.status_code != 200:
	error = json.loads(response.text)
	print("Wasn't able to download report due to: "+str(error))
	sys.exit(1)
file_name = _REPORT_NAME + '-' + datetime.today().strftime('%Y%m%d') + '.csv'
print('Creates file: '+file_name)

with open(file_name, 'wb') as out_file:
#with codecs.open(file_name, 'wb', encoding='utf-8', errors='ignore') as out_file:

    shutil.copyfileobj(response.raw, out_file)
print('Wrote response to file: '+file_name)


df = pd.read_csv(os.path.join(__location__, file_name));
df_columns = len(df.columns);  # how many columns in dataframe
newdf = df[df.columns[0:10]]  # Select the columns you want
# print(newdf.head(5))
newdf = newdf.fillna(0)

print(newdf)
dfList = newdf.values.tolist()
dfList[0].extend([yesterday_year,yesterday_month,yesterday_day])
print(dfList)

try:
    secretFile = 'google_client_secret.json'
    scopes = ["https://www.googleapis.com/auth/spreadsheets","https://www.googleapis.com/auth/drive.file","https://www.googleapis.com/auth/drive"]
    secret_file = (os.path.join(__location__, secretFile))
    credentials = service_account.Credentials.from_service_account_file(secret_file, scopes=scopes)
    service = discovery.build('sheets', 'v4', credentials=credentials)
    spreadsheet_id = '1DUc2utbbaSo-vwmB-bRutfpIZIz1rvYZthUBClNjKGc'
    range_name = 'All_Record!A1:E1'
    values = dfList

    if not values:
      data = {
        'values': 'no-data-found'
      }
    else:
      data = {
      'values': values
      }

    #delete the temp csv file
    if os.path.exists(file_name):
      if os.path.exists(os.path.join(__location__, file_name)):
        os.remove(file_name)
      #print("The file was deleted")
    else:
      print("The file does not exist")

    request = service.spreadsheets().values().append(spreadsheetId=spreadsheet_id, range=range_name, valueInputOption='USER_ENTERED', body=data)
    response = request.execute()

except OSError as e:
    print (e)
