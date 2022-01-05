import logging
import datetime
import re
import pandas_gbq
from datetime import datetime, timedelta, date
from google.oauth2 import service_account
import gspread
import numpy as np
from oauth2client.service_account import ServiceAccountCredentials
from gspread_dataframe import get_as_dataframe, set_with_dataframe
from inspect import currentframe, getframeinfo
from tabulate import tabulate
from pathlib import Path
import sys
import pandas as pd
from pydrive.drive import GoogleDrive
from pydrive.auth import GoogleAuth
from configparser import ConfigParser

""" Error """
#todo send slack/email when we hit error
class Error_func:
    def __init__(self,frameinfo, err_msg):
        self.filename = ' File name is: ' + (frameinfo.filename)
        self.lineno = ' Line number is: ' + str(frameinfo.lineno)
        self.function = ' Function: ' + str(frameinfo.function)
        self.err_msg = err_msg
        logging.basicConfig(level=logging.DEBUG,
                            format='%(asctime)s | %(levelname)s | %(funcName)s |%(message)s',
                            datefmt='%a, %d %b %Y %H:%M:%S %p',
                            filename='error.log',
                            filemode='w')

        msg = err_msg + '//' + self.lineno + '//' + self.filename + '//' + self.function
        logging.warning(msg)
        raise Exception(msg)

""" Dates """

def date_ninety_days_ago():
   ninety = datetime.strftime(datetime.now() - timedelta(days=180), '%Y%m%d')
   #print (yesterday)
   return ninety

def date_x_days_ago(daysago):
   dateago = datetime.strftime(datetime.now() - timedelta(days=daysago), '%Y-%m-%d')
   #print (yesterday)
   return dateago

def date_x_days_ago_slash(daysago):
   dateago = datetime.strftime(datetime.now() - timedelta(days=daysago), '%#m/%#d/%Y')
   #print (yesterday)
   return dateago

def yesterday_date_full():
   yesterday = datetime.strftime(datetime.now() - timedelta(days=1, hours=4), '%Y%m%d')
   #print (yesterday)
   return (yesterday)
def prev_to_yesterday_date_full():
    yesterday_before = datetime.strftime(datetime.now() - timedelta(days=2, hours=4), '%Y%m%d')
    #print(yesterday_before)
    return (yesterday_before)
def yesterday_day_only():
    yest_date_int = datetime.strftime(datetime.now() - timedelta(days=1, hours=4), '%d')
    #print (yest_date_int)
    return int(yest_date_int)
def yesterday_month_only():
   yest_mo_int = datetime.strftime(datetime.now() - timedelta(days=1, hours=4), '%m')
   return int(yest_mo_int)
def yesterday_year_only():
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
    first_day = first_day_of_month(datetime.date(yesterday_year_only(), yesterday_month_only(), yesterday_day_only()))
    #print (first_day)

    return first_day
def last_day_of_last_month():
    today = datetime.date.today()
    first = today.replace(day=1)
    lastMonth = first - datetime.timedelta(days=1)
    lastMonth = (lastMonth.strftime('%Y%m%d'))
    return lastMonth

def date_this_month(): ##this is used to check if a tab exists
   month = datetime.strftime(datetime.now() - timedelta(days=1, hours=4), '%B-%Y')
   return month

def date_yesterday_date_again(): ##to check cell value I need to get yesterdays date in a different format
   yesterday_2 = datetime.strftime(datetime.now() - timedelta(days=1, hours=4), '%Y-%m-%d').lstrip("0").replace(" 0", " ")
   #print (yesterday_2)
   return yesterday_2

def date_today_date():
    today = date.today()
    d1 = today.strftime("%d")
    #d1 = '01'
    #print(d1)
    return d1

def date_last_month_name():
    today = date.today()
    first = today.replace(day=1)
    lastMonth = first - timedelta(days=1)
    LastMonth1 = lastMonth.strftime('%b-%Y')
    #print (LastMonth1)
    return LastMonth1

def add_dash_to_dates(date):
    dash_date = datetime.strptime(date, '%Y%m%d').date().isoformat()
    return dash_date

""" Google Sheets """

def gspread_func():
    try:
        SCOPES = ['https://www.googleapis.com/auth/spreadsheets', "https://www.googleapis.com/auth/drive.file",
                  "https://www.googleapis.com/auth/drive"]
        abs_path = str(Path(__file__).parent.absolute())
        creds = ServiceAccountCredentials.from_json_keyfile_name(abs_path + '/' + 'creds/sheets_secret.json', SCOPES)
        gc = gspread.authorize(creds)
        return gc

    except:
        Error_func(getframeinfo(currentframe()), 'Sheet Api error')
        sys.exit(1)

def callback():
    callback_list = []
    return callback_list

class sheet_func:

    def __init__(self, which_file=None, which_tab=None):
        self.which_file = which_file
        self.which_tab = which_tab
        self.gc = gspread_func()

    def filename_check(self):
        try:
            self.gc.open(self.which_file)
            return True
        except:
            Error_func(getframeinfo(currentframe()), 'Spreadsheet does not exist')

    def tab_name_check(self):
        try:
            sh = self.gc.open(self.which_file)
            sh.worksheet(self.which_tab)  # By title
            return True

        except:
            Error_func(getframeinfo(currentframe()), 'Worksheet i.e Tab: does not exist')



    def create_google_sheet(self):
        sh = self.gc.create(self.which_file)
        emails = ['accubi.analytics@accuweather.com','asif.rahman@accuweather.com']
        for i in emails:
            sh.share(i, perm_type='user', role='writer')
        print('im creating a sheet')

    def next_available_row(self, which_tab, which_col):
        str_list = list(filter(None, which_tab.col_values(which_col)))
        return str(len(str_list) + 1)

    def get_value_of_second_to_last_cell(self,which_col):
        sc = sheet_func() #need to initiate the class because we are referencing a function(next_available_row) from within a function(get_value...)
        sh = self.gc.open(self.which_file)
        worksheet = sh.worksheet(self.which_tab)
        next_row = sc.next_available_row(worksheet,which_col)
        print ('the row number is : ' + next_row)
        if next_row == '1':
            print ('empty')
            return ('empty')

        else:
            which_row = ((int(next_row)) -1)
            print (which_row)
            print ('row number above')
            cell_value = worksheet.cell(which_row, 1).value
            return (cell_value)

    def get_value_of_cell(self,which_cell):

        sh = self.gc.open(self.which_file)
        worksheet = sh.worksheet(self.which_tab)

        if ':' in which_cell: #If we passed a range do this
            range_cells = worksheet.range(which_cell)
            cell_value = []
            for cell in range_cells:
                cellValue = cell.value
                cell_value.append(cellValue)
            cell_value = list(filter(None, cell_value))

        else: #we didint pass a range just a cell
            cell_value = worksheet.acell(which_cell).value

        return cell_value

    def clear_tab(self): #clear a worksheet
        sh = self.gc.open(self.which_file)
        worksheet = sh.worksheet(self.which_tab)
        worksheet.clear()

    def delete_row(self, row_num):
        if isinstance(row_num, int):
            sh = self.gc.open(self.which_file)
            worksheet = sh.worksheet(self.which_tab)
            worksheet.delete_row(row_num)
        else:
            print('Argument for this function has to be an integer')

    def clear_range_of_cells(self,cell_range, **which):
        sh = self.gc.open(self.which_file)

        if 'which_clear' in which:  #IF WHICH clear exists
            if which["which_clear"] == 'del_prev': #and which clear says del_prev which means delete the same rows and columns from the top of the dataset
                cell_list = (cell_range.split(','))
                startRowIndex = int(cell_list[0])
                endRowIndex = int(cell_list[3])
                endRowIndex = endRowIndex + 1
                startColumnIndex = int(cell_list[0])
                startColumnIndex = startColumnIndex - 1
                endColumnIndex = int(cell_list[2])

                sheetId = sh.worksheet(self.which_tab)._properties['sheetId']
                print (sheetId)
                sh.batch_update({
                    'requests': [
                        {
                            'deleteRange': {
                                'range': {
                                    'sheetId': sheetId,
                                    'startRowIndex': startRowIndex,
                                    'endRowIndex': endRowIndex,
                                    'startColumnIndex': startColumnIndex,
                                    'endColumnIndex': endColumnIndex,
                                },
                                'shiftDimension': 'ROWS',
                            }
                        }
                    ]
                })
            elif which["which_clear"] == 'del_prev_date': #and which clear says del_prev which means delete the same rows and columns from the top of the dataset
                cell_list = (cell_range.split(','))
                startRowIndex = int(cell_list[0])
                endRowIndex = int(cell_list[1])
                endRowIndex = endRowIndex + 1
                startColumnIndex = int(cell_list[0])
                startColumnIndex = startColumnIndex - 1
                endColumnIndex = int(cell_list[2])

                sheetId = sh.worksheet(self.which_tab)._properties['sheetId']
                print (sheetId)
                sh.batch_update({
                    'requests': [
                        {
                            'deleteRange': {
                                'range': {
                                    'sheetId': sheetId,
                                    'startRowIndex': startRowIndex,
                                    'endRowIndex': endRowIndex,
                                    'startColumnIndex': startColumnIndex,
                                    'endColumnIndex': endColumnIndex,
                                },
                                'shiftDimension': 'ROWS',
                            }
                        }
                    ]
                })
            else:
                pass
        else:

            if ":" in cell_range: #this is a regular cell clear it does not shift cells, just deletes and leaves blank cells
                #cell_range = 'A3:B4'
                print (cell_range)
                concat_arg = "'"+self.which_tab +"'" + '!' +cell_range
                sh.values_clear(concat_arg)

            else: #not passing like : a3:b4 but instead like 1, 2 #USED mostly to clear same range size as a dataframe
                print ('im printing from inside of clear function ' + cell_range)
                cell_list = (cell_range.split(','))
                starting_col = int(cell_list[0])
                rows_in_starting_col = int(cell_list[1])
                df_col = int(cell_list[2])
                ending_col = (df_col + starting_col) -1
                starting_col_ltr = (chr(ord('@') + starting_col)) #turn columns number into column letter
                ending_col_ltr = (chr(ord('@') + ending_col)) #turn columns number into column letter
                new_cell_range = str(starting_col_ltr) + '1' + ':' + str(ending_col_ltr) +str(rows_in_starting_col)
                print (new_cell_range)

                new_cell_range = "'" + self.which_tab + "'" + '!' + new_cell_range
                sh.values_clear(new_cell_range)

    def write_data_to_tab(self,**which):
        sh = self.gc.open(self.which_file)
        worksheet = sh.worksheet(self.which_tab)


        def write_data_to_tab_duplicate():
            print ('OK with Dupes FUNCTION and Not Dataframe')

            if 'which_row' and 'which_col' in which:  # if passing col and row do this
                worksheet.update_cell(which["which_row"], which["which_col"], which["which_data"])

            elif 'which_cells' in which:  # for passing range of cells key = A1:B2 value = [[1, 2], [3, 4]]
                create_cell_reference(which["which_cells"], which["which_data"])
                worksheet.update(which["which_cells"], which["which_data"])

            else:  # for passing a cell reference -- just cell number as B14 or T8 then do this
                worksheet.update(which["which_cell"], which["which_data"])

        def write_data_to_tab_no_duplicate():
            print('NO Data Frame and NO Dupes FUNCTION')
            #todo add no duplicate statements for NON dataframe items

        def write_dataframe_to_tab():

            print ('OK with Dupes FUNCTION and I am a Dataframe')

            if 'which_row' in which and 'which_col' in which and 'which_duplicate' in which:
                print('I have all agruments')

                if isinstance((which["which_row"]), int):
                    print('Im adding data')

                    if which["which_clear"] == 'clear_range':
                        print ('I am clearning the cells which I am going to write to')
                        col = str(which["which_col"]) #starting column - comes from caller function
                        row = str(self.next_available_row(worksheet, which["which_col"])) #length of items in that column - latter half comes from caller function
                        df_cols = str(len((which["which_data"]).columns)) # length of dataframe to get the size of data comes from caller function
                        cell_range = col + ',' +row + ',' + df_cols #add all together to pass to function
                        print ('this is the cell range from the caller function: ' + cell_range)

                        sheet_func(self.which_file, self.which_tab).clear_range_of_cells(cell_range) #clear range

                    set_with_dataframe(worksheet, which["which_data"], row=int(which["which_row"]), #send data
                                           col=int(which["which_col"]), include_index=False,
                                           include_column_header=True, resize=False, allow_formulas=True)

                elif isinstance((which["which_row"]), str) and which["which_row"] == 'next':
                    print('Im adding data to the next available row')

                    if 'which_clear' in which:
                        if which["which_clear"] == 'del_prev':
                            print (which["which_clear"])
                            print('I am a DATAFRAME - I will add data to next avail row - and I will remove the same amount of rows I am adding from the top')
                            df = (which["which_data"])
                            col = str(which["which_col"]) #starting column - comes from caller function
                            row = str(self.next_available_row(worksheet, which["which_col"])) #length of items in that column - latter half comes from caller function
                            df_cols = str(len((which["which_data"]).columns)) # length of dataframe to get the size of data comes from caller function
                            df_count_row = str(df.shape[0]) # gives number of row count
                            df_count_col = str(df.shape[1])  # gives number of col count
                            cell_range = col + ',' +row + ',' + df_count_col + ',' + df_count_row #add all together to pass to function
                            print ('this is the cell range from the caller function: ' + cell_range)

                            sheet_func(self.which_file, self.which_tab).clear_range_of_cells(cell_range, which_clear='del_prev')

                        elif which["which_clear"] == 'del_prev_date':
                                print (which["which_clear"])
                                #print ('hello bob')
                                cell_val = (self.get_value_of_cell('A2'))#this assumes there is a header so we skip the first row AND we assume
                                print (cell_val)
                                #that there is a date in the first column

                                data = worksheet.get_all_values()
                                headers = data.pop(0)

                                dfa = pd.DataFrame(data, columns=headers) #create a dataframe from the google sheet content
                                #print(dfa)
                                new_row = (len(dfa[dfa['event_date'] == cell_val])) #this is the number of rows with the same value as cell A2
                                #we will use this to delete the number of rows
                                #todo change event_date to be dynamic

                                df = (which["which_data"])
                                col = str(which["which_col"])  # starting column - comes from caller function
                                row = str(new_row)
                                df_cols = str(len((which[
                                    "which_data"]).columns))  # length of dataframe to get the size of data comes from caller function
                                df_count_row = str(df.shape[0])  # gives number of row count
                                df_count_col = str(df.shape[1])  # gives number of col count
                                cell_range = col + ',' + row + ',' + df_count_col + ',' + df_count_row  # add all together to pass to function
                                print('this is the cell range from the caller function: ' + cell_range)

                                sheet_func(self.which_file, self.which_tab).clear_range_of_cells(cell_range,
                                                                                                 which_clear='del_prev_date')
                        else:
                            pass

                        #print ('I need to remove X columns')

                    next_row = self.next_available_row(worksheet, which["which_col"])
                    #print (next_row)

                    set_with_dataframe(worksheet, which["which_data"], row=int(next_row), col=int(which["which_col"]),
                                       include_index=False,
                                      include_column_header=False, resize=False, allow_formulas=True)
                else:
                    print ('Something is wrong')

        def write_dataframe_to_tab_dupe_checker():
            value_to_find = self.get_value_of_second_to_last_cell(which["which_col"])
            print (value_to_find)
            print ('print value to find above')
            df = which["which_data"]
            if value_to_find == 'empty' and len(df) != 0:  # last part double check to make sure dataframe is not empty
                print('There cant be any duplicates Sheet has no data')
                write_dataframe_to_tab()
                #todo check this on a empty sheet to see if it still works
            else:
                print ('Sheet has data I will check now')
                col_names = (list(df.columns))
                first_col_name = (col_names[0])
                first_col_last_value = df[first_col_name].iloc[-1]
                print('df last col value: ' + first_col_last_value)

                if value_to_find == first_col_last_value:
                    print('Not going to overwrite the data as I have duplicates:' + value_to_find + ' AND ' + first_col_last_value)
                else:
                    write_dataframe_to_tab()
                    pass

        #######################################################################################

        if ('which_duplicate' in which) and (not isinstance(which["which_data"], pd.DataFrame)):
            print (which["which_data"])
            print('I am NOT A DATAFRAME')
            if which["which_duplicate"] == 'y':
                print ('im ok with duplicates')
                write_data_to_tab_duplicate()

            else:
                print ('no duplicate for me please')
                write_data_to_tab_no_duplicate()

        elif 'which_duplicate' in which and isinstance(which["which_data"], pd.DataFrame):
                print ('I AM A DATAFRAME')


                if which["which_duplicate"] == 'y':
                    print('im wont do a duplicate data check')
                    write_dataframe_to_tab()

                elif which["which_duplicate"] == 'n':
                    print('no duplicate for me please - run duplicate checker function')
                    write_dataframe_to_tab_dupe_checker()
                else:
                    pass

        else:
            Error_func(getframeinfo(currentframe()), 'There is a problem uploading data to google sheet - from write data function')

def create_cell_reference(cell_start, cell_values):
    num_rows = len(cell_values) #get the rows needed
    print (num_rows)
    inner_list_items = []
    for i in cell_values:
        #print (len(i))
        inner_list_items.append(len(i))
    def all_same(items):
        return all(x == items[0] for x in items)

    if all_same(inner_list_items):
            print ('Inner list items Same, do something')
            res = int(inner_list_items[0]) #get the columns needed
            print (res)
            res2 = (chr(ord('@') + res)) #turn columns number into column letter
            print(res2)
            ending_cell = (str(res2) + str(num_rows)) #join the column letter and row
            print(ending_cell)
            cell_ref = cell_start + ':' + ending_cell #join the column and row to create cell reference
            print (cell_ref)
    else:
        msg = ('Items being passed are not the same size')
        Error_func(getframeinfo(currentframe()), msg)

""" Pandas Dataframe """

def dataframe_get_sheet_make_df(sheet_name,tab_name,columns): #you must pass the columns you want like this: columns = [7, 8, 9, 10, 11, 12]

    gc = gspread_func()
    sh = gc.open(sheet_name)
    worksheet = sh.worksheet(tab_name)
    df = get_as_dataframe(worksheet, resize = True, parse_dates=True, usecols=columns, header=0, keep_default_na=True,index=False)
    print('Created the DF from : ' + tab_name)
    return (df)

""" Bigquery """

def bigquery_create_new_bq_table(df_name, project_id, dataset_id, table_id):
    #To use this function : df_name is just your data frame
    #project_id : get this from bq
    #dataset_id : get from bq
    #table_id : make up your own -- and the function will create it for you if this is a new table OR get the table name from bq as well
    try:
        import pandas_gbq
        from google.oauth2 import service_account
    except ImportError:
        raise Exception(
            "Could not import libraries `pandas_gbq` or `google.oauth2`, which are "
            "required to be installed in your environment in order "
            "to upload data to BigQuery"
        )

    creds_abs_path = str(Path(__file__).parent.absolute())
    credentials = service_account.Credentials.from_service_account_file(creds_abs_path + '/' + 'creds/bq_secret.json',)

    id_for_gbq = dataset_id + '.' + table_id

    logger = logging.getLogger('pandas_gbq')
    logger.setLevel(logging.DEBUG)
    logger.addHandler(logging.StreamHandler())

    try:
        pandas_gbq.to_gbq(
            df_name, id_for_gbq, project_id=project_id, credentials=credentials, if_exists='fail'
        )
    except pandas_gbq.gbq.TableCreationError as e:
        e = str(e)
        print ('Table Already Exists'+ e)
        try:
            pandas_gbq.to_gbq(
                df_name, id_for_gbq, project_id=project_id, credentials=credentials, if_exists='append'

            )
        except Exception as e:
            e = str(e)
            msg = ('I could not append either:' + e)
            print (msg)
            Error_func(getframeinfo(currentframe()), msg)

            #TODO CREATE FUNCTIONS FOR
            #CREATING A NEW TABLE / APPENDING TO AN EXISTING TABLE / CHECKING IF A TABLE EXISTS / DELETE A TABLE / DELETE ROWS FROM TABLE

#lookup a single value in bq table
def bigquery_lookup_single_value(project_id, dataset_id, table_id, column_name, value_to_find):

   sql = """
SELECT COUNT(1)
FROM `{}.{}.{}`
WHERE {} = '{}'
   """.format(project_id, dataset_id, table_id, column_name, value_to_find)
#yest_date
   df = pandas_gbq.read_gbq(
       sql,
       project_id=project_id,
       dialect="standard"
   )
   print (df)
   if df.empty:
       Error_func(utility.getframeinfo(utility.currentframe()), 'Dataframe is empty')

   else:
       date_list = df.values.tolist() #turn df in to a list
       date_list_1 = (date_list[0]) #get first list item
       numbers = [int(x) for x in date_list_1] #turn list into integer
       x = 0 #we are looking for values greater than zero, which would mean there are rows greater than zero with existing data in bq
       if any(y > x for y in numbers):
            print ('yes target value are already in bigquery')
            value_exists = 'y'
            return value_exists
       else:
           print ('could not find target value in bq - no') #dates do not exist in bq, we are good to upload KEEP GOING
           value_exists = 'n'
           return value_exists


class Send_update_func:
    def __init__(self, frameinfo, update_kind,update_message):
        script_dash_sheet = 'AWX Script Dashboard'
        self.filename = ' File name is: ' + (frameinfo.filename)
        self.lineno = ' Line number is: ' + str(frameinfo.lineno)
        if update_kind == 'Error':
            update_msg = update_message + self.filename + 'on line: ' + self.lineno
        else:
            update_msg = update_message + self.filename + 'on line: ' + self.lineno

""" Google Drive """

def pydrive_func():
    try:
        gauth = GoogleAuth()
        gauth.LocalWebserverAuth()
        drive = GoogleDrive(gauth)
        return drive

    except:
        Error_func(getframeinfo(currentframe()), 'drive Api error')
        sys.exit(1)

def pydrive_get_file_id(fldr_id,file_name):
    try:
      file_list = pydrive_func().ListFile({'q': "'{}' in parents".format(fldr_id)}).GetList()
      #print (file_list)
      title_list = []
      id_list = []
      for files in file_list:
          title_list.append(files['title'])
          id_list.append(files['id'])
      file_dict = dict(zip(title_list, id_list))
      #print (file_dict)

      file_id = (file_dict.get(file_name))
      print('file id is: ' + file_id)
      return file_id

    except:
        print ('error')

def pydrive_download_file(fileName, folder_id, file_name):
    file_downloaded = pydrive_func().CreateFile({'id': pydrive_get_file_id(folder_id, file_name)})
    file_downloaded.GetContentFile(fileName)


""" Task Scheduler - Win Run Time  """

import win32com.client

def get_task_schedule_info(kwd):
    tasker = []
    TASK_ENUM_HIDDEN = 1
    TASK_STATE = {0: 'Unknown',
                  1: 'Disabled',
                  2: 'Queued',
                  3: 'Ready',
                  4: 'Running'}

    scheduler = win32com.client.Dispatch('Schedule.Service')
    scheduler.Connect()

    n = 0
    folders = [scheduler.GetFolder('\\')]
    while folders:
        folder = folders.pop(0)
        folders += list(folder.GetFolders(0))
        tasks = list(folder.GetTasks(TASK_ENUM_HIDDEN))
        n += len(tasks)
        for task in tasks:

            settings = task.Definition.Settings
            if kwd in task.Path:
                Path = ('Path : %s' % task.Path)
                Hidden = ('Hidden: %s' % settings.Hidden)
                State = ('State: %s' % TASK_STATE[task.State])
                Last_Run = ('Last Run: %s' % task.LastRunTime)
                Last_Result = ('Last Result: %s' % task.LastTaskResult)

                return_string = Path + ' // ' + State + ' // ' + Last_Run + ' // ' + Last_Result
                #print (return_string)

                tasker.append(return_string)

                my_string = ' <br><br> '.join(tasker)
    print(tasker)


    return (tasker)
    #print (my_string)

def run_time_info(send_source,kwd):
    #get_task_schedule_info(kwd)
    if send_source == 'email':
        print ('run time  content')

        pre_str = '<li>'
        post_str = '</li>'
        my_string = [pre_str + s + post_str for s in get_task_schedule_info(kwd)]
        print(my_string)
        my_string = ''.join(my_string)
        my_string = '<ul>' + my_string + '</ul>'
        print(my_string)
        #return my_string
        #todo somethere here

    elif send_source == 'slack':
        my_string =  'Error Sending Daily Metrics Email'

    else:
        pass

    return my_string

""" Google Analytics Data via API  """


from googleapiclient.errors import HttpError
from googleapiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials
import random
import time

SCOPES = ['https://www.googleapis.com/auth/analytics.readonly']
KEY_FILE_LOCATION = 'client_secrets.json'
#VIEW_ID = '193013533'

def initialize_analyticsreporting():
  credentials = ServiceAccountCredentials.from_json_keyfile_name(
      KEY_FILE_LOCATION, SCOPES)

  # Build the service object.
  analytics = build('analyticsreporting', 'v4', credentials=credentials)
  print (analytics)
  return analytics

def get_report(analytics, viewID,startDate,endDate,dims,metrics):
    #VIEW_ID = '193097645' #US Only
    VIEW_ID = viewID
    #DIMS = ['ga:date','ga:hour', 'ga:deviceCategory', 'ga:country']
    #METRICS = ['ga:sessions', 'ga:pageviews']
    DIMS = dims
    METRICS = metrics
    return analytics.reports().batchGet(
          body={
            'reportRequests': [
            {
              'viewId': VIEW_ID,
                'dateRanges': [{'startDate': startDate, 'endDate': endDate}],
                'dimensions': [{'name': name} for name in DIMS],
                'metrics': [{'expression': exp} for exp in METRICS]
            }]
          }
      ).execute()

def print_response(response):
        list = []
        # get report data
        for report in response.get('reports', []):
            # set column headers
            columnHeader = report.get('columnHeader', {})
            dimensionHeaders = columnHeader.get('dimensions', [])
            metricHeaders = columnHeader.get('metricHeader', {}).get('metricHeaderEntries', [])
            rows = report.get('data', {}).get('rows', [])

            for row in rows:
                # create dict for each row
                dict = {}
                dimensions = row.get('dimensions', [])
                dateRangeValues = row.get('metrics', [])

                # fill dict with dimension header (key) and dimension value (value)
                for header, dimension in zip(dimensionHeaders, dimensions):
                    dict[header] = dimension

                # fill dict with metric header (key) and metric value (value)
                for i, values in enumerate(dateRangeValues):
                    for metric, value in zip(metricHeaders, values.get('values')):
                        # set int as int, float a float
                        if ',' in value or '.' in value:
                            dict[metric.get('name')] = float(value)
                        else:
                            dict[metric.get('name')] = int(value)

                list.append(dict)

            df = pd.DataFrame(list)
            print (df)
            return df

def makeRequestWithExponentialBackoff(analytics,viewID,startDate,endDate,dims,metrics):

  for n in range(0, 5):
    try:
      return get_report(analytics, viewID,startDate,endDate,dims,metrics)
    except HttpError as error:
        if error.resp.reason in ['userRateLimitExceeded', 'quotaExceeded',
                                 'internalServerError', 'backendError']:
            time.sleep((2 ** n) + random.random())
        else:
            break
  print ("There has been an error, the request never succeeded.")
  Error_func(getframeinfo(utility.currentframe()), 'There has been an error, the request never succeeded.')


def sendGAreportToSheet(sheetName,viewID,startDate,endDate,dims,metrics):
      analytics = initialize_analyticsreporting()
    #response = get_report(analytics)
      sheets_file_name = "GA Real-time via API"
      sheets_tab_name = sheetName
      response = makeRequestWithExponentialBackoff(analytics,viewID,startDate,endDate,dims,metrics)
      d_frame = print_response(response)

      if sheet_func(sheets_file_name, sheets_tab_name).filename_check() is True:
          print('sheet here')
          if sheet_func(sheets_file_name, sheets_tab_name).tab_name_check() is True:
              print('tab is here')

              if len(d_frame) != 0:
                  sheet_func(sheets_file_name, sheets_tab_name).clear_range_of_cells("A2:k5000")

                  some_list = \
                      sheet_func(sheets_file_name, sheets_tab_name).write_data_to_tab(which_data=d_frame, which_row='next',
                                                                            which_col=1,which_duplicate='n')
          else:
              Error_func(utility.getframeinfo(utility.currentframe()), 'Cant find:' + sheets_tab_name)
      else:
          Error_func(utility.getframeinfo(utility.currentframe()), 'Cant find:' + sheets_file_name)
