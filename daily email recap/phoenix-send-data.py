import gspread
from oauth2client.service_account import ServiceAccountCredentials
from gspread_dataframe import get_as_dataframe, set_with_dataframe
from Phoenix_Get_Data import yesterday_date, daily_country_data,monthly_country_data,yearly_country_data,daily_metrics_data,monthly_metrics_data, first_day_of_month
import datetime
from datetime import datetime, timedelta, date
import time

import sys
""" Error Logging below """
import logging
logging.basicConfig(filename='error.log',level=logging.WARNING, format='%(asctime)s %(message)s')
""" End Error Logging below """

def this_month(): ##this is used to check if a tab exists
   month = datetime.strftime(datetime.now() - timedelta(days=1, hours=4), '%B-%Y')
   return month

def yesterday_date_again(): ##to check cell value I need to get yesterdays date in a different format
   yesterday_2 = datetime.strftime(datetime.now() - timedelta(days=1, hours=4), '%Y-%m-%d').lstrip("0").replace(" 0", " ")
   #print (yesterday_2)
   return yesterday_2

def today_date():
    today = date.today()
    d1 = today.strftime("%d")
    #d1 = '01'
    print(d1)
    return d1

def last_month_name():
    today = date.today()
    first = today.replace(day=1)
    lastMonth = first - timedelta(days=1)
    return(lastMonth.strftime('%B-%Y'))

SCOPES = ['https://www.googleapis.com/auth/spreadsheets', "https://www.googleapis.com/auth/drive.file", "https://www.googleapis.com/auth/drive"]
creds = ServiceAccountCredentials.from_json_keyfile_name('sheets_secret.json', SCOPES)
gc = gspread.authorize(creds)
sheets_file_name = "Phoenix Beta Data - Python - BQ"
sh = gc.open(sheets_file_name)
today_day_of_month = date.today()
second_day_of_month = datetime.today().date().replace(day=2)
first_day_of_cur_mo = datetime.strftime(datetime.today().replace(day=1),'%Y-%m-%d')

def next_available_row(worksheet):
   str_list = list(filter(None, worksheet.col_values(1)))
   return str(len(str_list)+1)

def get_value_of_second_to_last_cell(which_sheet):

    worksheet = sh.worksheet(which_sheet)
    next_row = next_available_row(worksheet)
    which_row =  ((int(next_row))) - 1
    cell_value = worksheet.cell(which_row,1).value
    return cell_value

def filename_check():
    try:
        gc.open(sheets_file_name)
        return True
    except gspread.exceptions.SpreadsheetNotFound as e:
        error_msg = "Oops!", e.__class__, "occurred: " + sheets_file_name
        logging.warning(error_msg)
        sys.exit(1)

def create_sheet(sh_name):

    worksheet = sh.add_worksheet(title=sh_name, rows="100", cols="40")
    cell_list = worksheet.range('A1:l1')
    cell_values = ['Date', 'OS', 'Country', 'Session', 'Screen Views', 'Users', 'blank', 'blank', 'Date',
                   'Day of the month', 'Month of the year', 'Year']
    for i, val in enumerate(cell_values):
        cell_list[i].value = val
    worksheet.update_cells(cell_list)

def send_daily_data_to_sheet(which_data):
    worksheet = sh.worksheet(which_data)
    next_row = next_available_row(worksheet)
    second_to_last_row = int(next_row) - 1
    overwrite_row = int(next_row) - 4
    last_row_val = worksheet.cell(second_to_last_row, 1).value
    check_for_yesterday_date = yesterday_date_again() in last_row_val
    print ('------=========++++++++')
    print (today_day_of_month)
    print (last_row_val)
    print(yesterday_date_again())
    print ('------=========++++++++')

    if check_for_yesterday_date == True:
        error_msg = "Oops! This date already exists in the google sheet, so Im not going to over write it"
        logging.warning(error_msg)

    elif today_day_of_month == last_row_val:
        error_msg = "Oops! This date already exists in the google sheet, so Im not going to over write it"
        logging.warning(error_msg)

    else:
        set_with_dataframe(worksheet, daily_metrics_data(), row=int(next_row), col=1, include_index=False,
                           include_column_header=False, resize=False, allow_formulas=True)

def check_if_sheet_exists(input_sheet_name):
    try:

        sh.worksheet(input_sheet_name)
        return True
    except gspread.exceptions.WorksheetNotFound as e:
        error_msg = "Oops!", e.__class__, input_sheet_name + ' in:' + sheets_file_name
        logging.warning(error_msg)
        return False
def copy_paste_month(donor_mo, recepient_mo):

    donor_sheet = sh.worksheet(donor_mo)
    donor_values = donor_sheet.get_all_values()
    sh.values_clear(recepient_mo+"!A1:M10000")
    sh.values_update(
        recepient_mo+'!A1',
        params={'valueInputOption': 'USER_ENTERED'},
        body={'values': donor_values}
    )
def copy_or_create_month(month, donor_month):
    if check_if_sheet_exists(month) is True:
        copy_paste_month(donor_month,month)
    else:

        sh.add_worksheet(title=month, rows="200", cols="100")
        copy_paste_month(donor_month,month)
def worksheet_check(sheet_name_to_check):
    global this_month_sheet_id
    if filename_check() is True:
        sh = gc.open(sheets_file_name)
        worksheet_list = sh.worksheets()
        sheet_titles_list = []
        for spreadsheet in worksheet_list:
            sheet_titles_list.append(spreadsheet.title)
        if sheet_name_to_check in sheet_titles_list:
            this_month_sheet_id = spreadsheet.id
            send_daily_data_to_sheet(this_month())
            return True
        else:
            create_sheet(sheet_name_to_check)
            #after new sheet is created get the ID of this new sheet for copying this sheet later
            worksheet_list = sh.worksheets()
            sheet_titles_list = []
            for spreadsheet in worksheet_list:
                sheet_titles_list.append(spreadsheet.title)
            if sheet_name_to_check in sheet_titles_list:
                this_month_sheet_id = spreadsheet.id

            send_daily_data_to_sheet(this_month())
            return False

def copy_last_month_or_no():
    if today_date() == '02': #skip the first day as the data is one day behind, when this script runs the previous day is the 1st.

        copy_or_create_month('Last Month', last_month_name())
        error_msg = "Btw. I just copied last month to a new tab as its the first"
        logging.warning(error_msg)
    else:
        pass

def send_monthly_data():
   worksheet = sh.worksheet("Monthly")
   next_row = next_available_row(worksheet)
   second_to_last_row = int(next_row) - 1
   overwrite_row = int(next_row) - 4
   last_row_val = worksheet.cell(second_to_last_row, 1).value

   if today_day_of_month == first_day_of_cur_mo: #if first of month
       set_with_dataframe(worksheet, monthly_metrics_data(), row=int(next_row), col=1, include_index=False,
                          include_column_header=False, resize=False, allow_formulas=True)

   elif first_day_of_cur_mo == last_row_val: #if not first of the month but already have this months data
        #i have this months data already dont add this month again, just overwrite the last 4 rows
       set_with_dataframe(worksheet, monthly_metrics_data(), row=int(overwrite_row), col=1, include_index=False,
                          include_column_header=False, resize=False, allow_formulas=True)

   else: pass
   #if today_day_of_month == first_day_of_cur_mo:
   #    set_with_dataframe(worksheet, monthly_metrics_data(), row=int(overwrite_row), col=1, include_index=False,
   #                       include_column_header=False, resize=False, allow_formulas=True)

   #elif first_day_of_cur_mo == last_row_val:
   #     #i have this months data already dont add this month again, just overwrite the last 4 rows
   #    set_with_dataframe(worksheet, monthly_metrics_data(), row=int(overwrite_row), col=1, include_index=False,
   #                       include_column_header=False, resize=False, allow_formulas=True)

   #else: #if today is not the 1st
   #    set_with_dataframe(worksheet, monthly_metrics_data(), row=int(next_row), col=1, include_index=False,
   #                       include_column_header=False, resize=False, allow_formulas=True)

def create_monthly_data_worksheets():
    if check_if_sheet_exists('Monthly') is True:
        send_monthly_data()
    else:
        create_sheet('Monthly')
        send_monthly_data()

def send_country_date_to_all(range_to_clear, dataframe_name, column_to_start, header_or_not):

    worksheet = sh.worksheet("Country")
    range_of_cells = worksheet.range(range_to_clear)  # clear the range to overwrite
    for cell in range_of_cells:
        cell.value = ''
    worksheet.update_cells(range_of_cells)

    set_with_dataframe(worksheet, dataframe_name, row=1, col=column_to_start, include_index=False, include_column_header=header_or_not,
                       resize=False, allow_formulas=True)

def create_and_send_country_sheet():
    if check_if_sheet_exists('Country') is True:
       # print ('country sheet exists, sending data now')
        send_country_date_to_all('a2:F500',daily_country_data(),1,True)
        send_country_date_to_all('H1:M500', monthly_country_data(), 8, True)
        send_country_date_to_all('O1:T500', yearly_country_data(), 15, True)
    else:

        worksheet = sh.add_worksheet(title='Country', rows="500", cols="100")
        cell_list = worksheet.range('A1:l1')
        cell_values = ['Date', 'Country', 'OS', 'Session', 'Screen Views', 'Users']
        for i, val in enumerate(cell_values):
            cell_list[i].value = val
        worksheet.update_cells(cell_list)
        #print ('country sheet NOT exists, created one and sending data now')

        send_country_date_to_all('a2:F500',daily_country_data(),1,True)
        send_country_date_to_all('H1:M500', monthly_country_data(), 8, True)
        send_country_date_to_all('O1:T500', yearly_country_data(), 15, True)


if __name__ == "__main__":
    worksheet_check(this_month())
    time.sleep(5)
    copy_or_create_month('Current Month', this_month())
    time.sleep(5)
    copy_last_month_or_no()
    time.sleep(5)
    create_monthly_data_worksheets()
    time.sleep(5)
    create_and_send_country_sheet()
