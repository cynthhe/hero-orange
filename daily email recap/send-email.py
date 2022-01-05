import gspread
import win32com.client
from oauth2client.service_account import ServiceAccountCredentials
import datetime
from datetime import datetime, timedelta
from inspect import currentframe
import logging
from Send_Slack import send_slack_message
import time
from Win_Run_Times import get_task_schedule_info, run_time_info
from inspect import currentframe, getframeinfo

logging.basicConfig(filename='error.log',level=logging.WARNING, format='%(asctime)s %(message)s')
SCOPES = ['https://www.googleapis.com/auth/spreadsheets', "https://www.googleapis.com/auth/drive.file", "https://www.googleapis.com/auth/drive"]
creds = ServiceAccountCredentials.from_json_keyfile_name('sheets_secret.json', SCOPES)
gc = gspread.authorize(creds)

sheets_file_name = "Daily Metrics Recap"

def filename_check(sheets_file_name):
    try:
        gc.open(sheets_file_name)
        return True
    except gspread.exceptions.SpreadsheetNotFound as e:
        error_msg = "Oops!", e.__class__, "this file does not exist: " + sheets_file_name
        logging.warning(error_msg)
        return False
def check_if_sheet_exists(input_sheet_name):
    try:
        sh=gc.open(sheets_file_name)
        sh.worksheet(input_sheet_name)
        return True
    except gspread.exceptions.WorksheetNotFound as e:
        error_msg = "Oops!", e.__class__, input_sheet_name + ' in:' + sheets_file_name
        logging.warning(error_msg)
        return False
def get_email_distro():
    if check_if_sheet_exists('distro') == True:
        #print('Take Away Sheet exists')
        sh = gc.open(sheets_file_name)
        sh_name = 'distro'
        worksheet = sh.worksheet(sh_name)
        range_of_cells = worksheet.range('A1:A20')
        #print (range_of_cells)
        text_storage = []

        for cell in range_of_cells:
            cellValue = cell.value
            #print (cellValue)
            text_storage.append(cellValue)
        #print (text_storage)
        text_storage = list(filter(None, text_storage))
        #print (text_storage)
    return text_storage

class Error_func:
    def __init__(self, frameinfo, err_msg):
        #frameinfo = getframeinfo(currentframe())
        self.filename = ' File name is: ' + (frameinfo.filename)
        self.lineno = ' Line number is: ' + str(frameinfo.lineno)
        error_msg = err_msg + ' in this file: '+ self.filename + 'on line: ' + self.lineno

        logging.warning(error_msg)

def daily_data_html():
    if check_if_sheet_exists('Take Away') == True:
        #print('Take Away Sheet exists')
        sh = gc.open(sheets_file_name)
        sh_name = 'Take Away'
        worksheet = sh.worksheet(sh_name)
        range_of_cells = worksheet.range('H3:H35')
        #print (range_of_cells)
        text_storage = []

        for cell in range_of_cells:
            time.sleep(1)
            cellValue = cell.value
            #print (cellValue)
            text_storage.append(cellValue)
            #print (text_storage)
            my_string = ','.join(text_storage)
            contents = my_string.replace(',', '\n')
        #print (contents)
        return (contents)


    else:
        ErrorMsgForTeam = "Sheet does not exist"
        logging.warning(ErrorMsgForTeam + ' - this error fired from line: ' + str(get_linenumber()))

def daily_data_error_html():
    if check_if_sheet_exists('Take Away') == True:
        #print('Take Away Sheet exists')
        sh = gc.open(sheets_file_name)
        sh_name = 'Take Away'
        worksheet = sh.worksheet(sh_name)
        range_of_cells = worksheet.range('I3:I35')
        #print (range_of_cells)
        text_storage = []

        for cell in range_of_cells:
            cellValue = cell.value
            #print (cellValue)
            time.sleep(1)
            text_storage.append(cellValue)
            #print (text_storage)
            my_string = ','.join(text_storage)
            contents = my_string.replace(',', '\n')
        print (contents)
        content = contents + run_time_info('email', 'Analytics')
        #todo append the win run time info to content
        return (content)

def get_linenumber():
    cf = currentframe()
    return cf.f_back.f_lineno

def yesterday_date_again(): #to check cell value I need to get yesterdays date in a different format
    yesterday_2 = datetime.strftime(datetime.now() - timedelta(days=1, hours=4), '%#m/%d/%Y')
    return yesterday_2

good_email_distro = get_email_distro() #recipient_list = ['asif.rahman@accuweather.com']
good_email_copy_distro = ['analytics@accuweather.com'] #copies_list = ['asifrahman13@gmail.com']
good_email_subject = 'Daily Digital Usage Summary'
good_email_body = daily_data_html()

error_email_distro = ['asif.rahman@accuweather.com'] #recipient_list = ['asif.rahman@accuweather.com']
error_email_copy_distro = ['analytics@accuweather.com'] #copies_list = ['asifrahman13@gmail.com']
error_email_subject = 'Error with Daily Digital Usage Summary Report'
error_email_body =daily_data_error_html()
error_slack_body = 'Slack body'

def send_outlook_html_mail(recipients, recipient_list, subject='No Subject', body='Blank', send_or_display='Display', copies=None):

    if len(recipients) > 0 and isinstance(recipient_list, list):
        outlook = win32com.client.Dispatch("Outlook.Application")

        ol_msg = outlook.CreateItem(0)

        str_to = ""
        for recipient in recipients:
            str_to += recipient + ";"

        ol_msg.To = str_to

        if copies is not None:
            str_cc = ""
            for cc in copies:
                str_cc += cc + ";"

            ol_msg.CC = str_cc

        ol_msg.Subject = subject
        ol_msg.HTMLBody = body

        if send_or_display.upper() == 'SEND':
            ol_msg.Send()
        else:
            ol_msg.Display()
    else:
        ErrorMsgForTeam = "Recipient email address - NOT FOUND"
        Error_func(getframeinfo(currentframe()), ErrorMsgForTeam)


def send_email(email_type):

    if email_type == 'Good':
            #print('Send Good Email')
            send_outlook_html_mail(recipients=good_email_distro, recipient_list=good_email_distro, subject=good_email_subject, body=good_email_body,
                                   send_or_display='Send',
                                   copies=good_email_copy_distro)
    elif email_type == 'Error':
            #print('Send Error Email')
            send_outlook_html_mail(recipients=error_email_distro, recipient_list=error_email_distro, subject=error_email_subject, body=error_email_body,
                                   send_or_display='Send',
                                   copies=error_email_copy_distro)
    else:
            ErrorMsgForTeam = "I could not send the daily metrics email for some reason"
            Error_func(getframeinfo(currentframe()), ErrorMsgForTeam)

def reset_manual_override():
    sh = gc.open(sheets_file_name)
    sh_name = 'Python'
    overwrite_cell_to_check = 'B9'
    worksheet = sh.worksheet(sh_name)
    OverwritecellValue = worksheet.acell(overwrite_cell_to_check).value
    if OverwritecellValue == "Y":
        time.sleep(1)
        worksheet.update(overwrite_cell_to_check, 'N')
        print('Overwrite cell has value')
    else:
        pass
        print ('Overwrite cell has been cleared')

def initial_check():
    sh = gc.open(sheets_file_name)
    cell_to_check = 'B8'
    overwrite_cell_to_check = 'B9'
    err_msg_cell_to_check = 'E28'
    if filename_check(sheets_file_name) == True:
        #print('file name exists')
        time.sleep(1)
        if check_if_sheet_exists('Python') == True:
            time.sleep(1)
            #print ('Sheet exists')
            sh_name = 'Python'
            worksheet = sh.worksheet(sh_name)
            cellValue = worksheet.acell(cell_to_check).value
            OverwritecellValue = worksheet.acell(overwrite_cell_to_check).value
            error_cell_value = worksheet.acell(err_msg_cell_to_check).value
            if cellValue == "YYYY":
                print ('send good email')
                time.sleep(1)
                send_email('Good')
            elif OverwritecellValue == "Y":
                print('overwrite -- send good email')
                time.sleep(1)
                send_email('Good')
                reset_manual_override()
            else:
                send_email('Error')
                time.sleep(1)
                send_slack_message(run_time_info('slack', 'Analytics'))
                ErrorMsgForTeam = "Error"
                Error_func(getframeinfo(currentframe()), ErrorMsgForTeam)


        else:
            ErrorMsgForTeam = "Sheet does not exist"
            Error_func(getframeinfo(currentframe()), ErrorMsgForTeam)


    else:
        ErrorMsgForTeam = "File does not exist"
        Error_func(getframeinfo(currentframe()), ErrorMsgForTeam)

if __name__ == '__main__':
    initial_check()

#this is the ONLY file to check
