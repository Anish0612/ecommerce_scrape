import pandas as pd
import os,base64
from tqdm import tqdm
import mysql.connector
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication



def send_notificiation(website,data_changed,data_not_present,column):
    if 'coles' == website:
        column_2 = column.copy()
        column_2.append('id')
        column_2.append('catlog')        
    elif 'aldi' == website:
        column_2 = column.copy()
        column_2.append('image_url')
    elif 'woolworths' == website:
        column_2 = column.copy()
        column_2.append('stockcode')
        column_2.append('source')
    elif 'iga' == website:
        column_2 = column.copy()
        column_2.append('product_id')
        column_2.append('catlog_name')

    SCOPES = ['https://www.googleapis.com/auth/gmail.send']
    creds = None
    if os.path.exists('../token.json'):
        creds = Credentials.from_authorized_user_file('../token.json', SCOPES)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                '../credentials.json', SCOPES)
            creds = flow.run_local_server(port=0)
        with open('../token.json', 'w') as token:
            token.write(creds.to_json())
    service = build('gmail', 'v1', credentials=creds)
    try:
        message = MIMEMultipart()
        # message['To'] = 'anishnadar77@gmail.com,johndoeidk001@gmail.com'
        message['To'] = 'johndoeidk001@gmail.com,johndoeidk002@gmail.com,johndoeidk003@gmail.com,johndoeidk004@gmail.com,johndoeidk005@gmail.com,johndoeidk006@gmail.com,johndoeidk007@gmail.com,johndoeidk008@gmail.com,johndoeidk009@gmail.com,johndoeidk010@gmail.com,johndoeidk011@gmail.com,johndoeidk012@gmail.com,johndoeidk013@gmail.com,johndoeidk014@gmail.com,johndoeidk015@gmail.com,johndoeidk016@gmail.com,johndoeidk017@gmail.com,johndoeidk018@gmail.com,johndoeidk019@gmail.com,johndoeidk020@gmail.com'
        # message['To'] = 'anishnadar77@gmail.com'
        message['From'] = 'anishnadar703@gmail.com'
        message['Subject'] = f'{website.capitalize()} Notification'
        if len(data_not_present) == 0 and len(data_changed) == 0:
            body = 'No Data is updated'
        else:
            body = 'Here is status of recently scraped data'
            # message.set_content(body)
            if data_not_present:
                body = body + f'\nNo of data newly added {len(data_not_present)}'
                df = pd.DataFrame(data_not_present, columns=column)
                csv_data = df.to_csv(index=False)
                if len(csv_data) > 24 * 1024 * 1024:
                    body = body + f'\n data_not_present.csv file size exceeds 24 MB. Cannot send the file.'
                else:
                    attachment = MIMEApplication(csv_data)
                    attachment.add_header('Content-Disposition', 'attachment', filename='data_not_present.csv')
                    message.attach(attachment)
            if data_changed:
                body = body + f'\nNo of data updated {len(data_changed)}'
                df1 = pd.DataFrame(data_changed, columns=column_2)
                csv_data_1 = df1.to_csv(index=False)
                if len(csv_data_1) > 24 * 1024 * 1024:
                    body = body + f'\n data_changed.csv file size exceeds 24 MB. Cannot send the file.'
                else:
                    attachment = MIMEApplication(csv_data_1)
                    attachment.add_header('Content-Disposition', 'attachment', filename='data_changed.csv')
                    message.attach(attachment)

        message.attach(MIMEText(body, 'plain'))
        composed_email = message.as_string()
        encoded_message = base64.urlsafe_b64encode(composed_email.encode()).decode()

        create_message = {
            'raw': encoded_message
        }
        send_message = (service.users().messages().send
                        (userId="me", body=create_message).execute())
        print(F'Message Id: {send_message["id"]}')
    except HttpError as error:
        print(F'An error occurred: {error}')
        send_message = None


def sql_connect():
    mydb = mysql.connector.connect(
    host='108.167.157.249',
    user='upx0o9yq_python_script',
    password='pYZHzQlXJv17',
    database='upx0o9yq_all_database'
    )
    mycursor = mydb.cursor()
    return mydb , mycursor

def sql_close(mydb,mycursor):
    mycursor.close()
    mydb.close()

def fetch_sql_data(website,column):
    mydb , mycursor = sql_connect()
    columns_string = ', '.join(column)
    mycursor.execute(f"SELECT {columns_string} FROM {website};")
    myresult = mycursor.fetchall()
    sql_data = {}
    if 'coles' == website:
        for result in myresult:
            product_id = result[2]
            catlog = result[3]
            sql_data[f'{product_id}_{catlog}']=result                        # product_id and catlog
    elif 'aldi' == website:
        for result in myresult:
            sql_data[result[-2]]=result                                     # url
    elif 'woolworths' == website:
        for result in myresult:
            stockcode = result[2]
            source = result[9]
            sql_data[f'{stockcode}_{source}'] = result                      # stockcode(product_id) and source(catlog)
    elif 'iga' == website:
        for result in myresult:
            product_id = result[1]
            catlog_name = result[2]
            sql_data[f'{product_id}_{catlog_name}']=result                        # product_id and catlog_name
    sql_close(mydb,mycursor)
    return sql_data


def update_mysql(website,data_changed,data_not_present,all_data,column):
    mydb , mycursor = sql_connect()
    sql_column = tuple(column)
    if 'coles' == website:
        sql_where = 'product_id = %s AND catlog = %s'
    elif 'aldi' == website:
        sql_where = 'url = %s'
    elif 'woolworths' == website:
        sql_where = 'stockcode = %s AND source = %s'
    elif 'iga' == website:
        sql_where = 'product_id = %s AND catlog_name = %s'
        
    if data_not_present:                      # add database
        print(f"len of data not present {len(data_not_present)}")
        columns = ', '.join(sql_column)
        placeholders = ', '.join(['%s'] * len(sql_column))
        sql = f"INSERT INTO {website} ({columns}) VALUES ({placeholders});"
        mycursor.executemany(sql, data_not_present)
        mydb.commit()

    if data_changed:
        print(f"len of data changed {len(data_changed)}")
        columns = ', '.join([f"{col} = %s" for col in sql_column])
        sql = f"UPDATE {website} SET {columns} WHERE {sql_where};"
        mycursor.executemany(sql, data_changed)
        mydb.commit()

    if all_data:
        sql_record_column  = sql_column+('datetime',)
        placeholders = ', '.join(['%s'] * len(sql_record_column))
        sql_record_column = ', '.join(sql_record_column)
        new_website = website+'_records'
        sql = f"INSERT INTO {new_website} ({sql_record_column}) VALUES ({placeholders});"
        mycursor.executemany(sql, all_data)
        mydb.commit()
    sql_close(mydb,mycursor)
    send_notificiation(website,data_changed,data_not_present,column)
   