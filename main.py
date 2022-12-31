import sqlite3
from contextlib import closing
import os
import json
import re
import pandas as pd
from datetime import datetime, date, timedelta
import csv
import configparser
import pygsheets

def truncate_timestamp(timestamp_ms):
    dt = datetime.fromtimestamp(timestamp_ms/1000)
    return date(dt.year, dt.month, dt.day)

def get_messages():
    
    with closing(sqlite3.connect('rc_chat_log.db')) as connection:
        with closing(connection.cursor()) as cursor:

            messages = cursor.execute("                                                             \
                SELECT                                                                              \
                    name,                                                                           \
                    message_type,                                                                   \
                    timestamp_ms                                                                    \
                FROM                                                                                \
                    messages                                                                        \
                    JOIN message_types ON messages.message_type_id = message_types.message_type_id  \
                    JOIN names ON messages.name_id = names.name_id                                  \
                WHERE message_type != 'unsent'                                                      \
                ").fetchall()
            
    df = pd.DataFrame(messages, columns=['name', 'message_type', 'timestamp_ms'])
    df['date'] = df['timestamp_ms'].map(truncate_timestamp)
    df.drop(columns=['timestamp_ms'], inplace=True)
    return df

def filter_by_time(df, latest_date, begin, end):
    begin_date = latest_date + timedelta(begin)
    end_date = latest_date + timedelta(end)
    return df[(df['date']>=begin_date) & (df['date']<=end_date)]
    
def message_counts(df):
    print(df)
    return df.groupby(['name', 'message_type'], as_index=False)['count'].count()

def total_messages(df):
    return df.groupby(['date'], as_index=False)['count'].count()

def load_json():
    message_list = []
    for filename in os.listdir('json'):
        with open(os.path.join('json', filename)) as file:
            data = json.load(file)
    
        for message in data['messages']:
            if 'content' in message and re.search('^.*reacted.*to your message $', message['content']):
                pass
            else:
                if 'is_unsent' in message and message['is_unsent']:
                    message_type = 'unsent'
                    count = 1
                elif 'content' in message:
                    message_type = 'content'
                    count = len(message['content'].split())
                elif 'gifs' in message:
                    message_type = 'gifs'
                    count = len(message['gifs'])
                elif 'photos' in message:
                    message_type = 'photos'
                    count = len(message['photos'])
                elif 'audio_files' in message:
                    message_type = 'audio_files'
                    count = len(message['audio_files'])
                elif 'videos' in message:
                    message_type = 'videos'
                    count = len(message['videos'])
                elif 'sticker' in message:
                    message_type = 'sticker'
                    count = 1
                new_row = [message['timestamp_ms'], message['sender_name'], message_type, count]
                message_list.append(new_row)
            
    df = pd.DataFrame(message_list, columns=['timestamp_ms', 'name', 'message_type', 'count'])
    df['date'] = df['timestamp_ms'].map(truncate_timestamp)
    return df[df['message_type'] != 'unsent']

def check_nicknames(df):
    with open(os.path.join('csv', 'updated_nicknames.csv'), mode='r') as infile:
        reader = csv.reader(infile)
        names_dict = {rows[0]:rows[1] for rows in reader}
    no_nickname = set(df['name']) - set(names_dict.keys())
    if no_nickname:
        nickname_file_is_complete = False
        print(no_nickname)
    else:
        nickname_file_is_complete = True
    return nickname_file_is_complete, names_dict    
    
def combine_message_counts(df):
    BEGIN_DATE = df['date'].min()
    END_DATE = df['date'].max()
    yesterday = message_counts(filter_by_time(df, END_DATE, 0, 0))
    yesterday['period'] = 'a. ' + END_DATE.strftime('%b %d')
    day_before = message_counts(filter_by_time(df, END_DATE, -1, -1))
    day_before['period'] = 'b. ' + (END_DATE + timedelta(-1)).strftime('%b %d')
    week = message_counts(filter_by_time(df, END_DATE, -6, 0))
    week['period'] = 'c. ' + (END_DATE + timedelta(-6)).strftime('%b %d') + ' to ' + END_DATE.strftime('%b %d')
    beginning = message_counts(df)
    beginning['period'] = 'd. ' + BEGIN_DATE.strftime('%b %d') + ' to date'
    return pd.concat([yesterday, day_before, week, beginning], axis=0)

def deidentify(df, names_dict):        
    df['nickname'] = df['name'].map(names_dict)
    print(df[df['nickname'].isnull()])
    df.drop(['name'], axis=1, inplace=True)
    return df
    
def set_workbook():
    
    config = configparser.ConfigParser()
    config.read('config.ini')
    
    # google sheets authentication
    filename = os.listdir('auth')[0]
    creds = os.path.join('auth', filename)
    api = pygsheets.authorize(service_file=creds)
    return api.open(config['DEFAULT']['sheets_filename'])

def update_sheet(wb, sheet_name, df):

    # open the sheet by name
    sheet = wb.worksheet_by_title(sheet_name)
    sheet.clear()
    sheet.set_dataframe(df, (1,1))

if __name__ == '__main__':
#    messages = get_messages()
    messages = load_json()
    nickname_file_is_complete, names_dict = check_nicknames(messages)
    if nickname_file_is_complete:
        counts = combine_message_counts(messages)
        deid = deidentify(counts, names_dict)
        totals = total_messages(messages)
        print(deid)
        print(totals)
        
#        workbook = set_workbook()
#        update_sheet(workbook, 'Member messages', deid)
#        update_sheet(workbook, 'Total messages', totals)