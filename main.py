import os
import json
import re
import pandas as pd
from datetime import datetime, date, timedelta
import csv

CURRENT_DATE = date(2022, 10, 4)

def truncate_timestamp(timestamp_ms):
    dt = datetime.fromtimestamp(timestamp_ms/1000)
    return date(dt.year, dt.month, dt.day)

def filter_by_time(df, current_date = date.today(), begin = None, end = -1):
    begin_date = current_date + timedelta(begin)
    end_date = current_date + timedelta(end)
    return df[(df['date']>=begin_date) & (df['date']<=end_date)]
    
def message_counts(df):
    counts = df.groupby(['name'])['count'].count()
    return pd.DataFrame({'name':counts.index, 'count':counts.values})

def deidentify(df):
    with open('nicknames.csv', mode='r') as infile:
        reader = csv.reader(infile)
        names_dict = {rows[0]:rows[1] for rows in reader}
        
    df['nickname'] = df['name'].map(names_dict)
    return df

def load_json():
    message_list = []
    for filename in os.listdir('json'):
        with open(os.path.join('json', filename)) as file:
            data = json.load(file)
    
        for message in data['messages']:
            if 'content' in message and re.search('^.*reacted.*to your message $', message['content']):
                pass
            else:
                if message['is_unsent']:
                    message_type = 'unsent'
                    count = 1
                if 'content' in message:
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
    
def combine_message_counts(df):
    yesterday = message_counts(filter_by_time(df, CURRENT_DATE, -1, -1))
    return deidentify(counts)

if __name__ == '__main__':
    sent_messages = load_json()
    d = combine_message_counts(sent_messages)
    print(d)