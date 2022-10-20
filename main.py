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

def filter_stats(df, current_date = date.today(), begin = None, end = -1):
    begin_date = current_date + timedelta(begin)
    end_date = current_date + timedelta(end)
    by_date = df[(df['date']>=begin_date) & (df['date']<=end_date)]
    return by_date.groupby(['name'])['count'].count()

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
    print(df)
    sent = df[df['message_type'] != 'unsent']
    print(sent)
    print(df.groupby(['message_type']).count())
    
    f = filter_stats(df, CURRENT_DATE, -1, -1)
    
    with open('nicknames.csv', mode='r') as infile:
        reader = csv.reader(infile)
        names_dict = {rows[0]:rows[1] for rows in reader}
        
    #f['nickname'] = f['name'].map(names_dict)
    f['nickname'] = 'blah'
    print(f)
    print(type(f))
    print(type(sent))
    #print(f['name'])
    z = pd.DataFrame({'name':f.index, 'count':f.values})
    print(z)
    z['nickname'] = z['name'].map(names_dict)
    print(z)
    

if __name__ == '__main__':
    load_json()