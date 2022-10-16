import json
import re
import pandas as pd
from datetime import datetime

def truncate_timestamp(timestamp_ms):
    dt = datetime.fromtimestamp(timestamp_ms/1000)
    return datetime(dt.year, dt.month, dt.day)

def load_json():
    with open('json/message_1.json') as file:
        data = json.load(file)
        
    df = pd.DataFrame()
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
            new_row = {'timestamp_ms':message['timestamp_ms'], 'name':message['sender_name'], 'message_type':message_type, 'count':count}
            df = pd.concat([df, pd.DataFrame([new_row])])
    df['date'] = df['timestamp_ms'].map(truncate_timestamp)
    print(df)
    print(df.groupby(['message_type']).count())


if __name__ == '__main__':
    load_json()