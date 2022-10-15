import json
import re
import pandas as pd

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
            if 'content' in message:
                message_type = 'content'
            elif 'gifs' in message:
                message_type = 'gifs'
            elif 'photos' in message:
                message_type = 'photos'
            elif 'audio_files' in message:
                message_type = 'audio_files'
            elif 'videos' in message:
                message_type = 'videos'
            elif 'sticker' in message:
                message_type = 'sticker'
            new_row = {'timestamp':message['timestamp_ms'], 'name':message['sender_name'], 'message_type':message_type}
            df = pd.concat([df, pd.DataFrame([new_row])])
    print(df)
    print(df.groupby(['message_type']).count())


if __name__ == '__main__':
    load_json()