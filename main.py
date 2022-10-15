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
            new_row = {'timestamp':message['timestamp_ms'], 'name':message['sender_name']}
            df = pd.concat([df, pd.DataFrame([new_row])])
    print(df)
    
    

if __name__ == '__main__':
    load_json()