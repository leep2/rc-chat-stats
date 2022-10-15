import json
import re

def load_json():
    with open('json/message_1.json') as file:
        data = json.load(file)
        
    count = 0
    x = 0
    for message in data['messages']:
        if 'content' in message:
            x += 1
            cont = message['content']
            if not re.search('^.*reacted.*to your message $', cont):
                count += 1
    print(count)
    print(x)

if __name__ == '__main__':
    load_json()