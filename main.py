import json

def load_json():
    with open('json/message_1.json') as file:
        data = json.load(file)

if __name__ == '__main__':
    load_json()