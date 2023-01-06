import sqlite3
from contextlib import closing
import os
import time
import pandas as pd
from date_handling import truncate_timestamp
import json
import re

def check_data_file(cursor):
    
    db_timestamps = cursor.execute("    \
        SELECT                          \
            timestamp_ms                \
        FROM                            \
            messages                    \
            ").fetchall()
    df = pd.DataFrame(db_timestamps, columns=['timestamp_ms'])
    df['date'] = df['timestamp_ms'].map(truncate_timestamp)
    db_dates = df['date'].unique()
    print(db_dates)
            
    file_dates = set()
    for filename in os.listdir('json'):
        if filename != 'loaded':
            with open(os.path.join('json', filename)) as file:
                data = json.load(file)
                        
                for message in data['messages']:
                    file_dates.add(truncate_timestamp(message['timestamp_ms']))
    print(file_dates)
    print(not file_dates.isdisjoint(db_dates))
    if not file_dates.isdisjoint(db_dates):
        print('Dates already loaded into database:')
        for item in file_dates.intersection(db_dates):
            print(item.strftime('%Y-%m-%d'))
        return False
    return True        

def insert_data():
    
    with closing(sqlite3.connect('rc_chat_log.db')) as connection:
        with closing(connection.cursor()) as cursor:
            
            os.environ['TZ'] = 'America/Chicago'
            time.tzset()

            if check_data_file(cursor):
    
                for filename in os.listdir('json'):
                    if filename != 'loaded':
                        with open(os.path.join('json', filename)) as file:
                            data = json.load(file)

                            for message in data['messages']:
                                if 'content' in message and re.search('^.*reacted.*to your message $', message['content']):
                                    pass
                                elif 'content' in message and re.search('^.*set the nickname for.*to.*$', message['content']):
                                    pass
                                elif 'content' in message and re.search('^.*set your nickname to.*$', message['content']):
                                    pass
                                else:
                                    content = None
                                    if 'is_unsent' in message and message['is_unsent']:
                                        message_type = 'unsent'
                                        count = 1
                                    elif 'content' in message:
                                        message_type = 'content'
                                        content = message['content'].encode('latin1').decode('utf8')
                                        count = len(content.split())
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

                                    name = message['sender_name']
                                    name_id_result = cursor.execute("   \
                                        SELECT                          \
                                            name_id                     \
                                        FROM                            \
                                            names                       \
                                        WHERE                           \
                                            name = ?                    \
                                        ", (name,)).fetchone()
                                    if not name_id_result:
                                        cursor.execute("                \
                                            INSERT INTO names (name)    \
                                            VALUES                      \
                                                (?)                     \
                                            ", (name,))
                                        name_id_result = cursor.execute("   \
                                            SELECT                          \
                                                name_id                     \
                                            FROM                            \
                                                names                       \
                                            WHERE                           \
                                                name = ?                    \
                                            ", (name,)).fetchone()

                                    message_type_id_result = cursor.execute("   \
                                        SELECT                                  \
                                            message_type_id                     \
                                        FROM                                    \
                                            message_types                       \
                                        WHERE                                   \
                                            message_type = ?                    \
                                        ", (message_type,)).fetchone()
                                    if not message_type_id_result:
                                        cursor.execute("                                \
                                            INSERT INTO message_types (message_type)    \
                                            VALUES                                      \
                                                (?)", (message_type,))
                                        message_type_id_result = cursor.execute("   \
                                            SELECT                                  \
                                                message_type_id                     \
                                            FROM                                    \
                                                message_types                       \
                                            WHERE                                   \
                                                message_type = ?                    \
                                            ", (message_type,)).fetchone()

                                    cursor.execute("                                \
                                        INSERT INTO messages (                      \
                                            name_id, message_type_id, timestamp_ms, \
                                            item_count, content                     \
                                        )                                           \
                                        VALUES                                      \
                                            (?, ?, ?, ?, ?)                         \
                                        ", (name_id_result[0], message_type_id_result[0], message['timestamp_ms'], count, content))

        connection.commit()
        
if __name__ == '__main__':
    insert_data()