import sqlite3
from contextlib import closing
import os
import time
from zipfile import ZipFile
import pandas as pd
from date_handling import truncate_timestamp
import json
import re

def handle_zip_file():
    
    for filename in os.listdir('zip'):
        with ZipFile(os.path.join('zip', filename)) as zip:
            lst = zip.namelist()

            for item in lst:
                if re.search('^.*/ccsfrelationships_.*/message_1.json$', item):
                    break
            zip.extract(item)

        with open(item) as file:                    
            data = json.load(file)
                
        file_dates = set()
        for message in data['messages']:
            file_dates.add(truncate_timestamp(message['timestamp_ms']))
            
        begin = min(file_dates)
        end = max(file_dates)
        if begin == end:
            file_suffix = end.strftime('%Y%m%d')
        else:
            file_suffix = begin.strftime('%Y%m%d') + '_' + end.strftime('%Y%m%d')
        os.system('mv ' + item + ' json/message_' + file_suffix + '.json')
        os.system('rm -r ' + item[:item.find('/')])

def check_data_file(cursor):
    
    db_timestamps = cursor.execute("    \
        SELECT                          \
            timestamp_ms                \
        FROM                            \
            messages                    \
            ").fetchall()
    df = pd.DataFrame(db_timestamps, columns=['timestamp_ms'])
    df['date'] = df['timestamp_ms'].map(truncate_timestamp)
    db_dates = set(df['date'].unique())
            
    file_dates = set()
    for filename in os.listdir('json'):
        if filename != 'loaded':
            with open(os.path.join('json', filename)) as file:
                data = json.load(file)
                        
                for message in data['messages']:
                    file_dates.add(truncate_timestamp(message['timestamp_ms']))
                        
    if db_dates.isdisjoint(file_dates):
        is_data_new = True
    else:
        print('Dates already loaded into database:')
        for dt in db_dates.intersection(file_dates):
            print(dt.strftime('%Y-%m-%d'))
        is_data_new = False
        
    return is_data_new, db_dates, file_dates

def confirm_data_load(db_dates, file_dates):
    
    if len(db_dates) > 0:
    
        begin = min(db_dates)
        end = max(db_dates)

        print('Dates in database:')
        if (end - begin).days + 1 == len(db_dates):
            print(begin.strftime('%Y-%m-%d') + ' to ' + end.strftime('%Y-%m-%d'))
        else:
            for dt in db_dates:
                print(dt.strftime('%Y-%m-%d'))
    
    print('\nDates loaded:')
    for dt in file_dates:
        print(dt.strftime('%Y-%m-%d'))

def load_data():
    
    with closing(sqlite3.connect('rc_chat_log.db')) as connection:
        with closing(connection.cursor()) as cursor:
            
            os.environ['TZ'] = 'America/Chicago'
            time.tzset()

            is_data_new, db_dates, file_dates = check_data_file(cursor)
            if is_data_new:
    
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
                confirm_data_load(db_dates, file_dates)
        
if __name__ == '__main__':
    #load_data()
    handle_zip_file()