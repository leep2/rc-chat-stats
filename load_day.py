import os
import time
import json
from datetime import datetime, timedelta
import sqlite3
from contextlib import closing
import re
from db_functions import update_nickname

os.environ['TZ'] = 'America/Chicago'
time.tzset()

selected_date = input('Enter selected date (YYYY-MM-DD): ')
sel_dt = datetime.strptime(selected_date, '%Y-%m-%d')

filelist = sorted(os.listdir('json'))
filelist.remove('loaded')

#filename = 'message_20230205_PT.json'

with closing(sqlite3.connect('rc_chat_log.db')) as connection:
    with closing(connection.cursor()) as cursor:

        for filename in filelist:
            with open(os.path.join('json', filename)) as file:
                data = json.load(file)

            for message in reversed(data['messages']):
                timestamp = datetime.fromtimestamp(message['timestamp_ms']/1000)
                if timestamp >= sel_dt and timestamp < sel_dt + timedelta(days=1):
                
                    if 'content' in message and re.search('^.*reacted.*to your message $', message['content']):
                        pass
                    elif 'content' in message and (re.search('^.*set the nickname for.*to.*$', message['content']) or re.search('^.*set h(er|is) own nickname to.*$', message['content'])):
                        updated_nicknames.append(update_nickname(message['content'].encode('latin1').decode('utf8'), message['sender_name'], cursor))
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

                        username = message['sender_name']
                        username_id_result = cursor.execute("   \
                            SELECT                              \
                                username_id                     \
                            FROM                                \
                                usernames                       \
                            WHERE                               \
                                username = ?                    \
                            ", (username,)).fetchone()
                        if not username_id_result:
                            cursor.execute("                        \
                                INSERT INTO usernames (username)    \
                                VALUES                              \
                                    (?)                             \
                                ", (username,))
                            username_id_result = cursor.execute("   \
                                SELECT                              \
                                    username_id                     \
                                FROM                                \
                                    usernames                       \
                                WHERE                               \
                                    username = ?                    \
                                ", (username,)).fetchone()

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

                        cursor.execute("                                    \
                            INSERT INTO messages (                          \
                                username_id, message_type_id, timestamp_ms, \
                                item_count, content                         \
                            )                                               \
                            VALUES                                          \
                                (?, ?, ?, ?, ?)                             \
                            ", (username_id_result[0], message_type_id_result[0], message['timestamp_ms'], count, content))

        connection.commit()
    
#for message in reversed(data['messages']):
    #print(datetime.fromtimestamp(message['timestamp_ms']/1000))
    
#timestamps = [datetime.fromtimestamp(message['timestamp_ms']/1000) for message in data['messages']]
#print(min(timestamps))
#print(max(timestamps))

#selected_date = input('Enter selected date (YYYY-MM-DD): ')
#sel_dt = datetime.strptime(selected_date, '%Y-%m-%d')
#print(sel_dt)
#print(sel_dt + timedelta(days=1))

#day = [timestamp for timestamp in timestamps if timestamp >= sel_dt and timestamp < sel_dt + timedelta(days=1)]
#print(len(day))
