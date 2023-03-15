import os
from zipfile import ZipFile
import pandas as pd
from date_handling import truncate_timestamp
import json
import re
from datetime import datetime, date

def handle_zip_file():
    
    if len(os.listdir('zip')) > 0:
    
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
            os.system('mv ' + item + os.path.join(' json', 'message_') + file_suffix + '.json')
            os.system('rm -r ' + item[:item.find('/')])

        os.system('rm ' + os.path.join('zip', '*.zip'))
        print('Extracted from zip file(s) to json folder')

def check_data_file(cursor):
    
    if len(os.listdir('json')) == 1:
        print('Json folder is empty')
        return False, set(), set()
    
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

def update_nickname(content, sender_name, cursor):

    if re.search('^.*set the nickname for.*to.*$', content):
        begin_str = 'set the nickname for'
        begin_ind = content.index(begin_str) + len(begin_str) + 1
        end_ind = content.index(' to ', begin_ind)
        username = content[begin_ind:end_ind]
        nickname = content[end_ind + 4:-1]
    else:
        if re.search('^.*set her own nickname to.*$', content):
            gender = 'her'
        else:
            gender = 'his'
        begin_str = 'set ' + gender + ' own nickname to'
        end_ind = content.index(begin_str) + len(begin_str) + 1
        username = sender_name
        nickname = content[end_ind:]
            
    cursor.execute("        \
        UPDATE              \
            usernames       \
        SET                 \
            nickname = ?    \
        WHERE               \
            username = ?    \
        ", (nickname, username))
    return username + ', ' + nickname

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
    os.system('mv ' + os.path.join('json', '*.json') + ' ' + os.path.join('json', 'loaded'))

def load_data(connection, cursor):
    
    handle_zip_file()
               
    is_data_new, db_dates, file_dates = check_data_file(cursor)
    if is_data_new:
        
        updated_nicknames = []
    
        for filename in os.listdir('json'):
            if filename != 'loaded':
                with open(os.path.join('json', filename)) as file:
                    data = json.load(file)

                for message in reversed(data['messages']):
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

        if updated_nicknames:
            print('Updated nicknames:')
            for nickname in updated_nicknames:
                print(nickname)

        confirm_data_load(db_dates, file_dates)
        
def get_messages(cursor):
    
    messages = cursor.execute("                                                             \
        SELECT                                                                              \
            username,                                                                       \
            nickname,                                                                       \
            message_type,                                                                   \
            timestamp_ms                                                                    \
        FROM                                                                                \
            messages                                                                        \
            JOIN message_types ON messages.message_type_id = message_types.message_type_id  \
            JOIN usernames ON messages.username_id = usernames.username_id                  \
        WHERE message_type != 'unsent'                                                      \
            ").fetchall()
            
    df = pd.DataFrame(messages, columns=['username', 'nickname', 'message_type', 'timestamp_ms'])
    missing_nicknames = df[df['nickname'].isna()]['username'].unique()
    if missing_nicknames.size == 0:
        df['date'] = df['timestamp_ms'].map(truncate_timestamp)
        df.drop(columns=['username', 'timestamp_ms'], inplace=True)
    else:
        print('Missing nicknames:')
        print(missing_nicknames)
        df = pd.DataFrame()

    return df

def get_message_content(cursor):
    end_timestamp = cursor.execute("    \
        SELECT                          \
            MAX(timestamp_ms)           \
        FROM                            \
            messages                    \
        ").fetchone()[0]
    dt = datetime.fromtimestamp(end_timestamp/1000)
    day = 1000 * datetime(dt.year, dt.month, dt.day).timestamp()
    
    message_content = cursor.execute("                                                      \
        SELECT                                                                              \
            content                                                                         \
        FROM                                                                                \
            messages                                                                        \
            JOIN message_types ON messages.message_type_id = message_types.message_type_id  \
        WHERE                                                                               \
            message_type = 'content'                                                        \
            AND timestamp_ms >= ?", (day,)).fetchall()
    
    return message_content