import sqlite3
from contextlib import closing
import os
import json
import re

if __name__ == '__main__':
    with closing(sqlite3.connect('messages.db')) as connection:
        with closing(connection.cursor()) as cursor:

            for filename in os.listdir('json'):
                with open(os.path.join('json', filename)) as file:
                    data = json.load(file)

                    for message in data['messages']:
                        if 'content' in message and re.search('^.*reacted.*to your message $', message['content']):
                            pass
                        else:
                            content = None
                            if 'is_unsent' in message and message['is_unsent']:
                                message_type = 'unsent'
                                count = 1
                            elif 'content' in message:
                                message_type = 'content'
                                count = len(message['content'].split())
                                content = message['content'].encode('latin1').decode('utf8')
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
                            name_id_result = cursor.execute("SELECT name_id FROM names WHERE name = ?", (name,)).fetchone()
                            if not name_id_result:
                                cursor.execute("INSERT INTO names (name) VALUES (?)", (name,))
                                name_id_result = cursor.execute("SELECT name_id FROM names WHERE name = ?", (name,)).fetchone()
                            
                            message_type_id_result = cursor.execute('SELECT message_type_id FROM message_types WHERE message_type = ?', (message_type,)).fetchone()
                            if not message_type_id_result:
                                cursor.execute("INSERT INTO message_types (message_type) VALUES (?)", (message_type,))
                                message_type_id_result = cursor.execute('SELECT message_type_id FROM message_types WHERE message_type = ?', (message_type,)).fetchone()
                            
                            cursor.execute("INSERT INTO messages (name_id, message_type_id, timestamp_ms, count, content) VALUES (?, ?, ?, ?, ?)", (name_id_result[0], message_type_id_result[0], message['timestamp_ms'], count, content))

        connection.commit()