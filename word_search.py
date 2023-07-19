import csv
import sqlite3
from contextlib import closing
from datetime import datetime

word = input('Enter word to search for: ')

with closing(sqlite3.connect('rc_chat_log.db')) as connection:
    with closing (connection.cursor()) as cursor:
        messages = cursor.execute("                                             \
            SELECT                                                              \
                timestamp_ms,                                                   \
                username,                                                       \
                content                                                         \
            FROM                                                                \
                messages                                                        \
                JOIN usernames ON messages.username_id = usernames.username_id  \
            WHERE                                                               \
                content LIKE '%' || ? || '%'                                    \
            ORDER BY                                                            \
                timestamp_ms                                                    \
            ", (word,)).fetchall()
        
for message in messages:
    print(datetime.fromtimestamp(message[0]/1000).strftime('%Y-%m-%d'), message[1], '"' + message[2] + '"')