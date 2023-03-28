import csv
import sqlite3
from contextlib import closing

def sort_key(record):
    return record[1]

with closing(sqlite3.connect('rc_chat_log.db')) as connection:
    with closing (connection.cursor()) as cursor:
        
        names = cursor.execute('SELECT username, nickname FROM usernames').fetchall()
        names.sort(key=sort_key)
        for name in names:
            print(name[1] + ', ' + name[0])