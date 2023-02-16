import csv
import sqlite3
from contextlib import closing
        
with closing(sqlite3.connect('rc_chat_log.db')) as connection:
    with closing (connection.cursor()) as cursor:
        
        names = cursor.execute('SELECT username, nickname FROM usernames').fetchall()
        print(names)