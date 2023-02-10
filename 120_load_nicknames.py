import csv
import sqlite3
from contextlib import closing

with open('nicknames.csv', mode='r') as infile:
    reader = csv.reader(infile)
    
    usernames = list(reader)

with closing(sqlite3.connect('rc_chat_log.db')) as connection:
    with closing (connection.cursor()) as cursor:
        
        for username in usernames:
            cursor.execute("UPDATE usernames SET nickname = ? WHERE username = ?", (username[1], username[0]))

        connection.commit()