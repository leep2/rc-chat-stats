import csv
import sqlite3
from contextlib import closing

if __name__ == '__main__':
        
    with closing(sqlite3.connect('rc_chat_log.db')) as connection:
        with closing (connection.cursor()) as cursor:

            cursor.execute("    \
                ALTER TABLE     \
                    names RENAME TO usernames')
            cursor.execute("    \
                ALTER TABLE     \
                    usernames RENAME COLUMN name_id to username_id")
            cursor.execute("    \
                ALTER TABLE     \
                    usernames RENAME COLUMN name TO username")

            cursor.execute("    \
                ALTER TABLE     \
                    messages RENAME COLUMN name_id to username_id")