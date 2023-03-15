import sqlite3
from contextlib import closing

if __name__ == '__main__':
    with closing(sqlite3.connect('rc_chat_log.db')) as connection:
        with closing (connection.cursor()) as cursor:
            cursor.execute("                    \
                DELETE FROM                     \
                    usernames                   \
                WHERE                           \
                    username = 'Facebook user'  \
                ")
        connection.commit()