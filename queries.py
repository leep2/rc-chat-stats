import sqlite3
from contextlib import closing

if __name__ == '__main__':
    with closing(sqlite3.connect('messages.db')) as connection:
        with closing(connection.cursor()) as cursor:
            print(cursor.execute("SELECT * FROM names").fetchall())
            print(cursor.execute("SELECT * FROM message_types").fetchall())
            print(cursor.execute("SELECT * FROM messages").fetchmany(10))
            print(len(cursor.execute("SELECT * FROM messages").fetchall()))