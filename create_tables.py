import sqlite3
from contextlib import closing

if __name__ == '__main__':
    with closing(sqlite3.connect('messages.db')) as connection:
        with closing(connection.cursor()) as cursor:

            cursor.execute("CREATE TABLE names (                        \
                                name_id INTEGER PRIMARY KEY,            \
                                name TEXT NOT NULL,                     \
                                nickname TEXT)")

            cursor.execute("CREATE TABLE message_types (                \
                                message_type_id INTEGER PRIMARY KEY,    \
                                message_type TEXT NOT NULL)")

            cursor.execute("CREATE TABLE messages (                                                         \
                                message_id INTEGER PRIMARY KEY,                                             \
                                name_id INTEGER NOT NULL,                                                   \
                                message_type_id INTEGER NOT NULL,                                           \
                                timestamp_ms INTEGER NOT NULL,                                              \
                                count INTEGER NOT NULL,                                                     \
                                content TEXT,                                                               \
                                FOREIGN KEY (name_id) REFERENCES names (name_id),                           \
                                FOREIGN KEY (message_type_id) REFERENCES message_types (message_type_id))")