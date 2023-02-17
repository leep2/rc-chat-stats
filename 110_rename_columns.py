import sqlite3
from contextlib import closing

if __name__ == '__main__':
        
    with closing(sqlite3.connect('rc_chat_log.db')) as connection:
        with closing (connection.cursor()) as cursor:

            cursor.execute("    \
                ALTER TABLE     \
                    names RENAME TO usernames")
            
            cursor.execute("                            \
                CREATE TABLE usernamesTemp (            \
                    username_id INTEGER PRIMARY KEY,    \
                    username TEXT NOT NULL,             \
                    nickname TEXT)                      \
                    ")

            cursor.execute("                                                                    \
                CREATE TABLE messagesTemp (                                                     \
                    message_id INTEGER PRIMARY KEY,                                             \
                    username_id INTEGER NOT NULL,                                               \
                    message_type_id INTEGER NOT NULL,                                           \
                    timestamp_ms INTEGER NOT NULL,                                              \
                    item_count INTEGER NOT NULL,                                                \
                    content TEXT,                                                               \
                    FOREIGN KEY (username_id) REFERENCES usernames (username_id),               \
                    FOREIGN KEY (message_type_id) REFERENCES message_types (message_type_id))   \
                    ")
            
            cursor.execute("                                                \
                INSERT INTO usernamesTemp (username_id, username, nickname) \
                SELECT                                                      \
                    name_id,                                                \
                    name,                                                   \
                    nickname                                                \
                FROM                                                        \
                    usernames                                               \
                    ")
            
            cursor.execute("                                    \
                INSERT INTO messagesTemp (                      \
                    message_id, username_id, message_type_id,   \
                    timestamp_ms, item_count, content           \
                )                                               \
                SELECT                                          \
                    message_id,                                 \
                    name_id,                                    \
                    message_type_id,                            \
                    timestamp_ms,                               \
                    item_count,                                 \
                    content                                     \
                FROM                                            \
                    messages                                    \
                    ")
            
            cursor.execute("        \
                DROP                \
                    TABLE usernames \
                    ")
            
            cursor.execute("        \
                DROP                \
                    TABLE messages  \
                    ")
            
            cursor.execute("                            \
                ALTER TABLE                             \
                    usernamesTemp RENAME TO usernames   \
                    ")
            
            cursor.execute("                            \
                ALTER TABLE                             \
                    messagesTemp RENAME TO messages     \
                    ")
            
        connection.commit()