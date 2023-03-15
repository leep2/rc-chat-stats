import os
import time
import sqlite3
from contextlib import closing
from datetime import datetime

if __name__ == '__main__':

    os.environ['TZ'] = 'America/Chicago'
    time.tzset()

    with closing(sqlite3.connect('rc_chat_log.db')) as connection:
        with closing (connection.cursor()) as cursor:

            end_timestamp = cursor.execute("    \
                SELECT                          \
                    MAX(timestamp_ms)           \
                FROM                            \
                    messages                    \
                ").fetchone()[0]
            dt = datetime.fromtimestamp(end_timestamp/1000)
            day = 1000 * datetime(dt.year, dt.month, dt.day).timestamp()            

            cursor.execute("            \
                DELETE FROM             \
                    messages            \
                WHERE                   \
                    timestamp_ms >= ?   \
                ", (day,))
            
        connection.commit()