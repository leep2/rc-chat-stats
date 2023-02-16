import os
import time
import sqlite3
from contextlib import closing
import pandas as pd
from date_handling import truncate_timestamp

if __name__ == '__main__':
    
    os.environ['TZ'] = 'America/Chicago'
    time.tzset()
    
    with closing(sqlite3.connect('rc_chat_log.db')) as connection:
        with closing(connection.cursor()) as cursor:
            db_timestamps = cursor.execute("    \
                SELECT                          \
                    timestamp_ms                \
                FROM                            \
                    messages                    \
                    ").fetchall()
            
    df = pd.DataFrame(db_timestamps, columns=['timestamp_ms'])
    df['date'] = df['timestamp_ms'].map(truncate_timestamp)
    db_dates = set(df['date'].unique())
    
    begin = min(db_dates)
    end = max(db_dates)
    
    print('Dates in database:')
    if (end - begin).days + 1 == len(db_dates):
        print(begin.strftime('%Y-%m-%d') + ' to ' + end.strftime('%Y-%m-%d'))
    else:
        for dt in db_dates:
            print(dt.strftime('%Y-%m-%d'))