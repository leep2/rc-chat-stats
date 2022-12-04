import sqlite3
from contextlib import closing
from datetime import date, datetime, timedelta
import pandas as pd

def convert_day(timestamp_ms):
    return datetime.fromtimestamp(timestamp_ms/1000).strftime('%A')
#    return date(dt.year, dt.month, dt.day)

if __name__ == '__main__':
    with closing(sqlite3.connect('messages.db')) as connection:
        with closing(connection.cursor()) as cursor:
            
            END_DATE = date(2022, 12, 2)
            begin = 1000 * int(END_DATE.strftime('%s'))
            end = 1000 * int((END_DATE + timedelta(-28)).strftime('%s'))
            
#            timestamps = cursor.execute("SELECT timestamp_ms FROM messages WHERE timestamp_ms >= ? AND timestamp_ms < ?", (begin, end)).fetchall()
#            print(cursor.execute("SELECT MAX(timestamp_ms) FROM messages").fetchmany(10))
#            print(len(timestamps))
            
            timestamps = cursor.execute("SELECT timestamp_ms FROM messages WHERE message_type_id != ? AND timestamp_ms >= ? AND timestamp_ms < ?", (4, 1667538000000, 1669960800000)).fetchall()
            df = pd.DataFrame(timestamps, columns=['timestamp_ms'])
            df['day'] = df['timestamp_ms'].map(convert_day)
            
            counts = df.groupby(['day'])['day'].count()
            print(counts)