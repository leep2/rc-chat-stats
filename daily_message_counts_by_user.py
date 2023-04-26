import os
import time
import sqlite3
from contextlib import closing
from db_functions import get_messages
import pygsheets
from main import update_sheet

os.environ['TZ'] = 'America/Chicago'
time.tzset()
    
with closing(sqlite3.connect('rc_chat_log.db')) as connection:
    with closing(connection.cursor()) as cursor:
        messages = get_messages(cursor)
        
counts = messages.groupby(['nickname', 'date'], as_index=False).count()

filename = os.listdir('auth')[0]
creds = os.path.join('auth', filename)
api = pygsheets.authorize(service_file=creds)
workbook = api.open('Daily message counts by user')

update_sheet(workbook, 'Sheet1', counts)