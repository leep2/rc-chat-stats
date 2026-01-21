from contextlib import closing
from datetime import datetime
import sqlite3

CENTRAL_TIMEZONE = '12:00:00-0500'

selected_date = input('Enter selected date (YYYY-MM-DD): ')
dt = 1000 * datetime.strptime(f'{selected_date} {CENTRAL_TIMEZONE}', '%Y-%m-%d %H:%M:%S%z').timestamp()

with closing(sqlite3.connect('rc_chat_log.db')) as connection:
	with closing(connection.cursor()) as cursor:

		cursor.execute("INSERT INTO messages (				\
			username_id, message_type_id, timestamp_ms,		\
			item_count										\
		)													\
		VALUES												\
			(15, 5, ?, 1)									\
		", (dt,))

	connection.commit()