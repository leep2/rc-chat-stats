import sqlite3
from contextlib import closing

def username_sort(record):
    return record[0]

def nickname_sort(record):
    return record[1]

with closing(sqlite3.connect('rc_chat_log.db')) as connection:
    with closing (connection.cursor()) as cursor:

        names = cursor.execute('SELECT username, nickname FROM usernames').fetchall()

names2 = []
for name in names:
    nick = name[1]
    if nick is None:
        nick = ''
    names2.append([name[0], nick])

s = input('Sort by (u)sername; nickname by default:')

if s == 'u':
    names2.sort(key=username_sort)
    for name in names2:
        print(name[0] + ', ' + name[1])
else:
    names2.sort(key=nickname_sort)
    for name in names2:
        print(name[1] + ', ' + name[0])