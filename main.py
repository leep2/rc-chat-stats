from datetime import datetime, date, timedelta
import sqlite3
from contextlib import closing
from db_functions import load_data, get_messages, get_message_content
import pandas as pd
import os
import time
import spacy
from collections import Counter
from sklearn.feature_extraction.text import TfidfVectorizer
import configparser
import pygsheets

def filter_by_time(df, latest_date, begin, end):
    begin_date = latest_date + timedelta(begin)
    end_date = latest_date + timedelta(end)
    return df[(df['date']>=begin_date) & (df['date']<=end_date)]
    
def message_counts(df):
    return df.groupby(['nickname', 'message_type'], as_index=False).count()

def total_messages(df):
    df.drop(columns=['nickname'], inplace=True)
    return df.groupby(['date'], as_index=False).count()
    
def combine_message_counts(df):
    BEGIN_DATE = df['date'].min()
    END_DATE = df['date'].max()
    yesterday = message_counts(filter_by_time(df, END_DATE, 0, 0))
    yesterday['period'] = 'a. ' + END_DATE.strftime('%b %d')
    day_before = message_counts(filter_by_time(df, END_DATE, -1, -1))
    day_before['period'] = 'b. ' + (END_DATE + timedelta(-1)).strftime('%b %d')
    week = message_counts(filter_by_time(df, END_DATE, -6, 0))
    week['period'] = 'c. ' + (END_DATE + timedelta(-6)).strftime('%b %d') + ' to ' + END_DATE.strftime('%b %d')
    beginning = message_counts(df)
    beginning['period'] = 'd. ' + BEGIN_DATE.strftime('%b %d') + ' to date'
    return pd.concat([yesterday, day_before, week, beginning], axis=0)

def calc_tfidf(message_content, begin):
    
    week = [[] for i in range(7)]

    for message in message_content:
        if message[1] is not None:
            week[(datetime.fromtimestamp(message[0]/1000) - begin).days].append(message[1].replace('\n', ''))

    texts = [' '.join(day) for day in week]

    nlp = spacy.load('en_core_web_sm')
    nlp.add_pipe("emoji", first=True)
    docs = list(nlp.pipe(texts))

    lemmas_df = pd.DataFrame()
    lemmas_week = []
    for i in range(len(week)):
        lemmas = [token.lemma_ for token in docs[i] if not token._.is_emoji and not token.is_stop and not token.is_punct]
        lemmas_week.append(' '.join(lemmas))
        df_day = pd.DataFrame(index=list(set(lemmas)))
        df_day['date'] = begin + timedelta(i)
        lemmas_df = pd.concat([lemmas_df, df_day], axis=0)

    tfIdfVectorizer = TfidfVectorizer(use_idf=True)
    tfIdf = tfIdfVectorizer.fit_transform(lemmas_week)
    tfidf_df = pd.DataFrame(tfIdf[0].T.todense(), index=tfIdfVectorizer.get_feature_names_out(), columns=["TF-IDF"])

    df = pd.merge(lemmas_df, tfidf_df, left_index=True, right_index=True)
    last_day = df[df['date'] == begin + timedelta(6)]
    last_day = last_day.drop('date', axis=1)

    return last_day
    
def set_workbook():
    
    config = configparser.ConfigParser()
    config.read('config.ini')
    
    # google sheets authentication
    filename = os.listdir('auth')[0]
    creds = os.path.join('auth', filename)
    api = pygsheets.authorize(service_file=creds)
    return api.open(config['DEFAULT']['sheets_filename'])

def update_sheet(wb, sheet_name, df, copy_index=False):

    # open the sheet by name
    sheet = wb.worksheet_by_title(sheet_name)
    sheet.clear()
    sheet.set_dataframe(df, (1,1), copy_index)

if __name__ == '__main__':
    
    os.environ['TZ'] = 'America/Chicago'
    time.tzset()
    
    with closing(sqlite3.connect('rc_chat_log.db')) as connection:
        with closing(connection.cursor()) as cursor:
            load_data(connection, cursor)
            messages = get_messages(cursor)
            message_content, begin = get_message_content(cursor)
            
    if not messages.empty:

        counts = combine_message_counts(messages)
        totals = total_messages(messages)
        tfidf = calc_tfidf(message_content, begin)

        workbook = set_workbook()
        update_sheet(workbook, 'Member messages', counts)
        update_sheet(workbook, 'Total messages', totals)
        update_sheet(workbook, 'Word cloud', tfidf, True)
        print('Writing to Google Sheets')