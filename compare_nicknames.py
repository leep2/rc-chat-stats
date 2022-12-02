import os
import pandas as pd

if __name__ == '__main__':
    old_file = os.path.join('csv', 'old_nicknames.csv')
    new_file = os.path.join('csv', 'new_nicknames.csv')
    old = pd.read_csv(old_file, names=['name', 'old_nickname'])
    new = pd.read_csv(new_file, names=['name', 'new_nickname'])
    df = old.merge(new, left_on='name', right_on='name', how='outer')
    print(df[df['old_nickname'] != df['new_nickname']])