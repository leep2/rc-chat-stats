import os
import pandas as pd

if __name__ == '__main__':
    old_file = os.path.join('csv', 'old_nicknames.csv')
    new_file = os.path.join('csv', 'new_nicknames.csv')
    old = pd.read_csv(old_file, names=['name', 'nickname'])
    new = pd.read_csv(new_file, names=['name', 'nickname'])
    
    not_in_new = set(old['name']) - set(new['name'])
    old2 = old[old['name'].isin(not_in_new)]
    updated = pd.concat([new, old2])
    
    updated_file = os.path.join('csv', 'updated_nicknames.csv')
    updated.to_csv(updated_file, header=False, index=False)