import os
import pandas as pd
#import csv

if __name__ == '__main__':
#def update_nicknames():
    old_file = os.path.join('csv', 'old_nicknames.csv')
    new_file = os.path.join('csv', 'new_nicknames.csv')
    old = pd.read_csv(old_file, names=['name', 'nickname'])
    new = pd.read_csv(new_file, names=['name', 'nickname'])
    
#    old2 = pd.read_csv('nicknames.csv', names=['name', 'nickname'])
#    new2 = pd.read_csv('new_nicknames.csv', names=['name', 'nickname'])
    
#    old_names = set(old['name'])
#    new_names = set(new['name'])
    not_in_new = set(old['name']) - set(new['name'])
#    print(not_in_new)
    old2 = old[old['name'].isin(not_in_new)]
    updated = pd.concat([new, old2])
#    print(updated.to_string())
    
    updated_file = os.path.join('csv', 'updated_nicknames.csv')
    updated.to_csv(updated_file, header=False, index=False)
    
#    with open('updated_nicknames.csv', mode='r') as infile:
#        reader = csv.reader(infile)
#        names_dict = {rows[0]:rows[1] for rows in reader}
#    print(len(names_dict))



#    update_nicknames()