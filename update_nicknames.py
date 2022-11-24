import pandas as pd
import csv

def update_nicknames():
    old = pd.read_csv('nicknames.csv', names=['name', 'nickname1'])
    new = pd.read_csv('new_nicknames.csv', names=['name', 'nickname2'])
    df = old.merge(new, left_on='name', right_on='name', how='outer')
    #print(df.to_string())
    change = df[df['nickname1'] != df['nickname2']]
    print(change)
    
    old2 = pd.read_csv('nicknames.csv', names=['name', 'nickname'])
    new2 = pd.read_csv('new_nicknames.csv', names=['name', 'nickname'])
    
    old_set = set(old2['name'])
    new_set = set(new2['name'])
    not_in_new = old_set - new_set
    print(not_in_new)
    old3 = old2[old2['name'].isin(not_in_new)]
    updated = pd.concat([new2, old3])
    print(updated.to_string())
    
    updated.to_csv('updated_nicknames.csv', header=False, index=False)
    
    with open('updated_nicknames.csv', mode='r') as infile:
        reader = csv.reader(infile)
        names_dict = {rows[0]:rows[1] for rows in reader}
    print(len(names_dict))
    
    #df2 = old2.combine_first(new2)
    #print(df2)
    #df3 = new2.combine_first(old2)
    #print(df3.to_string())
    #old2.update(new2)
    #print(old2.to_string())

if __name__ == '__main__':
    update_nicknames()