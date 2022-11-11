import pandas as pd
import os
import pygsheets

# sample dataframe
numbers = [9, 5, 7, 5, 6, 2, 8, 4]
letters = ['A', 'B', 'D', 'G', 'Z', 'J', 'G', 'N']
df = pd.DataFrame({'name': letters, 'count': numbers})

# google sheets authentication
filename = os.listdir('auth')[0]
creds = os.path.join('auth', filename)
api = pygsheets.authorize(service_file=creds)
wb = api.open('Tableau test')

# open the sheet by name
sheet = wb.worksheet_by_title(f'Sheet1')
sheet.set_dataframe(df, (1,1))