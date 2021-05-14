# -*- coding: utf-8 -*-
"""1b.ipynb

Automatically generated by Colaboratory.

Original file is located at
    https://colab.research.google.com/drive/1ytAl1ZXQbeX-S5qtFsdNmL3iAB5lazQp
"""

#from google.colab import files
#uploaded = files.upload()

#from google.colab import files
#up = files.upload()

import json
import pandas as pd
import sys

input_file1 = sys.argv[1]
input_file2 = sys.argv[2]
output_file = sys.argv[3]

roster_df = pd.read_csv(input_file2)
roster_df

roster_df[['Last','First']] = roster_df.Name.str.split(',', expand = True) 

#roster_df

roster_df = roster_df.drop('Name', axis=1)
#roster_df

roster_df['Name'] = roster_df[['First', 'Last']].agg(' '.join, axis=1)

roster_df = roster_df.drop(['Last', 'First'], axis =1)

#roster_df

roster_df = roster_df[['Name','Participating from']]

roster_df

chats_df = pd.read_csv(input_file1, sep = '\t', header = None)
chats_df

chats_df.columns = ['Time', 'Person', 'Message']

chats_df['Message'] = chats_df['Message'].str.replace('\r','')

chats_df

chats_df = chats_df.drop(['Time'], axis=1)

chats_df['Person']= chats_df['Person'].str.replace(':','')
chats_df['Message'] = chats_df['Message'].str.replace('\r','')

person = []
message = []

for i in range(len(chats_df)):
  if chats_df['Message'][i] == '1':
    #new_dict.update({df['Person'][i]: df['Message'][i]})
    person.append(chats_df['Person'][i])
    message.append(chats_df['Message'][i])

len(message)

len(person)

nochats_df = pd.DataFrame(list(zip(person, message)), 
               columns =['Person', 'Message'])

nochats_df

updated_nochats_df = nochats_df.drop_duplicates(keep='last')

updated_nochats_df = updated_nochats_df.reset_index()


updated_nochats_df

roster_names = roster_df['Name']

roster_names

nochats = updated_nochats_df['Person']
nochats

import re

result = {}



ct = 0

dict1 = {}

for i in range(len(roster_names)):
  for j in range(len(nochats)):
    if nochats[j].lower() in roster_names[i].lower():
      ct+=1 
      while ct < 49:
        dict1.update(dict({roster_df['Name'][j].lstrip():roster_df['Participating from'][j]}))
        break
      
      #print(dict1)
      #result.update(dict1)

dict1

with open(output_file, 'w+') as outfile:
    json.dump(dict1, outfile)
