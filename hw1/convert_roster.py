# -*- coding: utf-8 -*-
"""2b.ipynb

Automatically generated by Colaboratory.

Original file is located at
    https://colab.research.google.com/drive/1wNaAQZuTcgXxW3LEhvtJ56nraZTzbn51
"""

#from google.colab import files
#uploaded = files.upload()

import pandas as pd
import json
import sys

input_file = sys.argv[1]
output_file = sys.argv[2]

df = pd.read_csv(input_file)
df

df[['Last Name','First Name']] = df['Name'].str.split(',',expand=True)
df

df = df.drop(columns='Name', axis =1)
df

df['Name'] = df["First Name"] + ' ' + df['Last Name']
df= df.drop(columns = ["Last Name", "First Name"], axis = 1)

df

df = df[["Name", "Participating from"]]

df

result = df.to_json(orient="records")
parsed = json.loads(result)

parsed

with open(output_file , 'w+') as f:
    json.dump(parsed, f)
