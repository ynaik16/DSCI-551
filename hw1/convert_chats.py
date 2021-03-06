# -*- coding: utf-8 -*-
"""2a.ipynb

Automatically generated by Colaboratory.

Original file is located at
    https://colab.research.google.com/drive/1iX24m3K1ov_uiW3iww9OhLtRkxPKgtHw
"""

#from google.colab import files
#uploaded = files.upload()

import pandas as pd
import json
import sys

input_file = sys.argv[1]
output_file = sys.argv[2]

df = pd.read_csv(input_file, sep = '\t', header = None)
df

df.columns=['Time', 'Person', 'Message']
df

result = df.to_json(orient="records")
parsed = json.loads(result)

parsed

with open(output_file, 'w+') as of:
    json.dump(parsed, of)

