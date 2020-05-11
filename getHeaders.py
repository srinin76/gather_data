# getHeaders.py

import os
import pandas as pd


path = '/home/dvlpr/Documents/Stocks/dwnld'
paramlist = []
for file in os.listdir(path):
    filename = path + '/' + file
    df = pd.read_csv(filename)
    paramlist.append(df.columns)
params = [param for param in i for i in paramlist]
unq_params = set(params)
unq_params_lst = list(unq_params)
df = pd.DataFrame(unq_params_lst)
df.to_csv('./param.csv')
