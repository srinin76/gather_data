import os
import pandas as pd 

from sqlalchemy.types import Integer,String,Numeric
import sqlalchemy 

fldr= '/home/dvlpr/Documents/Stocks/data/index'
files= os.listdir(fldr)
cols = ['idx_name', 'Symbol','Industry']
df_tgt = pd.DataFrame(columns = cols)
for file in files:
    path = fldr + '/' + file
    df = pd.read_csv(path)
    df['idx_name'] = file
    df_tgt = df_tgt.append(df)

db_string = "postgresql://postgres:aa123456@localhost:5433/postgres"
engine = sqlalchemy.create_engine(db_string)
conn = engine.connect()
trans = conn.begin()
df_tgt.to_sql('indices',conn,if_exists="replace")
trans.commit()