import pandas as pd
from sqlalchemy.types import Integer,String,Numeric, Date
import sqlalchemy
import numpy
import logging
import os

logging.basicConfig(filename="./uploadbhav.log", format='%(levelname)s : %(asctime)s : %(message)s', level=logging.DEBUG)


path = '/home/dvlpr/Documents/Stocks/historicaleod/download/csv'

sel_col = ['SYMBOL', 'TIMESTAMP', 'OPEN', 'HIGH', 'LOW', 'CLOSE', 'TOTTRDQTY', 'TOTALTRADES', 'PREVCLOSE']

headings = ['symbol', 'timestamp', 'open', 'high', 'low', 'close', 'tottrdqty', 'tottrades', 'prevclose']

dtypes = {'symbol' : String, 'timestamp' : Date, 'open' : Numeric, 'high' : Numeric, 'low' : Numeric, 
'close' : Numeric, 'tottrdqty' : Numeric, 'tottrades': Numeric, 'prevclose': Numeric}

db_string = "postgresql://postgres:aa123456@localhost:5433/postgres"
engine = sqlalchemy.create_engine(db_string)
conn = engine.connect()

for fle in os.listdir(path):
    fname = path + '/' + fle
    df = pd.read_csv(fname)
    logging.info(fname)
    # manage mischief
    df = df[df.columns.intersection(sel_col)]
    if (len(df.columns) < len(sel_col)):
            for col in sel_col:
                df[col] = numpy.nan
    df.columns = headings
    tran = conn.begin()
    df.to_sql('bhav', conn, if_exists = 'append',index = False,dtype = dtypes)
    tran.commit()