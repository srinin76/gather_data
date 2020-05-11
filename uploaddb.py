import pandas as pd
from sqlalchemy.types import Integer,String,Numeric, Date
import sqlalchemy
import numpy
import logging
import os

logging.basicConfig(filename="./uploadDb.log", format='%(levelname)s : %(asctime)s : %(message)s', level=logging.DEBUG)


path = '/home/dvlpr/Documents/Stocks/data/bulk_block_deals/bulk'

sel_col = ['Date', 'Symbol', 'Security Name', 'Client Name', 'Buy / Sell',
'Quantity Traded', 'Trade Price / Wght. Avg. Price', 'Remarks']

headings = ['txndate', 'xsymbol', 'security_name', 'client_name', 'buy_or_sell',
'quantity_traded', 'trade_price_or_wght_avg_price', 'remarks']

dtypes = {'txndate' : Date , 'xsymbol' : String , 'security_name' : String , 'client_name' : String ,
'buy_or_sell' : String , 'quantity_traded ' : Numeric , 'trade_price_or_wght_avg_price' : Numeric , 
'remarks' : String}

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
    df.to_sql('bulk_trades', conn, if_exists = 'append',index = False,dtype = dtypes)
    tran.commit()