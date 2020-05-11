# Merge Quandl and historical data 

import pandas as pd
from sqlalchemy.types import Integer,String,Numeric
from random import randint
from datetime import time
import sqlalchemy 
import requests
import time
import logging
import sys, traceback
logging.basicConfig(filename="./MergeData.log", format='%(levelname)s : %(asctime)s : %(message)s', level=logging.ERROR)


# select distinct company list from bhav
# find the data from Quandl, push it to a new table, 
# fill in the missing data

db_string = "postgresql://postgres:aa123456@localhost:5433/postgres"
engine = sqlalchemy.create_engine(db_string)
conn = engine.connect()


sql_bhav = 'SELECT DISTINCT symbol FROM BHAV'
df = pd.read_sql_query(sql_bhav, engine=conn)


# bhav = symbol, timestamp, open, high, low, close, trd qty
# quandl = cname, high, last, low, trade_qty, txndate, xopen, xclose

sel_quandl = 'SELECT * FROM quandl_daily where cname = {}'


def getQuandl(symbol):
    df = pdf.read_sql_query(sel_quandl.format(symbol), engine=conn)
    