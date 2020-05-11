import quandl 
from sqlalchemy.types import Integer,String,Numeric, Date
import sqlalchemy
import numpy
import logging
import sys, traceback
import pandas as pd

#Alphavantage = api key = ZXFIIFGXLYOK1EOF
#WORLD TRADING DATA API KEY = YSi7ZaBJC7a7JtR1x0TsbPuLLnA5k4jK06A4FgnfVtRR89pm227aNynmKbv1  
# quandl.get('AMFI/CODE')
quandl.ApiConfig.api_key = 'S6vtpUaj4-S25bUQ3MQz'

logging.basicConfig(filename="./quandl.log", format='%(levelname)s : %(asctime)s : %(message)s', level=logging.DEBUG)


df_meta = pd.read_csv('/home/dvlpr/Documents/Stocks/data/metadata/XNSE_metadata.csv')

df_meta.drop(df_meta[df_meta.code.str.contains('_UADJ')].index, inplace=True)
df_meta.drop(df_meta[df_meta.code.str.contains('Asset Manage')].index, inplace=True)
df_meta.drop(df_meta[df_meta.name.str.contains('Investment Manage')].index, inplace=True)
df_meta.drop(df_meta[df_meta.name.str.contains('Life AMC')].index, inplace=True)


#'open', 'high', 'low', 'last', 'close', 'total trade quantity', 'turnover (lacs)'


sel_col = [ 'Date', 'Open', 'High', 'Low', 'Last', 'Close', 'Total Trade Quantity', 'Turnover (Lacs)']

headings = ['txndate' , 'xopen', 'high', 'low', 'last', 'xclose', 'trade_qty', 
'turnover_lacs' , 'cname' ]

dtypes = {'txndate' : Date,  'xopen' : Numeric, 'high'  : Numeric, 'low'  : Numeric, 'last'  : Numeric, 
'xclose'  : Numeric, 'trade_qty'  : Numeric, 'turnover_lacs'  : Numeric , 'cname' : String}

db_string = "postgresql://postgres:aa123456@localhost:5433/postgres"
engine = sqlalchemy.create_engine(db_string)
conn = engine.connect()
start_dt = '2019-01-04'
for company in df_meta.code:
    try:
        coname = 'NSE/' + company
        df = quandl.get(coname) 
        df = df.reset_index() ### will come to this line only if the company exists
        df = df[df.columns.intersection(sel_col)]
        df['cname'] = company
        if (len(df.columns) < len(sel_col)):
                for col in sel_col:
                    df[col] = numpy.nan
        df.columns = headings
        tran = conn.begin()
        df.to_sql('quandl_daily', conn, if_exists = 'append',index = False,dtype = dtypes)
        tran.commit()        
        pass
    except:
        logging.error(' ERROR occured : Company : {} Error : {}'.format(company, traceback.format_exc()))
        pass
# df_meta.drop(df_meta[df_meta.code.str.contains('SRTTRANS')].index, inplace=True)
# df_meta.drop(df_meta[df_meta.name.str.contains('SBI Funds Management')].index, inplace=True)
# df_meta.drop(df_meta[df_meta.code.str.contains('Asset Manage')].index, inplace=True)
# df_meta.drop(df_meta[df_meta.name.str.contains('Sundaram Asset Management')].index, inplace=True)
# df_meta.drop(df_meta[df_meta.name.str.contains('Reliance Capital Asset')].index, inplace=True)
# df_meta.drop(df_meta[df_meta.name.str.contains('Reliance Nippon')].index, inplace=True)
# df_meta.drop(df_meta[df_meta.name.str.contains('Kotak Mahindra A')].index, inplace=True)





