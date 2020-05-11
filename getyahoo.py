import yfinance as yf
import pandas as pd
import logging
import sys, traceback
import sqlalchemy 

logging.basicConfig(filename="./getyahoo.log", format='%(levelname)s : %(asctime)s : %(message)s', level=logging.ERROR)
db_string = "postgresql://postgres:aa123456@localhost:5433/postgres"
engine = sqlalchemy.create_engine(db_string)
conn = engine.connect()

def log(logstr):
    logging.error(logstr)

def get_history(stock):
    try:
        ticker = yf.Ticker(stock)
        hist = ticker.history(period='max', auto_adjust=False)
        hist['stock'] = stock
        trans = conn.begin()
        hist.to_sql('yahoo_quotes',conn, if_exists='append')
        trans.commit()
    except:
        log('---- Failed for symbol; -----' +stock ) 
        log (traceback.format_exc())
        log('--------------------------------------')


meta_data = '/home/dvlpr/Documents/Stocks/nse-2-yahoo-tickers.csv'
df = pd.read_csv(meta_data)
df.to_sql('nse2yahoo',conn,if_exists='replace', index = False)
df['calculated'].apply(get_history)
