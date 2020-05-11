# get Mutual funds data
from requests import session
from bs4 import BeautifulSoup as bs
import pandas as pd
from sqlalchemy.types import Integer,String,Numeric
import pandas as pd
from random import randint
from datetime import time
import sqlalchemy 
import requests
import time
import logging
import sys, traceback
logging.basicConfig(filename="./scraping.log", format='%(levelname)s : %(asctime)s : %(message)s', level=logging.ERROR)

# sessionid

# TODO: do one initial download of these parameters - after a week - change the parameters to remaining
# and get the rest of the data

base = 'https://www.valueresearchonline.com/funds'

# TODO: go to the base URL scheme wise fund details are there - navigate to each URL
# list of all the funds are there
# navigate to each fund
# go to the portfolio table 
# get the stockes it has been invested in 


session = requests.session()
resp = session.get(login)

"""
data: {"nt":322,"dp":692,"pr":2070,"ts":1583083793798,
"apikey":"5f0402658a414ad3829a39452a931d18","
request":{"url":"https://www.valueresearchonline.com/funds",
"ua":"Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) snap Chromium/80.0.3987.122 Chrome/80.0.3987.122 Safari/537.36",
"w":1366,"h":768},"tags":[],"user":null,
"aid":"3529dda972f7440a95b59af0fb87275f","sid":"ce20e0215d9b486e935a8d49c312dd28","v":"","_v":"3.0.9"}

"""
params = { "id_username": 'srinivasan76@gmail.com' ,"id_password": 'aa123456','csrfmiddlewaretoken' : session.cookies['csrftoken']}
resp = session.post(login,data=params,headers=dict(Referer=login))
# key is to set the session id from the browser to have the direct authentication
jar = requests.cookies.RequestsCookieJar()
# https://www.youtube.com/watch?time_continue=448&v=PpaCpudEh2o&feature=emb_logo
jar.set =  jar.set ('sessionid','gxakx7e9ge2kqwnn0lwl57u6m6ynymvs')
session.cookies = jar
session.headers.update ({'User-Agent' : 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) snap Chromium/80.0.3987.87 Chrome/80.0.3987.87 Safari/537.36'})

scrn_ctr = 0
ins_scrn = 'INSERT into screens values({},{!r},{!r})'
cofilename = './downlaods/{} pg {}.csv'


# TODO: do one initial download of these parameters - after a week - change the parameters to remaining
# and get the rest of the data

sel_col = ['S.No.', 'Name', 'CMP  Rs.', 'Sales  Rs.Cr.', 'Sales Ann  Rs.Cr.', 'Sales Prev Ann  Rs.Cr.',
'Sales Var 3Yrs  %', 'Sales Var 5Yrs  %', 'OPM  %', 'OPM Qtr  %', 'OPM Ann  %', 'OPM Prev Ann  %',
'PAT 12M  Rs.Cr.', 'EPS 12M  Rs.', 'EPS Prev Ann  Rs.', 'PAT Ann  Rs.Cr.', 'NP 12M  Rs.Cr.', 
'NP Prev Ann  Rs.Cr.', 'NPM Prev Ann  %', 'NPM Ann  %', 'Profit Var 5Yrs  %',
'Gross Block  Rs.Cr.', 'Net Block  Rs.Cr.', 'CWIP  Rs.Cr.', 'ROCE  %', 'Investments  Rs.Cr.', 
'Cur Assets  Rs.Cr.', 'Cur Liab  Rs.Cr.', 'Cont Liab  Rs.Cr.', 'Profit Var 3Yrs  %', 
'Chg in Prom Hold 3Yr  %', 'ROA 12M  %', 'CMP / BV', 'Debt / Eq', 'ROE  %', 'Earnings Yield  %',
'Pledged  %', 'Ind PE', 'G Factor', 'Quick Rat', 'ROIC  %', 'Ind PBV', '52w High  Rs.', 
'ROA 3Yr  %', 'ROCE 3Yr  %', 'ROCE 5Yr  %', 'Inven TO 3Yr', 'ROE 3Yr  %', 'ROE 5Yr  %', 
'Annual Free Cash Flow 3Yrs  Rs.Cr.', 'Free Cash Flow 5Yrs  Rs.Cr.', 'Mar Cap  Rs.Cr.']

headings = ['sno' , 'name' , 'cmp' , 'sales' , 'sales_ann' , 'sales_prev_ann' , 'sales_var_3yrs_perc' , 
'sales_var_5yrs_perc' , 'opm_perc' , 'opm_qtr_perc' , 'opm_ann_perc' , 'opm_prev_ann_perc' , 'pat_12m' , 
'eps_12m' , 'eps_prev_ann' , 'pat_ann' , 'np_12m' , 'np_prev_ann' , 'npm_prev_ann_perc' , 'npm_ann_perc' , 
'profit_var_5yrs_perc' , 'gross_block' , 'net_block' , 'cwip' , 'roce_perc' , 'investments' , 'cur_assets' , 
'cur_liab' , 'contingent_liab' , 'profit_var_3yrs_perc' , 'chg_in_prom_hold_3yr_perc' , 'roa_12m_perc' , 
'cmp_bv' , 'debt_eq' , 'roe_perc' , 'earnings_yield_perc' , 'pledged_perc' , 'ind_pe' , 'g_factor' , 'quick_rat' , 
'roic_perc' , 'ind_pbv' , 'wk52_high' , 'roa_3yr_perc' , 'roce_3yr_perc' , 'roce_5yr_perc' , 'inventory_turnover_3yr' , 
'roe_3yr_perc' , 'roe_5yr_perc' , 'annual_free_cash_flow_3yrs' , 'free_cash_flow_5yrs' , 'bv']


dtypes = {'sno' : String , 'name' : String , 'cmp' : Numeric , 'sales' : Numeric , 'sales_ann' : Numeric , 
'sales_prev_ann' : Numeric , 'sales_var_3yrs_perc' : Numeric , 'sales_var_5yrs_perc' : Numeric , 
'opm_perc' : Numeric , 'opm_qtr_perc' : Numeric , 'opm_ann_perc' : Numeric , 'opm_prev_ann_perc' : Numeric , 
'pat_12m' : Numeric , 'eps_12m' : Numeric , 'eps_prev_ann' : Numeric , 'pat_ann' : Numeric , 'np_12m' : Numeric , 
'np_prev_ann' : Numeric , 'npm_prev_ann_perc' : Numeric , 'npm_ann_perc' : Numeric , 
'profit_var_5yrs_perc' : Numeric , 'gross_block' : Numeric , 'net_block' : Numeric , 'cwip' : Numeric , 
'roce_perc' : Numeric , 'investments' : Numeric , 'cur_assets' : Numeric, 'cur_liab' : Numeric , 
'contingent_liab' : Numeric , 'profit_var_3yrs_perc' : Numeric , 'chg_in_prom_hold_3yr_perc' : Numeric , 
'roa_12m_perc' : Numeric , 'cmp_bv' : Numeric , 'debt_eq' : Numeric , 'roe_perc' : Numeric , 
'earnings_yield_perc' : Numeric , 'pledged_perc' : Numeric , 'ind_pe' : Numeric , 'g_factor' : Numeric , 
'quick_rat' : Numeric , 'roic_perc' : Numeric , 'ind_pbv' : Numeric , 'wk52_high' : Numeric , 
'roa_3yr_perc' : Numeric , 'roce_3yr_perc' : Numeric , 'roce_5yr_perc' : Numeric , 
'inventory_turnover_3yr' : Numeric , 'roe_3yr_perc' : Numeric , 'roe_5yr_perc' : Numeric , 
'annual_free_cash_flow_3yrs': Numeric , 'free_cash_flow_5yrs' : Numeric , 'bv': Numeric}

logging.debug('%s', str(sel_col))
db_string = "postgresql://postgres:aa123456@localhost:5433/postgres"
engine = sqlalchemy.create_engine(db_string)
conn = engine.connect()

# get list of screens already cached and to check and then add
sel_url = 'SELECT url FROM screens'

rslt = conn.execute(sel_url)

url_exists = [url[0] for url in rslt]
