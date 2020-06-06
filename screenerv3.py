from requests import session
from bs4 import BeautifulSoup as bs
import pandas as pd
from sqlalchemy.types import Integer,String,Numeric
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

base = 'https://screener.in'
login = 'https://screener.in/login'
session = requests.session()
resp = session.get(login)
params = { "id_username": 'srinivasan76@gmail.com' ,"id_password": 'xxxx','csrfmiddlewaretoken' : session.cookies['csrftoken']}
resp = session.post(login,data=params,headers=dict(Referer=login))
# key is to set the session id from the browser to have the direct authentication
jar = requests.cookies.RequestsCookieJar()
# https://www.youtube.com/watch?time_continue=448&v=PpaCpudEh2o&feature=emb_logo
jar.set =  jar.set ('sessionid','extract new')
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
sel_unq = 'SELECT url, query FROM screens'

rslt = conn.execute(sel_unq)

url_exists = [url[0] for url in rslt]
rslt = conn.execute(sel_unq)
query_exists = [qry[1] for qry in rslt]


# trans = conn.begin()
# trunc = 'truncate table screens'
# conn.execute(trunc)

# trunc = 'truncate table company_list'
# conn.execute(trunc)
# trans.commit()

# TODO: raise exception if resp.status <> 200 
#>>> verbs = requests.options(r.url)
#>>> verbs.status_code
# resp.status_code == requests.codes.ok 

def getContent(url):
    resp = session.get(url)
    soup = bs(resp.content,'html.parser')
    return soup,resp

def getPgCount(soup):
    select = soup.find('div',{'class:','options'})
    content = str(select)
    start_pos = content.find('of') + len('of') + 1
    end_pos = content.find('.')
    tot_pgs = int(content[start_pos:end_pos])
    return tot_pgs

def getLastScreen_id():
    sel = 'SELECT COALESCE(MAX(screen_id),0) + 1 AS screen_id FROM screens'
    rslt = conn.execute(sel)
    screen_id = rslt.first()[0]
    return screen_id

# TODO: try catch and rollback
def writeScreens(screen_id,url,qry):
    trans = conn.begin()
    try:
        conn.execute(sqlalchemy.text(ins_scrn.format(screen_id,url,qry)))
        trans.commit()
    except:
        trans.rollback()
        raise

#TODO: extend log to write to file - catch exceptions, set warning levels
def log(logstr):
    logging.error(logstr)



def getScreens(urlList):  # parameter is a list of URLs
    # for each URL in the list
    # find if the URL already exist in DB if so skip it
    # if not add it to the DB
    # problem with the approach is that if the screen has changed then it wont get reflected - so no need to 
    # optimize - just truncate the table and extract the data from scratch 
    screen_id =getLastScreen_id()
    for url in urlList:
        # insert in screens - get the screen id 
        try:
            soup,resp = getContent(url)
            qry = str(soup.find('textarea',{'name' : 'query'}).contents[0])  # get query 
            qry = qry.replace('\r',' ').replace('\n',' ')
            if (qry not in query_exists):
                try: 
                    writeScreens(screen_id,url,qry)
                    url_exists.append(url) ## append to url_exists so as to avoid failures in future
                    query_exists.append(qry) ## append to qry list to avoid failures in future
                    screen_id = screen_id +1
                    tot_pgs = min(getPgCount(soup), 5)
                    url = url + '?page={}'
                    co_list_url = []
                    log('Getting pages for Screen {} Total pages: {}.'.format(screen_id, tot_pgs))
                    for pg_ctr in range (0,tot_pgs): ### if there are more than 5 pages - the screener is not useful
                        # need to append the company url to this dataframe below 
                        # TODO: match with table name 
                        # #industry-filter-results > div.responsive-holder > table
                        # xpath = //*[@id="industry-filter-results"]/div[2]/table
                        dfs = pd.read_html(resp.content) ## list of dataframe objects 
                        df = dfs[0]
                        df = df[df.columns.intersection(sel_col)] # get matching columns
                        if (len(df.columns) < len(headings)): 
                            # add missing columns with default values
                            for col in sel_col:
                                if col not in df.columns:
                                    df[col] = 0 # should have been numpy.nan - but i know it is ok to have 0
                        df.columns = headings
                        df.drop(df[df["sno"] == 'S.No.'].index,inplace=True)
                        select = soup.find_all('a')
                        co_list_url = [company.attrs['href'] for company in select if str(company).find('company') > 0]
                        # getCompany(company_url)
                        df['company_url'] = co_list_url
                        df['screen_id'] = screen_id
                        trans = conn.begin()
                        df.to_sql('company_list',conn,if_exists = 'append',index = False,dtype = dtypes)
                        trans.commit()
                        soup,resp = getContent(url.format(pg_ctr+1))
                        log('Page {} of Screen {} done.'.format(pg_ctr+1, screen_id))
                        wait_time = randint(5,10)
                        time.sleep(wait_time)
                except:
                    log('------------================================================-----------')
                    log (traceback.format_exc())
                    log (url)
                    log('------------================================================-----------')
                pass
        except:
            log('------------================================================-----------')
            log (traceback.format_exc())
            log (url)
            log('------------================================================-----------')
            pass

def getPages(url, pg_ctr):
    soup ,resp = getContent(url.format(pg_ctr))
    tot_pgs = getPgCount(soup)
    # if recent then add recent towards the end else get popular screens
    for pg_ctr in range(pg_ctr, tot_pgs ): ## TODO: change 10 to tot_pgs
        select = [  base + screen.find('a').attrs['href'] for screen in soup.find_all('li')]
        # remove screens that are already in DB
        select = list(set(select) - set(url_exists))
        log('{} - {}'.format(pg_ctr, len(select)))
        if (len(select) > 0 ):
            log('Getting Screens for page {} total pages {} select count {}'.format(pg_ctr,tot_pgs, len(select)))
            getScreens(select)
            log('Completed Screens for page {} total pages {} select count {}'.format(pg_ctr,tot_pgs, len(select)))
        soup, resp = getContent(url.format(pg_ctr+1))

url = 'https://www.screener.in/screens/?page={}&recent'
getPages(url, 172)

log ('-------- getting popular screens ------------')

url = 'https://www.screener.in/screens/?page={}'
getPages(url, 1)

# url = 'https://www.screener.in/screens/?page={}'
# getPages(url)



#getPages ('https://www.screener.in/screens/?recent')
