from requests import get
import requests 
from decimal import * # for dataframe conversions
from requests.exceptions import RequestException
from contextlib import closing
from bs4 import BeautifulSoup as bs
from datetime import time
import time
from sqlalchemy.types import Integer, String, Numeric
import pandas as pd
from random import randint

import sqlalchemy 

Session = requests.session()
db_string = "postgresql://postgres:aa123456@localhost:5433/postgres"
#engine = create_engine('postgresql://scott:tiger@localhost:5432/mydatabase')
engine = sqlalchemy.create_engine(db_string)
conn = engine.connect()

scrn_ctr = 0
ins_scrn = 'INSERT into screens values({}, {!r}, {!r})'

headings = ['sno', 'cname', 'cmp', 'pe', 'mar_cap', 'div_yld', 'np_qtr', 'qtr_profit_var', 'sales_qtr','qtr_sales_var', 'roce', 'roe']
dtypes = {'sno' :  String(), 'cname' :  String, 'cmp' :  Numeric, 'pe' :  Numeric, 'mar_cap' :  Numeric, 'div_yld' :  Numeric, 'np_qtr' :  Numeric, 
'qtr_profit_var' :  Numeric, 'sales_qtr' :  Numeric,'qtr_sales_var' :  Numeric,    
'roce' :  Numeric, 'roe' :  Numeric, 'company_url' :  String, 'screen_id' :  Integer}

base = 'https://www.screener.in'

trans = conn.begin()
trunc = 'truncate table screens'
conn.execute(trunc)

trunc = 'truncate table company_list'
conn.execute(trunc)
trans.commit()

cofilename = './dwnld/{} pg {}.csv'

def getCompany(url, screen_id):
    print(url)
    #<button formaction="/user/company/export/6595613/" class="button-text no-margin">
    resp = requests.get(url)
    dfs = pd.read_html(resp.content) # returns a list of data frames
    # TODO: can read specific tables 
    # TODO: get the NSE equivalent and BSE equivalent code for the company - so it is easy to correlate with daily volumes
    for frame in dfs: # transpose all the frame
        frame_t = frame.transpose()
        frame_t[0] # sales if index = 0  

def getScreens(urlList):  # parameter is a list of URLs
    # for each URL in the list
    # find if the URL already exist in DB if so skip it
    # if not add it to the DB
    # problem with the approach is that if the screen has changed then it wont get reflected - so no need to 
    # optimize - just truncate the table and extract the data from scratch 

    sel = 'SELECT COALESCE(MAX(screen_id),0) + 1 AS screen_id FROM screens'
    rslt = conn.execute(sel)
    screen_id = rslt.first()[0]
    #soup.find('textarea', {'name' : 'query'}).contents  # get query 
    for url in urlList:
        # insert in screens - get the screen id 

        resp= requests.get(url) # read_html can also get the data from url - but avoiding multiple server hits
        soup = bs(resp.content, 'html.parser')
        qry = str(soup.find('textarea', {'name' : 'query'}).contents[0])  # get query 
        qry = qry.replace('\r', ' ').replace('\n', ' ')
        trans = conn.begin()
        print (url, ins_scrn.format(screen_id, url,qry))
        conn.execute(sqlalchemy.text(ins_scrn.format(screen_id, url,qry)))
        screen_id = screen_id +1
        print(screen_id)
        trans.commit()
        # insert in company list pass screen id
        # need to iterate across pages for this company list
        tot_pgs = getPgCount(soup)
        url = url + '?page={}'
        co_list_url = []
        for pg_ctr in range (0, tot_pgs):
            # need to append the company url to this dataframe below 
            # TODO: match with table name 
            # #industry-filter-results > div.responsive-holder > table
            # xpath = //*[@id="industry-filter-results"]/div[2]/table
            dfs = pd.read_html(resp.content) ## list of dataframe objects 
            df=dfs[0]
            df.drop(df[df["S.No."] == 'S.No.'].index, inplace=True)
            df['screen_id'] = screen_id
            df.to_csv (cofilename.format(screen_id, pg_ctr))
            # trans = conn.begin()
            # df.to_sql('company_list', conn, if_exists = 'append', index = False, dtype = dtypes)
            # trans.commit()
            select = soup.find_all('a')
            for company in select:
                costr = str(company)
                if (costr.find('company')> 0):
                    company_url = base + company.attrs['href']
                    #co_list_url.append(company_url,screen_id)
                    # getCompany(company_url)
            wait_time = randint(3, 5)
            time.sleep(wait_time)
            resp = requests.get(url.format(pg_ctr+1))
            soup = bs(resp.content,  'html.parser')
            


def getPgCount(soup):
    select = soup.find('div', {'class:', 'options'})
    content = str(select)
    start_pos = content.find('of') + len('of') + 1
    end_pos = content.find('.')
    tot_pgs = int(content[start_pos:end_pos])
    return tot_pgs


def getPages(url):
    resp = requests.get(url)
    soup = bs(resp.content, 'html.parser')
    # get total number of pages
    tot_pgs = getPgCount(soup)
    pg_ctr = 1
    i = 0
    screens = []
    url = 'https://www.screener.in/screens/?page={}&recent='
    for pg_ctr in range(0, 1000 ): ## TODO: change 10 to tot_pgs
        select = [  base + screen.find('a').attrs['href'] for screen in soup.find_all('li')]
        getScreens(select)
        resp = requests.get(url.format(pg_ctr+1))
        soup = bs(resp.content, 'html.parser')

getPages('https://www.screener.in/screens/?recent') # New Screens
getPages('https://www.screener.in/screens/?') # Popular Screens

#TODO : fetch details for a set of identified stocks - 
# stocks might be from smallcase, or from high value retail investors or a portfolio of stocks
# the input is a list of stocks from a csv file


"""
scrappy commands
escrapy startproject screens
scrapy genspider -t crawl getScreens httsp://www.screener.in
i dont need to do the grunt work of maitaining the session et-al it will start crawling


The data flow in Scrapy is controlled by the execution engine, and goes like this:

The Engine gets the initial Requests to crawl from the Spider.
The Engine schedules the Requests in the Scheduler and asks for the next Requests to crawl.
The Scheduler returns the next Requests to the Engine.
The Engine sends the Requests to the Downloader, passing through the Downloader Middlewares (see process_request()).
Once the page finishes downloading the Downloader generates a Response (with that page) and sends it to the Engine, passing through the Downloader Middlewares (see process_response()).
The Engine receives the Response from the Downloader and sends it to the Spider for processing, passing through the Spider Middleware (see process_spider_input()).
The Spider processes the Response and returns scraped items and new Requests (to follow) to the Engine, passing through the Spider Middleware (see process_spider_output()).
The Engine sends processed items to Item Pipelines, then send processed Requests to the Scheduler and asks for possible next Requests to crawl.
The process repeats (from step 1) until there are no more requests from the Scheduler.

so the flow will be  
- https://www.screener.in/screens 
- build a list of all screens to go through
    key attrs - screen name, content, formula
    results - can paginate - so will have to go through each one separately 
then you login to each of the page and get the details separately



"""