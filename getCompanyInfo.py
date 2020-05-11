# getCompanyInfo.py
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
logging.basicConfig(filename="./companyinfo.log", format='%(levelname)s : %(asctime)s : %(message)s', level=logging.ERROR)

# sessionid

# TODO: do one initial download of these parameters - after a week - change the parameters to remaining
# and get the rest of the data

base = 'https://screener.in'
login = 'https://screener.in/login'
session = requests.session()
resp = session.get(login)
params = { "id_username": 'srinivasan76@gmail.com' ,"id_password": 'aa123456','csrfmiddlewaretoken' : session.cookies['csrftoken']}
resp = session.post(login,data=params,headers=dict(Referer=login))
# key is to set the session id from the browser to have the direct authentication
jar = requests.cookies.RequestsCookieJar()
# https://www.youtube.com/watch?time_continue=448&v=PpaCpudEh2o&feature=emb_logo
jar.set =  jar.set ('sessionid','gxakx7e9ge2kqwnn0lwl57u6m6ynymvs')
session.cookies = jar
session.headers.update ({'User-Agent' : 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) snap Chromium/80.0.3987.87 Chrome/80.0.3987.87 Safari/537.36'})



