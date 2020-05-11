from requests import get
import requests 

from requests.exceptions import RequestException
from contextlib import closing
from bs4 import BeautifulSoup

Session = requests.session()

class screen():
    token = ""
#    name = ""
#    query = ""
#    results = []
#
#    def setScreen(asName, asQuery, asRslts):
#        name = asName
#        query = asQuery
#        results = asRslts

def simple_get(url):
    """
    Attempts to get the content at `url` by making an HTTP GET request.
    If the content-type of response is some kind of HTML/XML, return the
    text content, otherwise return None.
    """
    try:
        with closing(get(url, stream=True)) as resp:
            if is_good_response(resp):
                return resp.content
            else:
                return None

    except RequestException as e:
        log_error('Error during requests to {0} : {1}'.format(url, str(e)))
        return None


def is_good_response(resp):
    """
    Returns True if the response seems to be HTML, False otherwise.
    """
    content_type = resp.headers['Content-Type'].lower()
    return (resp.status_code == 200 
            and content_type is not None 
            and content_type.find('html') > -1)

def log_error(e):
    """
    It is always a good idea to log errors. 
    This function just prints them, but you can
    make it do anything.
    """
    print(e)

# no point of logging in - it is going to default page without the login
def login():
resp = simple_get(base)
soup =bs(resp, 'html.parser')
select = soup.find_all('a', {'class:','nav-item'})
for i  in select:
    if (i.attrs['href'].find('login') > 0):
        login = base + i.attrs['href']
    if(i.attrs['href'].find('screen')>0):
        screens= base + i.attrs['href']


    # simple_get will not work with base1 - as we need to post with session going forward
session = requests.session()
resp = session.get(login)

## ### TODO: response Set-Cookie - contains csrf token
## ### it is also available in session.cookies

soup = bs(resp.text,'html.parser')
select = soup.find('input', {'name': 'csrfmiddlewaretoken'})
params = { "id_username": 'srinivasan76@gmail.com' , "id_password": 'aa123456', 'csrfmiddlewaretoken' : session.cookies['csrftoken']}
resp = session.post(login, data = params,headers={'referer': base})

def getCompanyProfile(asURL):
    resp = requests.get(asURL)
    soup = BeautifulSoup(resp.text, 'html.parser')
    select = soup.find_all('a', {'class:','nav-item'})
    for i  in select:
        if (i.attrs['href'].find('login') > 0):
            login = base + i.attrs['href']
        if(i.attrs['href'].find('screen')>0):
            screens= base + i.attrs['href']
    session = requests.session()
    resp = session.get(login)
    soup = BeautifulSoup(resp.text,'html.parser')
    select = soup.find('input', {'name': 'csrfmiddlewaretoken'})
    params = { "id_username": 'srinivasan76@gmail.com' , "id_password": 'aa123456', select.attrs['name']: select.attrs['value']}
    resp = session.post(screens, data = params,headers={'referer': base})






base = 'https://www.screener.in'
login()
getCompanyProfile('')

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