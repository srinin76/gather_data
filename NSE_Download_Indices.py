import requests
from bs4 import BeautifulSoup as bs
import pandas as pd


URL = 'https://www1.nseindia.com/products/content/equities/indices/about_indices.htm'

resp = requests.get(URL)
soup = bs(resp.content, 'html.parser')