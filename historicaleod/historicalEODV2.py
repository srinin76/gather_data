
import pandas as pd
import urllib.request
import pytz
import pandas as pd
from bs4 import BeautifulSoup
from datetime import datetime
import sqlite3
import time
from random import randint
import csv
from datetime import date, timedelta
import requests

def bhavcopy(start_date,     end_date):

    delta = end_date - start_date         # timedelta
    print(delta)

    req_cols = ['SYMBOL','TIMESTAMP','OPEN','HIGH','LOW','CLOSE','TOTTRDQTY', 'TOTALTRADES', 'PREVCLOSE']
    for i in range(delta.days + 1):
        cur_day= (start_date+ timedelta(days=i))
        WkdaY = cur_day.weekday()
    #     print WkdaY
        if(WkdaY !=5 and WkdaY !=6):
            YeaR= str(cur_day.year)
            MontH = cur_day.strftime("%b").upper() # cur_day.month
            DaY = cur_day.strftime('%d')
            #https://www1.nseindia.com/content/historical/EQUITIES/2010/FEB/cm08FEB2010bhav.csv.zip
            #https://www1.nseindia.com/content/historical/EQUITIES/2020/FEB/cm07FEB2020bhav.csv.zip
            #http://www1.nseindia.com/content/historical/EQUITIES/2010/FEB/cm08FEB2010bhav.csv.zip
            url = "https://www1.nseindia.com/content/historical/EQUITIES/"+YeaR+"/"+MontH+"/cm"+DaY+MontH+YeaR+"bhav.csv.zip"
            headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36'}
            response = requests.get(url, headers=headers)
            filename = "./download/" + YeaR+"_"+MontH+"_"+DaY+".zip"
            with open(filename, "wb") as code:
                code.write(response.content)
            wait_time = randint(5, 10)
            try:
                A= pd.read_csv(filename, compression='zip', sep=',', quotechar='"')
                filename = "NSE"+YeaR+"_"+MontH+"_"+DaY+".csv"
                A[req_cols].to_csv(filename, index=False)
                print("Waiting .. "+str(wait_time), " .. seconds")
                time.sleep(wait_time)
            except:
                print("Could not get data for this day .. "+str(cur_day))
                pass
            

def get_bhavopy():
    d1 = date(2019, 1, 26)  # start date - can download from 2016
    d2 = date(2020, 5, 8)  # end date
    bhavcopy(d1,d2)


def main():

    get_bhavopy()
    


if __name__ == "__main__":
    main() 
