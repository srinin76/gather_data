import xlrd
import os
from datetime import date

class Ticker:
    name = ""
    face_value = 0.00
    market_cap = 0.00
    number_shares = 0
    price = 0.00
    PnLs = []
    class PnL:
        reportingon = ""
        reportingdt = date(1900,1,1) # use datetime.datetime.strptime(reportingon, '%b-%y') to convert to Date
        sales = 0.00 
        expenses = 0.00
        #todo: create a tuple to read from data sheet and populate in this       
        operating_profit = 0.00
        other_income = 0.00
        depreciation = 0.00
        interest = 0.00
        pbt = 0.00
        tax = 0.00
        eps = 0.00
        pe = 0.00
        price = 0.00
        dividendpayout = 0.00
        opm = 0.00
        trsales10yr=0.00
        trsales5yr= 0.00
        trsales3yr = 0.00
        trsalesrecent=0.00
        trsalesbest=0.00
        trsalesworst = 0.00
        tropm10yr = 0.00
        tropm5yr = 0.00
        tropm3yr = 0.00
        tropmrecent = 0.00
        tropmbest = 0.00
        tropmworst=0.00
        trpe10yr = 0.00
        trpe5yr = 0.00
        trpe3yr = 0.00
        trperecent = 0.00
        trpebest = 0.00
        trpeworst = 0.00
        


def readQs(asfname):
    print("1")

def readCashflows(asfname):

def readDataSheet(asfname):
    # To read current price, market cap, number of shares() face value
    # use online yahoo apis to find other parameters like PE, Debt to Equity, EPS, Graham number
    # read pnl from data sheet to get breakup of the expenses use a dictionary

path = '/home/dvlpr/Documents/Stocks/screener data/'

fname = '/home/dvlpr/Documents/Stocks/screener data/Vindhya Telelink.xlsx'
readPnL(fname)
lobjticker= Ticker()

sheet =wb.sheet_by_name('Data Sheet')
lobjticker.name= sheet.cell_value(0,1) # name
lobjticker.number_shares = sheet.cell_value(5,1)
lobjticker.face_value=sheet.cell_value(6,1) 
lobjticker.price = sheet.cell_value(7,1)
lobjticker.market_cap = sheet.cell_value(8,1)

# reading pnl 
lstpnl = sheet.row_values(15)
lstnpl.pop(0) # pop the first text column

df = pd.DataFrame(sheet.row_values(row) for row in range(15,31)) # read PnL
#dt = datetime.fromordinal(datetime(1900, 1, 1).toordinal() + (int)(excel_dt) - 2)
df.loc[0,1:] = [datetime.fromordinal(datetime(1900, 1, 1).toordinal() + (int)(excel_dt) - 2) for excel_dt in df.loc[0, 1:]]
df.columns = df.iloc[0]
df= df.drop(df.index[0])
df_t = df.transpose()

# 
# for lfile in os.listdir(path):
#     fsname = os.path.join(path, lfile)
#     readPnL(fsname)