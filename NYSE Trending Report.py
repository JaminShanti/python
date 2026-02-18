#!/usr/bin/env python


import yfinance as yf
import re
import pandas as pd
from tqdm import tqdm
from datetime import date, datetime, timedelta, time
from contextlib import redirect_stdout
import os
import imgkit
from IPython.display import Image
import io
from pandas.tseries.offsets import BDay

trap = io.StringIO()


# Always use the last 2 business days
today = date.today()
two_business_days_ago = (today - BDay(2)).strftime("%Y-%m-%d")
one_business_day_ago = (today - BDay(1)).strftime("%Y-%m-%d")

# Use the last business day as the end date
market_last_close_add_a_day = today.strftime("%Y-%m-%d")
compare_market_close = two_business_days_ago

# In[3]:


# Load S&P 500 symbols dynamically with cache fallback and pre-validation
CACHE_PATH = 'sp500_symbols.csv'
WIKI_URL = 'https://en.wikipedia.org/wiki/List_of_S%26P_500_companies'

def get_sp500_symbols(cache_path=CACHE_PATH):
    symbols = None
    try:
        import requests
        headers = {'User-Agent': 'Mozilla/5.0'}
        resp = requests.get(WIKI_URL, headers=headers, timeout=20)
        resp.raise_for_status()
        tables = pd.read_html(io.StringIO(resp.text), match='Symbol')
        df = tables[0]
        col = 'Symbol' if 'Symbol' in df.columns else df.columns[0]
        symbols = df[col].astype(str).tolist()
        symbols = [re.sub(r'\.', '-', s) for s in symbols]
        # cache to disk for resilience
        pd.DataFrame({'symbol': symbols}).to_csv(cache_path, index=False)
    except Exception:
        if os.path.exists(cache_path):
            try:
                df = pd.read_csv(cache_path)
                col = 'symbol' if 'symbol' in df.columns else df.columns[0]
                symbols = df[col].astype(str).tolist()
            except Exception:
                pass
        if symbols is None:
            raise
    return symbols


def prefilter_symbols(symbols):
    valid_symbols = []
    for symbol in symbols:
        try:
            # Validate if the ticker exists using yfinance
            data = yf.Ticker(symbol).info
            if data and "symbol" in data:
                valid_symbols.append(symbol)
        except Exception:
            continue
    return valid_symbols


# Toggle validation on/off
VALIDATE_TICKERS = False  # Disable validation to avoid the NameError

symbols = get_sp500_symbols()
if VALIDATE_TICKERS:
    symbols = prefilter_symbols(symbols)


# In[4]:


market_list = []
error_list = {}
for symbol in tqdm(symbols,position=0, leave=True):
    company_trend = None
    data_row = {}
    with redirect_stdout(trap):  
        try:
            print(f"Downloading Data for {symbol}")
            try:
                company_trend = yf.download(symbol, start=compare_market_close, end=market_last_close_add_a_day,
                                            threads=False, progress=False)
            except Exception as e:
                error_list[symbol] = e
            print(company_trend)
            # Validate data and compute prices using non-deprecated indexing
            if company_trend is None or company_trend.empty:
                error_list[symbol] = "no data"
                continue
            if not isinstance(company_trend.index, pd.DatetimeIndex):
                company_trend.index = pd.to_datetime(company_trend.index, errors='coerce')
                company_trend = company_trend[company_trend.index.notna()]
            start_dt = pd.to_datetime(compare_market_close)
            end_dt = pd.to_datetime(market_last_close_add_a_day)
            window = company_trend.loc[(company_trend.index >= start_dt) & (company_trend.index < end_dt)]
            if window.empty or 'Close' not in window:
                error_list[symbol] = "no window/close"
                continue
            compare_price = window['Close'].iloc[0]
            close_price = window['Close'].iloc[-1]
            stock_change_percentage = ( close_price / compare_price ) * 100 - 100
            data_row['symbol'] = symbol
            data_row['stock_change_percentage'] = stock_change_percentage
            data_row['close_price'] = close_price
            market_list.append(data_row)
            print(data_row,compare_price)
        except Exception as e: 
            error_list[symbol] = e
market_list = pd.DataFrame(market_list)
if not market_list.empty:
    market_list.set_index('symbol', inplace=True)
else:
    print("No valid data collected. Check error_list for details:")
    for symbol, error in error_list.items():
        print(f"{symbol}: {error}")
    exit(1)  # Stop execution if no data

# In[5]:


market_list

# In[6]


market_list.info()


# In[7]


for key, value in error_list.items():
    print(key,value)


# In[8]


top_25 = market_list.nlargest(25, ['stock_change_percentage']) 
#top_50 = {key: market_list[key] for key in sorted(market_list, key=market_list.get, reverse=True)[:25]}


# In[9]


top_25.head()


# In[10]


top_25.count()


# In[11]


top_info_list = []
for symbol, row in tqdm(top_25.iterrows(),position=0, leave=True):
    company = None
    with redirect_stdout(trap):  
        try:
            company = yf.Ticker(symbol).info
            top_info_list.append(company)
        except Exception as e:
            error_list[symbol] = e


# In[12]


market_panda = pd.DataFrame(top_info_list)
market_panda.set_index('symbol',inplace=True)
market_panda = pd.concat([market_panda, top_25], axis=1)


# In[13]


pd.set_option('display.max_columns', 999)
market_panda.iloc[[0]]


# In[14]


if 'MCC' in market_panda.index:
    market_panda = market_panda.drop('MCC')
if 'FMO' in market_panda.index:
    market_panda = market_panda.drop('FMO')
if 'SSI' in market_panda.index:
    market_panda = market_panda.drop('SSI')


# In[15]


pd.options.display.float_format = "{:,.2f}".format
market_panda[['longName','stock_change_percentage','close_price','fiftyTwoWeekHigh','sector']]


# In[16]


html_string = '''
<html>
  <head><title>HTML Pandas Dataframe with CSS</title></head>
  <link rel="stylesheet" type="text/css" href="df_style.css"/>
  <body>
    {table}
  </body>
</html>.
'''
# OUTPUT AN HTML FILE
with open('table_report.html', 'w') as f:
    f.write(html_string.format(table=market_panda[['longName','stock_change_percentage','close_price','fiftyTwoWeekHigh','sector']].to_html(classes='mystyle')))


# In[17]


path_wkthmltoimage = r'C:\Program Files\wkhtmltopdf\bin\wkhtmltoimage.exe'
try:
    config = imgkit.config(wkhtmltoimage=path_wkthmltoimage)
    imgkit.from_file('table_report.html', 'table_report.jpg', config=config, options={'enable-local-file-access': ''})
except OSError as e:
    print(f"Skipping image export: wkhtmltoimage not found at {path_wkthmltoimage}. {e}")


# In[18]


Image(filename='table_report.jpg')
