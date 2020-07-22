#!/usr/bin/env python
# coding: utf-8

# In[1]:


import yfinance as yf
import re
import pandas as pd
from heapq import nlargest
import numpy as np
from tqdm import tqdm
from datetime import date, datetime, timedelta, time
from pandas_datareader import data as pdr
from pandas.tseries.offsets import BDay
from contextlib import redirect_stdout
import imgkit
from IPython.display import Image
import io
yf.pdr_override() # <== that's all it takes :-)
trap = io.StringIO()


# In[2]:


#market closes at 4pm
two_business_day_ago = (date.today() - BDay(2)).strftime("%Y-%m-%d")
one_business_day_ago =  (date.today() - BDay(1)).strftime("%Y-%m-%d")
today = date.today().strftime("%Y-%m-%d")
tomorrow = (date.today() + timedelta(days=1)).strftime("%Y-%m-%d")
if datetime.now().time() > time(16,00):
    market_last_close_add_a_day = tomorrow
    compare_market_close = one_business_day_ago
elif datetime.today().weekday() < 5 or datetime.today().weekday() == 0:
    market_last_close_add_a_day = today
    compare_market_close = two_business_day_ago


# In[3]:


snp500 = pd.read_csv('C:\\Users\\Radjammin\\IdeaProjects\\stock_market\\us_company_names.csv')  
#snp500=pd.read_html('https://en.wikipedia.org/wiki/List_of_S%26P_500_companies')[0]
symbols = list(snp500.symbol) 
# replacing dots with dashes for Yahoo Finanace
symbols = [re.sub("\.","-",x) for x in symbols]


# In[ ]:


market_list = []
error_list = {}
for symbol in tqdm(symbols,position=0, leave=True):
    company_trend = None
    data_row = {}
    with redirect_stdout(trap):  
        try:
            print(f"Downloading Data for {symbol}")
            while company_trend is None:
                try:
                    company_trend = pdr.get_data_yahoo(symbol, start=compare_market_close, end=market_last_close_add_a_day, threads= False)
                except Exception as e: 
                    error_list[symbol] = e 
            print(company_trend)
            compare_price = company_trend.first('1D')[['Close']].values[0][0]
            close_price = company_trend.last('1D')[['Close']].values[-1][-1]
            stock_change_percentage = ( close_price / compare_price ) * 100 - 100
            data_row['symbol'] = symbol
            data_row['stock_change_percentage'] = stock_change_percentage
            data_row['close_price'] = close_price
            market_list.append(data_row)
            print(data_row,compare_price)
        except Exception as e: 
            error_list[symbol] = e
market_list = pd.DataFrame(market_list)
market_list.set_index('symbol',inplace=True)


# In[ ]:


market_list


# In[ ]:


market_list.info()


# In[ ]:


for key, value in error_list.items():
    print(key,value)


# In[ ]:


top_25 = market_list.nlargest(25, ['stock_change_percentage']) 
#top_50 = {key: market_list[key] for key in sorted(market_list, key=market_list.get, reverse=True)[:25]}


# In[ ]:


top_25.head()


# In[ ]:


top_info_list = []
for symbol, row in tqdm(top_25.iterrows(),position=0, leave=True):
    company = None
    with redirect_stdout(trap):  
        try:
            company = yf.Ticker(symbol).info
            top_info_list.append(company)
        except Exception as e:
            error_list[symbol] = e


# In[ ]:


market_panda = pd.DataFrame(top_info_list)
market_panda.set_index('symbol',inplace=True)
market_panda = pd.concat([market_panda, top_25], axis=1)


# In[ ]:


pd.set_option('display.max_columns', 999)
market_panda.iloc[[0]]


# In[ ]:


market_panda = market_panda.drop('SSI')


# In[ ]:


pd.options.display.float_format = "{:,.2f}".format
market_panda[['longName','stock_change_percentage','close_price','fiftyTwoWeekHigh','sector']]


# In[ ]:


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


# In[ ]:


path_wkthmltoimage = r'C:\Program Files\wkhtmltopdf\bin\wkhtmltoimage.exe'
config = imgkit.config(wkhtmltoimage=path_wkthmltoimage)
imgkit.from_file('table_report.html', 'table_report.jpg',config=config,options={'enable-local-file-access': ''})  


# In[ ]:


Image(filename='table_report.jpg') 


# In[ ]:




