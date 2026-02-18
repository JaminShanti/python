#!/usr/bin/env python

import yfinance as yf
import re
import pandas as pd
from tqdm import tqdm
from datetime import date, timedelta
from contextlib import redirect_stdout
import os
import io
import logging
import requests
from pandas.tseries.offsets import BDay
import imgkit
from IPython.display import Image

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class NyseTrendingReport:
    """
    A class to generate a trending report for S&P 500 stocks based on
    recent market performance.
    """

    def __init__(self, cache_path='sp500_symbols.csv', 
                 wkhtmltoimage_path=r'C:\Program Files\wkhtmltopdf\bin\wkhtmltoimage.exe'):
        self.cache_path = cache_path
        self.wkhtmltoimage_path = wkhtmltoimage_path
        self.trap = io.StringIO()
        
        # Date calculations
        today = date.today()
        self.compare_market_close = (today - BDay(2)).strftime("%Y-%m-%d")
        self.market_last_close_add_a_day = today.strftime("%Y-%m-%d")
        
        logger.info(f"Report Date Range: {self.compare_market_close} to {self.market_last_close_add_a_day}")

        self.market_data = pd.DataFrame()
        self.top_performers = pd.DataFrame()

    def get_sp500_symbols(self):
        """
        Fetches S&P 500 symbols from Wikipedia or a local cache.
        """
        wiki_url = 'https://en.wikipedia.org/wiki/List_of_S%26P_500_companies'
        symbols = []
        
        try:
            headers = {'User-Agent': 'Mozilla/5.0'}
            resp = requests.get(wiki_url, headers=headers, timeout=20)
            resp.raise_for_status()
            tables = pd.read_html(io.StringIO(resp.text), match='Symbol')
            df = tables[0]
            col = 'Symbol' if 'Symbol' in df.columns else df.columns[0]
            symbols = df[col].astype(str).tolist()
            symbols = [re.sub(r'\.', '-', s) for s in symbols]
            
            # Cache to disk
            pd.DataFrame({'symbol': symbols}).to_csv(self.cache_path, index=False)
            logger.info(f"Fetched {len(symbols)} symbols from Wikipedia.")
            
        except Exception as e:
            logger.warning(f"Failed to fetch from Wikipedia: {e}. Trying cache.")
            if os.path.exists(self.cache_path):
                try:
                    df = pd.read_csv(self.cache_path)
                    col = 'symbol' if 'symbol' in df.columns else df.columns[0]
                    symbols = df[col].astype(str).tolist()
                    logger.info(f"Loaded {len(symbols)} symbols from cache.")
                except Exception as ex:
                    logger.error(f"Failed to load from cache: {ex}")
            else:
                logger.error("No cache file found.")
                
        return symbols

    def collect_market_data(self, symbols):
        """
        Downloads market data for the given symbols and calculates percentage change.
        """
        market_list = []
        error_list = {}
        
        logger.info("Starting market data collection...")
        
        for symbol in tqdm(symbols, position=0, leave=True):
            data_row = {}
            with redirect_stdout(self.trap):  
                try:
                    # Download data
                    company_trend = yf.download(symbol, start=self.compare_market_close, 
                                              end=self.market_last_close_add_a_day,
                                              threads=False, progress=False)
                    
                    if company_trend is None or company_trend.empty:
                        error_list[symbol] = "no data"
                        continue

                    # Normalize index
                    if not isinstance(company_trend.index, pd.DatetimeIndex):
                        company_trend.index = pd.to_datetime(company_trend.index, errors='coerce')
                        company_trend = company_trend[company_trend.index.notna()]

                    # Filter window
                    start_dt = pd.to_datetime(self.compare_market_close)
                    end_dt = pd.to_datetime(self.market_last_close_add_a_day)
                    window = company_trend.loc[(company_trend.index >= start_dt) & (company_trend.index < end_dt)]

                    if window.empty or 'Close' not in window:
                        error_list[symbol] = "no window/close"
                        continue

                    # Calculate change
                    # Handle potential MultiIndex columns if yfinance returns them
                    close_series = window['Close']
                    if isinstance(close_series, pd.DataFrame):
                         close_series = close_series.iloc[:, 0]

                    compare_price = close_series.iloc[0]
                    close_price = close_series.iloc[-1]
                    
                    stock_change_percentage = (close_price / compare_price) * 100 - 100
                    
                    data_row['symbol'] = symbol
                    data_row['stock_change_percentage'] = stock_change_percentage
                    data_row['close_price'] = close_price
                    market_list.append(data_row)
                    
                except Exception as e: 
                    error_list[symbol] = str(e)

        if error_list:
            logger.debug(f"Errors encountered for {len(error_list)} symbols.")

        self.market_data = pd.DataFrame(market_list)
        if not self.market_data.empty:
            self.market_data.set_index('symbol', inplace=True)
        else:
            logger.error("No valid data collected.")

    def process_top_performers(self, top_n=25):
        """
        Identifies top performers and fetches additional details.
        """
        if self.market_data.empty:
            logger.warning("Market data is empty. Cannot process top performers.")
            return

        logger.info(f"Identifying top {top_n} performers...")
        top_performers = self.market_data.nlargest(top_n, ['stock_change_percentage'])
        
        top_info_list = []
        for symbol, _ in tqdm(top_performers.iterrows(), total=top_performers.shape[0], position=0, leave=True):
            with redirect_stdout(self.trap):  
                try:
                    company = yf.Ticker(symbol).info
                    # Keep only relevant info to avoid massive dataframes
                    relevant_info = {
                        'symbol': symbol,
                        'longName': company.get('longName'),
                        'fiftyTwoWeekHigh': company.get('fiftyTwoWeekHigh'),
                        'sector': company.get('sector')
                    }
                    top_info_list.append(relevant_info)
                except Exception as e:
                    logger.error(f"Error fetching info for {symbol}: {e}")

        info_df = pd.DataFrame(top_info_list)
        if not info_df.empty:
            info_df.set_index('symbol', inplace=True)
            self.top_performers = pd.concat([info_df, top_performers], axis=1)
        else:
            self.top_performers = top_performers

        # Clean up specific tickers if needed (legacy logic)
        for exclude in ['MCC', 'FMO', 'SSI']:
            if exclude in self.top_performers.index:
                self.top_performers.drop(exclude, inplace=True)

    def generate_report(self, output_html='table_report.html', output_img='table_report.jpg'):
        """
        Generates an HTML report and converts it to an image.
        """
        if self.top_performers.empty:
            logger.warning("No top performers to report.")
            return

        logger.info("Generating report...")
        
        # Format for display
        display_df = self.top_performers[['longName', 'stock_change_percentage', 'close_price', 'fiftyTwoWeekHigh', 'sector']].copy()
        
        # Apply formatting
        pd.options.display.float_format = "{:,.2f}".format
        
        html_string = '''
        <html>
          <head><title>NYSE Trending Report</title></head>
          <link rel="stylesheet" type="text/css" href="df_style.css"/>
          <style>
            .mystyle {
                font-family: Arial, sans-serif;
                border-collapse: collapse;
                width: 100%;
            }
            .mystyle td, .mystyle th {
                border: 1px solid #ddd;
                padding: 8px;
            }
            .mystyle tr:nth-child(even){background-color: #f2f2f2;}
            .mystyle th {
                padding-top: 12px;
                padding-bottom: 12px;
                text-align: left;
                background-color: #4CAF50;
                color: white;
            }
          </style>
          <body>
            <h2>Top Performing S&P 500 Stocks</h2>
            {table}
          </body>
        </html>
        '''
        
        with open(output_html, 'w') as f:
            f.write(html_string.format(table=display_df.to_html(classes='mystyle', float_format=lambda x: '{:,.2f}'.format(x))))
        
        logger.info(f"HTML report saved to {output_html}")

        # Convert to Image
        try:
            config = imgkit.config(wkhtmltoimage=self.wkhtmltoimage_path)
            imgkit.from_file(output_html, output_img, config=config, options={'enable-local-file-access': ''})
            logger.info(f"Image report saved to {output_img}")
        except OSError as e:
            logger.error(f"Skipping image export: wkhtmltoimage not found or error occurred. {e}")
        except Exception as e:
            logger.error(f"An error occurred during image generation: {e}")

    def run(self):
        """
        Executes the full reporting pipeline.
        """
        symbols = self.get_sp500_symbols()
        if not symbols:
            logger.error("No symbols found. Exiting.")
            return

        self.collect_market_data(symbols)
        self.process_top_performers()
        self.generate_report()

if __name__ == "__main__":
    report = NyseTrendingReport()
    report.run()
