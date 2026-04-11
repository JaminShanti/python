#!/usr/bin/env python

import yfinance as yf
import re
import pandas as pd
from tqdm import tqdm
from datetime import date, datetime, timedelta
import os
import io
import logging
import requests
import time
import random
from concurrent.futures import ThreadPoolExecutor

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Suppress yfinance internal noise
logging.getLogger('yfinance').setLevel(logging.CRITICAL)

class NyseTrendingReport:
    """
    Stabilized Dividend Report. 
    Uses rate-limit protection and robust Wikipedia scraping.
    """

    def __init__(self, cache_path='market_symbols.csv'):
        self.cache_path = os.path.abspath(cache_path)
        self.market_data = pd.DataFrame()
        self.final_report = pd.DataFrame()
        self.symbol_index_map = {}
        self.months = ["JANUARY", "FEBRUARY", "MARCH", "APRIL", "MAY", "JUNE", 
                       "JULY", "AUGUST", "SEPTEMBER", "OCTOBER", "NOVEMBER", "DECEMBER"]
        
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36'
        })

    def _get_wikipedia_table(self, url, match_str):
        try:
            resp = self.session.get(url, timeout=20)
            resp.raise_for_status()
            # Force html.parser to avoid missing dependency issues
            tables = pd.read_html(io.StringIO(resp.text), match=match_str, flavor='bs4')
            df = max(tables, key=len)
            
            ticker_col = next((col for col in df.columns if any(x in col for x in ['Symbol', 'Ticker', 'Ticker symbol'])), None)
            if ticker_col is None: return []

            raw_symbols = [str(s).strip().split()[0].upper() for s in df[ticker_col].tolist()]
            clean_symbols = []
            for s in raw_symbols:
                s = re.sub(r'\.', '-', s)
                if re.match(r'^[A-Z-]{1,5}$', s) and s not in self.months:
                    clean_symbols.append(s)
            return list(set(clean_symbols))
        except Exception as e:
            logger.debug(f"Failed to fetch from {url}: {e}")
            return []

    def get_all_symbols_with_indices(self):
        if os.path.exists(self.cache_path):
            file_age = datetime.now() - datetime.fromtimestamp(os.path.getmtime(self.cache_path))
            if file_age < timedelta(days=30):
                cache_df = pd.read_csv(self.cache_path)
                if 'symbol' in cache_df.columns and 'index' in cache_df.columns:
                    logger.info("Using cached market symbols.")
                    self.symbol_index_map = dict(zip(cache_df['symbol'], cache_df['index']))
                    return cache_df['symbol'].tolist()

        logger.info("SSD Cache expired. Fetching fresh S&P 500, 400, and 600 lists...")
        indices = {
            'S&P 500': ('https://en.wikipedia.org/wiki/List_of_S%26P_500_companies', 'Symbol'),
            'S&P 400': ('https://en.wikipedia.org/wiki/List_of_S%26P_400_companies', 'Ticker symbol'),
            'S&P 600': ('https://en.wikipedia.org/wiki/List_of_S%26P_600_companies', 'Ticker symbol')
        }
        
        all_data = []
        for index_name, (url, match) in indices.items():
            symbols = self._get_wikipedia_table(url, match)
            if symbols:
                logger.info(f"Retrieved {len(symbols)} tickers for {index_name}")
                for s in symbols:
                    all_data.append({'symbol': s, 'index': index_name})
        
        if not all_data:
            logger.error("No symbols retrieved. Check internet or dependencies.")
            return []

        df = pd.DataFrame(all_data).drop_duplicates(subset=['symbol'], keep='first')
        df.to_csv(self.cache_path, index=False)
        self.symbol_index_map = dict(zip(df['symbol'], df['index']))
        return df['symbol'].tolist()

    def fetch_stock_details(self, symbol):
        """Worker to get dividend info with built-in retry and delay."""
        # Random sleep to avoid pattern-based rate limiting
        time.sleep(random.uniform(0.2, 0.8))
        
        for attempt in range(2): # Try twice
            try:
                t = yf.Ticker(symbol, session=self.session)
                info = t.info
                div_yield = info.get('dividendYield', 0)
                if div_yield is None: div_yield = 0
                
                # We also need price
                hist = t.history(period="2d")
                price = hist['Close'].iloc[-1] if not hist.empty else 0
                
                return {
                    'Symbol': symbol,
                    'Name': info.get('longName', 'N/A'),
                    'Index': self.symbol_index_map.get(symbol, 'N/A'),
                    'Price': price,
                    'Div Yield (%)': round(div_yield * 100, 2),
                    'Sector': info.get('sector', 'N/A'),
                    '52W High': info.get('fiftyTwoWeekHigh', 0),
                    'P/E Ratio': info.get('trailingPE', 'N/A')
                }
            except Exception:
                if attempt == 0:
                    time.sleep(2) # Wait a bit more on first fail
                continue
        return None

    def collect_market_data(self, symbols):
        logger.info(f"Collecting dividend data for {len(symbols)} stocks (throttled for safety)...")
        
        results = []
        # Lowered workers to 5 to avoid 401/Rate Limit blocks
        with ThreadPoolExecutor(max_workers=5) as executor:
            list_results = list(tqdm(executor.map(self.fetch_stock_details, symbols), total=len(symbols)))
        
        results = [r for r in list_results if r is not None]
        self.market_data = pd.DataFrame(results)
        logger.info(f"Successfully processed {len(self.market_data)} stocks.")

    def filter_and_rank(self, top_n=100):
        if self.market_data.empty: return
        # Filter: Div Yield > 1.5% (Ryder is around 2%)
        self.final_report = self.market_data[self.market_data['Div Yield (%)'] > 1.5]
        self.final_report = self.final_report.sort_values(by=['Div Yield (%)', 'Price'], ascending=[False, False]).head(top_n)

    def generate_report(self):
        if self.final_report.empty:
            logger.warning("No dividend stocks matching criteria found.")
            return
            
        output_file = 'dividend_report.html'
        pd.options.display.float_format = "{:,.2f}".format
        cols = ['Symbol', 'Name', 'Index', 'Price', 'Div Yield (%)', 'Sector', '52W High', 'P/E Ratio']
        
        html_content = f"""
        <html>
        <head>
            <style>
                body {{ font-family: 'Segoe UI', Arial; background-color: #121212; color: #e0e0e0; padding: 20px; }}
                table {{ border-collapse: collapse; width: 100%; background-color: #1e1e1e; }}
                th, td {{ padding: 12px; text-align: left; border-bottom: 1px solid #333; }}
                th {{ background-color: #004d40; color: white; }}
                tr:hover {{ background-color: #262626; }}
                h2 {{ color: #4db6ac; }}
            </style>
        </head>
        <body>
            <h2>S&P 500/400/600 Dividend Performance Report ({date.today()})</h2>
            <p>Scanning results for High Dividend candidates (>1.5% yield).</p>
            {self.final_report[cols].to_html(index=False)}
        </body>
        </html>
        """
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write(html_content)
        logger.info(f"Report generated: {output_file}")

    def run(self):
        symbols = self.get_all_symbols_with_indices()
        if not symbols: return
        self.collect_market_data(symbols)
        self.filter_and_rank()
        self.generate_report()

if __name__ == "__main__":
    start = datetime.now()
    NyseTrendingReport().run()
    logger.info(f"Total Execution Time: {datetime.now() - start}")
