import os, io, re, time, logging, requests, yfinance as yf, pandas as pd, numpy as np
from datetime import datetime, date, timedelta
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed
from playwright.sync_api import sync_playwright
from bs4 import BeautifulSoup
import random

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)
logging.getLogger('yfinance').setLevel(logging.CRITICAL)

class NyseTrendingReport:
    """
    Stabilized Dividend Report with Sharpe Ratio and Stale-if-Error Caching.
    Filters the S&P 500, 400, and 600 for reliable yield opportunities.
    """
    def __init__(self, cache_path='market_symbols.csv', data_cache_path='market_data_cache.csv'):
        self.cache_path = os.path.abspath(cache_path)
        self.data_cache_path = os.path.abspath(data_cache_path)
        self.cache_ttl = 4 * 3600
        self.symbol_ttl = 7 * 24 * 3600
        self.market_data = pd.DataFrame()
        self.final_report = pd.DataFrame()
        self.symbol_index_map = {}
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36'
        })

    def _get_wikipedia_table(self, url):
        """Robustly parse Wikipedia tables to find stock symbols."""
        try:
            resp = self.session.get(url, timeout=20)
            resp.raise_for_status()
            soup = BeautifulSoup(resp.text, 'html.parser')
            all_symbols = []
            for table in soup.find_all('table', class_='wikitable'):
                header_row = table.find('tr')
                if not header_row: continue
                headers = [th.get_text(strip=True) for th in header_row.find_all(['th', 'td'])]
                ticker_idx = -1
                for i, h in enumerate(headers):
                    if any(x == h or x in h for x in ['Symbol', 'Ticker', 'Ticker symbol', 'Ticker Symbol']):
                        ticker_idx = i
                        break
                if ticker_idx != -1:
                    for row in table.find_all('tr')[1:]:
                        cols = row.find_all('td')
                        if len(cols) > ticker_idx:
                            text = cols[ticker_idx].get_text(strip=True)
                            parts = text.split()
                            if parts:
                                symbol = parts[0].upper()
                                symbol = re.sub(r'[\.\/]', '-', symbol)
                                if 0 < len(symbol) < 7:
                                    all_symbols.append(symbol)
            return sorted(list(set(all_symbols)))
        except Exception as e:
            logger.error(f"Error scraping {url}: {e}")
            return []

    def get_all_symbols_with_indices(self):
        """Loads or scrapes S&P 500/400/600 symbols from Wikipedia."""
        if os.path.exists(self.cache_path):
            file_age = time.time() - os.path.getmtime(self.cache_path)
            if file_age < self.symbol_ttl:
                try:
                    cache_df = pd.read_csv(self.cache_path)
                    if not cache_df.empty and 'symbol' in cache_df.columns:
                        self.symbol_index_map = dict(zip(cache_df['symbol'], cache_df['index']))
                        return cache_df['symbol'].tolist()
                except Exception: pass

        logger.info("Scraping fresh symbols from Wikipedia...")
        indices = {
            'S&P 500': 'https://en.wikipedia.org/wiki/List_of_S%26P_500_companies',
            'S&P 400': 'https://en.wikipedia.org/wiki/List_of_S%26P_400_companies',
            'S&P 600': 'https://en.wikipedia.org/wiki/List_of_S%26P_600_companies'
        }
        all_data = []
        for name, url in indices.items():
            symbols = self._get_wikipedia_table(url)
            logger.info(f"Found {len(symbols)} symbols for {name}")
            for s in symbols: all_data.append({'symbol': s, 'index': name})
        
        if not all_data: return []
        df = pd.DataFrame(all_data).drop_duplicates('symbol')
        df.to_csv(self.cache_path, index=False)
        self.symbol_index_map = dict(zip(df['symbol'], df['index']))
        return df['symbol'].tolist()

    def fetch_single_ticker(self, symbol):
        """Fetches and calculates metrics for a single stock."""
        try:
            time.sleep(random.uniform(0.1, 0.4)) # Polite jitter
            t = yf.Ticker(symbol)
            
            # 1. Price History for Sharpe Ratio
            hist = t.history(period="1y")
            if hist.empty or len(hist) < 50:
                return None
            
            price = hist['Close'].iloc[-1]
            returns = hist['Close'].pct_change().dropna()
            ann_return = returns.mean() * 252
            ann_vol = returns.std() * np.sqrt(252)
            rf_rate = 0.04 # 4% Risk-Free Rate
            sharpe = (ann_return - rf_rate) / ann_vol if ann_vol > 0 else -1.0
            
            # Skip unreliable or zero-yield stocks immediately
            if sharpe <= 0: return None

            # 2. Fundamental Dividend Data
            info = t.info
            active_rate = info.get('dividendRate') or info.get('trailingAnnualDividendRate') or 0
            div_yield_pct = (active_rate / price) * 100
            
            if div_yield_pct > 25 or div_yield_pct < 1.0: # Filter yields < 1%
                return None

            return {
                'Symbol': symbol,
                'Name': info.get('longName', info.get('shortName', 'N/A')),
                'Price': price,
                'Div Yield (%)': round(float(div_yield_pct), 2),
                'Sharpe Ratio': round(float(sharpe), 2),
                '52W High': info.get('fiftyTwoWeekHigh', 0),
                'P/E Ratio': info.get('trailingPE', 'N/A'),
                'Index': self.symbol_index_map.get(symbol, 'N/A'),
                'Sector': info.get('sector', 'N/A')
            }
        except Exception:
            return None

    def collect_market_data(self, symbols):
        """Orchestrates data collection with 4-hour caching."""
        cache_exists = os.path.exists(self.data_cache_path)
        if cache_exists:
            file_age = time.time() - os.path.getmtime(self.data_cache_path)
            if file_age < self.cache_ttl:
                try:
                    df_all = pd.read_csv(self.data_cache_path)
                    self.market_data = df_all[(df_all['Div Yield (%)'] > 1.5) & (df_all['Sharpe Ratio'] > 0)].copy()
                    logger.info(f"Using cache ({round(file_age/60)}m old)")
                    return
                except Exception: pass

        logger.info(f"Analyzing {len(symbols)} symbols via yfinance...")
        results = []
        with ThreadPoolExecutor(max_workers=10) as executor:
            future_to_symbol = {executor.submit(self.fetch_single_ticker, s): s for s in symbols}
            for future in tqdm(as_completed(future_to_symbol), total=len(symbols), desc="Downloading"):
                res = future.result()
                if res: results.append(res)
        
        if results:
            df_all = pd.DataFrame(results)
            df_all.to_csv(self.data_cache_path, index=False)
            self.market_data = df_all.copy()
            logger.info(f"Found {len(self.market_data)} reliable dividend stocks.")
        elif cache_exists:
            logger.warning("Fetch failed. Falling back to stale cache.")
            df_all = pd.read_csv(self.data_cache_path)
            self.market_data = df_all.copy()
        else:
            logger.error("No market data available.")

    def filter_and_rank(self, top_n=100):
        """Ranks by Sharpe Ratio first (Reliability), then Yield."""
        if self.market_data.empty: return
        self.final_report = self.market_data.sort_values(
            by=['Sharpe Ratio', 'Div Yield (%)'], 
            ascending=[False, False]
        ).head(top_n)

    def generate_report(self):
        """Generates HTML and PDF reports."""
        if self.final_report.empty:
            logger.warning("No matches for report.")
            return
        
        output_html = 'dividend_report.html'
        pd.options.display.float_format = "{:,.2f}".format
        cols = ['Symbol', 'Name', 'Index', 'Price', 'Div Yield (%)', 'Sharpe Ratio', 'Sector', '52W High', 'P/E Ratio']
        current_time = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        
        html_content = f"""
        <html>
        <head>
            <title>S&P Reliable Dividend Report</title>
            <style>
                body {{ font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; padding: 30px; color: #333; background-color: #f4f7f6; }}
                .container {{ background: white; padding: 20px; border-radius: 8px; box-shadow: 0 4px 6px rgba(0,0,0,0.1); }}
                h2 {{ color: #2c3e50; margin-top: 0; border-bottom: 2px solid #3498db; padding-bottom: 10px; }}
                table {{ border-collapse: collapse; width: 100%; margin-top: 20px; font-size: 14px; }}
                th, td {{ padding: 12px 15px; border-bottom: 1px solid #ddd; text-align: left; }}
                th {{ background: #34495e; color: white; }}
                tr:nth-child(even) {{ background-color: #f9f9f9; }}
                tr:hover {{ background-color: #f1f1f1; transition: 0.3s; }}
                .footer {{ margin-top: 20px; font-size: 12px; color: #7f8c8d; }}
            </style>
        </head>
        <body>
            <div class="container">
                <h2>S&P Reliable Dividend Report — {date.today()}</h2>
                <p>Filter Criteria: <strong>Sharpe Ratio > 0</strong> (Risk-Adjusted Return) and <strong>Forward Yield > 1%</strong>.</p>
                {self.final_report[cols].to_html(index=False, border=0)}
                <div class="footer">Data sourced via yfinance for S&P 500/400/600. Generated at {current_time}.</div>
            </div>
        </body>
        </html>
        """
        
        with open(output_html, 'w', encoding='utf-8') as f: f.write(html_content)
        
        try:
            with sync_playwright() as p:
                browser = p.chromium.launch()
                page = browser.new_page()
                page.set_content(html_content)
                pdf_path = f"dividend_report_{current_time}.pdf"
                page.pdf(path=pdf_path, format='A4', landscape=True, print_background=True)
                browser.close()
            logger.info(f"PDF Report saved: {pdf_path}")
        except Exception as e:
            logger.error(f"PDF error: {e}")

    def run(self):
        symbols = self.get_all_symbols_with_indices()
        if symbols:
            self.collect_market_data(symbols)
            self.filter_and_rank()
            self.generate_report()

if __name__ == "__main__":
    NyseTrendingReport().run()
