import os, io, re, time, logging, requests, yfinance as yf, pandas as pd
from datetime import datetime, date
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed
from playwright.sync_api import sync_playwright

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)
logging.getLogger('yfinance').setLevel(logging.CRITICAL)

class NyseTrendingReport:
    """
    Stabilized Dividend Report with Threaded yfinance Fetching and Caching.
    TTL of 4 hours for market metrics to prevent API blocks.
    Iterates PDF filenames with timestamps to avoid overwriting.
    """
    def __init__(self, cache_path='market_symbols.csv', data_cache_path='market_data_cache.csv'):
        self.cache_path = os.path.abspath(cache_path)
        self.data_cache_path = os.path.abspath(data_cache_path)
        self.cache_ttl = 4 * 3600  # 4 hours in seconds
        self.market_data = pd.DataFrame()
        self.final_report = pd.DataFrame()
        self.symbol_index_map = {}
        self.session = requests.Session()
        self.session.headers.update({'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36'})

    def _get_wikipedia_table(self, url, match_str):
        try:
            resp = self.session.get(url, timeout=20)
            resp.raise_for_status()
            tables = pd.read_html(io.StringIO(resp.text), match=match_str, flavor='bs4')
            df = max(tables, key=len)
            ticker_col = next((col for col in df.columns if any(x in col for x in ['Symbol', 'Ticker', 'Ticker symbol'])), None)
            if ticker_col is None: return []
            return [re.sub(r'\.', '-', str(s).strip().split()[0].upper()) for s in df[ticker_col].tolist() if len(str(s)) < 7]
        except: return []

    def get_all_symbols_with_indices(self):
        if os.path.exists(self.cache_path):
            cache_df = pd.read_csv(self.cache_path)
            self.symbol_index_map = dict(zip(cache_df['symbol'], cache_df['index']))
            return cache_df['symbol'].tolist()

        indices = {'S&P 500': ('https://en.wikipedia.org/wiki/List_of_S%26P_500_companies', 'Symbol'),
                   'S&P 400': ('https://en.wikipedia.org/wiki/List_of_S%26P_400_companies', 'Ticker symbol'),
                   'S&P 600': ('https://en.wikipedia.org/wiki/List_of_S%26P_600_companies', 'Ticker symbol')}
        all_data = []
        for name, (url, match) in indices.items():
            symbols = self._get_wikipedia_table(url, match)
            for s in symbols: all_data.append({'symbol': s, 'index': name})
        df = pd.DataFrame(all_data).drop_duplicates('symbol')
        df.to_csv(self.cache_path, index=False)
        self.symbol_index_map = dict(zip(df['symbol'], df['index']))
        return df['symbol'].tolist()

    def fetch_single_ticker(self, symbol):
        """Fetches info and isolates recurring dividends."""
        try:
            t = yf.Ticker(symbol)
            info = t.info
            
            price = info.get('currentPrice') or info.get('regularMarketPrice') or 0
            if price == 0: return None

            forward_rate = info.get('dividendRate') or 0
            trailing_rate = info.get('trailingAnnualDividendRate') or 0
            
            active_rate = forward_rate if forward_rate > 0 else trailing_rate
            div_yield_pct = (active_rate / price) * 100
            
            if div_yield_pct > 20 or div_yield_pct < 0.1:
                return None

            return {
                'Symbol': symbol,
                'Name': info.get('longName', info.get('shortName', 'N/A')),
                'Price': price,
                'Div Yield (%)': round(float(div_yield_pct), 2),
                '52W High': info.get('fiftyTwoWeekHigh', 0),
                'P/E Ratio': info.get('trailingPE', 'N/A'),
                'Index': self.symbol_index_map.get(symbol, 'N/A'),
                'Sector': info.get('sector', 'N/A')
            }
        except:
            return None

    def collect_market_data(self, symbols):
        # Check for valid cache
        if os.path.exists(self.data_cache_path):
            file_age = time.time() - os.path.getmtime(self.data_cache_path)
            if file_age < self.cache_ttl:
                logger.info(f"Loading market data from cache (Last updated {round(file_age/60)} minutes ago)...")
                df_all = pd.read_csv(self.data_cache_path)
                self.market_data = df_all[df_all['Div Yield (%)'] > 1.5].copy()
                logger.info(f"Found {len(self.market_data)} candidates in cache.")
                return

        logger.info(f"Step 1: Fetching recurring dividend data for {len(symbols)} symbols via yfinance...")
        results = []
        
        with ThreadPoolExecutor(max_workers=10) as executor:
            future_to_symbol = {executor.submit(self.fetch_single_ticker, s): s for s in symbols}
            for future in tqdm(as_completed(future_to_symbol), total=len(symbols), desc="Downloading"):
                res = future.result()
                if res: results.append(res)
        
        df_all = pd.DataFrame(results)
        
        if not df_all.empty:
            # Save to cache
            df_all.to_csv(self.data_cache_path, index=False)
            logger.info(f"Market data cached to {self.data_cache_path}")
            
            logger.info("Step 2: Filtering for recurring yields > 1.5%...")
            self.market_data = df_all[df_all['Div Yield (%)'] > 1.5].copy()
            logger.info(f"Found {len(self.market_data)} recurring dividend stocks.")
        else:
            logger.error("No market data could be retrieved.")

    def filter_and_rank(self, top_n=100):
        if self.market_data.empty: return
        self.final_report = self.market_data.sort_values(by=['Div Yield (%)', 'Price'], ascending=[False, False]).head(top_n)

    def generate_report(self):
        if self.final_report.empty:
            logger.warning("No matches found.")
            return
        
        output_html = 'dividend_report.html'
        pd.options.display.float_format = "{:,.2f}".format
        cols = ['Symbol', 'Name', 'Index', 'Price', 'Div Yield (%)', 'Sector', '52W High', 'P/E Ratio']
        
        current_time = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        
        html_content = f"""
        <html>
        <head>
            <title>S&P Recurring Dividend Report</title>
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
                <h2>S&P Recurring Dividend Report — {date.today()}</h2>
                <p>Calculated using <strong>Forward Dividend Rates</strong> to exclude one-time special payouts. Filtered for recurring yields > 1.5%.</p>
                {self.final_report[cols].to_html(index=False, border=0)}
                <div class="footer">Data sourced via yfinance for S&P 500, 400, and 600 indices. Market metrics cached for 4 hours. Generated at {current_time}.</div>
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
