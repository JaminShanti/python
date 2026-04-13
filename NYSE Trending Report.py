import os, io, re, time, logging, requests, yfinance as yf, pandas as pd
from datetime import date
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed
from playwright.sync_api import sync_playwright

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)
logging.getLogger('yfinance').setLevel(logging.CRITICAL)

class NyseTrendingReport:
    """
    Stabilized Dividend Report with Threaded yfinance Fetching.
    Manual yield calculation for high financial accuracy.
    """
    def __init__(self, cache_path='market_symbols.csv'):
        self.cache_path = os.path.abspath(cache_path)
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
        """Fetches detailed info and calculates yield manually for accuracy."""
        try:
            t = yf.Ticker(symbol)
            info = t.info
            
            price = info.get('currentPrice') or info.get('regularMarketPrice') or 0
            if price == 0: return None

            # Calculate yield manually: (Annual Dividend / Current Price) * 100
            # This is much more reliable than the pre-calculated fields
            div_rate = info.get('dividendRate') or info.get('trailingAnnualDividendRate') or 0
            
            # If rate is 0, check the dividendYield field as fallback but normalize it
            if div_rate == 0:
                y = info.get('dividendYield') or info.get('trailingAnnualDividendYield') or 0
                # yfinance 'dividendYield' is usually a decimal (0.05 = 5%)
                # but if it's already > 1 (like 5.0), we treat it as a percentage
                div_yield_pct = y * 100 if y < 1 else y
            else:
                div_yield_pct = (div_rate / price) * 100

            # Round to 2 decimal places
            div_yield_pct = round(float(div_yield_pct), 2)
            
            # Sanity check: Realistically, S&P dividends rarely exceed 20%
            if div_yield_pct > 25:
                # Likely a data error or special dividend we want to exclude for trend analysis
                return None

            return {
                'Symbol': symbol,
                'Name': info.get('longName', info.get('shortName', 'N/A')),
                'Price': price,
                'Div Yield (%)': div_yield_pct,
                '52W High': info.get('fiftyTwoWeekHigh', 0),
                'P/E Ratio': info.get('trailingPE', 'N/A'),
                'Index': self.symbol_index_map.get(symbol, 'N/A'),
                'Sector': info.get('sector', 'N/A')
            }
        except:
            return None

    def collect_market_data(self, symbols):
        logger.info(f"Step 1: Fetching data for {len(symbols)} symbols via yfinance...")
        results = []
        
        with ThreadPoolExecutor(max_workers=10) as executor:
            future_to_symbol = {executor.submit(self.fetch_single_ticker, s): s for s in symbols}
            for future in tqdm(as_completed(future_to_symbol), total=len(symbols), desc="Downloading"):
                res = future.result()
                if res: results.append(res)
        
        df_all = pd.DataFrame(results)
        
        if df_all.empty:
            logger.error("No market data could be retrieved.")
            return

        logger.info("Step 2: Filtering for stocks with > 1.5% yield...")
        self.market_data = df_all[df_all['Div Yield (%)'] > 1.5].copy()
        logger.info(f"Filtered down to {len(self.market_data)} candidates.")

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
        
        html_content = f"""
        <html>
        <head>
            <style>
                body {{ font-family: sans-serif; padding: 20px; color: #333; }}
                h2 {{ color: #2c3e50; }}
                table {{ border-collapse: collapse; width: 100%; margin-top: 20px; }}
                th, td {{ padding: 10px; border-bottom: 1px solid #ddd; text-align: left; }}
                th {{ background: #f8f9fa; font-weight: bold; }}
                tr:nth-child(even) {{ background-color: #fcfcfc; }}
            </style>
        </head>
        <body>
            <h2>S&P Dividend Report — {date.today()}</h2>
            <p>Calculated using (Annual Dividend Rate / Current Price). Filtered for > 1.5% yield.</p>
            {self.final_report[cols].to_html(index=False)}
        </body>
        </html>
        """
        
        with open(output_html, 'w', encoding='utf-8') as f: f.write(html_content)
        
        try:
            with sync_playwright() as p:
                browser = p.chromium.launch()
                page = browser.new_page()
                page.set_content(html_content)
                pdf_path = f"dividend_report_{date.today()}.pdf"
                page.pdf(path=pdf_path, format='A4', landscape=True)
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
