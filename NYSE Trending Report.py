import os, io, re, time, logging, requests, yfinance as yf, pandas as pd
from datetime import date
from tqdm import tqdm
from playwright.sync_api import sync_playwright

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)
logging.getLogger('yfinance').setLevel(logging.CRITICAL)

class NyseTrendingReport:
    """
    Stabilized Dividend Report with Optimized Bulk Fetching.
    Consolidated imports for a cleaner header.
    """
    def __init__(self, cache_path='market_symbols.csv'):
        self.cache_path = os.path.abspath(cache_path)
        self.market_data = pd.DataFrame()
        self.final_report = pd.DataFrame()
        self.symbol_index_map = {}
        self.session = requests.Session()
        self.session.headers.update({'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/124.0.0.0'})

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

    def fetch_bulk_quotes(self, symbols):
        results = []
        chunk_size = 200
        for i in range(0, len(symbols), chunk_size):
            chunk = symbols[i:i + chunk_size]
            url = f"https://query2.finance.yahoo.com/v7/finance/quote?symbols={','.join(chunk)}"
            try:
                resp = self.session.get(url, timeout=15)
                data = resp.json().get('quoteResponse', {}).get('result', [])
                for quote in data:
                    div_yield = quote.get('dividendYield', quote.get('trailingAnnualDividendYield', 0))
                    results.append({
                        'Symbol': quote.get('symbol'),
                        'Name': quote.get('longName', quote.get('shortName', 'N/A')),
                        'Price': quote.get('regularMarketPrice', 0),
                        'Div Yield (%)': round((div_yield or 0) * 100, 2),
                        '52W High': quote.get('fiftyTwoWeekHigh', 0),
                        'P/E Ratio': quote.get('trailingPE', 'N/A'),
                        'Index': self.symbol_index_map.get(quote.get('symbol'), 'N/A')
                    })
            except Exception as e:
                logger.error(f"Quote fetch error: {e}")
        return pd.DataFrame(results)

    def collect_market_data(self, symbols):
        logger.info(f"Step 1: Bulk fetching quotes for {len(symbols)} symbols...")
        df_all = self.fetch_bulk_quotes(symbols)
        logger.info("Step 2: Filtering for stocks with > 1.5% yield...")
        df_filtered = df_all[df_all['Div Yield (%)'] > 1.5].copy()
        logger.info(f"Found {len(df_filtered)} candidates. Fetching missing sectors...")
        sectors = []
        for symbol in tqdm(df_filtered['Symbol'], desc="Enriching Sectors"):
            try:
                info = yf.Ticker(symbol).info
                sectors.append(info.get('sector', 'N/A'))
                time.sleep(0.1)
            except: sectors.append('N/A')
        df_filtered['Sector'] = sectors
        self.market_data = df_filtered

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
        html_content = f"<html><head><style>body {{ font-family: sans-serif; padding: 20px; }} table {{ border-collapse: collapse; width: 100%; }} th, td {{ padding: 8px; border-bottom: 1px solid #ddd; }} th {{ background: #f4f4f4; }}</style></head><body><h2>S&P Dividend Report — {date.today()}</h2>{self.final_report[cols].to_html(index=False)}</body></html>"
        with open(output_html, 'w', encoding='utf-8') as f: f.write(html_content)
        logger.info(f"Report saved: {output_html}")
        try:
            with sync_playwright() as p:
                browser = p.chromium.launch()
                page = browser.new_page()
                page.set_content(html_content)
                page.pdf(path=f"dividend_report_{date.today()}.pdf", format='A4', landscape=True)
                browser.close()
        except Exception as e: logger.error(f"PDF error: {e}")

    def run(self):
        symbols = self.get_all_symbols_with_indices()
        if symbols:
            self.collect_market_data(symbols)
            self.filter_and_rank()
            self.generate_report()

if __name__ == "__main__":
    NyseTrendingReport().run()
