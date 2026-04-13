import requests
import pandas as pd
import os
import logging
import json
import re
import gzip
import pickle
from datetime import datetime, timedelta
from tqdm import tqdm
from bs4 import BeautifulSoup

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class MTGDipDetector:
    """
    MTG Price Drop Detector v9.1
    Optimized for cEDH (EDHTop16) and EDH (EDHRec) staples.
    Generates a professional-grade PDF report and TCGplayer-compatible import file.
    """
    def __init__(self, cache_dir='mtg_cache', high_window_days=90, min_drop_dollars=0.75, min_drop_pct=35.0, use_pickle_cache=True):
        self.cache_dir = os.path.abspath(cache_dir)
        os.makedirs(self.cache_dir, exist_ok=True)
        
        self.high_window_days = high_window_days
        self.min_drop_dollars = min_drop_dollars
        self.min_drop_pct = min_drop_pct
        self.use_pickle_cache = use_pickle_cache
        
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
            'Accept-Encoding': 'gzip'
        })

        self.illegal_sets = {
            'WC97', 'WC98', 'WC99', 'WC00', 'WC01', 'WC02', 'WC03', 'WC04', 
            'CED', 'CEI', 'UST', 'UNH', 'UGL', 'UND', 'UNF', 'PLIST'
        }
        self.ui_noise = {
            "staples", "rank", "count", "percent", "commander", "partner", "decklist", 
            "filter", "share", "name", "price", "color", "type", "next", "previous", 
            "search", "menu", "mountain", "forest", "island", "swamp", "plains"
        }
        
        self.TTL_STAPLES = 24       
        self.TTL_IDENTIFIERS = 168  
        self.TTL_PRICES = 24        

    @staticmethod
    def _extract_names_recursively(obj, found_set):
        if isinstance(obj, dict):
            for k, v in obj.items():
                if k == 'name' and isinstance(v, str) and 3 < len(v) < 45:
                    if not any(char.isdigit() for char in v):
                        found_set.add(v.lower().strip())
                else:
                    MTGDipDetector._extract_names_recursively(v, found_set)
        elif isinstance(obj, list):
            for item in obj:
                MTGDipDetector._extract_names_recursively(item, found_set)

    def _fast_harvest(self, url, found_set):
        try:
            r = self.session.get(url, timeout=20)
            if r.status_code == 200:
                json_match = re.search(r'id="__NEXT_DATA__".*?>(.*?)</script>', r.text, re.DOTALL)
                if json_match:
                    data = json.loads(json_match.group(1))
                    self._extract_names_recursively(data, found_set)
                
                soup = BeautifulSoup(r.text, 'html.parser')
                for element in soup.find_all(['a', 'td', 'span', 'div']):
                    text = element.get_text(strip=True).lower()
                    if 3 < len(text) < 45 and not any(c.isdigit() for c in text):
                        if text not in self.ui_noise:
                            found_set.add(text)
                return len(found_set) >= 25
        except Exception as e:
            logger.warning(f"Harvest failed for {url}: {e}")
        return False

    def _get_staples(self):
        cache_file = os.path.join(self.cache_dir, "staple_cache.json")
        if os.path.exists(cache_file):
            file_age = datetime.now() - datetime.fromtimestamp(os.path.getmtime(cache_file))
            if file_age < timedelta(hours=self.TTL_STAPLES):
                try:
                    with open(cache_file, 'r', encoding='utf-8') as f:
                        data = json.load(f)
                        t16, rec = data.get('top16', []), data.get('rec', [])
                        if len(t16) >= 25:
                            return set(t16), set(rec)
                except Exception: pass

        logger.info("Harvesting fresh staples...")
        edhtop16_staples, edhrec_staples = set(), set()
        self._fast_harvest("https://edhtop16.com/staples", edhtop16_staples)
        self._fast_harvest("https://edhrec.com/top", edhrec_staples)

        edhtop16_staples -= self.ui_noise
        edhrec_staples -= self.ui_noise

        if len(edhtop16_staples) >= 25 and len(edhrec_staples) >= 25:
            with open(cache_file, 'w', encoding='utf-8') as f:
                json.dump({'top16': list(edhtop16_staples), 'rec': list(edhrec_staples)}, f)
        else:
            raise RuntimeError(f"FATAL: Insufficient staple results found.")

        return edhtop16_staples, edhrec_staples

    def _get_json_data(self, url, filename, ttl_hours=24):
        cache_path_gz = os.path.join(self.cache_dir, filename + ".gz")
        cache_path_bin = os.path.join(self.cache_dir, filename + ".pkl")
        
        if self.use_pickle_cache and os.path.exists(cache_path_bin):
            file_age = datetime.now() - datetime.fromtimestamp(os.path.getmtime(cache_path_bin))
            if file_age < timedelta(hours=ttl_hours):
                try:
                    with open(cache_path_bin, 'rb') as f:
                        return pickle.load(f)
                except Exception: pass

        if not os.path.exists(cache_path_gz) or (datetime.now() - datetime.fromtimestamp(os.path.getmtime(cache_path_gz)) > timedelta(hours=ttl_hours)):
            logger.info(f"Downloading {filename}.gz...")
            r = self.session.get(url + ".gz", stream=True, timeout=600)
            with open(cache_path_gz, 'wb') as f:
                for chunk in r.iter_content(chunk_size=1024*1024): f.write(chunk)
            
        with gzip.open(cache_path_gz, 'rt', encoding='utf-8') as f:
            data = json.load(f).get("data", {})
            
        if self.use_pickle_cache:
            with open(cache_path_bin, 'wb') as f:
                pickle.dump(data, f)
                
        return data

    def generate_tcgplayer_import(self, df):
        """Generates a text file formatted for TCGplayer Mass Entry with better compatibility."""
        timestamp = datetime.now().strftime('%Y-%m-%d_%H-%M')
        filename = f"TCGplayer_Import_{timestamp}.txt"
        
        # Mapping for common MTGJSON -> TCGplayer set code mismatches
        set_map = {
            'FCA': 'PIP',            # Fallout Commander
            'PZA': 'PLST',           # The List (Often represented as PZA in some exports)
            'SPG': 'Special Guests', # Special Guests
            'MH3': 'Modern Horizons 3'
        }
        
        try:
            with open(filename, 'w', encoding='utf-8') as f:
                for _, row in df.iterrows():
                    # Handle Double Faced Cards: TCGplayer expects only the front half name
                    # e.g. "Witch Enchanter // Witch-Blessed Meadow" -> "Witch Enchanter"
                    full_name = row['Card Name']
                    clean_name = full_name.split(' // ')[0]
                    
                    set_code = row['Set']
                    
                    # Apply mappings
                    final_set = set_map.get(set_code, set_code)
                    
                    # Logic for problematic sets: If it's a known problematic code or 
                    # one that often fails, it's safer to just provide the name.
                    if set_code in ['PZA', 'FCA'] or len(final_set) > 3:
                        # For long set names or PZA/FCA, try name only to allow TCGplayer to match
                        f.write(f"1 {clean_name}\n")
                    else:
                        f.write(f"1 {clean_name} [{final_set}]\n")

            logger.info(f"TCGplayer import file saved to: {os.path.abspath(filename)}")
        except Exception as e:
            logger.error(f"Failed to generate TCGplayer import file: {e}")

    def generate_pdf(self, df, high_label):
        """Generates both PDF and PNG report images with improved layout and card name fitting."""
        try:
            import matplotlib.pyplot as plt
            from matplotlib.backends.backend_pdf import PdfPages
        except ImportError:
            logger.error("Matplotlib is required for PDF/PNG. Install with: pip install matplotlib")
            return

        timestamp = datetime.now().strftime('%Y-%m-%d_%H-%M')
        pdf_filename = f"MTG_Dips_{timestamp}.pdf"
        png_filename = f"MTG_Dips_{timestamp}.png"
        logger.info(f"Generating report files: {pdf_filename} and {png_filename}")

        pdf_df = df.copy()
        pdf_df['Price'] = pdf_df['Price'].map('${:,.2f}'.format)
        pdf_df[high_label] = pdf_df[high_label].map('${:,.2f}'.format)
        pdf_df['Drop %'] = pdf_df['Drop %'].map('{:.1f}%'.format)

        # Better Page sizing logic: Standard Letter (8.5 x 11) is 1:1.3 ratio
        # We'll use 8.5 x 11 and scale the table to fit
        fig, ax = plt.subplots(figsize=(8.5, 11))
        ax.axis('tight')
        ax.axis('off')
        
        title = f"MTG Staple Dips Report\n{datetime.now().strftime('%Y-%m-%d')}\n" \
                f"({high_label}, ${self.min_drop_dollars} min drop, {self.min_drop_pct}% min drop)"
        plt.title(title, fontsize=12, fontweight='bold', pad=30)

        # Place the table at the top of the page rather than centering it
        table = ax.table(
            cellText=pdf_df.values,
            colLabels=pdf_df.columns,
            cellLoc='left', # Left align names for readability
            loc='upper center'
        )

        # Professional styling
        table.auto_set_font_size(False)
        table.set_fontsize(8)
        
        # Adjust column widths: Card Name needs more space (Column 0)
        col_widths = [0.35, 0.1, 0.15, 0.12, 0.15, 0.13]
        for i, width in enumerate(col_widths):
            for row in range(len(pdf_df) + 1):
                table.get_celld()[(row, i)].set_width(width)

        # Color and borders
        for (row, col), cell in table.get_celld().items():
            cell.set_linewidth(0.5)
            if row == 0:
                cell.set_text_props(weight='bold', color='white')
                cell.set_facecolor('#2d3436')
                cell.set_text_props(ha='center')
            else:
                if row % 2 == 0:
                    cell.set_facecolor('#f5f6fa')
                if col > 0: # Center numerical data
                    cell.set_text_props(ha='center')

        # Save as PDF
        with PdfPages(pdf_filename) as pdf:
            pdf.savefig(fig, bbox_inches='tight', dpi=300)
        logger.info(f"PDF successfully saved to: {os.path.abspath(pdf_filename)}")
        
        # Save as PNG (high resolution for social media)
        fig.savefig(png_filename, bbox_inches='tight', dpi=300, format='png', facecolor='white')
        logger.info(f"PNG successfully saved to: {os.path.abspath(png_filename)}")
        
        plt.close()

    def get_market_dips(self, export_pdf=True, export_tcg=True):
        top16_set, rec_set = self._get_staples()
        all_staples = top16_set.union(rec_set)
        if not all_staples: return

        ids_data = self._get_json_data("https://mtgjson.com/api/v5/AllIdentifiers.json", "AllIdentifiers", ttl_hours=self.TTL_IDENTIFIERS)
        prices_data = self._get_json_data("https://mtgjson.com/api/v5/AllPrices.json", "AllPrices", ttl_hours=self.TTL_PRICES)
        
        date_limit = (datetime.now() - timedelta(days=self.high_window_days)).strftime("%Y-%m-%d")
        high_label = f"{self.high_window_days}D High"

        logger.info(f"Filtering {len(all_staples)} staples for price analysis...")
        card_to_best = {}
        staple_uuids = []
        for uuid, card in ids_data.items():
            name = card.get("name", "").lower()
            if name in all_staples and card.get("setCode") not in self.illegal_sets:
                staple_uuids.append((uuid, card.get("name"), name, card.get("setCode")))
        
        del ids_data

        for uuid, display_name, lower_name, set_code in tqdm(staple_uuids, desc="Pricing Analysis"):
            hist = prices_data.get(uuid, {}).get("paper", {}).get("tcgplayer", {}).get("retail", {}).get("normal", {})
            if hist:
                latest_date = max(hist.keys())
                curr_price = float(hist[latest_date])
                
                if curr_price > 0.40:
                    if lower_name not in card_to_best or curr_price < card_to_best[lower_name]['curr']:
                        valid_hist = [float(v) for d, v in hist.items() if d >= date_limit]
                        if valid_hist:
                            card_to_best[lower_name] = {
                                'curr': curr_price, 
                                'high': max(valid_hist), 
                                'set': set_code, 
                                'name': display_name, 
                                'source': "edhtop16" if lower_name in top16_set else "edhrec"
                            }
        
        del prices_data
        
        results = []
        for data in card_to_best.values():
            drop_amt = data['high'] - data['curr']
            drop_pct = (drop_amt / data['high']) * 100
            
            if drop_pct >= self.min_drop_pct and drop_amt >= self.min_drop_dollars:
                results.append({
                    "Card Name": data['name'], "Set": data['set'], "Source": data['source'],
                    "Price": data['curr'], high_label: data['high'], "Drop %": round(drop_pct, 2)
                })

        df = pd.DataFrame(results)
        if df.empty:
            logger.info("No significant dips found matching current filters.")
            return

        report_df = df.sort_values("Drop %", ascending=False).copy()
        
        print_df = report_df.copy()
        print_df['Price'] = print_df['Price'].map('${:,.2f}'.format)
        print_df[high_label] = print_df[high_label].map('${:,.2f}'.format)
        logger.info(f"\n[REPORT] Merged MTG Staple Dips:")
        print(print_df[["Card Name", "Set", "Source", "Price", high_label, "Drop %"]].to_string(index=False))

        if export_pdf:
            self.generate_pdf(report_df, high_label)
        if export_tcg:
            self.generate_tcgplayer_import(report_df)

if __name__ == "__main__":
    start = datetime.now()
    MTGDipDetector().get_market_dips(export_pdf=True, export_tcg=True)
    logger.info(f"Total Run Time: {datetime.now() - start}")
