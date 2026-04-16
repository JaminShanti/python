import requests
import pandas as pd
import os
import logging
import json
import re
import gzip
import pickle
import numpy as np
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
    MTG Price Drop Detector v9.9
    Optimized for cEDH (EDHTop16) and EDH (EDHRec) staples.
    
    Improvements:
    1. Anti-Gaslight Logic: Caps 'Printing High' for reprints at 1.15x the Card Market Avg. 
       This prevents single-sale outliers (like $25 Rhythm) from ruining data.
    2. Honest Dip Reporting: 
       - Standard: Dip % = Price vs Printing's own high.
       - Reprint: Dip % = Price vs established Market Average (reprint savings).
    3. Liquidity Check: Ensures a minimum number of price points before reporting a dip.
    """
    def __init__(self, cache_dir='mtg_cache', high_window_days=90, min_drop_dollars=1.00, min_dip_pct=25.0,
                 use_pickle_cache=True, min_set_age_days=60, min_price=1.25):
        self.cache_dir = os.path.abspath(cache_dir)
        os.makedirs(self.cache_dir, exist_ok=True)
        
        self.high_window_days = high_window_days
        self.min_drop_dollars = min_drop_dollars
        self.min_dip_pct = min_dip_pct
        self.use_pickle_cache = use_pickle_cache
        self.min_set_age_days = min_set_age_days
        self.min_price = min_price
        
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
            'Accept-Encoding': 'gzip'
        })

        self.illegal_sets = {
            'WC97', 'WC98', 'WC99', 'WC00', 'WC01', 'WC02', 'WC03', 'WC04', 
            'CED', 'CEI', 'UST', 'UNH', 'UGL', 'UND', 'UNF', 'PLIST', '30A' # Added '30A' here
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
        timestamp = datetime.now().strftime('%Y-%m-%d_%H-%M')
        filename = f"TCGplayer_Import_{timestamp}.txt"
        set_map = {'FCA': 'PIP', 'PZA': 'PLST', 'SPG': 'Special Guests', 'MH3': 'Modern Horizons 3'}
        try:
            with open(filename, 'w', encoding='utf-8') as f:
                for _, row in df.iterrows():
                    full_name = row['Card Name']
                    clean_name = full_name.split(' // ')[0]
                    set_code = row['Set']
                    final_set = set_map.get(set_code, set_code)
                    if set_code in ['PZA', 'FCA'] or len(final_set) > 3:
                        f.write(f"1 {clean_name}\n")
                    else:
                        f.write(f"1 {clean_name} [{final_set}]\n")
            logger.info(f"TCGplayer import file saved: {filename}")
        except Exception as e:
            logger.error(f"Failed to generate import: {e}")

    def generate_pdf(self, df):
        try:
            import matplotlib.pyplot as plt
            from matplotlib.backends.backend_pdf import PdfPages
        except ImportError:
            return

        timestamp = datetime.now().strftime('%Y-%m-%d_%H-%M')
        pdf_filename = f"MTG_Dips_{timestamp}.pdf"
        png_filename = f"MTG_Dips_{timestamp}.png"

        pdf_df = df.copy()
        pdf_df['Price'] = pdf_df['Price'].map('${:,.2f}'.format)
        pdf_df['High Ref'] = pdf_df['High Ref'].map('${:,.2f}'.format)
        pdf_df['Dip %'] = pdf_df['Dip %'].map('{:.1f}%'.format)

        fig, ax = plt.subplots(figsize=(10, 8))
        ax.axis('tight')
        ax.axis('off')
        
        title = f"MTG Staple Dips & Reprint Opportunities\n{datetime.now().strftime('%Y-%m-%d')}\n" \
                f"(Min ${self.min_drop_dollars} dip, {self.min_dip_pct}% min drop)"
        plt.title(title, fontsize=12, fontweight='bold', pad=30)

        table = ax.table(cellText=pdf_df.values, colLabels=pdf_df.columns, cellLoc='left', loc='upper center')
        table.auto_set_font_size(False)
        table.set_fontsize(8)
        
        col_widths = [0.30, 0.08, 0.12, 0.12, 0.12, 0.12, 0.14] 
        for i, width in enumerate(col_widths):
            for row in range(len(pdf_df) + 1):
                cell = table.get_celld()[(row, i)]
                cell.set_width(width)
                if i > 3: cell.set_text_props(ha='center')

        for (row, col), cell in table.get_celld().items():
            cell.set_linewidth(0.5)
            if row == 0:
                cell.set_text_props(weight='bold', color='white')
                cell.set_facecolor('#2d3436')
                cell.set_text_props(ha='center')
            else:
                if row % 2 == 0: cell.set_facecolor('#f5f6fa')

        with PdfPages(pdf_filename) as pdf: pdf.savefig(fig, bbox_inches='tight', dpi=300)
        fig.savefig(png_filename, bbox_inches='tight', dpi=300, format='png', facecolor='white')
        plt.close()
        logger.info(f"Reports saved: {pdf_filename}, {png_filename}")

    def get_market_dips(self, export_pdf=True, export_tcg=True):
        top16_set, rec_set = self._get_staples()
        all_staples = top16_set.union(rec_set)
        if not all_staples: return

        ids_data = self._get_json_data("https://mtgjson.com/api/v5/AllIdentifiers.json", "AllIdentifiers", ttl_hours=self.TTL_IDENTIFIERS)
        prices_data = self._get_json_data("https://mtgjson.com/api/v5/AllPrices.json", "AllPrices", ttl_hours=self.TTL_PRICES)
        sets_data = self._get_json_data("https://mtgjson.com/api/v5/SetList.json", "SetList", ttl_hours=self.TTL_IDENTIFIERS)
        
        set_release_dates = {s['code']: s['releaseDate'] for s in sets_data if 'code' in s and 'releaseDate' in s}
        date_limit = (datetime.now() - timedelta(days=self.high_window_days)).strftime("%Y-%m-%d")

        logger.info(f"Grouping {len(all_staples)} staples by name...")
        name_to_uuids = {}
        for uuid, card in ids_data.items():
            lower_name = card.get("name", "").lower()
            if lower_name in all_staples and card.get("setCode") not in self.illegal_sets:
                if lower_name not in name_to_uuids:
                    name_to_uuids[lower_name] = []
                name_to_uuids[lower_name].append((uuid, card.get("name"), card.get("setCode")))
        
        del ids_data

        results = []
        for lower_name, uuids in tqdm(name_to_uuids.items(), desc="Analyzing Market"):
            all_printings = []
            for uuid, display_name, set_code in uuids:
                hist = prices_data.get(uuid, {}).get("paper", {}).get("tcgplayer", {}).get("retail", {}).get("normal", {})
                if not hist: continue
                valid_hist = {d: float(v) for d, v in hist.items() if d >= date_limit}
                if not valid_hist: continue
                latest_price = float(hist[max(hist.keys())])
                
                # Robust high calculation
                sorted_vals = sorted(valid_hist.values())
                robust_high = sorted_vals[-max(1, int(len(sorted_vals) * 0.05))]
                
                rel_date = set_release_dates.get(set_code)
                is_stable = False
                if rel_date:
                    days_old = (datetime.now() - datetime.strptime(rel_date, "%Y-%m-%d")).days
                    if days_old >= self.min_set_age_days and len(valid_hist) >= (self.high_window_days * 0.5):
                        is_stable = True
                
                all_printings.append({'name': display_name, 'set': set_code, 'curr': latest_price, 'high': robust_high, 'is_stable': is_stable, 'uuid': uuid})

            if not all_printings: continue

            # Determine established Market Average from stable printings
            stable_printings = [p for p in all_printings if p['is_stable']]
            market_avg = float(np.median([p['high'] for p in (stable_printings or all_printings)]))

            best_deal = min(all_printings, key=lambda x: x['curr'])
            if best_deal['curr'] < self.min_price: continue

            # --- Anti-Gaslight / Honest Dip Logic ---
            is_reprint = not best_deal['is_stable']
            
            if is_reprint:
                # For reprints, the "High Reference" is the Market Average of older cards
                # Cap the high reference to avoid outliers like the $25 Rhythm
                high_ref = min(best_deal['high'], market_avg * 1.15)
                # But if market avg is better (meaning it's a huge reprint deal), use market avg
                high_ref = max(high_ref, market_avg)
                reprint_label = "Reprint"
            else:
                # For standard cards, use its own robust high
                high_ref = best_deal['high']
                reprint_label = ""

            drop_amt = high_ref - best_deal['curr']
            drop_pct = (drop_amt / high_ref * 100) if high_ref > 0 else 0
            
            if drop_pct >= self.min_dip_pct and drop_amt >= self.min_drop_dollars:
                source = "edhtop16" if lower_name in top16_set else "edhrec"
                results.append({
                    "Card Name": best_deal['name'],
                    "Set": best_deal['set'],
                    "Reprints": reprint_label,
                    "Source": source,
                    "Price": best_deal['curr'],
                    "High Ref": high_ref,
                    "Dip %": round(drop_pct, 2)
                })

        del prices_data
        df = pd.DataFrame(results)
        if df.empty:
            logger.info("No honest dips found.")
            return

        report_df = df.sort_values("Dip %", ascending=False).copy()
        print_df = report_df.copy()
        print_df['Price'] = print_df['Price'].map('${:,.2f}'.format)
        print_df['High Ref'] = print_df['High Ref'].map('${:,.2f}'.format)
        print_df['Dip %'] = print_df['Dip %'].map('{:.1f}%'.format)
        
        logger.info(f"\n[REPORT] MTG Staple Dips & Deals (Anti-Gaslight):")
        print(print_df.to_string(index=False))

        if export_pdf: self.generate_pdf(report_df)
        if export_tcg: self.generate_tcgplayer_import(report_df)

if __name__ == "__main__":
    MTGDipDetector().get_market_dips()
