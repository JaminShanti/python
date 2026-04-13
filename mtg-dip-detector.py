import requests
import pandas as pd
import os
import logging
import json
import re
import gzip
import time
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
    MTG Price Drop Detector v8.5
    Optimized for cEDH (EDHTop16) and EDH (EDHRec) staples.
    Uses BeautifulSoup4 for robust extraction.
    Merged reporting sorted by Drop %, with configurable filters.
    Exclusively uses TCGplayer retail pricing.
    """
    def __init__(self, cache_dir='mtg_cache', high_window_days=90, min_drop_dollars=0.75, min_drop_pct=35.0):
        """
        Initialize the detector with configurable filters.
        
        :param cache_dir: Directory for storing cache files.
        :param high_window_days: Number of days to look back for the "High" price (default 90).
        :param min_drop_dollars: Minimum dollar amount of drop to include in results (default 0.75).
        :param min_drop_pct: Minimum percentage drop to include in results (default 35.0).
        """
        self.cache_dir = os.path.abspath(cache_dir)
        os.makedirs(self.cache_dir, exist_ok=True)
        
        self.high_window_days = high_window_days
        self.min_drop_dollars = min_drop_dollars
        self.min_drop_pct = min_drop_pct
        
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
            'Accept-Encoding': 'gzip'
        })
        self.excluded_cards = {"mountain", "forest", "island", "swamp", "plains"}
        self.illegal_sets = {
            'WC97', 'WC98', 'WC99', 'WC00', 'WC01', 'WC02', 'WC03', 'WC04', 
            'CED', 'CEI', 'UST', 'UNH', 'UGL', 'UND', 'UNF', 'PLIST'
        }
        
        # Define TTLs in hours
        self.TTL_STAPLES = 24       # Daily refresh for top lists
        self.TTL_IDENTIFIERS = 168  # Weekly refresh (rarely changes)
        self.TTL_PRICES = 24        # Daily refresh for pricing data

    def _extract_names_recursively(self, obj, found_set):
        if isinstance(obj, dict):
            for k, v in obj.items():
                if k == 'name' and isinstance(v, str) and 3 < len(v) < 45:
                    if not any(char.isdigit() for char in v): # Card names rarely have numbers
                        found_set.add(v.lower().strip())
                else:
                    self._extract_names_recursively(v, found_set)
        elif isinstance(obj, list):
            for item in obj:
                self._extract_names_recursively(item, found_set)

    def _fast_harvest(self, url, found_set):
        """Extracts data using BeautifulSoup and JSON blobs."""
        try:
            r = self.session.get(url, timeout=20)
            if r.status_code == 200:
                # 1. Try Next.js JSON blob
                json_match = re.search(r'id="__NEXT_DATA__".*?>(.*?)</script>', r.text, re.DOTALL)
                if json_match:
                    data = json.loads(json_match.group(1))
                    self._extract_names_recursively(data, found_set)
                
                # 2. BeautifulSoup extraction
                soup = BeautifulSoup(r.text, 'html.parser')
                # Look for names in common container elements
                for element in soup.find_all(['a', 'td', 'span', 'div']):
                    text = element.get_text(strip=True)
                    if 3 < len(text) < 45 and not any(c.isdigit() for c in text):
                        # Filter out common UI noise
                        if text.lower() not in {"staples", "rank", "count", "percent", "commander", "partner", "decklist", "filter", "share", "name", "price", "color", "type"}:
                            found_set.add(text.lower())
                return len(found_set) >= 25
        except Exception as e:
            logger.warning(f"Harvest failed for {url}: {e}")
        return False

    def _get_staples(self):
        edhtop16_staples = set()
        edhrec_staples = set()
        
        cache_file = os.path.join(self.cache_dir, "staple_cache.json")
        if os.path.exists(cache_file):
            file_age = datetime.now() - datetime.fromtimestamp(os.path.getmtime(cache_file))
            if file_age < timedelta(hours=self.TTL_STAPLES):
                try:
                    with open(cache_file, 'r', encoding='utf-8') as f:
                        data = json.load(f)
                        t16, rec = data.get('top16', []), data.get('rec', [])
                        if len(t16) >= 25:
                            logger.info(f"Using cached staples: {len(t16)} cEDH, {len(rec)} EDH.")
                            return set(t16), set(rec)
                except: pass

        logger.info("Harvesting staples via BS4...")
        self._fast_harvest("https://edhtop16.com/staples", edhtop16_staples)
        self._fast_harvest("https://edhrec.com/top", edhrec_staples)

        # Post-process cleanup
        noise = {"staples", "rank", "count", "percent", "commander", "partner", "decklist", "filter", "share", "name", "price", "color", "type", "next", "previous", "search", "menu"}
        edhtop16_staples = {s for s in edhtop16_staples if s not in noise}

        if len(edhtop16_staples) >= 25 and len(edhrec_staples) >= 25:
            with open(cache_file, 'w', encoding='utf-8') as f:
                json.dump({'top16': list(edhtop16_staples), 'rec': list(edhrec_staples)}, f)
            logger.info(f"Identified {len(edhtop16_staples)} cEDH and {len(edhrec_staples)} EDH staples.")
        else:
            error_msg = f"Discovery incomplete: Found {len(edhtop16_staples)} cEDH and {len(edhrec_staples)} EDH."
            logger.error(error_msg)
            if len(edhtop16_staples) < 25:
                raise RuntimeError(f"FATAL: Insufficient EDHTop16 results ({len(edhtop16_staples)}). Halted for investigation. {error_msg}")

        return edhtop16_staples, edhrec_staples

    def _get_json_data(self, url, filename, ttl_hours=24):
        gz_filename = filename + ".gz"
        cache_path = os.path.join(self.cache_dir, gz_filename)
        if os.path.exists(cache_path):
            file_age = datetime.now() - datetime.fromtimestamp(os.path.getmtime(cache_path))
            if file_age < timedelta(hours=ttl_hours):
                logger.info(f"Loading {gz_filename} from cache (age: {file_age})...")
                with gzip.open(cache_path, 'rt', encoding='utf-8') as f:
                    return json.load(f).get("data", {})
        
        gz_url = url + ".gz"
        logger.info(f"Cache expired or missing. Downloading {gz_url}...")
        r = self.session.get(gz_url, stream=True, timeout=600)
        with open(cache_path, 'wb') as f:
            for chunk in r.iter_content(chunk_size=1024*1024): f.write(chunk)
        with gzip.open(cache_path, 'rt', encoding='utf-8') as f:
            return json.load(f).get("data", {})

    def get_market_dips(self):
        top16_set, rec_set = self._get_staples()
        all_staples = top16_set.union(rec_set)
        
        if not all_staples: return

        # Apply specific TTLs to MTGJSON files
        ids_data = self._get_json_data("https://mtgjson.com/api/v5/AllIdentifiers.json", "AllIdentifiers.json", ttl_hours=self.TTL_IDENTIFIERS)
        prices_data = self._get_json_data("https://mtgjson.com/api/v5/AllPrices.json", "AllPrices.json", ttl_hours=self.TTL_PRICES)
        
        # Define the window for the "High" price based on initialized days
        date_limit = (datetime.now() - timedelta(days=self.high_window_days)).strftime("%Y-%m-%d")
        high_label = f"{self.high_window_days}D High"

        logger.info(f"Filtering for cheapest tournament-legal copies ({high_label})...")
        card_to_best = {}
        for uuid, card in tqdm(ids_data.items(), desc="Filtering"):
            name = card.get("name", "").lower()
            if name in all_staples:
                if card.get("setCode") in self.illegal_sets: continue
                paper = prices_data.get(uuid, {}).get("paper", {})
                
                # Exclusively use TCGplayer retail normal pricing
                hist = paper.get("tcgplayer", {}).get("retail", {}).get("normal", {})
                
                if hist:
                    latest = max(hist.keys())
                    curr = float(hist[latest])
                    if curr > 0.40:
                        if name not in card_to_best or curr < card_to_best[name]['curr']:
                            valid_hist = [float(v) for d, v in hist.items() if d >= date_limit]
                            if valid_hist:
                                # Prioritize edhtop16 if in both
                                source = "edhtop16" if name in top16_set else "edhrec"
                                card_to_best[name] = {
                                    'uuid': uuid, 
                                    'curr': curr, 
                                    'high': max(valid_hist), 
                                    'set': card.get("setCode"), 
                                    'name': card.get("name"), 
                                    'source': source
                                }
        
        del ids_data
        results = []
        for name, data in card_to_best.items():
            curr, high = data['curr'], data['high']
            drop_amt = high - curr
            drop_pct = (drop_amt / high) * 100
            
            # Hide if drop is less than configurable dollar or percentage thresholds
            if drop_pct >= self.min_drop_pct and drop_amt >= self.min_drop_dollars:
                results.append({
                    "Card Name": data['name'], 
                    "Set": data['set'], 
                    "Source": data['source'],
                    "Price": curr, 
                    high_label: high, 
                    "Drop %": round(drop_pct, 2)
                })

        df = pd.DataFrame(results)
        if df.empty:
            logger.info("No significant dips found.")
            return

        # Merged report sorted by Drop % descending
        report_df = df.sort_values("Drop %", ascending=False).copy()
        report_df['Price'] = report_df['Price'].map('${:,.2f}'.format)
        report_df[high_label] = report_df[high_label].map('${:,.2f}'.format)
        
        logger.info(f"\n[REPORT] Merged MTG Staple Dips ({high_label} window, ${self.min_drop_dollars} min drop, {self.min_drop_pct}% min drop):")
        print(report_df[["Card Name", "Set", "Source", "Price", high_label, "Drop %"]].to_string(index=False))

if __name__ == "__main__":
    start = datetime.now()
    # Initialize with default 90 days high window, $0.75 min drop, and 35% min drop
    MTGDipDetector(high_window_days=90, min_drop_dollars=0.75, min_drop_pct=35.0).get_market_dips()
    logger.info(f"Total Run Time: {datetime.now() - start}")
