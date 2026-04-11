import requests
import pandas as pd
import os
import logging
import json
import re
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class MTGDipDetector:
    """
    MTG Price Drop Detector optimized for cEDH and EDH Staples.
    Filters by cards found on edhtop16.com/staples and edhrec.com/top.
    Excludes basic lands.
    """
    def __init__(self, cache_dir='mtg_cache'):
        self.cache_dir = os.path.abspath(cache_dir)
        os.makedirs(self.cache_dir, exist_ok=True)
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'MTG-Staple-Scanner/5.1'
        })
        # Basic lands blocklist
        self.excluded_cards = {"mountain", "forest", "island", "swamp", "plains"}

    def _get_edhtop16_staples(self):
        """Fetches cEDH staples from EDHTop16."""
        logger.info("Fetching cEDH staples from EDHTop16...")
        url = "https://edhtop16.com/api/staples"
        try:
            response = self.session.get(url, timeout=30)
            if response.status_code == 200:
                return [item.get('name') for item in response.json() if item.get('name')]
        except Exception as e:
            logger.warning(f"EDHTop16 API failed: {e}")
        return []

    def _get_edhrec_top_cards(self):
        """Fetches top cards from EDHREC."""
        logger.info("Fetching top EDH cards from EDHREC...")
        url = "https://edhrec.com/top"
        try:
            response = self.session.get(url, timeout=30)
            if response.status_code == 200:
                content = response.text
                matches = re.findall(r'"name":"(.*?)"', content)
                if matches:
                    return list(set(matches))
        except Exception as e:
            logger.warning(f"EDHREC fetch failed: {e}")
        return []

    def _get_json_data(self, url, filename):
        cache_path = os.path.join(self.cache_dir, filename)
        if os.path.exists(cache_path):
            file_age = datetime.now() - datetime.fromtimestamp(os.path.getmtime(cache_path))
            if file_age < timedelta(hours=24):
                logger.info(f"Loading {filename} from SSD cache...")
                try:
                    with open(cache_path, 'r', encoding='utf-8') as f:
                        return json.load(f).get("data", {})
                except: pass

        logger.info(f"Downloading {filename}...")
        try:
            response = self.session.get(url, timeout=600)
            response.raise_for_status()
            full_data = response.json()
            with open(cache_path, 'w', encoding='utf-8') as f:
                json.dump(full_data, f)
            return full_data.get("data", {})
        except Exception as e:
            logger.error(f"Error retrieving {filename}: {e}")
            return {}

    def process_card(self, uuid, name, prices_data, six_months_ago_str):
        if uuid not in prices_data: return None
        try:
            paper_data = prices_data[uuid].get("paper", {})
            price_history = paper_data.get("tcgplayer", {}).get("retail", {}).get("normal", {})
            if not price_history:
                price_history = paper_data.get("cardkingdom", {}).get("retail", {}).get("normal", {})
            
            if not price_history: return None

            recent_prices = [float(p) for d, p in price_history.items() if d >= six_months_ago_str]
            if not recent_prices: return None

            current_price = recent_prices[-1]
            six_month_high = max(recent_prices)

            if six_month_high > current_price and current_price > 0:
                drop_pct = ((six_month_high - current_price) / six_month_high) * 100
                if drop_pct > 5:
                    return {
                        "Card Name": name,
                        "Current Price": current_price,
                        "6-Month High": six_month_high,
                        "Drop %": round(drop_pct, 2)
                    }
        except: pass
        return None

    def get_market_dips(self):
        # 1. Aggregate Staples from both sites
        edhtop16 = self._get_edhtop16_staples()
        edhrec = self._get_edhrec_top_cards()
        
        # Combine and filter out basic lands
        staple_names_set = {
            n.lower() for n in (edhtop16 + edhrec) 
            if n and n.lower() not in self.excluded_cards
        }
        logger.info(f"Aggregated {len(staple_names_set)} unique staple names (basics excluded).")

        # 2. Fetch MTGJSON Data
        ids_url = "https://mtgjson.com/api/v5/AllIdentifiers.json"
        prices_url = "https://mtgjson.com/api/v5/AllPrices.json"

        identifiers_data = self._get_json_data(ids_url, "AllIdentifiers.json")
        prices_data = self._get_json_data(prices_url, "AllPrices.json")

        if not identifiers_data or not prices_data:
            logger.error("Could not load MTGJSON data.")
            return

        # 3. Filter IDs by Staples
        logger.info("Filtering metadata for aggregated staples...")
        target_uuids = {}
        for uuid, card in identifiers_data.items():
            card_name = card.get("name", "").lower()
            if card_name in staple_names_set:
                target_uuids[uuid] = card.get("name")
        
        # Fallback to Reserved List (excluding basics) if both sites fail
        if not target_uuids:
            logger.warning("No staples found via sites. Falling back to Reserved List...")
            for uuid, card in identifiers_data.items():
                card_name = card.get("name", "").lower()
                if card.get("isReserved") and card.get("legalities", {}).get("commander") == "Legal" and card_name not in self.excluded_cards:
                    target_uuids[uuid] = card.get("name")
        
        del identifiers_data

        # 4. Parallel Processing
        six_months_ago_str = (datetime.now() - timedelta(days=180)).strftime("%Y-%m-%d")
        logger.info(f"Analyzing {len(target_uuids)} cards across CPU cores...")
        
        results = []
        with ThreadPoolExecutor(max_workers=os.cpu_count() * 2) as executor:
            futures = [
                executor.submit(self.process_card, uuid, name, prices_data, six_months_ago_str) 
                for uuid, name in target_uuids.items()
            ]
            for f in as_completed(futures):
                res = f.result()
                if res: results.append(res)

        # 5. Output
        df = pd.DataFrame(results)
        if not df.empty:
            df = df.sort_values("Drop %", ascending=False).drop_duplicates(subset=["Card Name"])
            df['Current Price'] = df['Current Price'].map('${:,.2f}'.format)
            df['6-Month High'] = df['6-Month High'].map('${:,.2f}'.format)
            
            logger.info("\nTop 6-Month Market Dips (Aggregated Staples):")
            print(df.head(50).to_string(index=False))
        else:
            logger.info("No significant dips found.")

if __name__ == "__main__":
    start = datetime.now()
    MTGDipDetector().get_market_dips()
    logger.info(f"Total Run Time: {datetime.now() - start}")
