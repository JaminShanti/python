import gzip, json, logging, os, pickle, re
from datetime import datetime, timedelta
import numpy as np, pandas as pd, requests
from bs4 import BeautifulSoup
from tqdm import tqdm

try:
    import matplotlib.pyplot as plt
    from matplotlib.backends.backend_pdf import PdfPages

    HAS_MATPLOTLIB = True
except ImportError:
    HAS_MATPLOTLIB = False

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)


class MTGDipDetector:
    def __init__(self, cache_dir='mtg_cache', high_window=45, min_dip=25.0, min_drop=1.00, min_set_age=60,
                 min_price=1.25):
        self.cache_dir = os.path.abspath(cache_dir)
        os.makedirs(self.cache_dir, exist_ok=True)
        self.high_window, self.min_dip, self.min_drop, self.min_set_age, self.min_price = high_window, min_dip, min_drop, min_set_age, min_price
        self.session = requests.Session()
        self.session.headers.update({'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'})
        self.illegal_sets = {'WC97', 'WC98', 'WC99', 'WC00', 'WC01', 'WC02', 'WC03', 'WC04', 'CED', 'CEI', 'UST', 'UNH',
                             'UGL', 'UND', 'UNF', 'PLIST', '30A'}
        self.ui_noise = {"staples", "rank", "count", "percent", "commander", "partner", "decklist", "filter", "share",
                         "name", "price", "color", "type", "next", "previous", "search", "menu", "mountain", "forest",
                         "island", "swamp", "plains", "vibrance", "themes", "reprints", "sets", "mana", "curve",
                         "average", "recent"}

    def _fast_harvest(self, url, found_set):
        try:
            r = self.session.get(url, timeout=20)
            if r.status_code == 200:
                soup = BeautifulSoup(r.text, 'html.parser')
                for el in soup.find_all(['a', 'td', 'span', 'div']):
                    text = el.get_text(strip=True).lower()
                    if 3 < len(text) < 45 and not any(c.isdigit() for c in text) and text not in self.ui_noise:
                        found_set.add(text)
                return len(found_set) >= 25
        except Exception:
            pass
        return False

    def _get_staples(self):
        cache = os.path.join(self.cache_dir, "staple_cache.json")
        if os.path.exists(cache) and (datetime.now() - datetime.fromtimestamp(os.path.getmtime(cache))) < timedelta(
                hours=24):
            with open(cache, 'r') as f:
                d = json.load(f)
                return set(d['top16']), set(d['rec'])
        logger.info("Harvesting fresh staples...")
        t16, rec = set(), set()
        self._fast_harvest("https://edhtop16.com/staples", t16)
        self._fast_harvest("https://edhrec.com/top", rec)
        with open(cache, 'w') as f: json.dump({'top16': list(t16), 'rec': list(rec)}, f)
        return t16, rec

    def _get_json(self, url, filename):
        gz, bin = os.path.join(self.cache_dir, filename + ".gz"), os.path.join(self.cache_dir, filename + ".pkl")
        if os.path.exists(bin) and (datetime.now() - datetime.fromtimestamp(os.path.getmtime(bin))) < timedelta(
                hours=24):
            with open(bin, 'rb') as f: return pickle.load(f)
        logger.info(f"Downloading {filename}...")
        r = self.session.get(url + ".gz", stream=True)
        with open(gz, 'wb') as f:
            for chunk in r.iter_content(chunk_size=1024 * 1024): f.write(chunk)
        with gzip.open(gz, 'rt', encoding='utf-8') as f:
            d = json.load(f).get("data", {})
        with open(bin, 'wb') as f:
            pickle.dump(d, f)
        return d

    def generate_tcg_import(self, df):
        fname = f"TCGplayer_Import_{datetime.now().strftime('%Y-%m-%d_%H-%M')}.txt"
        with open(fname, 'w', encoding='utf-8') as f:
            for _, r in df.iterrows():
                card_name = r['Card Name'].split(' // ')[0].strip()
                card_name = card_name.replace('’', "'").replace('“', '"').replace('”', '"')
                # Only write card name, without set information, for maximum TCGplayer compatibility
                f.write(f"1 {card_name}\n")
        logger.info(f"TCGplayer import saved: {fname}")

    def generate_pdf(self, df):
        if not HAS_MATPLOTLIB: return
        timestamp = datetime.now().strftime('%Y-%m-%d_%H-%M')
        pdf_f = f"MTG_Dips_{timestamp}.pdf"
        png_f = f"MTG_Dips_{timestamp}.png"

        fig, ax = plt.subplots(figsize=(11, 8));
        ax.axis('tight');
        ax.axis('off')
        pdf_df = df.copy()
        for col in ['Price', 'High Ref']:
            if col in pdf_df.columns: pdf_df[col] = pdf_df[col].map('${:,.2f}'.format)
        if 'Dip %' in pdf_df.columns: pdf_df['Dip %'] = pdf_df['Dip %'].map('{:.1f}%'.format)

        plt.title(f"MTG Staple Dips (TCGplayer Only)\n{datetime.now().strftime('%Y-%m-%d')}", fontsize=12,
                  fontweight='bold', pad=20)
        table = ax.table(cellText=pdf_df.values, colLabels=pdf_df.columns, cellLoc='left', loc='upper center')
        table.auto_set_font_size(False);
        table.set_fontsize(8)
        for i, w in enumerate([0.28, 0.08, 0.12, 0.10, 0.10, 0.10, 0.10, 0.12]):
            for row in range(len(pdf_df) + 1): table.get_celld()[(row, i)].set_width(w)

        # Save PDF
        with PdfPages(pdf_f) as pdf:
            pdf.savefig(fig, bbox_inches='tight', dpi=300);
        logger.info(f"PDF saved: {pdf_f}")

        # Save PNG
        plt.savefig(png_f, bbox_inches='tight', dpi=300)
        logger.info(f"PNG saved: {png_f}")

        plt.close(fig)

    def get_market_dips(self):
        t16, rec = self._get_staples();
        all_s = t16.union(rec)
        ids = self._get_json("https://mtgjson.com/api/v5/AllIdentifiers.json", "AllIdentifiers")
        prices = self._get_json("https://mtgjson.com/api/v5/AllPrices.json", "AllPrices")
        sets = self._get_json("https://mtgjson.com/api/v5/SetList.json", "SetList")
        rel_dates = {s['code']: s['releaseDate'] for s in sets if 'code' in s and 'releaseDate' in s}
        date_limit = (datetime.now() - timedelta(days=self.high_window)).strftime("%Y-%m-%d")

        n_to_p = {}
        for uuid, c in ids.items():
            name = c.get("name", "").lower()
            if name in all_s and c.get("language") == "English" and c.get("setCode") not in self.illegal_sets:
                if name not in n_to_p: n_to_p[name] = []
                n_to_p[name].append({'uuid': uuid, 'name': c.get("name"), 'set': c.get("setCode")})

        results = []
        for name, printings in tqdm(n_to_p.items(), desc="Analyzing TCGplayer"):
            proc = []
            for p in printings:
                hist = prices.get(p['uuid'], {}).get("paper", {}).get("tcgplayer", {}).get("retail", {}).get("normal",
                                                                                                             {})
                if not hist: continue
                v_hist = {d: float(v) for d, v in hist.items() if d >= date_limit}
                if not v_hist: continue
                curr, l_date = float(hist[max(hist.keys())]), max(hist.keys())
                vals = sorted(v_hist.values());
                med = float(np.median(vals))
                high = vals[int(len(vals) * 0.80)]
                if high > med * 2.5: high = med * 1.5
                stable = (datetime.now() - datetime.strptime(rel_dates.get(p['set'], "2000-01-01"),
                                                             "%Y-%m-%d")).days >= self.min_set_age
                fresh = (datetime.now() - datetime.strptime(l_date, "%Y-%m-%d")).days <= 10
                proc.append({**p, 'curr': curr, 'high': high, 'stable': stable, 'fresh': fresh})

            if not proc: continue
            g_med = np.median([p['high'] for p in proc])
            for p in proc:
                if p['high'] > g_med * 3.0: p['high'] = g_med

            stable_pool = [p for p in proc if p['stable']]
            st_ref = min([p for p in stable_pool if p['high'] <= np.median([p['high'] for p in stable_pool]) * 2.0],
                         key=lambda x: x['high']) if stable_pool else min(proc, key=lambda x: x['curr'])

            for p in proc:
                if not p['fresh']: continue
                analysis, h_ref, r_set = None, 0, ""
                if not p['stable']:
                    if p['curr'] <= st_ref['high'] * (1 - self.min_dip / 100): analysis, h_ref, r_set = "Reprint", \
                        st_ref['high'], st_ref['set']
                elif p['curr'] <= p['high'] * (1 - self.min_dip / 100):
                    analysis, h_ref, r_set = "", p['high'], p['set']

                if analysis is not None:
                    drop_amt = h_ref - p['curr']
                    if (drop_amt / h_ref * 100) >= self.min_dip and drop_amt >= self.min_drop:
                        results.append({"Card Name": p['name'], "Set": p['set'], "Analysis": analysis,
                                        "Source": "edhtop16" if name in t16 else "edhrec", "Price": p['curr'],
                                        "High Ref": h_ref, "Ref Set": "" if r_set == p['set'] else r_set,
                                        "Dip %": round((drop_amt / h_ref * 100), 2)})

        df = pd.DataFrame(results)
        if df.empty: return logger.info("No dips found.")
        df = df.sort_values("Dip %", ascending=False).drop_duplicates(subset=['Card Name', 'Analysis'])
        print(f"\n[REPORT] MTG Staple Dips (Anti-Gaslight):\n{df.to_string(index=False)}")
        self.generate_pdf(df);
        self.generate_tcg_import(df)


if __name__ == "__main__":
    MTGDipDetector().get_market_dips()
