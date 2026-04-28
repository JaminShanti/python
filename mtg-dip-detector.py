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
    def __init__(self, cache_dir='mtg_cache', high_window=45, min_dip=30.0, min_drop=1.00, min_set_age=60):
        self.cache_dir = os.path.abspath(cache_dir)
        os.makedirs(self.cache_dir, exist_ok=True)
        self.high_window, self.min_dip, self.min_drop, self.min_set_age = high_window, min_dip, min_drop, min_set_age
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
                for text in soup.stripped_strings:
                    low = text.lower()
                    if 3 < len(low) < 45 and not any(c.isdigit() for c in low) and low not in self.ui_noise:
                        found_set.add(low)
                return len(found_set) >= 25
        except Exception: pass
        return False

    def _get_staples(self):
        cache = os.path.join(self.cache_dir, "staple_cache.json")
        if os.path.exists(cache) and (datetime.now() - datetime.fromtimestamp(os.path.getmtime(cache))) < timedelta(hours=24):
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
        if os.path.exists(bin) and (datetime.now() - datetime.fromtimestamp(os.path.getmtime(bin))) < timedelta(hours=24):
            with open(bin, 'rb') as f: return pickle.load(f)
        
        logger.info(f"Downloading {filename}...")
        r = self.session.get(url + ".gz", stream=True)
        with open(gz, 'wb') as f:
            for chunk in r.iter_content(chunk_size=1024*1024): f.write(chunk)
        with gzip.open(gz, 'rt', encoding='utf-8') as f:
            d = json.load(f).get("data", {})
        with open(bin, 'wb') as f: pickle.dump(d, f)
        return d

    def generate_tcg_import(self, df):
        fname = f"TCGplayer_Import_{datetime.now().strftime('%Y-%m-%d_%H-%M')}.txt"
        with open(fname, 'w', encoding='utf-8') as f:
            for name in df['Card Name'].unique():
                clean_name = name.split(' // ')[0].strip().replace('’', "'").replace('“', '"').replace('”', '"')
                f.write(f"1 {clean_name}\n")
        logger.info(f"TCGplayer import saved: {fname}")

    def generate_pdf(self, df):
        if not HAS_MATPLOTLIB: return
        ts = datetime.now().strftime('%Y-%m-%d_%H-%M')
        pdf_f, png_f = f"MTG_Dips_{ts}.pdf", f"MTG_Dips_{ts}.png"

        # Content-based height calculation - ultra tight
        row_h, head_h = 0.35, 0.4
        title_h, foot_h = 0.15, 0.05 
        total_h = (len(df) * row_h) + head_h + title_h + foot_h
        
        fig, ax = plt.subplots(figsize=(15, total_h), facecolor='#1a1a1a') 
        ax.axis('off')

        pdf_df = df.copy()
        pdf_df['Price'] = pdf_df['Price'].map('${:,.2f}'.format)
        pdf_df['High Ref'] = pdf_df['High Ref'].map('${:,.2f}'.format)
        pdf_df['Dip %'] = pdf_df['Dip %'].map('{:.1f}%'.format)

        # Title/Subtitle - using extremely tight y-coords
        plt.suptitle("MTG STAPLE DIPS (TCGplayer Market Data)", fontsize=26, fontweight='bold', color='#ffffff', y=0.985) 
        fig.text(0.5, 0.95, f"Report Generated: {datetime.now().strftime('%B %d, %Y')}", ha='center', fontsize=14, color='#aaaaaa')

        # Table positioning
        t_bot = foot_h / total_h
        t_height = (total_h - title_h - foot_h) / total_h
        table = ax.table(cellText=pdf_df.values, colLabels=pdf_df.columns, cellLoc='center', loc='center',
                         bbox=[0, t_bot, 1, t_height])

        table.auto_set_font_size(False)
        table.set_fontsize(11)
        table.auto_set_column_width(col=list(range(len(pdf_df.columns))))

        for (r, c), cell in table.get_celld().items():
            cell.set_edgecolor('#333333')
            if r == 0:
                cell.set_text_props(weight='bold', color='white')
                cell.set_facecolor('#2e7d32')
            else:
                cell.set_text_props(color='#e0e0e0')
                cell.set_facecolor('#262626' if r % 2 == 0 else '#1e1e1e')
                if c == pdf_df.columns.get_loc('Dip %'): cell.set_text_props(weight='bold', color='#ff5252')
            if c == pdf_df.columns.get_loc('Card Name'): cell.set_text_props(ha='left')

        # Footer - extremely close to bottom
        fig.text(0.5, 0.01, "Data: MTGJSON, EDHREC, EDHTOP16", ha='center', fontsize=10, color='#666666', style='italic')
        
        # Save with zero padding to force the tightest possible crop
        for fmt, f in [('pdf', pdf_f), ('png', png_f)]:
            save_kwargs = {'bbox_inches': 'tight', 'pad_inches': 0.02, 'facecolor': '#1a1a1a', 'dpi': 300}
            if fmt == 'pdf':
                with PdfPages(f) as pdf: pdf.savefig(fig, **save_kwargs)
            else:
                plt.savefig(f, **save_kwargs)
        
        plt.close(fig)
        logger.info(f"Report saved: {png_f}")

    def get_market_dips(self):
        t16, rec = self._get_staples()
        all_s = t16.union(rec)
        ids = self._get_json("https://mtgjson.com/api/v5/AllIdentifiers.json", "AllIdentifiers")
        prices = self._get_json("https://mtgjson.com/api/v5/AllPrices.json", "AllPrices")
        sets = self._get_json("https://mtgjson.com/api/v5/SetList.json", "SetList")
        
        rel_dates = {s['code']: s['releaseDate'] for s in sets if 'code' in s and 'releaseDate' in s}
        date_limit = (datetime.now() - timedelta(days=self.high_window)).strftime("%Y-%m-%d")
        now_dt = datetime.now()

        n_to_p = {}
        for uuid, c in ids.items():
            name = c.get("name", "").lower()
            if name in all_s and c.get("language") == "English" and c.get("setCode") not in self.illegal_sets:
                n_to_p.setdefault(name, []).append({'uuid': uuid, 'name': c.get("name"), 'set': c.get("setCode")})

        results = []
        for name, printings in tqdm(n_to_p.items(), desc="Analyzing Dips"):
            proc = []
            for p in printings:
                try:
                    hist = prices[p['uuid']]['paper']['tcgplayer']['retail']['normal']
                    v_hist = [float(v) for d, v in hist.items() if d >= date_limit]
                    if not v_hist: continue
                    l_date = max(hist.keys())
                    curr, vals = float(hist[l_date]), sorted(v_hist)
                    high, med = vals[int(len(vals) * 0.80)], np.median(vals)
                    if high > med * 2.5: high = med * 1.5
                    stable = (now_dt - datetime.strptime(rel_dates.get(p['set'], "2000-01-01"), "%Y-%m-%d")).days >= self.min_set_age
                    fresh = (now_dt - datetime.strptime(l_date, "%Y-%m-%d")).days <= 10
                    proc.append({**p, 'curr': curr, 'high': high, 'stable': stable, 'fresh': fresh})
                except KeyError: continue

            if not proc: continue
            g_med = np.median([p['high'] for p in proc])
            for p in proc: 
                if p['high'] > g_med * 3.0: p['high'] = g_med
            stable_p = [p for p in proc if p['stable']]
            if stable_p:
                s_med = np.median([p['high'] for p in stable_p])
                s_filt = [p for p in stable_p if p['high'] <= s_med * 2.0]
                st_ref = min(s_filt if s_filt else stable_p, key=lambda x: x['high'])
            else: st_ref = min(proc, key=lambda x: x['curr'])

            for p in proc:
                if not p['fresh']: continue
                analysis, h_ref, r_set = None, 0, ""
                if not p['stable']:
                    if p['curr'] <= st_ref['high'] * (1 - self.min_dip / 100):
                        analysis, h_ref, r_set = "Reprint", st_ref['high'], st_ref['set']
                elif p['curr'] <= p['high'] * (1 - self.min_dip / 100):
                    analysis, h_ref, r_set = "", p['high'], p['set']

                if analysis is not None:
                    drop = h_ref - p['curr']
                    if drop >= self.min_drop:
                        results.append({"Card Name": p['name'], "Set": p['set'], "Analysis": analysis,
                                        "Source": "edhtop16" if name in t16 else "edhrec", "Price": p['curr'],
                                        "High Ref": h_ref, "Ref Set": "" if r_set == p['set'] else r_set,
                                        "Dip %": round((drop / h_ref * 100), 2)})

        df = pd.DataFrame(results)
        if df.empty: return logger.info("No dips found.")
        df['Analysis'] = df['Analysis'].replace('Dip', '')
        df = df.sort_values("Dip %", ascending=False).drop_duplicates(subset=['Card Name', 'Analysis'])
        print(f"\n[REPORT] MTG Staple Dips:\n{df.to_string(index=False)}")
        self.generate_pdf(df)
        self.generate_tcg_import(df)

if __name__ == "__main__":
    MTGDipDetector().get_market_dips()
