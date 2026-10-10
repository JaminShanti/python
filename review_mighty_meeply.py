import json, time, yaml, logging, requests, re, sys
from datetime import datetime, timedelta
from collections import defaultdict
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


class MTGMightyMeepleScanner:
    def __init__(self, cache_dir="meeple_cache", cfg="meeple_config.yaml"):
        self.cdir = Path(__file__).parent / cache_dir
        self.cdir.mkdir(exist_ok=True)
        self.cfg_path = self.cdir / cfg
        self.cache_path = self.cdir / "meeple_inventory_cache.json"
        self.state_path = self.cdir / "meeple_eval_state.json"

        self.session = requests.Session()
        self.session.headers.update({"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"})
        self.base_url = "https://mightymeeple.com/collections/mtg-singles-all-products/products.json"

        self.config = self._load_config()
        self.targets = self.config.get("targets", [])

    def _load_config(self):
        if not self.cfg_path.exists():
            cfg = {"cache_ttl_hours": 12, "targets": []}
            self.cfg_path.write_text(yaml.dump(cfg, default_flow_style=False))
            return cfg
        return yaml.safe_load(self.cfg_path.read_text()) or {}

    def _io(self, path: Path, write_data=None, default=None):
        if write_data is not None:
            path.write_text(json.dumps(write_data, indent=2))
        else:
            return json.loads(path.read_text()) if path.exists() else default

    def import_decklist(self, filepath, default_max=100.0):
        try:
            lines = Path(filepath).read_text(encoding="utf-8").splitlines()
        except FileNotFoundError:
            return print(f"Could not find decklist file: {filepath}")

        existing = {t["name"].lower() for t in self.targets}
        added = 0

        bad_prefixes = ("gathering", "scraping", "processing", "analyzing", "targeting", "->", "=", "#", "//")
        bad_exact = {"commander", "deck", "mainboard", "sideboard", "maybeboard", "creature", "creatures",
                     "artifact", "artifacts", "enchantment", "enchantments", "instant", "instants",
                     "sorcery", "sorceries", "planeswalker", "planeswalkers", "land", "lands"}

        for line in lines:
            line = line.strip()
            if not line or line.lower().startswith(bad_prefixes) or line.lower() in bad_exact:
                continue

            if match := re.match(r'^(?:(?:\d+)x?\s+)?([^\(\*\[]+)', line):
                c_name = match.group(1).strip()
                if c_name and c_name.lower() not in existing:
                    self.targets.append({"name": c_name, "max_price": default_max})
                    existing.add(c_name.lower())
                    added += 1

        if added:
            self.cfg_path.write_text(yaml.dump(self.config, default_flow_style=False))
            print(f"\n[+] Imported {added} new cards from '{filepath}'! (Default max price: ${default_max:.2f})")

    def fetch_inventory(self, force=False):
        valid = self.cache_path.exists() and not force and \
                (datetime.now() - datetime.fromtimestamp(self.cache_path.stat().st_mtime)) \
                < timedelta(hours=self.config.get("cache_ttl_hours", 12))

        cache = self._io(self.cache_path, default={"scanned_pages": [], "inventory": []}) if valid else {}
        pages = set(cache.get("scanned_pages", cache.get("pages", [])))
        inv = cache.get("inventory", cache.get("inv", []))
        page = 1

        logger.info("Starting/Resuming inventory harvest...")
        while True:
            if page in pages: page += 1; continue
            try:
                res = self.session.get(self.base_url, params={"limit": 250, "page": page}, timeout=15)
                if res.status_code != 200 or not (prods := res.json().get("products", [])): break

                inv.extend([
                    {"name": p.get("title", ""), "condition": v.get("title", ""),
                     "price": float(v.get("price", 0)), "variant_id": v.get("id")}
                    for p in prods for v in p.get("variants", []) if v.get("available")
                ])

                pages.add(page)
                self._io(self.cache_path, write_data={"scanned_pages": list(pages), "inventory": inv})
                if page % 5 == 0: logger.info(f"Page {page} cached ({len(prods)} products).")
                page += 1;
                time.sleep(0.2)
            except KeyboardInterrupt:
                logger.info("Harvest interrupted."); break
            except Exception as e:
                logger.error(f"Error on page {page}: {e}"); break
        return inv

    def evaluate_targets(self, inv):
        if not self.targets: return []
        logger.info("Evaluating inventory...")
        state = self._io(self.state_path, default={})
        processed = state.get("processed_targets", state.get("processed", []))
        cart = state.get("cart_variants", state.get("cart", []))

        hits = defaultdict(list)
        for item in inv:
            c_name, price, cond = item["name"].lower(), item["price"], item["condition"].lower()
            for t in self.targets:
                for k, v in zip(["excluded_conditions", "excluded_terms", "skip_card", "owned_enough"],
                                [[], [], False, False]):
                    t.setdefault(k, v)

                if t["name"].lower() in c_name:
                    if (t["skip_card"] or t["owned_enough"] or price > float(t["max_price"]) or
                            cond in [c.lower() for c in t["excluded_conditions"]] or
                            any(term.lower() in c_name for term in t["excluded_terms"])): continue
                    hits[t["name"]].append((item, t))

        if not hits: return cart
        print(f"\nFound matches for {len(hits)} target cards.")
        save_cfg, finished = False, True

        try:
            for idx, (t_name, matches) in enumerate(hits.items(), 1):
                if t_name in processed: continue
                matches.sort(key=lambda x: x[0]["price"])
                t = matches[0][1]

                print(f"\n[{idx}/{len(hits)}] Matches for: {t_name}")
                for i, (item, _) in enumerate(matches, 1):
                    print(f"  [{i}] {item['name']} ({item['condition']}) - ${item['price']:.2f}")

                print("\n  [#] Item number(s) (e.g. '1,3')  [n] Skip all  [q] Quit & save")
                print("  Rules: [p]rice high  [c]ondition bad  [a]rt wrong  [s]top asking  [o]wn enough")

                while True:
                    c = input("Choice (#/n/q/p/c/a/s/o): ").strip().lower()

                    if c == 'q':
                        finished = False; break
                    elif c == 'n':
                        break
                    elif c == 'p':
                        t["max_price"] = round(matches[0][0]["price"] - 0.01, 2)
                        print(f"-> max_price set to ${t['max_price']:.2f}");
                        save_cfg = True;
                        break
                    elif c == 'c':
                        if cond := input("  Exact condition to ban (or 'b' back): ").strip():
                            if cond.lower() != 'b': t["excluded_conditions"].append(cond); save_cfg = True; break
                    elif c == 'a':
                        if term := input("  Word to ban (or 'b' back): ").strip():
                            if term.lower() != 'b': t["excluded_terms"].append(term); save_cfg = True; break
                    elif c in ['s', 'o']:
                        t["skip_card" if c == 's' else "owned_enough"] = True
                        print("-> Marked to skip/owned.");
                        save_cfg = True;
                        break
                    else:
                        try:
                            ids = [int(x.strip()) for x in c.split(',')]
                            if all(1 <= i <= len(matches) for i in ids):
                                cart.extend([str(matches[i - 1][0]["variant_id"]) for i in ids])
                                print(f"Added {len(ids)} item(s).");
                                break
                            print("Invalid selection. Check the brackets [1], [2], etc.")
                        except ValueError:
                            print("Invalid input.")

                if not finished: break
                processed.append(t_name)
                self._io(self.state_path, write_data={"processed_targets": processed, "cart_variants": cart})

        except KeyboardInterrupt:
            finished = False

        if save_cfg: self.cfg_path.write_text(yaml.dump(self.config, default_flow_style=False))
        if finished and self.state_path.exists(): self.state_path.unlink()
        return cart

    def run(self, force=False):
        if not self.targets:
            print("\n" + "=" * 60)
            print(" NO TARGET CARDS FOUND IN CONFIG")
            print("=" * 60)
            print("\nHow to load cards into the scanner:\n")
            print("  1. Import a Decklist Text File:")
            print("     python review_mighty_meeply.py my_deck.txt\n")
            print("     Supported formats: Moxfield, Archidekt, TCGplayer,")
            print("     MTG Arena, or any plain list of card names.\n")
            print("  2. Manual Configuration:")
            print("     Add targets directly to 'meeple_cache/meeple_config.yaml'")
            print("=" * 60 + "\n")
            return

        cart = self.evaluate_targets(self.fetch_inventory(force))
        if not cart: return print("\nNo targets matched or all skipped.")

        print("\n" + "=" * 60 + f"\nAUTOMATED CART PERMALINKS ({len(cart)} items)\n" + "=" * 60)
        for i, chunk in enumerate([cart[i:i + 40] for i in range(0, len(cart), 40)], 1):
            url = f"https://mightymeeple.com/cart/{','.join(f'{v}:1' for v in chunk)}"
            print(f"Link {i} ({len(chunk)} items):\n{url}\n" + "-" * 60)


if __name__ == "__main__":
    scanner = MTGMightyMeepleScanner()
    if len(sys.argv) > 1 and sys.argv[1].endswith('.txt'):
        scanner.import_decklist(sys.argv[1])
    scanner.run()