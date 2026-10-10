import os
import json
import time
import yaml
import logging
import requests
from datetime import datetime, timedelta

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


class MTGMightyMeepleScanner:
    def __init__(self, cache_dir="meeple_cache", config_filename="meeple_config.yaml"):
        base_path = os.path.abspath(os.path.dirname(__file__))
        self.cache_dir = os.path.join(base_path, cache_dir)
        os.makedirs(self.cache_dir, exist_ok=True)

        self.config_file = os.path.join(self.cache_dir, config_filename)
        self.cache_file = os.path.join(self.cache_dir, "meeple_inventory_cache.json")
        self.state_file = os.path.join(self.cache_dir, "meeple_eval_state.json")
        self.base_url = "https://mightymeeple.com/collections/mtg-singles-all-products/products.json"

        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
        })

        self.config = self._load_config()
        self.cache_ttl = self.config.get("cache_ttl_hours", 12)
        self.targets = self.config.get("targets", [])

    def _load_config(self):
        if not os.path.exists(self.config_file):
            default_config = {
                "cache_ttl_hours": 12,
                "targets": [
                    {
                        "name": "Mana Crypt",
                        "max_price": 90.00,
                        "excluded_conditions": [],
                        "excluded_terms": [],
                        "skip_card": False,
                        "owned_enough": False
                    },
                ]
            }
            with open(self.config_file, "w") as f:
                yaml.dump(default_config, f, default_flow_style=False)
            logger.info(f"Created default configuration file at: {self.config_file}")
            return default_config

        try:
            with open(self.config_file, "r") as f:
                return yaml.safe_load(f) or {}
        except Exception as e:
            logger.error(f"Error reading configuration {self.config_file}: {e}")
            return {"cache_ttl_hours": 12, "targets": []}

    def _save_config(self):
        with open(self.config_file, "w") as f:
            yaml.dump(self.config, f, default_flow_style=False)
        logger.info("Configuration YAML updated with new filter rules.")

    def _load_eval_state(self):
        """Loads progress if you quit halfway through an evaluation."""
        if os.path.exists(self.state_file):
            try:
                with open(self.state_file, "r", encoding="utf-8") as f:
                    data = json.load(f)
                    # Support legacy state file formats by ensuring lists exist
                    return {
                        "processed_targets": data.get("processed_targets", []),
                        "cart_variants": data.get("cart_variants", [])
                    }
            except Exception:
                pass
        return {"processed_targets": [], "cart_variants": []}

    def _save_eval_state(self, processed_targets, cart):
        """Saves evaluation progress so you can resume later."""
        with open(self.state_file, "w", encoding="utf-8") as f:
            json.dump({"processed_targets": processed_targets, "cart_variants": cart}, f)

    def _clear_eval_state(self):
        if os.path.exists(self.state_file):
            os.remove(self.state_file)

    def _load_cache(self):
        if os.path.exists(self.cache_file):
            mtime = datetime.fromtimestamp(os.path.getmtime(self.cache_file))
            if (datetime.now() - mtime) < timedelta(hours=self.cache_ttl):
                try:
                    with open(self.cache_file, "r", encoding="utf-8") as f:
                        cache = json.load(f)
                        logger.info(f"Loaded valid cache ({len(cache.get('inventory', []))} items indexed).")
                        return cache
                except Exception as e:
                    logger.warning(f"Could not read cache file ({e}). Starting fresh.")
            else:
                logger.info("Cache expired based on TTL. Starting fresh scan...")

        return {"scanned_pages": [], "inventory": []}

    def _save_cache(self, cache_data):
        with open(self.cache_file, "w", encoding="utf-8") as f:
            json.dump(cache_data, f, indent=2)

    def fetch_inventory(self, force_refresh=False):
        cache = {"scanned_pages": [], "inventory": []} if force_refresh else self._load_cache()
        scanned_pages = set(cache.get("scanned_pages", []))
        inventory = cache.get("inventory", [])

        page = 1
        logger.info("Starting/Resuming inventory harvest...")

        while True:
            if page in scanned_pages:
                page += 1
                continue

            try:
                response = self.session.get(self.base_url, params={"limit": 250, "page": page}, timeout=15)
                if response.status_code != 200:
                    logger.info(f"Stopped fetching on page {page} (HTTP {response.status_code}).")
                    break

                products = response.json().get("products", [])
                if not products:
                    break

                for product in products:
                    for variant in product.get("variants", []):
                        if variant.get("available"):
                            inventory.append({
                                "name": product.get("title", "Unknown"),
                                "condition": variant.get("title", "N/A"),
                                "price": float(variant.get("price", 0.0)),
                                "variant_id": variant.get("id")
                            })

                scanned_pages.add(page)
                cache["scanned_pages"] = list(scanned_pages)
                cache["inventory"] = inventory
                self._save_cache(cache)

                if page % 5 == 0:
                    logger.info(f"Page {page} downloaded & cached ({len(products)} products).")
                page += 1
                time.sleep(0.2)

            except KeyboardInterrupt:
                logger.info(f"\n[!] Harvest interrupted by user. Safe checkpoint saved up to page {page - 1}.")
                break
            except Exception as e:
                logger.error(f"Network error on page {page}: {e}. Retaining saved progress.")
                break

        return inventory

    def evaluate_targets(self, inventory):
        logger.info("Evaluating inventory against target list...")

        # Load progress from previous aborted runs
        state = self._load_eval_state()
        processed_targets = state.get("processed_targets", [])
        cart_variants = state.get("cart_variants", [])

        grouped_hits = {}

        # Pass 1: Build the queue of potential matches, grouped by Target Card
        for item in inventory:
            card_name = item["name"].lower()
            for target in self.targets:
                # Initialize missing config keys dynamically if old yaml
                target.setdefault("excluded_conditions", [])
                target.setdefault("excluded_terms", [])
                target.setdefault("skip_card", False)
                target.setdefault("owned_enough", False)

                target_name = target["name"].lower()

                if target_name == card_name or target_name in card_name:
                    # Auto-Skip logic applied immediately
                    if target.get("skip_card") or target.get("owned_enough"):
                        continue
                    if item["price"] > float(target["max_price"]):
                        continue
                    if item["condition"].lower() in [c.lower() for c in target.get("excluded_conditions", [])]:
                        continue

                    skip_term = False
                    for term in target.get("excluded_terms", []):
                        if term.lower() in item["name"].lower():
                            skip_term = True
                            break
                    if skip_term:
                        continue

                    # Group valid hits by target name
                    if target["name"] not in grouped_hits:
                        grouped_hits[target["name"]] = []
                    grouped_hits[target["name"]].append((item, target))

        total_targets = len(grouped_hits)
        if total_targets == 0:
            return cart_variants

        print(f"\nFound matches for {total_targets} target cards.")
        config_needs_saving = False
        completed_full_loop = True

        # Pass 2: Interactive review (Grouped by Card)
        try:
            for idx, (t_name, hits) in enumerate(grouped_hits.items(), 1):

                # Skip if we already reviewed this specific target card in a previous session
                if t_name in processed_targets:
                    continue

                # The target configuration dictionary is the same for all hits in this group
                target = hits[0][1]

                # Sort hits by price so cheapest is always first
                hits.sort(key=lambda x: x[0]["price"])

                print(f"\n[{idx}/{total_targets}] Matches for: {t_name}")
                for i, (item, _) in enumerate(hits, 1):
                    print(f"  [{i}] {item['name']} ({item['condition']}) - ${item['price']:.2f}")

                print("\n  [#] Type item number(s) to add to cart (e.g., '1' or '1,3')")
                print("  [n] Skip all   [q] Quit & save")
                print("  Rules: [p]rice high  [c]ondition bad  [a]rt wrong  [s]top asking  [o]wn enough")

                while True:
                    user_choice = input("Choice (#/n/q/p/c/a/s/o): ").strip().lower()

                    if user_choice == 'q':
                        print("\nSaving progress... You can resume this exact spot next time.")
                        completed_full_loop = False
                        break

                    elif user_choice == 'n':
                        print("Skipped all versions.")
                        break

                    elif user_choice == 'p':
                        # Automatically lower the price to just below the cheapest option shown
                        lowest_price = hits[0][0]["price"]
                        new_max = lowest_price - 0.01
                        target["max_price"] = round(new_max, 2)
                        print(f"-> Reduced max_price to ${target['max_price']:.2f} (just below the cheapest option).")
                        config_needs_saving = True
                        break

                    elif user_choice == 'c':
                        cond = input(
                            "  Enter exact condition to exclude (e.g., 'Heavily Played', or 'b' to go back): ").strip()
                        if cond.lower() == 'b' or cond == '':
                            continue
                        target["excluded_conditions"].append(cond)
                        print(f"-> Banned condition '{cond}' for this card.")
                        config_needs_saving = True
                        break

                    elif user_choice == 'a':
                        term = input(
                            "  Enter word to exclude (e.g., 'Foil', 'Borderless', or 'b' to go back): ").strip()
                        if term.lower() == 'b' or term == '':
                            continue
                        target["excluded_terms"].append(term)
                        print(f"-> Banned version containing '{term}' for this card.")
                        config_needs_saving = True
                        break

                    elif user_choice == 's':
                        target["skip_card"] = True
                        print(f"-> Will stop searching for '{target['name']}'.")
                        config_needs_saving = True
                        break

                    elif user_choice == 'o':
                        target["owned_enough"] = True
                        print(f"-> Marked that you own enough of '{target['name']}'. Will skip future hits.")
                        config_needs_saving = True
                        break

                    else:
                        # Handle specific number selections (e.g., "2" or "1,3")
                        try:
                            indices = [int(x.strip()) for x in user_choice.split(',')]
                            valid = True
                            for i in indices:
                                if i < 1 or i > len(hits):
                                    valid = False

                            if valid:
                                for i in indices:
                                    cart_variants.append(str(hits[i - 1][0]["variant_id"]))
                                print(f"Added {len(indices)} item(s) to queue.")
                                break
                            else:
                                print("Invalid number selection. Look at the brackets [1], [2], etc.")
                        except ValueError:
                            print("Invalid input.")

                # If the user chose 'q', break out of the main card loop entirely
                if not completed_full_loop:
                    break

                processed_targets.append(t_name)
                self._save_eval_state(processed_targets, cart_variants)

        except KeyboardInterrupt:
            print("\n[!] Interrupted by user. Progress saved.")
            completed_full_loop = False

        if config_needs_saving:
            self._save_config()

        # Clean up the state file if we finished evaluating everything naturally
        if completed_full_loop:
            self._clear_eval_state()

        return cart_variants

    def run(self, force_refresh=False):
        inventory = self.fetch_inventory(force_refresh=force_refresh)
        logger.info(f"Total available cards indexed: {len(inventory)}")

        cart_variants = self.evaluate_targets(inventory)

        if cart_variants:
            chunk_size = 40
            chunks = [cart_variants[i:i + chunk_size] for i in range(0, len(cart_variants), chunk_size)]

            print("\n" + "=" * 60)
            print(f"AUTOMATED CART PERMALINKS ({len(cart_variants)} items approved)")
            print("=" * 60)
            for idx, chunk in enumerate(chunks, 1):
                cart_string = ",".join([f"{vid}:1" for vid in chunk])
                cart_url = f"https://mightymeeple.com/cart/{cart_string}"
                print(f"Link {idx} ({len(chunk)} items):")
                print(cart_url)
                print("-" * 60)
        else:
            print("\nNo targets matched your criteria or all items were skipped.")


if __name__ == "__main__":
    scanner = MTGMightyMeepleScanner()
    scanner.run()