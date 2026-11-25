import argparse
import json
import os
import pickle
import re
import time
from collections import Counter
from statistics import median
from urllib.parse import parse_qsl, quote, unquote, urlencode, urlparse, urlunparse

import requests
import yaml
from playwright.sync_api import sync_playwright

if __package__:
    from .mtg_deck_scraper import TopDeckDeckScraper
else:
    from mtg_deck_scraper import TopDeckDeckScraper


class MTGDeckScanner:
    def __init__(
        self,
        cache_dir=None,
        config_filename="mtg_scanner_config.yaml",
        b2_exclusions_filename="bracket_2_exclusions.yaml",
        min_percentage=0.20,
        debug=False,
    ):
        base_cache = os.getenv("CACHE_ROOT", os.path.abspath(os.path.dirname(__file__)))
        self.cache_dir = cache_dir or os.path.join(base_cache, "mtg_scanner_cache")
        self.min_percentage = min_percentage
        self.debug = debug

        os.makedirs(self.cache_dir, exist_ok=True)

        self.config_file = os.path.join(self.cache_dir, config_filename)
        self.b2_exclusions_filename = b2_exclusions_filename
        self.urls_cache_file = os.path.join(self.cache_dir, "topdeck_urls_cache.pkl")
        self.scryfall_cache_file = os.path.join(self.cache_dir, "scryfall_data.pkl")
        self.deck_cache_file = os.path.join(self.cache_dir, "topdeck_deck_data.pkl")
        self.excluded_file = os.path.join(self.cache_dir, "excluded_cards.txt")
        self.snapshot_file = os.path.join(self.cache_dir, "decklist_snapshots.json")

        self.type_order = [
            "Commander", "Creature", "Artifact", "Enchantment",
            "Instant", "Sorcery", "Planeswalker", "Land",
        ]
        self.type_hierarchy = (
            "Land", "Creature", "Artifact", "Enchantment",
            "Instant", "Sorcery", "Planeswalker",
        )
        self.ui_noise = [
            "Artifact", "Creature", "Instant", "Sorcery",
            "Enchantment", "Land", "Planeswalker",
        ]

        self.commander_targets = self._load_config_urls()
        self.excluded_cards = self._load_exclusions()
        self.b2_exclusions = self._load_bracket_2_exclusions()
        self.game_changers = self._fetch_game_changers()

        self.topdeck_cache = self._load_cache(self.urls_cache_file, ttl_seconds=7 * 86400)
        self.deck_cache = self._load_cache(self.deck_cache_file)
        self.scryfall_cache = self._load_cache(self.scryfall_cache_file)
        self.deck_scraper = TopDeckDeckScraper(
            self.deck_cache, self.deck_cache_file, self._save_cache, debug=self.debug
        )

    def _debug(self, message):
        if self.debug:
            print(message)

    def _load_config_urls(self):
        if os.path.exists(self.config_file):
            try:
                with open(self.config_file, "r", encoding="utf-8") as f:
                    config_data = yaml.safe_load(f)
                    if config_data and "commander_urls" in config_data:
                        targets = []
                        for item in config_data["commander_urls"]:
                            if isinstance(item, dict):
                                targets.append(item)
                            else:
                                targets.append({"url": item, "bracket": None})
                        return targets
            except Exception as e:
                print(f"Error parsing configuration file {self.config_file}: {e}")

        print(f"Warning: {self.config_file} not found or missing 'commander_urls'. Using empty list.")
        return []

    def _update_config_url(self, old_url, new_url):
        if not os.path.exists(self.config_file):
            return
        try:
            with open(self.config_file, "r", encoding="utf-8") as f:
                config_data = yaml.safe_load(f)

            if config_data and "commander_urls" in config_data:
                updated = False
                for i, item in enumerate(config_data["commander_urls"]):
                    if isinstance(item, dict) and item.get("url") == old_url:
                        config_data["commander_urls"][i]["url"] = new_url
                        updated = True
                        break
                    elif isinstance(item, str) and item == old_url:
                        config_data["commander_urls"][i] = new_url
                        updated = True
                        break

                if updated:
                    with open(self.config_file, "w", encoding="utf-8") as f:
                        yaml.dump(config_data, f, sort_keys=False, default_flow_style=False)
        except Exception as e:
            print(f"   -> Warning: Failed to write back to config file: {e}")

    def _load_exclusions(self):
        exclusions = {}
        if os.path.exists(self.excluded_file):
            with open(self.excluded_file, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    if "->" in line:
                        target, replacement = line.split("->", 1)
                        exclusions[target.strip().lower()] = replacement.strip()
                    else:
                        exclusions[line.lower()] = None
        return exclusions

    def _load_bracket_2_exclusions(self):
        search_paths = [
            os.path.join(self.cache_dir, self.b2_exclusions_filename),
            os.path.join(os.path.abspath(os.path.dirname(__file__)), self.b2_exclusions_filename),
        ]
        for path in search_paths:
            if os.path.exists(path):
                try:
                    with open(path, "r", encoding="utf-8") as f:
                        data = yaml.safe_load(f) or {}
                    categories = data.get("bracket_2_exclusions", data)
                    lookup = {}
                    if isinstance(categories, dict):
                        for reason, cards in categories.items():
                            if isinstance(cards, list):
                                for card in cards:
                                    if card:
                                        lookup[str(card).strip().lower()] = str(reason)
                    return lookup
                except Exception as e:
                    print(f"Error parsing Bracket 2 exclusions file {path}: {e}")
        return {}

    def _is_bracket_2_excluded(self, card_name):
        if not self.b2_exclusions:
            return False, ""
        full_lower = card_name.strip().lower()
        if full_lower in self.b2_exclusions:
            return True, self.b2_exclusions[full_lower]
        front_lower = card_name.split(" // ")[0].strip().lower()
        if front_lower in self.b2_exclusions:
            return True, self.b2_exclusions[front_lower]
        return False, ""

    def _is_card_allowed(self, card, scry_data, commander_name, bracket, gc_count, max_gc, verbose=False):
        """Centralized exclusion and bracket gatekeeper for all card-selection passes."""
        real_name, _, is_legal, is_gc = scry_data

        if (
            card in self.ui_noise
            or card.lower() == commander_name.lower()
            or real_name.lower() == commander_name.lower()
            or card.lower() in self.excluded_cards
            or real_name.lower() in self.excluded_cards
        ):
            return False

        if not is_legal:
            return False

        if is_gc and gc_count >= max_gc:
            if verbose:
                print(f"   -> Bracket {bracket} Game Changer cap reached. Excluded '{card}'.")
            return False

        if str(bracket) in ["1", "2"]:
            b2_hit, b2_reason = self._is_bracket_2_excluded(card)
            if not b2_hit and real_name != card:
                b2_hit, b2_reason = self._is_bracket_2_excluded(real_name)
            if b2_hit:
                if verbose:
                    print(f"   -> Bracket {bracket} [{b2_reason}] filter. Excluded '{card}'.")
                return False

        return True

    def _fetch_game_changers(self):
        gc_cache_file = os.path.join(self.cache_dir, "scryfall_game_changers.pkl")

        if os.path.exists(gc_cache_file):
            if time.time() - os.path.getmtime(gc_cache_file) < 7 * 86400:
                return self._load_cache(gc_cache_file)

        print("Fetching latest Game Changers list from Scryfall API...")
        gc_list = set()
        url = "https://api.scryfall.com/cards/search?q=is:gamechanger"

        try:
            while url:
                time.sleep(0.1)
                response = requests.get(
                    url, headers={"User-Agent": "EDH-Builder-Script/1.0"}, timeout=20
                )
                if response.status_code == 200:
                    data = response.json()
                    for card in data.get("data", []):
                        gc_list.add(card["name"].lower())
                    url = data.get("next_page") if data.get("has_more") else None
                else:
                    break

            if gc_list:
                self._save_cache(gc_list, gc_cache_file)
                return gc_list
        except Exception:
            pass

        if os.path.exists(gc_cache_file):
            return self._load_cache(gc_cache_file)
        return set()

    def _load_cache(self, filepath, ttl_seconds=None):
        if os.path.exists(filepath):
            if ttl_seconds and (time.time() - os.path.getmtime(filepath) > ttl_seconds):
                print(f"Cache expired for {os.path.basename(filepath)}. Fetching fresh data...")
                return {}
            with open(filepath, "rb") as f:
                return pickle.load(f)
        return {}

    def _save_cache(self, cache_dict, filepath):
        with open(filepath, "wb") as f:
            pickle.dump(cache_dict, f)

    def _load_snapshots(self):
        try:
            with open(self.snapshot_file, encoding="utf-8") as f:
                return json.load(f)
        except (OSError, ValueError):
            return {}

    def _find_new_cards(self, snapshot_key, deck_list):
        """Return cards absent from the previous run's list and save the current list.

        Returns None when no earlier snapshot exists, so a first run is not
        reported as entirely new.
        """
        current = {
            entry.split(" ", 1)[1]: category
            for category, entries in deck_list.items()
            for entry in entries
        }
        snapshots = self._load_snapshots()
        previous = snapshots.get(snapshot_key)
        snapshots[snapshot_key] = sorted(current)
        try:
            with open(self.snapshot_file, "w", encoding="utf-8") as f:
                json.dump(snapshots, f, indent=2)
        except OSError as e:
            print(f"   -> Warning: Failed to save decklist snapshot: {e}")
        if previous is None:
            return None
        return sorted((name, cat) for name, cat in current.items() if name not in set(previous))

    def auto_scroll_to_bottom(self, page, max_scrolls=30):
        last_height = page.evaluate("document.body.scrollHeight")
        for _ in range(max_scrolls):
            page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            page.wait_for_timeout(1500)

            # Click any pagination / "Load More" button if edhtop16 batches at 48 entries
            load_more = page.locator("button:has-text('Load More'), button:has-text('Show More')").first
            if load_more.count() > 0 and load_more.is_visible():
                try:
                    load_more.click(timeout=2000)
                    page.wait_for_timeout(1500)
                except Exception:
                    pass

            new_height = page.evaluate("document.body.scrollHeight")
            if new_height == last_height:
                return
            last_height = new_height
        print(f"Warning: Auto-scroll reached its {max_scrolls}-step limit; continuing with loaded links.")

    def get_topdeck_urls(self, page, commander_url, force_refresh=False):
        if commander_url in self.topdeck_cache and not force_refresh:
            return self.topdeck_cache[commander_url]

        page.goto(commander_url, wait_until="domcontentloaded", timeout=30000)
        page.wait_for_timeout(2000)
        self.auto_scroll_to_bottom(page)

        all_links = page.evaluate("() => Array.from(document.querySelectorAll('a')).map(a => a.href)")
        potential_decks = list(set(link for link in all_links if "/decklist/" in link.lower()))

        urls = [f"{link}?format=topdeck" for link in potential_decks]
        self.topdeck_cache[commander_url] = urls
        self._save_cache(self.topdeck_cache, self.urls_cache_file)
        return urls

    def get_scryfall_data(self, card_name):
        if card_name in self.scryfall_cache:
            cached_data = self.scryfall_cache[card_name]
            if len(cached_data) == 3:
                real_name, type_line, is_legal = cached_data
                is_gc = (
                    real_name.lower() in self.game_changers
                    or card_name.lower() in self.game_changers
                )
                cached_data = (real_name, type_line, is_legal, is_gc)
                self.scryfall_cache[card_name] = cached_data
                self._save_cache(self.scryfall_cache, self.scryfall_cache_file)

            if cached_data[1]:
                return cached_data

        time.sleep(0.1)

        url = f"https://api.scryfall.com/cards/named?exact={quote(card_name)}"
        try:
            response = requests.get(
                url, headers={"User-Agent": "EDH-Builder-Script/1.0"}, timeout=20
            )

            if response.status_code != 200:
                time.sleep(0.1)
                url = f"https://api.scryfall.com/cards/named?fuzzy={quote(card_name)}"
                response = requests.get(
                    url, headers={"User-Agent": "EDH-Builder-Script/1.0"}, timeout=20
                )

            if response.status_code == 200:
                res_data = response.json()
                type_line = res_data.get("type_line", "")
                if "card_faces" in res_data and not type_line:
                    type_line = res_data["card_faces"][0].get("type_line", "")

                legalities = res_data.get("legalities", {})
                is_legal = legalities.get("commander") == "legal"

                if "Sticker" in type_line or "Attraction" in type_line:
                    is_legal = False

                real_name = res_data.get("name", card_name)
                is_gc = (
                    real_name.lower() in self.game_changers
                    or card_name.lower() in self.game_changers
                )

                result = (real_name, type_line, is_legal, is_gc)
                self.scryfall_cache[card_name] = result
                self._save_cache(self.scryfall_cache, self.scryfall_cache_file)
                return result
        except Exception:
            pass

        is_gc = card_name.lower() in self.game_changers
        result = (card_name, "", True, is_gc)
        self.scryfall_cache[card_name] = result
        self._save_cache(self.scryfall_cache, self.scryfall_cache_file)
        return result

    def categorize_card(self, type_line):
        if not type_line:
            return "Creature"
        return next(
            (t for t in self.type_hierarchy if t.lower() in type_line.lower()),
            "Artifact",
        )

    def _select_recent_decks(self, page, commander_url):
        search_steps = [
            ("THREE_MONTHS", 50), ("THREE_MONTHS", 30), ("THREE_MONTHS", 16),
            ("SIX_MONTHS", 50), ("SIX_MONTHS", 30), ("SIX_MONTHS", 16),
            ("ONE_YEAR", 50), ("ONE_YEAR", 30), ("ONE_YEAR", 16),
            ("ALL_TIME", 0),
        ]
        parsed = urlparse(commander_url)
        other_params = [
            (key, value) for key, value in parse_qsl(parsed.query, keep_blank_values=True)
            if key not in ("timePeriod", "minEventSize")
        ]
        attempted = {}
        urls = []
        valid = []

        for time_period, min_size in search_steps:
            query = urlencode(other_params + [
                ("timePeriod", time_period), ("minEventSize", str(min_size)),
            ])
            target_url = urlunparse(parsed._replace(query=query))
            print(f"   -> Checking {time_period}, {min_size}+ players...", flush=True)
            urls = list(dict.fromkeys(self.get_topdeck_urls(page, target_url)))
            valid = []
            for i, url in enumerate(urls, 1):
                self._debug(f"   -> [DEBUG] Processing deck {i}/{len(urls)}.")
                if url not in attempted:
                    attempted[url] = self.deck_scraper.scrape_deck_data(page, url)
                cards, basics = attempted[url]
                if cards or sum(basics.values()):
                    valid.append((cards, basics))
            print(f"   -> {time_period}, {min_size}+ players: {len(valid)}/{len(urls)} usable decklists.")
            if len(valid) >= 30:
                break
        if len(valid) < 30:
            print(f"Warning: Only {len(valid)} usable decklists found across the maximum range.")
        return urls, valid

    def analyze_commander(self, page, commander_url, bracket=None):
        name_match = re.search(r"commander/([^?]+)", commander_url)
        commander_name = unquote(name_match.group(1)) if name_match else "Unknown Commander"

        bracket_text = f" (Bracket {bracket})" if bracket else ""
        print(f"\n{'=' * 60}\nGathering Consensus Data for: {commander_name}{bracket_text}\n{'=' * 60}")

        urls, valid_decks = self._select_recent_decks(page, commander_url)
        if not valid_decks:
            print(f"\nNo valid decklists could be processed for {commander_name}.")
            return

        raw_card_counter = Counter()
        card_counter = Counter()
        deck_card_lists = []
        deck_basic_counts = []
        total_basics = {
            "mountain": 0, "forest": 0, "plains": 0, "island": 0, "swamp": 0, "wastes": 0,
        }
        valid_deck_count = len(valid_decks)

        for cards, b_counts in valid_decks:
            raw_card_counter.update(cards)
            deck_card_lists.append(cards)
            deck_basic_counts.append(b_counts)

            processed_cards = set()
            for c in cards:
                c_lower = c.lower()
                if c_lower in self.excluded_cards:
                    replacement = self.excluded_cards[c_lower]
                    if replacement:
                        processed_cards.add(replacement)
                else:
                    processed_cards.add(c)

            card_counter.update(processed_cards)

            for k in total_basics:
                total_basics[k] += b_counts.get(k, 0)

        print(f"Using {valid_deck_count} of {len(urls)} decklists for land averages.")

        nonbasic_land_cards = set()
        total_instants_all_decks = 0
        total_fetchable_targets = {
            "Plains": 0, "Island": 0, "Swamp": 0, "Mountain": 0, "Forest": 0,
        }

        for land, total in total_basics.items():
            if land.capitalize() in total_fetchable_targets:
                total_fetchable_targets[land.capitalize()] += total

        raw_unique_cards = [
            card for card in raw_card_counter.keys()
            if card not in self.ui_noise and card.lower() != commander_name.lower()
        ]

        print(f"\nAnalyzing {len(raw_unique_cards)} unique cards for averages...")

        failed_lookups = []

        for idx, card in enumerate(raw_unique_cards, 1):
            print(
                f"Checking Scryfall typelines {idx}/{len(raw_unique_cards)}...",
                end="\r",
                flush=True,
            )
            scry_data = self.get_scryfall_data(card)
            type_line, is_legal = scry_data[1], scry_data[2]

            if not is_legal:
                continue

            if not type_line:
                failed_lookups.append(card)

            cat = self.categorize_card(type_line)
            if cat == "Land":
                nonbasic_land_cards.add(card)
                for land_type in total_fetchable_targets.keys():
                    if re.search(rf"\b{land_type}\b", type_line, re.IGNORECASE):
                        total_fetchable_targets[land_type] += raw_card_counter[card]
            elif cat == "Instant":
                total_instants_all_decks += raw_card_counter[card]

        if raw_unique_cards:
            print(" " * 80, end="\r")

        if failed_lookups:
            self._debug("\n   -> [DEBUG] The following cards failed Scryfall lookups and defaulted to 'Creature':")
            for f_card in failed_lookups:
                self._debug(f"      * {f_card}")

        deck_land_counts = [
            sum(basic_counts.values())
            + sum(card in nonbasic_land_cards for card in cards)
            for cards, basic_counts in zip(deck_card_lists, deck_basic_counts)
        ]
        median_lands = round(median(deck_land_counts))
        # A bounded median is a pragmatic compromise, not a proven mana-base model.
        # Revisit with bracket-appropriate mana sources and modal land/spell usage.
        target_total_lands = max(27, min(31, median_lands))
        dynamic_min_instants = round(total_instants_all_decks / valid_deck_count)
        dynamic_max_instants = dynamic_min_instants + 2

        print(
            f"Targeting: {target_total_lands} Lands (source median: {median_lands}) | "
            f"{dynamic_min_instants}-{dynamic_max_instants} Instants"
        )

        avg_basics = {}
        total_avg_basics = 0
        for land, total in total_basics.items():
            avg = round(total / valid_deck_count)
            if avg > 0:
                avg_basics[land.capitalize()] = avg
                total_avg_basics += avg

        avg_fetchable = {}
        for land_type, total in total_fetchable_targets.items():
            avg = round(total / valid_deck_count)
            if avg > 0:
                avg_fetchable[land_type] = avg

        print(f"Targeting Average Fetchable Subtypes: {avg_fetchable}")

        if str(bracket) in ["1", "2"]:
            max_gc = 0
        elif str(bracket) == "3":
            max_gc = 3
        else:
            max_gc = 100

        target_non_basics = max(0, target_total_lands - total_avg_basics)
        target_spells = 99 - target_total_lands

        high_consensus_pool = []
        gc_count = 0
        added_non_basics = 0
        added_spells = 0

        # Pass 1: Primary consensus selection (>= min_percentage)
        for card, count in card_counter.most_common():
            if (
                card in self.ui_noise
                or card.lower() == commander_name.lower()
                or card.lower() in self.excluded_cards
            ):
                continue

            if (count / valid_deck_count) >= self.min_percentage:
                scry_data = self.get_scryfall_data(card)
                if not self._is_card_allowed(
                    card, scry_data, commander_name, bracket, gc_count, max_gc, verbose=True
                ):
                    continue

                cat = self.categorize_card(scry_data[1])
                if cat == "Land":
                    if added_non_basics < target_non_basics:
                        high_consensus_pool.append(card)
                        added_non_basics += 1
                        if scry_data[3]:
                            gc_count += 1
                else:
                    if added_spells < target_spells:
                        high_consensus_pool.append(card)
                        added_spells += 1
                        if scry_data[3]:
                            gc_count += 1

            if added_non_basics >= target_non_basics and added_spells >= target_spells:
                break

        # Prefer spells appearing in at least 5% of decks, then use rarer eligible spells.
        for minimum_frequency in (0.05, 0):
            for card, count in card_counter.most_common():
                if added_spells >= target_spells:
                    break
                if (
                    card in high_consensus_pool
                    or card in self.ui_noise
                    or card.lower() == commander_name.lower()
                    or card.lower() in self.excluded_cards
                ):
                    continue

                if (count / valid_deck_count) >= minimum_frequency:
                    scry_data = self.get_scryfall_data(card)
                    if not self._is_card_allowed(
                        card, scry_data, commander_name, bracket, gc_count, max_gc, verbose=False
                    ):
                        continue

                    if self.categorize_card(scry_data[1]) != "Land":
                        high_consensus_pool.append(card)
                        added_spells += 1
                        if scry_data[3]:
                            gc_count += 1

        deck_list = {t: [] for t in self.type_order}
        deck_list["Commander"] = [f"1 {commander_name}"]

        current_count = 1
        for card in high_consensus_pool:
            scry_data = self.get_scryfall_data(card)
            real_name, type_line = scry_data[0], scry_data[1]
            category = self.categorize_card(type_line)

            display_name = f"{real_name} *" if (str(bracket) == "3" and scry_data[3]) else real_name
            deck_list[category].append(f"1 {display_name}")
            current_count += 1

        # Pass 3: Trim excess Instants and refill with non-Land, non-Instant spells
        current_instants = len(deck_list["Instant"])
        if current_instants > dynamic_max_instants:
            to_cut = current_instants - dynamic_max_instants
            cards_removed = 0
            for card in reversed(high_consensus_pool):
                if cards_removed >= to_cut:
                    break

                real_name, _, _, is_gc = self.get_scryfall_data(card)
                matched_item = next(
                    (
                        item for item in deck_list["Instant"]
                        if item.startswith("1 ") and item[2:].rstrip(" *") in (card, real_name)
                    ),
                    None,
                )
                if matched_item:
                    deck_list["Instant"].remove(matched_item)
                    current_count -= 1
                    cards_removed += 1
                    if is_gc:
                        gc_count = max(0, gc_count - 1)
                    print(f"   -> Cut '{card}' (Instant) to meet maximum cap of {dynamic_max_instants}.")

            refill_needed = cards_removed
            for card, _ in card_counter.most_common():
                if refill_needed == 0:
                    break
                if (
                    card in high_consensus_pool
                    or card in self.ui_noise
                    or card.lower() == commander_name.lower()
                    or card.lower() in self.excluded_cards
                ):
                    continue

                scry_data = self.get_scryfall_data(card)
                cat = self.categorize_card(scry_data[1])
                if cat not in ["Land", "Instant"]:
                    if not self._is_card_allowed(
                        card, scry_data, commander_name, bracket, gc_count, max_gc, verbose=False
                    ):
                        continue

                    real_name = scry_data[0]
                    display_name = (
                        f"{real_name} *" if (str(bracket) == "3" and scry_data[3]) else real_name
                    )
                    deck_list[cat].append(f"1 {display_name}")
                    high_consensus_pool.append(card)
                    current_count += 1
                    refill_needed -= 1
                    if scry_data[3]:
                        gc_count += 1

        # Pass 4: Backfill missing Instants while strictly enforcing all exclusions
        current_instants = len(deck_list["Instant"])
        if current_instants < dynamic_min_instants:
            instant_deficit = dynamic_min_instants - current_instants
            next_best_instants = []
            for card, _ in card_counter.most_common():
                if (
                    card in high_consensus_pool
                    or card in self.ui_noise
                    or card.lower() == commander_name.lower()
                    or card.lower() in self.excluded_cards
                ):
                    continue

                scry_data = self.get_scryfall_data(card)
                if self.categorize_card(scry_data[1]) == "Instant":
                    if not self._is_card_allowed(
                        card, scry_data, commander_name, bracket, gc_count, max_gc, verbose=False
                    ):
                        continue

                    next_best_instants.append(card)
                    if scry_data[3]:
                        gc_count += 1
                    if len(next_best_instants) == instant_deficit:
                        break

            instants_to_add = len(next_best_instants)
            cards_removed = 0
            for card in reversed(high_consensus_pool):
                if cards_removed >= instants_to_add:
                    break

                real_name, _, _, is_gc = self.get_scryfall_data(card)
                for category in self.type_order:
                    if category in ["Land", "Commander", "Instant"]:
                        continue

                    matched_item = next(
                        (
                            item for item in deck_list[category]
                            if item.startswith("1 ") and item[2:].rstrip(" *") in (card, real_name)
                        ),
                        None,
                    )
                    if matched_item:
                        deck_list[category].remove(matched_item)
                        current_count -= 1
                        cards_removed += 1
                        if is_gc:
                            gc_count = max(0, gc_count - 1)
                        print(f"   -> Cut '{card}' for required average Instants.")
                        break

            for card in next_best_instants:
                scry_data = self.get_scryfall_data(card)
                real_name = scry_data[0]
                display_name = (
                    f"{real_name} *" if (str(bracket) == "3" and scry_data[3]) else real_name
                )
                deck_list["Instant"].append(f"1 {display_name}")
                current_count += 1
                print(f"   -> Added '{card}' (Instant) to meet dynamic average.")

        # Pass 5: Ensure the target land slots remain available
        current_lands = len(deck_list["Land"])
        slots_remaining = 100 - current_count
        if (current_lands + slots_remaining) < target_total_lands:
            deficit = target_total_lands - (current_lands + slots_remaining)
            cards_removed = 0
            for card in reversed(high_consensus_pool):
                if cards_removed >= deficit:
                    break

                real_name, _, _, is_gc = self.get_scryfall_data(card)
                for category in self.type_order:
                    if category in ["Land", "Commander", "Instant"]:
                        continue
                    matched_item = next(
                        (
                            item for item in deck_list[category]
                            if item.startswith("1 ") and item[2:].rstrip(" *") in (card, real_name)
                        ),
                        None,
                    )
                    if matched_item:
                        deck_list[category].remove(matched_item)
                        current_count -= 1
                        slots_remaining += 1
                        cards_removed += 1
                        if is_gc:
                            gc_count = max(0, gc_count - 1)
                        print(f"   -> Cut '{card}' for required land target.")
                        break

        # =====================================================================
        # BASIC LAND INJECTION
        # =====================================================================
        sorted_basics = sorted(total_basics.items(), key=lambda x: x[1], reverse=True)
        basic_types_to_use = [k.capitalize() for k, v in sorted_basics if v > 0]

        if not basic_types_to_use:
            basic_types_to_use = ["Forest", "Island", "Swamp", "Mountain", "Plains", "Wastes"]

        land_slots = min(slots_remaining, max(0, target_total_lands - current_lands))
        for basic_name in basic_types_to_use:
            if avg_basics.get(basic_name, 0) > 0 and land_slots > 0:
                allocated = min(avg_basics[basic_name], land_slots)

                found_existing = False
                for i, entry in enumerate(deck_list["Land"]):
                    if entry.endswith(f" {basic_name}"):
                        old_count = int(entry.split(" ", 1)[0])
                        deck_list["Land"][i] = f"{old_count + allocated} {basic_name}"
                        found_existing = True
                        break
                if not found_existing:
                    deck_list["Land"].append(f"{allocated} {basic_name}")

                slots_remaining -= allocated
                land_slots -= allocated

        if land_slots > 0:
            distribution = {b: 0 for b in basic_types_to_use}
            idx = 0
            while land_slots > 0:
                distribution[basic_types_to_use[idx % len(basic_types_to_use)]] += 1
                slots_remaining -= 1
                land_slots -= 1
                idx += 1

            for basic_name, count in distribution.items():
                if count > 0:
                    found = False
                    for i, entry in enumerate(deck_list["Land"]):
                        if entry.endswith(f" {basic_name}"):
                            old_count = int(entry.split(" ", 1)[0])
                            deck_list["Land"][i] = f"{old_count + count} {basic_name}"
                            found = True
                            break
                    if not found:
                        deck_list["Land"].append(f"{count} {basic_name}")

        # =====================================================================
        # FETCHABLE TARGET SAFETY NET
        # =====================================================================
        current_fetchables = {"Plains": 0, "Island": 0, "Swamp": 0, "Mountain": 0, "Forest": 0}
        fetch_land_names = {
            "arid mesa", "bloodstained mire", "flooded strand", "marsh flats",
            "misty rainforest", "polluted delta", "scalding tarn", "verdant catacombs",
            "windswept heath", "wooded foothills",
        }

        for land_entry in deck_list["Land"]:
            parts = land_entry.split(" ", 1)
            count = int(parts[0])
            land_name = parts[1]

            scry_data = self.get_scryfall_data(land_name)
            type_line = scry_data[1]

            for ft in current_fetchables.keys():
                if re.search(rf"\b{ft}\b", type_line, re.IGNORECASE):
                    current_fetchables[ft] += count

        deficits = {
            k: v - current_fetchables.get(k, 0)
            for k, v in avg_fetchable.items()
            if v > current_fetchables.get(k, 0)
        }

        if deficits:
            replaceable_lands = []
            for land_entry in deck_list["Land"]:
                parts = land_entry.split(" ", 1)
                count = int(parts[0])
                land_name = parts[1]
                scry_data = self.get_scryfall_data(land_name)
                type_line = scry_data[1]

                is_fetch = land_name.lower() in fetch_land_names
                has_subtype = any(
                    re.search(rf"\b{ft}\b", type_line, re.IGNORECASE)
                    for ft in current_fetchables.keys()
                )

                if not is_fetch and not has_subtype:
                    for _ in range(count):
                        replaceable_lands.append(land_name)

            replaceable_lands.reverse()

            for ft, def_val in list(deficits.items()):
                while def_val > 0 and replaceable_lands:
                    to_cut = replaceable_lands.pop(0)
                    for i, entry in enumerate(deck_list["Land"]):
                        count_text, entry_name = entry.split(" ", 1)
                        if entry_name == to_cut:
                            old_count = int(count_text)
                            if old_count > 1:
                                deck_list["Land"][i] = f"{old_count - 1} {to_cut}"
                            else:
                                deck_list["Land"].pop(i)
                            break

                    basic_added = False
                    for i, entry in enumerate(deck_list["Land"]):
                        if entry.endswith(f" {ft}"):
                            old_count = int(entry.split(" ", 1)[0])
                            deck_list["Land"][i] = f"{old_count + 1} {ft}"
                            basic_added = True
                            break
                    if not basic_added:
                        deck_list["Land"].append(f"1 {ft}")

                    print(
                        f"   -> Cut '{to_cut}' (Utility Land) for Basic '{ft}' "
                        f"to meet fetchable subtype targets."
                    )
                    def_val -= 1

        # =====================================================================
        # FINAL CONSOLIDATION PASS
        # =====================================================================
        final_land_counts = Counter()
        for land_entry in deck_list["Land"]:
            parts = land_entry.split(" ", 1)
            count = int(parts[0])
            land_name = parts[1]
            final_land_counts[land_name] += count

        deck_list["Land"] = [f"{count} {name}" for name, count in final_land_counts.items()]

        total_cards = sum(int(entry.split(" ", 1)[0]) for entries in deck_list.values() for entry in entries)
        land_count = sum(final_land_counts.values())
        if total_cards != 100 or not 27 <= land_count <= 31:
            print(
                f"Warning: Could not build a complete deck within the land range: "
                f"{total_cards} cards, {land_count} lands."
            )

        print(f"\n\n### {commander_name} Meta Optimized Decklist")
        new_cards = self._find_new_cards(f"{commander_name}|{bracket}", deck_list)
        if new_cards:
            print(f"\n### New Since Last Run ({len(new_cards)})")
            for name, category in new_cards:
                print(f"{name} [{category}]")
        for category in self.type_order:
            cards_in_cat = deck_list[category]
            if cards_in_cat:
                cat_total = sum(int(c.split(" ", 1)[0]) for c in cards_in_cat)
                print(f"\n### {category} ({cat_total})")
                for card_entry in sorted(cards_in_cat, key=lambda x: x.split(" ", 1)[1]):
                    print(card_entry)

    def run(self):
        if not self.commander_targets:
            print(f"No URLs configured to scrape. Ensure your configuration file exists at: {self.config_file}")
            return

        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            context = browser.new_context(permissions=["clipboard-read", "clipboard-write"])
            page = context.new_page()

            for target in self.commander_targets:
                self.analyze_commander(page, target["url"], target.get("bracket"))

            browser.close()


def main(argv=None):
    parser = argparse.ArgumentParser(description="Analyze Commander decklists.")
    parser.add_argument(
        "--debug",
        action="store_true",
        help="show deck parsing and lookup diagnostics",
    )
    args = parser.parse_args(argv)
    scanner = MTGDeckScanner(debug=args.debug)
    scanner.run()


if __name__ == "__main__":
    main()