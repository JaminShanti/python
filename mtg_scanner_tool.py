import time, re, os, pickle, requests
from urllib.parse import unquote, quote
from collections import Counter
from playwright.sync_api import sync_playwright


class MTGDeckScanner:
    def __init__(self, cache_dir="mtg_scanner_cache", min_percentage=0.20):
        # Configuration
        self.cache_dir = cache_dir
        self.min_percentage = min_percentage
        os.makedirs(self.cache_dir, exist_ok=True)

        # File Paths
        self.urls_cache_file = os.path.join(self.cache_dir, "topdeck_urls_cache.pkl")
        self.scryfall_cache_file = os.path.join(self.cache_dir, "scryfall_data.pkl")
        self.deck_cache_file = os.path.join(self.cache_dir, "topdeck_deck_data.pkl")
        self.excluded_file = os.path.join(self.cache_dir, "excluded_cards.txt")

        # Categorization Rules
        self.type_order = ["Commander", "Creature", "Artifact", "Enchantment", "Instant", "Sorcery", "Planeswalker",
                           "Land"]
        self.type_hierarchy = ("Land", "Creature", "Artifact", "Enchantment", "Instant", "Sorcery", "Planeswalker")

        # Noise filters to clean data
        self.ui_noise = [
            "Mountain", "Forest", "Plains", "Island", "Swamp",
            "Snow-Covered Mountain", "Snow-Covered Forest", "Snow-Covered Plains",
            "Snow-Covered Island", "Snow-Covered Swamp", "Artifact", "Creature",
            "Instant", "Sorcery", "Enchantment", "Land", "Planeswalker"
        ]

        # Target Decklists
        self.commander_urls = [
            "https://edhtop16.com/commander/Marwyn%2C%20the%20Nurturer?timePeriod=ONE_YEAR",
            "https://edhtop16.com/commander/Magda%2C%20Brazen%20Outlaw?timePeriod=THREE_MONTHS",
            "https://edhtop16.com/commander/Winota%2C%20Joiner%20of%20Forces?timePeriod=THREE_MONTHS",
            "https://edhtop16.com/commander/Rocco%2C%20Cabaretti%20Caterer?timePeriod=THREE_MONTHS",
            "https://edhtop16.com/commander/Azami%2C%20Lady%20of%20Scrolls?timePeriod=SIX_MONTHS",
        ]

        # Load Exclusions and Caches
        self.excluded_cards = self._load_exclusions()
        self.topdeck_cache = self._load_cache(self.urls_cache_file)
        self.deck_cache = self._load_cache(self.deck_cache_file)
        self.scryfall_cache = self._load_cache(self.scryfall_cache_file)

    def _load_exclusions(self):
        """Loads user-defined card exclusions and replacements from a text file."""
        exclusions = {}
        if os.path.exists(self.excluded_file):
            with open(self.excluded_file, 'r') as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    if '->' in line:
                        target, replacement = line.split('->', 1)
                        exclusions[target.strip().lower()] = replacement.strip()
                    else:
                        exclusions[line.lower()] = None
            return exclusions
        print(f"Warning: {self.excluded_file} not found. No cards will be excluded.")
        return exclusions

    def _load_cache(self, filepath):
        """Helper to load a pickle cache."""
        if os.path.exists(filepath):
            with open(filepath, 'rb') as f:
                return pickle.load(f)
        return {}

    def _save_cache(self, cache_dict, filepath):
        """Helper to save a pickle cache."""
        with open(filepath, 'wb') as f:
            pickle.dump(cache_dict, f)

    def auto_scroll_to_bottom(self, page):
        """Scrolls to the bottom until the page height stops changing, loading all lists."""
        last_height = page.evaluate("document.body.scrollHeight")
        while True:
            page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            page.wait_for_timeout(2000)  # Wait for lazy-loaded content
            new_height = page.evaluate("document.body.scrollHeight")
            if new_height == last_height:
                break
            last_height = new_height

    def get_topdeck_urls(self, page, commander_url, force_refresh=False):
        """Fetches and caches individual decklist URLs from a commander page."""
        if commander_url in self.topdeck_cache and not force_refresh:
            return self.topdeck_cache[commander_url]

        page.goto(commander_url, wait_until="networkidle")
        self.auto_scroll_to_bottom(page)

        html_content = page.content()
        td_regex = re.compile(r"topdeck\.gg/deck/[a-zA-Z0-9_-]+/[a-zA-Z0-9_-]+")
        matches = td_regex.findall(html_content)
        urls = list(set([f"https://{match}" for match in matches]))

        self.topdeck_cache[commander_url] = urls
        self._save_cache(self.topdeck_cache, self.urls_cache_file)
        return urls

    def scrape_deck_data(self, page, url):
        """Scrapes and caches the card list and basic lands from a single decklist."""
        if url in self.deck_cache:
            return self.deck_cache[url]

        try:
            page.goto(url, wait_until="networkidle")
            page.wait_for_timeout(1500)

            data = page.evaluate('''() => {
                let found = new Set();
                let basicCounts = { mountain: 0, forest: 0, plains: 0, island: 0, swamp: 0 };
                const cleanName = (name) => name.replace(/^\\s*\\d+x?\\s+/i, '').trim();

                document.querySelectorAll('img[alt]').forEach(img => {
                    let name = img.getAttribute('alt');
                    if (!name || name.length < 2) return;

                    let lowerName = name.toLowerCase();
                    let basicTypes = ["mountain", "forest", "plains", "island", "swamp", 
                                      "snow-covered mountain", "snow-covered forest", 
                                      "snow-covered plains", "snow-covered island", "snow-covered swamp"];

                    if (basicTypes.includes(lowerName)) {
                        let text = img.parentElement.innerText || "";
                        let match = text.match(/(\\d+)/);
                        let count = match ? parseInt(match[1]) : 1;

                        if (lowerName.includes("mountain")) basicCounts.mountain += count;
                        else if (lowerName.includes("forest")) basicCounts.forest += count;
                        else if (lowerName.includes("plains")) basicCounts.plains += count;
                        else if (lowerName.includes("island")) basicCounts.island += count;
                        else if (lowerName.includes("swamp")) basicCounts.swamp += count;
                    } else {
                        found.add(cleanName(name));
                    }
                });
                return { cards: Array.from(found), basicCounts };
            }''')

            bad = ["topdeck", "logo", "avatar", "profile", "banner", "discord", "twitter",
                   "match history", "standings", "deck", "event", "buy", "card image"]

            clean_cards = [n for n in data['cards'] if not any(b in n.lower() for b in bad) and not re.match(r'^\d', n)]

            result = (clean_cards, data['basicCounts'])
            self.deck_cache[url] = result
            self._save_cache(self.deck_cache, self.deck_cache_file)

            return result

        except Exception as e:
            print(f"Error reading {url}: {e}")
            return [], {"mountain": 0, "forest": 0, "plains": 0, "island": 0, "swamp": 0}

    def get_scryfall_data(self, card_name):
        """Fetches clean layout data and exact type rules from Scryfall."""
        if card_name in self.scryfall_cache:
            return self.scryfall_cache[card_name]

        time.sleep(0.1)  # Polite rate limiting rule for Scryfall API
        url = f"https://api.scryfall.com/cards/named?exact={quote(card_name)}"
        try:
            response = requests.get(url, headers={"User-Agent": "EDH-Builder-Script/1.0"})
            if response.status_code == 200:
                res_data = response.json()
                type_line = res_data.get("type_line", "")
                if "card_faces" in res_data and not type_line:
                    type_line = res_data["card_faces"][0].get("type_line", "")
                result = (res_data.get("name", card_name), type_line)

                self.scryfall_cache[card_name] = result
                self._save_cache(self.scryfall_cache, self.scryfall_cache_file)
                return result
        except Exception:
            pass

        result = (card_name, "")
        self.scryfall_cache[card_name] = result
        self._save_cache(self.scryfall_cache, self.scryfall_cache_file)
        return result

    def categorize_card(self, type_line):
        """Sorts card types based on the predefined hierarchy."""
        if not type_line:
            return "Creature"

        # Handle double-faced cards (MDFCs / Transform cards)
        if "//" in type_line:
            front, back = type_line.split("//", 1)
            # If the front face has a valid non-land type (e.g., Instant, Sorcery, Enchantment), categorize it by that.
            front_match = next((t for t in self.type_hierarchy if t.lower() in front.lower() and t != "Land"), None)
            if front_match:
                return front_match

        # Standard fallback for single-faced cards, artifact lands, etc.
        return next((t for t in self.type_hierarchy if t.lower() in type_line.lower()), "Artifact")

    def analyze_commander(self, page, commander_url):
        """Main operational logic for a single commander."""
        name_match = re.search(r"commander/([^?]+)", commander_url)
        commander_name = unquote(name_match.group(1)) if name_match else "Unknown Commander"

        print(f"\n{'=' * 60}")
        print(f"Gathering Consensus Data for: {commander_name}")
        print(f"{'=' * 60}")

        urls = self.get_topdeck_urls(page, commander_url)
        if not urls:
            print("No lists found!")
            return

        print(f"Scraping {len(urls)} tournament decklists...")

        raw_card_counter = Counter()
        card_counter = Counter()
        total_basics = {"mountain": 0, "forest": 0, "plains": 0, "island": 0, "swamp": 0}

        valid_deck_count = 0

        for i, url in enumerate(urls, 1):
            print(f"Processing decklist {i}/{len(urls)}...", end="\r")
            cards, b_counts = self.scrape_deck_data(page, url)

            if not cards and sum(b_counts.values()) == 0:
                continue

            valid_deck_count += 1
            raw_card_counter.update(cards)

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

        if valid_deck_count == 0:
            print(f"\nNo valid decklists could be processed for {commander_name}.")
            return

        total_lands_all_decks = sum(total_basics.values())
        total_instants_all_decks = 0

        raw_unique_cards = [card for card in raw_card_counter.keys() if card not in self.ui_noise
                            and card.lower() != commander_name.lower()]

        print(f"\nAnalyzing {len(raw_unique_cards)} unique cards to calculate true category averages...")
        for idx, card in enumerate(raw_unique_cards, 1):
            print(f"Checking Scryfall typelines {idx}/{len(raw_unique_cards)}...", end="\r")
            _, type_line = self.get_scryfall_data(card)
            cat = self.categorize_card(type_line)
            if cat == "Land":
                total_lands_all_decks += raw_card_counter[card]
            elif cat == "Instant":
                total_instants_all_decks += raw_card_counter[card]

        print(" " * 80, end="\r")

        dynamic_min_lands = round(total_lands_all_decks / valid_deck_count)
        dynamic_min_instants = round(total_instants_all_decks / valid_deck_count)
        print(f"Targeting dynamic averages: {dynamic_min_lands} Lands | {dynamic_min_instants} Instants")

        avg_basics = {}
        total_avg_basics = 0
        for land, total in total_basics.items():
            avg = round(total / valid_deck_count)
            if avg > 0:
                avg_basics[land.capitalize()] = avg
                total_avg_basics += avg

        valid_cards = []
        for card, count in card_counter.most_common():
            if card in self.ui_noise or card.lower() == commander_name.lower() or card.lower() in self.excluded_cards:
                continue
            if (count / valid_deck_count) >= self.min_percentage:
                valid_cards.append(card)

        max_non_basics = 99 - total_avg_basics
        high_consensus_pool = valid_cards[:max_non_basics]

        deck_list = {t: [] for t in self.type_order}
        deck_list["Commander"] = [f"1 {commander_name}"]

        current_count = 1
        for idx, card in enumerate(high_consensus_pool, 1):
            real_name, type_line = self.get_scryfall_data(card)
            category = self.categorize_card(type_line)
            deck_list[category].append(f"1 {real_name}")
            current_count += 1

        current_instants = len(deck_list["Instant"])
        if current_instants < dynamic_min_instants:
            instant_deficit = dynamic_min_instants - current_instants

            next_best_instants = []
            for card, count in card_counter.most_common():
                if card in self.ui_noise or card.lower() == commander_name.lower() or card.lower() in self.excluded_cards:
                    continue
                if card not in high_consensus_pool:
                    _, type_line = self.get_scryfall_data(card)
                    if self.categorize_card(type_line) == "Instant":
                        next_best_instants.append(card)
                        if len(next_best_instants) == instant_deficit:
                            break

            instants_to_add = len(next_best_instants)
            cards_removed = 0
            for card in reversed(high_consensus_pool):
                if cards_removed >= instants_to_add:
                    break

                for category in self.type_order:
                    if category in ["Land", "Commander", "Instant"]:
                        continue

                    matched_item = next((item for item in deck_list[category] if item.endswith(f" {card}")), None)
                    if matched_item:
                        deck_list[category].remove(matched_item)
                        current_count -= 1
                        cards_removed += 1
                        print(f"   -> Cut '{card}' to make room for required average Instants.")
                        break

            for card in next_best_instants:
                real_name, type_line = self.get_scryfall_data(card)
                deck_list["Instant"].append(f"1 {real_name}")
                current_count += 1
                print(f"   -> Added '{card}' (Instant) to meet dynamic average.")

        current_lands = len(deck_list["Land"])
        slots_remaining = 100 - current_count

        if (current_lands + slots_remaining) < dynamic_min_lands:
            deficit = dynamic_min_lands - (current_lands + slots_remaining)

            cards_removed = 0
            for card in reversed(high_consensus_pool):
                if cards_removed >= deficit:
                    break

                for category in self.type_order:
                    if category in ["Land", "Commander", "Instant"]:
                        continue

                    matched_item = next((item for item in deck_list[category] if item.endswith(f" {card}")), None)
                    if matched_item:
                        deck_list[category].remove(matched_item)
                        current_count -= 1
                        slots_remaining += 1
                        cards_removed += 1
                        print(f"   -> Cut '{card}' to make room for required average Lands.")
                        break

        basic_types_to_use = [k.capitalize() for k, v in sorted(total_basics.items(), key=lambda x: x[1], reverse=True)
                              if v > 0]

        if not basic_types_to_use:
            basic_types_to_use = ["Forest", "Island", "Swamp", "Mountain", "Plains"]

        for basic_name in basic_types_to_use:
            if avg_basics.get(basic_name, 0) > 0 and slots_remaining > 0:
                allocated = min(avg_basics[basic_name], slots_remaining)
                deck_list["Land"].append(f"{allocated} {basic_name}")
                slots_remaining -= allocated

        if slots_remaining > 0:
            distribution = {b: 0 for b in basic_types_to_use}
            idx = 0
            while slots_remaining > 0:
                distribution[basic_types_to_use[idx % len(basic_types_to_use)]] += 1
                slots_remaining -= 1
                idx += 1

            for basic_name, count in distribution.items():
                if count > 0:
                    found = False
                    for i, entry in enumerate(deck_list["Land"]):
                        if entry.endswith(f" {basic_name}"):
                            old_count = int(entry.split(' ', 1)[0])
                            deck_list["Land"][i] = f"{old_count + count} {basic_name}"
                            found = True
                            break
                    if not found:
                        deck_list["Land"].append(f"{count} {basic_name}")

        print(f"\n\n### {commander_name} - Meta Optimized Decklist")
        for category in self.type_order:
            cards_in_cat = deck_list[category]
            if cards_in_cat:
                cat_total = sum([int(c.split(' ', 1)[0]) for c in cards_in_cat])
                print(f"\n### {category} ({cat_total})")
                for card_entry in sorted(cards_in_cat, key=lambda x: x.split(' ', 1)[1]):
                    print(card_entry)

    def run(self):
        """Entry point to launch the browser and process all target URLs."""
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            page = browser.new_page()

            for url in self.commander_urls:
                self.analyze_commander(page, url)

            browser.close()


if __name__ == "__main__":
    scanner = MTGDeckScanner()
    scanner.run()