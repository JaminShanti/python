import time, re, os, pickle, requests
from urllib.parse import unquote, quote
from collections import Counter
from playwright.sync_api import sync_playwright

# Cache directory for mtg_scanner_tool
SCANNER_CACHE_DIR = "mtg_scanner_cache"
os.makedirs(SCANNER_CACHE_DIR, exist_ok=True)

# Define paths for cache files
TOPDECK_URLS_CACHE_FILE = os.path.join(SCANNER_CACHE_DIR, "topdeck_urls_cache.pkl")
SCRYFALL_DATA_CACHE_FILE = os.path.join(SCANNER_CACHE_DIR, "scryfall_data.pkl")
TOPDECK_DECK_DATA_CACHE_FILE = os.path.join(SCANNER_CACHE_DIR, "topdeck_deck_data.pkl")

# Add or remove EDHTop16 URLs here
COMMANDER_URLS = [
    "https://edhtop16.com/commander/Marwyn%2C%20the%20Nurturer?timePeriod=ONE_YEAR",
    "https://edhtop16.com/commander/Magda%2C%20Brazen%20Outlaw?timePeriod=THREE_MONTHS",
    "https://edhtop16.com/commander/Winota%2C%20Joiner%20of%20Forces?timePeriod=THREE_MONTHS",
    "https://edhtop16.com/commander/Rocco%2C%20Cabaretti%20Caterer?timePeriod=THREE_MONTHS",
]

# Lowered threshold to catch split utility lands and duals
MIN_PERCENTAGE = 0.20

# Load EXCLUDED_CARDS from file
EXCLUDED_CARDS_FILE = os.path.join(SCANNER_CACHE_DIR, "excluded_cards.txt")
EXCLUDED_CARDS = set()
if os.path.exists(EXCLUDED_CARDS_FILE):
    with open(EXCLUDED_CARDS_FILE, 'r') as f:
        EXCLUDED_CARDS = {line.strip().lower() for line in f if line.strip()}
else:
    print(f"Warning: {EXCLUDED_CARDS_FILE} not found. No cards will be excluded.")

# Strict categorization rules matching modern MTG layouts
TYPE_ORDER = ["Commander", "Creature", "Artifact", "Enchantment", "Instant", "Sorcery", "Planeswalker", "Land"]


def auto_scroll_to_bottom(page):
    """Scrolls to the bottom until the page height stops changing, loading all lists."""
    last_height = page.evaluate("document.body.scrollHeight")
    while True:
        page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        page.wait_for_timeout(2000)  # Wait for lazy-loaded content
        new_height = page.evaluate("document.body.scrollHeight")
        if new_height == last_height:
            break
        last_height = new_height


def get_topdeck_urls(page, commander_url, force_refresh=False):
    topdeck_cache = {}
    if os.path.exists(TOPDECK_URLS_CACHE_FILE):
        with open(TOPDECK_URLS_CACHE_FILE, 'rb') as f:
            topdeck_cache = pickle.load(f)

    if commander_url in topdeck_cache and not force_refresh:
        return topdeck_cache[commander_url]

    page.goto(commander_url, wait_until="networkidle")
    auto_scroll_to_bottom(page)

    html_content = page.content()
    td_regex = re.compile(r"topdeck\.gg/deck/[a-zA-Z0-9_-]+/[a-zA-Z0-9_-]+")
    matches = td_regex.findall(html_content)
    urls = list(set([f"https://{match}" for match in matches]))

    topdeck_cache[commander_url] = urls
    with open(TOPDECK_URLS_CACHE_FILE, 'wb') as f:
        pickle.dump(topdeck_cache, f)

    return urls


# Global topdeck deck cache
_topdeck_deck_cache = {}
if os.path.exists(TOPDECK_DECK_DATA_CACHE_FILE):
    with open(TOPDECK_DECK_DATA_CACHE_FILE, 'rb') as f:
        _topdeck_deck_cache = pickle.load(f)


def scrape_deck_data(page, url):
    if url in _topdeck_deck_cache:
        return _topdeck_deck_cache[url]

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
        _topdeck_deck_cache[url] = result
        with open(TOPDECK_DECK_DATA_CACHE_FILE, 'wb') as f:
            pickle.dump(_topdeck_deck_cache, f)

        return result

    except Exception as e:
        print(f"Error reading {url}: {e}")
        return [], {"mountain": 0, "forest": 0, "plains": 0, "island": 0, "swamp": 0}


# Global Scryfall cache
_scryfall_cache = {}
if os.path.exists(SCRYFALL_DATA_CACHE_FILE):
    with open(SCRYFALL_DATA_CACHE_FILE, 'rb') as f:
        _scryfall_cache = pickle.load(f)


def get_scryfall_data(card_name):
    if card_name in _scryfall_cache:
        return _scryfall_cache[card_name]

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

            _scryfall_cache[card_name] = result
            with open(SCRYFALL_DATA_CACHE_FILE, 'wb') as f:
                pickle.dump(_scryfall_cache, f)
            return result
    except Exception:
        pass

    result = (card_name, "")
    _scryfall_cache[card_name] = result
    with open(SCRYFALL_DATA_CACHE_FILE, 'wb') as f:
        pickle.dump(_scryfall_cache, f)
    return result


def categorize_card(type_line):
    if not type_line:
        return "Creature"
    tl = type_line.lower()

    if "land" in tl:
        return "Land"
    if "creature" in tl:
        return "Creature"
    if "artifact" in tl:
        return "Artifact"
    if "enchantment" in tl:
        return "Enchantment"
    if "instant" in tl:
        return "Instant"
    if "sorcery" in tl:
        return "Sorcery"
    if "planeswalker" in tl:
        return "Planeswalker"
    return "Artifact"


def analyze_commander(page, commander_url):
    name_match = re.search(r"commander/([^?]+)", commander_url)
    commander_name = unquote(name_match.group(1)) if name_match else "Unknown Commander"

    print(f"\n{'=' * 60}")
    print(f"Gathering Consensus Data for: {commander_name}")
    print(f"{'=' * 60}")

    urls = get_topdeck_urls(page, commander_url)
    if not urls:
        print("No lists found!")
        return

    print(f"Scraping {len(urls)} tournament decklists...")

    card_counter = Counter()
    total_basics = {"mountain": 0, "forest": 0, "plains": 0, "island": 0, "swamp": 0}

    for i, url in enumerate(urls, 1):
        print(f"Processing decklist {i}/{len(urls)}...", end="\r")
        cards, b_counts = scrape_deck_data(page, url)
        card_counter.update(cards)
        for k in total_basics:
            total_basics[k] += b_counts.get(k, 0)

    ui_noise = ["Mountain", "Forest", "Plains", "Island", "Swamp",
                "Snow-Covered Mountain", "Snow-Covered Forest", "Snow-Covered Plains",
                "Snow-Covered Island", "Snow-Covered Swamp",
                "Artifact", "Creature", "Instant", "Sorcery", "Enchantment", "Land", "Planeswalker"]
    excluded_lower = EXCLUDED_CARDS

    # --- NEW: Calculate True Average Category Counts ---
    total_lands_all_decks = sum(total_basics.values())
    total_instants_all_decks = 0

    unique_cards = [card for card in card_counter.keys()
                    if
                    card not in ui_noise and card.lower() not in excluded_lower and card.lower() != commander_name.lower()]

    print(f"\nAnalyzing {len(unique_cards)} unique cards to calculate true category averages...")
    for idx, card in enumerate(unique_cards, 1):
        print(f"Checking Scryfall typelines {idx}/{len(unique_cards)}...", end="\r")
        _, type_line = get_scryfall_data(card)
        cat = categorize_card(type_line)
        if cat == "Land":
            total_lands_all_decks += card_counter[card]
        elif cat == "Instant":
            total_instants_all_decks += card_counter[card]

    print(" " * 80, end="\r")  # Clear the loading line

    dynamic_min_lands = round(total_lands_all_decks / len(urls))
    dynamic_min_instants = round(total_instants_all_decks / len(urls))
    print(f"Targeting dynamic averages: {dynamic_min_lands} Lands | {dynamic_min_instants} Instants")

    # Calculate average basic lands
    avg_basics = {}
    total_avg_basics = 0
    for land, total in total_basics.items():
        avg = round(total / len(urls))
        if avg > 0:
            avg_basics[land.capitalize()] = avg
            total_avg_basics += avg

    valid_cards = []
    for card, count in card_counter.most_common():
        if card in ui_noise or card.lower() == commander_name.lower():
            continue
        if card.lower() in excluded_lower:
            continue
        if (count / len(urls)) >= MIN_PERCENTAGE:
            valid_cards.append(card)

    max_non_basics = 99 - total_avg_basics
    high_consensus_pool = valid_cards[:max_non_basics]

    deck_list = {t: [] for t in TYPE_ORDER}
    deck_list["Commander"] = [f"1 {commander_name}"]

    current_count = 1
    for idx, card in enumerate(high_consensus_pool, 1):
        real_name, type_line = get_scryfall_data(card)
        category = categorize_card(type_line)
        deck_list[category].append(f"1 {real_name}")
        current_count += 1

    # --- SAFETY NET: Enforce Dynamic Minimum Instant Count ---
    current_instants = len(deck_list["Instant"])
    if current_instants < dynamic_min_instants:
        instant_deficit = dynamic_min_instants - current_instants

        # Find the next best instants from the overall pool that didn't make the consensus cut
        next_best_instants = []
        for card, count in card_counter.most_common():
            if card in ui_noise or card.lower() == commander_name.lower() or card.lower() in excluded_lower:
                continue
            if card not in high_consensus_pool:
                _, type_line = get_scryfall_data(card)
                if categorize_card(type_line) == "Instant":
                    next_best_instants.append(card)
                    if len(next_best_instants) == instant_deficit:
                        break

        # Swap out the weakest non-land, non-instant cards to make room
        instants_to_add = len(next_best_instants)
        cards_removed = 0
        for card in reversed(high_consensus_pool):
            if cards_removed >= instants_to_add:
                break

            for category in TYPE_ORDER:
                if category in ["Land", "Commander", "Instant"]:
                    continue

                matched_item = next((item for item in deck_list[category] if item.endswith(f" {card}")), None)
                if matched_item:
                    deck_list[category].remove(matched_item)
                    current_count -= 1
                    cards_removed += 1
                    print(f"   -> Cut '{card}' to make room for required average Instants.")
                    break

        # Add the rescued instants into the deck
        for card in next_best_instants:
            real_name, type_line = get_scryfall_data(card)
            deck_list["Instant"].append(f"1 {real_name}")
            current_count += 1
            print(f"   -> Added '{card}' (Instant) to meet dynamic average.")

    # --- SAFETY NET: Enforce Dynamic Minimum Land Count ---
    current_lands = len(deck_list["Land"])
    slots_remaining = 100 - current_count

    if (current_lands + slots_remaining) < dynamic_min_lands:
        deficit = dynamic_min_lands - (current_lands + slots_remaining)

        cards_removed = 0
        for card in reversed(high_consensus_pool):
            if cards_removed >= deficit:
                break

            for category in TYPE_ORDER:
                # Protect both Lands AND our carefully curated Instants from being cut here
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

    # --- INJECT BASIC LANDS ---
    primary_basic = "Forest"
    if avg_basics:
        primary_basic = sorted(avg_basics.items(), key=lambda x: x[1], reverse=True)[0][0]
    else:
        most_seen = max(total_basics, key=total_basics.get)
        if total_basics[most_seen] > 0:
            primary_basic = most_seen.capitalize()

    if slots_remaining > 0:
        if avg_basics:
            sorted_basics = sorted(avg_basics.items(), key=lambda x: x[1], reverse=True)
            for land_name, count in sorted_basics:
                if slots_remaining <= 0:
                    break
                allocated = min(count, slots_remaining)
                deck_list["Land"].append(f"{allocated} {land_name}")
                slots_remaining -= allocated

        if slots_remaining > 0:
            found = False
            for idx, entry in enumerate(deck_list["Land"]):
                if primary_basic in entry:
                    old_count = int(entry.split(' ', 1)[0])
                    deck_list["Land"][idx] = f"{old_count + slots_remaining} {primary_basic}"
                    found = True
                    break

            if not found:
                deck_list["Land"].append(f"{slots_remaining} {primary_basic}")

    # Final Output Rendering
    print(f"\n\n### {commander_name} - Meta Optimized Decklist")
    for category in TYPE_ORDER:
        cards_in_cat = deck_list[category]
        if cards_in_cat:
            cat_total = sum([int(c.split(' ', 1)[0]) for c in cards_in_cat])
            print(f"\n### {category} ({cat_total})")
            for card_entry in sorted(cards_in_cat, key=lambda x: x.split(' ', 1)[1]):
                print(card_entry)


def main():
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()

        for url in COMMANDER_URLS:
            analyze_commander(page, url)

        browser.close()


if __name__ == "__main__":
    main()