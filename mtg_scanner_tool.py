import time
import re
from urllib.parse import unquote, quote
from collections import Counter
import requests
from playwright.sync_api import sync_playwright

# Add or remove EDHTop16 URLs here
COMMANDER_URLS = [
    "https://edhtop16.com/commander/Marwyn%2C%20the%20Nurturer?timePeriod=ONE_YEAR",
    "https://edhtop16.com/commander/Magda%2C%20Brazen%20Outlaw?timePeriod=THREE_MONTHS",
    "https://edhtop16.com/commander/Winota%2C%20Joiner%20of%20Forces?timePeriod=THREE_MONTHS",
    "https://edhtop16.com/commander/Rocco%2C%20Cabaretti%20Caterer?timePeriod=THREE_MONTHS",
]

# Lowered threshold to catch split utility lands and duals
MIN_PERCENTAGE = 0.20

# Add cards you DO NOT want in your generated lists here (e.g., expensive staples you want to skip)
EXCLUDED_CARDS = [
    "Talon Gates of Madara",
    "Gaea's Cradle",
    "Crop Rotation",
    "Legolas's Quick Reflexes",
    "Survival of the Fittest ",
    "Bazaar of Baghdad",
    "Tabernacle at Pendrell Vale",
    "Boseiju, Who Endures",
    "City of Traitors",
    "Last March of the Ents"
]

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


def get_topdeck_urls(page, commander_url):
    page.goto(commander_url, wait_until="networkidle")
    auto_scroll_to_bottom(page)

    html_content = page.content()
    td_regex = re.compile(r"topdeck\.gg/deck/[a-zA-Z0-9_-]+/[a-zA-Z0-9_-]+")
    matches = td_regex.findall(html_content)

    return list(set([f"https://{match}" for match in matches]))


def scrape_deck_data(page, url):
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
        return clean_cards, data['basicCounts']
    except Exception as e:
        print(f"Error reading {url}: {e}")
        return [], {"mountain": 0, "forest": 0, "plains": 0, "island": 0, "swamp": 0}


def get_scryfall_data(card_name):
    """Fetches clean layout data and exact type rules from Scryfall."""
    time.sleep(0.1)  # Polite rate limiting rule for Scryfall API (10 requests/second)
    url = f"https://api.scryfall.com/cards/named?exact={quote(card_name)}"
    try:
        response = requests.get(url, headers={"User-Agent": "EDH-Builder-Script/1.0"})
        if response.status_code == 200:
            res_data = response.json()
            # Handle split/MDFCs card type lines seamlessly
            type_line = res_data.get("type_line", "")
            if "card_faces" in res_data and not type_line:
                type_line = res_data["card_faces"][0].get("type_line", "")
            return res_data.get("name", card_name), type_line
    except Exception:
        pass
    return card_name, ""


def categorize_card(type_line):
    """Sorts card types strictly prioritizing Land -> Creature -> down the line."""
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

    # Clean UI artifacts out of data collection
    ui_noise = ["Mountain", "Forest", "Plains", "Island", "Swamp",
                "Snow-Covered Mountain", "Snow-Covered Forest", "Snow-Covered Plains",
                "Snow-Covered Island", "Snow-Covered Swamp",
                "Artifact", "Creature", "Instant", "Sorcery", "Enchantment", "Land", "Planeswalker"]

    # Normalize exclusions to lowercase for safe matching
    excluded_lower = {c.lower() for c in EXCLUDED_CARDS}

    # Calculate average basic lands
    avg_basics = {}
    total_avg_basics = 0
    for land, total in total_basics.items():
        avg = round(total / len(urls))
        if avg > 0:
            avg_basics[land.capitalize()] = avg
            total_avg_basics += avg

    # Compile non-basic cards reaching consensus thresholds
    valid_cards = []
    for card, count in card_counter.most_common():
        if card in ui_noise or card.lower() == commander_name.lower():
            continue
        # Skip card if it's in the exclusion list
        if card.lower() in excluded_lower:
            continue

        if (count / len(urls)) >= MIN_PERCENTAGE:
            valid_cards.append(card)

    # Cap the non-basics to fit the 100-card limit (99 - average basics)
    max_non_basics = 99 - total_avg_basics
    high_consensus_pool = valid_cards[:max_non_basics]

    print(f"\nVerifying card mechanics via Scryfall for {len(high_consensus_pool)} meta cards...")

    deck_list = {t: [] for t in TYPE_ORDER}
    deck_list["Commander"] = [f"1 {commander_name}"]

    current_count = 1  # Starting with commander
    for idx, card in enumerate(high_consensus_pool, 1):
        print(f"Mapping card types {idx}/{len(high_consensus_pool)}...", end="\r")
        real_name, type_line = get_scryfall_data(card)
        category = categorize_card(type_line)
        deck_list[category].append(f"1 {real_name}")
        current_count += 1

    print(" " * 60, end="\r")  # Clear the line

    # Dynamically inject basic lands to hit exactly 100 cards
    slots_remaining = 100 - current_count
    if slots_remaining > 0 and avg_basics:
        # Sort basics by average occurrence rate
        sorted_basics = sorted(avg_basics.items(), key=lambda x: x[1], reverse=True)

        for land_name, count in sorted_basics:
            if slots_remaining <= 0:
                break
            allocated = min(count, slots_remaining)
            deck_list["Land"].append(f"{allocated} {land_name}")
            slots_remaining -= allocated

        # Edge-case: If we still have slots open, dump the remaining slots into the most played basic land
        if slots_remaining > 0:
            primary_land = sorted_basics[0][0]
            # Find it and update the count
            for idx, entry in enumerate(deck_list["Land"]):
                if primary_land in entry:
                    old_count = int(entry.split(' ', 1)[0])
                    deck_list["Land"][idx] = f"{old_count + slots_remaining} {primary_land}"
                    break

    # Final Output Rendering - Clean TCGPlayer Format
    print(f"\n\n### {commander_name} - Meta Optimized Decklist")
    for category in TYPE_ORDER:
        cards_in_cat = deck_list[category]
        if cards_in_cat:
            # Sum up card counts dynamically inside category block strings
            cat_total = sum([int(c.split(' ', 1)[0]) for c in cards_in_cat])
            print(f"\n### {category} ({cat_total})")
            for card_entry in sorted(cards_in_cat,
                                     key=lambda x: x.split(' ', 1)[1]):  # Sort alphabetically by card name
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