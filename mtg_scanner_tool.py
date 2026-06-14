import time
import re
from urllib.parse import unquote
from collections import Counter
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


def analyze_commander(page, commander_url):
    name_match = re.search(r"commander/([^?]+)", commander_url)
    commander_name = unquote(name_match.group(1)) if name_match else "Unknown Commander"

    print(f"\n{'=' * 50}")
    print(f"Analyzing: {commander_name}")
    print(f"{'=' * 50}")

    urls = get_topdeck_urls(page, commander_url)
    if not urls:
        print("No lists found!")
        return

    print(f"Parsing {len(urls)} decklists...")

    card_counter = Counter()
    total_basics = {"mountain": 0, "forest": 0, "plains": 0, "island": 0, "swamp": 0}

    for i, url in enumerate(urls, 1):
        print(f"Processing {i}/{len(urls)}...", end="\r")
        cards, b_counts = scrape_deck_data(page, url)
        card_counter.update(cards)
        for k in total_basics:
            total_basics[k] += b_counts.get(k, 0)

    print("\n\n--- Average Basic Lands ---")
    for land, total in total_basics.items():
        avg = total / len(urls)
        if avg > 0.1:
            print(f"Average {land.capitalize()} count: {avg:.1f}")

    print(f"\n--- Popular Cards (>= {MIN_PERCENTAGE * 100:.0f}% Meta) ---")

    ui_noise = ["Mountain", "Forest", "Plains", "Island", "Swamp",
                "Snow-Covered Mountain", "Snow-Covered Forest", "Snow-Covered Plains",
                "Snow-Covered Island", "Snow-Covered Swamp",
                "Artifact", "Creature", "Instant", "Sorcery", "Enchantment", "Land", "Planeswalker"]

    for card, count in card_counter.most_common():
        percentage = count / len(urls)
        if percentage >= MIN_PERCENTAGE and card not in ui_noise:
            print(f"{percentage * 100:>5.1f}% | {card} ({count} decks)")


def main():
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()

        for url in COMMANDER_URLS:
            analyze_commander(page, url)

        browser.close()


if __name__ == "__main__":
    main()