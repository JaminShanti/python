import re
import time


class TopDeckDeckScraper:
    NEGATIVE_CACHE_TTL = 24 * 60 * 60
    CACHE_VERSION = 2

    def __init__(self, deck_cache, deck_cache_file, save_cache, debug=False):
        self.deck_cache = deck_cache
        self.deck_cache_file = deck_cache_file
        self.save_cache = save_cache
        self.debug = debug

    def _debug(self, message):
        if self.debug:
            print(message)

    def scrape_deck_data(self, page, url):
        if url in self.deck_cache:
            cached_entry = self.deck_cache[url]
            if len(cached_entry) == 3:
                cached_cards, cached_basics, cached_at = cached_entry
                if time.time() - cached_at < self.NEGATIVE_CACHE_TTL:
                    self._debug(f"   -> [DEBUG] Using temporarily cached incomplete deck: {url}")
                    return cached_cards, cached_basics
                del self.deck_cache[url]
                self._debug(f"   -> [DEBUG] Retrying expired incomplete deck cache: {url}")
            elif len(cached_entry) == 4 and cached_entry[3] == self.CACHE_VERSION:
                cached_cards, cached_basics, _, _ = cached_entry
                if sum(cached_basics.values()) > 40:
                    self._debug(
                        f"   -> [DEBUG] Ignoring malformed cached deck from {url}: "
                        f"{sum(cached_basics.values())} basic lands."
                    )
                    del self.deck_cache[url]
                else:
                    self._debug(f"   -> [DEBUG] Using cached deck: {url}")
                    return cached_cards, cached_basics
            else:
                del self.deck_cache[url]
                self._debug(f"   -> [DEBUG] Refreshing outdated deck cache: {url}")

        try:
            for attempt in range(1, 3):
                try:
                    page.goto(url, wait_until="domcontentloaded", timeout=30000)
                    copy_btn = page.get_by_text("Copy decklist", exact=False).first
                    copy_btn.wait_for(state="visible", timeout=15000)
                    copy_btn.click()
                    break
                except Exception as e:
                    if attempt == 2:
                        print(f"   -> Warning: Could not load deck listing {url}: {e}")
                        return [], self._empty_basic_counts()
                    print(f"   -> Warning: Deck listing attempt {attempt} failed for {url}; retrying: {e}")

            page.wait_for_timeout(1000)
            clipboard_text = page.evaluate("navigator.clipboard.readText()")

            if not clipboard_text:
                return [], self._empty_basic_counts()

            found = set()
            basic_counts = self._empty_basic_counts()
            basic_types = [
                "mountain", "forest", "plains", "island", "swamp", "wastes",
                "snow-covered mountain", "snow-covered forest", "snow-covered plains",
                "snow-covered island", "snow-covered swamp", "snow-covered wastes"
            ]
            total_parsed_cards = 0
            parsed_card_lines = 0
            section_headers = {"commander", "sideboard", "maybeboard", "deck"}

            for line in clipboard_text.split('\n'):
                line = line.strip()
                if not line:
                    continue

                match = re.match(r'^(\d+)[xX]\s*(.+)$', line)
                if match is None:
                    match = re.match(r'^(\d+)\s+[xX]\s+(.+)$', line)
                if match is None:
                    match = re.match(r'^(\d+)\s+(.+)$', line)
                if match:
                    count = int(match.group(1))
                    raw_name = match.group(2).strip()
                else:
                    count = 1
                    raw_name = line

                name = re.sub(r'\s+[\(\[].*$', '', raw_name).strip()
                name = re.sub(r'\s+\*[a-zA-Z0-9_]+\*.*$', '', name).strip()
                lower_name = name.lower()

                if not name or lower_name.rstrip(":") in section_headers:
                    continue

                parsed_card_lines += 1
                total_parsed_cards += count

                if lower_name in basic_types:
                    for basic in basic_counts:
                        if basic in lower_name:
                            basic_counts[basic] += count
                            break
                    if sum(basic_counts.values()) > 40:
                        self._debug(
                            f"   -> [DEBUG] Rejected malformed deck from {url}: "
                            f"{sum(basic_counts.values())} basic lands."
                        )
                        self._cache_rejected_deck(url)
                        return [], self._empty_basic_counts()
                else:
                    found.add(name)

            if total_parsed_cards < 99:
                self._debug(
                    f"   -> [DEBUG] Rejected incomplete deck from {url}: parsed "
                    f"{total_parsed_cards} cards across {parsed_card_lines} card lines."
                )
                self._cache_rejected_deck(url)
                return [], self._empty_basic_counts()

            result = (list(found), basic_counts)
            self.deck_cache[url] = (*result, time.time(), self.CACHE_VERSION)
            self.save_cache(self.deck_cache, self.deck_cache_file)
            return result

        except Exception as e:
            print(f"Error reading {url}: {e}")
            return [], self._empty_basic_counts()

    def _cache_rejected_deck(self, url):
        self.deck_cache[url] = ([], self._empty_basic_counts(), time.time())
        self.save_cache(self.deck_cache, self.deck_cache_file)

    @staticmethod
    def _empty_basic_counts():
        return {
            "mountain": 0, "forest": 0, "plains": 0,
            "island": 0, "swamp": 0, "wastes": 0
        }
