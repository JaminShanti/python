from magic_game_scripts import mtg_scanner_tool as module
from pathlib import Path
import pytest

def test_imports():
    assert module is not None


def test_default_cache_is_script_relative(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("CACHE_ROOT", raising=False)
    monkeypatch.setattr(module.MTGDeckScanner, "_fetch_game_changers", lambda self: set())
    monkeypatch.setattr(module.MTGDeckScanner, "_load_config_urls", lambda self: [])
    monkeypatch.setattr(module.MTGDeckScanner, "_load_exclusions", lambda self: {})
    monkeypatch.setattr(module.MTGDeckScanner, "_load_bracket_2_exclusions", lambda self: {})
    monkeypatch.setattr(module.MTGDeckScanner, "_load_cache", lambda self, *args, **kwargs: {})
    scanner = module.MTGDeckScanner()
    assert Path(scanner.cache_dir) == Path(module.__file__).resolve().parent / "mtg_scanner_cache"


def test_cache_root_override(monkeypatch, tmp_path):
    monkeypatch.setenv("CACHE_ROOT", str(tmp_path))
    monkeypatch.setattr(module.MTGDeckScanner, "_fetch_game_changers", lambda self: set())
    scanner = module.MTGDeckScanner()
    assert Path(scanner.cache_dir) == tmp_path / "mtg_scanner_cache"


def test_topdeck_url_cache_expires_after_seven_days(monkeypatch, tmp_path):
    cache_loads = []
    monkeypatch.setattr(module.MTGDeckScanner, "_fetch_game_changers", lambda self: set())
    monkeypatch.setattr(
        module.MTGDeckScanner, "_load_cache",
        lambda self, path, ttl_seconds=None: cache_loads.append((path, ttl_seconds)) or {},
    )

    scanner = module.MTGDeckScanner(cache_dir=str(tmp_path))

    assert (scanner.urls_cache_file, 7 * 86400) in cache_loads


def test_main_enables_debug_from_command_line(monkeypatch):
    instances = []

    class FakeScanner:
        def __init__(self, debug):
            self.debug = debug
            self.ran = False
            instances.append(self)

        def run(self):
            self.ran = True

    monkeypatch.setattr(module, "MTGDeckScanner", FakeScanner)

    module.main(["--debug"])

    assert instances[0].debug is True
    assert instances[0].ran is True


def test_main_disables_debug_by_default(monkeypatch):
    instances = []

    class FakeScanner:
        def __init__(self, debug):
            instances.append(debug)

        def run(self):
            pass

    monkeypatch.setattr(module, "MTGDeckScanner", FakeScanner)

    module.main([])

    assert instances == [False]


class ClipboardPage:
    def __init__(self, clipboard_text, fail_first_navigation=False):
        self.clipboard_text = clipboard_text
        self.fail_first_navigation = fail_first_navigation
        self.navigation_count = 0

    @property
    def first(self):
        return self

    def goto(self, url, wait_until, timeout):
        self.navigation_count += 1
        if self.fail_first_navigation and self.navigation_count == 1:
            raise TimeoutError("simulated navigation timeout")

    def wait_for_timeout(self, timeout):
        pass

    def get_by_text(self, text, exact):
        return self

    def count(self):
        return 1

    def is_visible(self):
        return True

    def wait_for(self, state, timeout):
        pass

    def click(self):
        pass

    def evaluate(self, script):
        return self.clipboard_text


def create_deck_scraper(debug=False):
    return module.TopDeckDeckScraper({}, "unused", lambda cache, path: None, debug=debug)


def test_scrape_deck_data_counts_quantity_and_plain_name_lines(capsys):
    clipboard = "\n".join(
        ["Commander:", *["Lightning Bolt (M21) 199" for _ in range(97)],
         "1x Island", "1 Forest"]
    )

    cards, basics = create_deck_scraper().scrape_deck_data(ClipboardPage(clipboard), "deck-url")

    assert cards == ["Lightning Bolt"]
    assert basics["island"] == 1
    assert basics["forest"] == 1
    assert "Rejected incomplete deck" not in capsys.readouterr().out


def test_scrape_deck_data_reports_incomplete_deck(capsys):
    clipboard = "\n".join(["Lightning Bolt" for _ in range(98)])

    cards, basics = create_deck_scraper(debug=True).scrape_deck_data(ClipboardPage(clipboard), "deck-url")

    assert cards == []
    assert sum(basics.values()) == 0
    assert "parsed 98 cards across 98 card lines" in capsys.readouterr().out


def test_scrape_deck_data_caches_incomplete_deck_temporarily():
    clipboard = "\n".join(["Lightning Bolt" for _ in range(98)])
    scraper = create_deck_scraper()
    page = ClipboardPage(clipboard)

    scraper.scrape_deck_data(page, "deck-url")
    cards, basics = scraper.scrape_deck_data(page, "deck-url")

    assert cards == []
    assert sum(basics.values()) == 0
    assert page.navigation_count == 1
    assert len(scraper.deck_cache["deck-url"]) == 3


def test_scrape_deck_data_retries_expired_incomplete_cache():
    clipboard = "\n".join(["Lightning Bolt" for _ in range(99)])
    scraper = create_deck_scraper()
    scraper.deck_cache["deck-url"] = (
        [],
        scraper._empty_basic_counts(),
        0,
    )
    page = ClipboardPage(clipboard)

    cards, _ = scraper.scrape_deck_data(page, "deck-url")

    assert cards == ["Lightning Bolt"]
    assert page.navigation_count == 1
    assert len(scraper.deck_cache["deck-url"]) == 4


def test_scrape_deck_data_hides_diagnostics_when_debug_is_disabled(capsys):
    clipboard = "\n".join(["Lightning Bolt" for _ in range(98)])

    create_deck_scraper().scrape_deck_data(ClipboardPage(clipboard), "deck-url")

    assert "[DEBUG]" not in capsys.readouterr().out


def test_scrape_deck_data_retries_timed_out_navigation():
    clipboard = "\n".join(["Lightning Bolt" for _ in range(99)])
    page = ClipboardPage(clipboard, fail_first_navigation=True)

    cards, _ = create_deck_scraper().scrape_deck_data(page, "deck-url")

    assert cards == ["Lightning Bolt"]
    assert page.navigation_count == 2


def test_scrape_deck_data_rejects_implausible_basic_land_count(capsys):
    clipboard = "\n".join(["1 Braids, Arisen Nightmare", "99 Swamp"])

    cards, basics = create_deck_scraper(debug=True).scrape_deck_data(ClipboardPage(clipboard), "deck-url")

    assert cards == []
    assert sum(basics.values()) == 0
    assert "Rejected malformed deck" in capsys.readouterr().out


def test_scrape_deck_data_discards_malformed_cached_deck():
    clipboard = "\n".join(["1 Lightning Bolt" for _ in range(99)])
    scraper = create_deck_scraper()
    scraper.deck_cache["deck-url"] = (
        [],
        {"mountain": 0, "forest": 0, "plains": 0, "island": 0, "swamp": 99, "wastes": 0},
    )

    cards, _ = scraper.scrape_deck_data(ClipboardPage(clipboard), "deck-url")

    assert cards == ["Lightning Bolt"]


def test_scrape_deck_data_preserves_names_starting_with_x():
    clipboard = "\n".join(
        ["1 Xorn", "1 x Xantcha, Sleeper Agent", "1x Xenagos, God of Revels",
         "1 X Xyris, the Writhing Storm", "95 Island"]
    )
    clipboard = clipboard.replace("95 Island", "35 Island\n64 Lightning Bolt")
    scraper = create_deck_scraper()

    cards, basics = scraper.scrape_deck_data(ClipboardPage(clipboard), "deck-url")

    assert set(cards) == {"Xorn", "Xantcha, Sleeper Agent", "Xenagos, God of Revels",
                          "Xyris, the Writhing Storm", "Lightning Bolt"}
    assert basics["island"] == 35


def test_scrape_deck_data_refreshes_old_cache_and_reuses_fixed_entry():
    scraper = create_deck_scraper()
    scraper.deck_cache["deck-url"] = (["orn"], scraper._empty_basic_counts())
    page = ClipboardPage("1 Xorn\n98 Lightning Bolt")

    cards, _ = scraper.scrape_deck_data(page, "deck-url")
    cached_cards, _ = scraper.scrape_deck_data(page, "deck-url")

    assert set(cards) == {"Xorn", "Lightning Bolt"}
    assert set(cached_cards) == set(cards)
    assert page.navigation_count == 1
    assert scraper.deck_cache["deck-url"][3] == scraper.CACHE_VERSION


def test_recent_search_counts_usable_decks_and_reuses_attempts(capsys):
    scanner = module.MTGDeckScanner.__new__(module.MTGDeckScanner)
    scanner.debug = False
    searches = []
    calls = []

    def get_urls(page, url):
        query = module.parse_qsl(module.urlparse(url).query)
        searches.append(dict(query))
        if len(searches) == 1:
            return [f"deck-{i}" for i in range(31)]
        return [f"deck-{i}" for i in range(35)]

    class Scraper:
        def scrape_deck_data(self, page, url):
            calls.append(url)
            if url in {"deck-0", "deck-1"}:
                return [], {}
            return [url], {}

    scanner.get_topdeck_urls = get_urls
    scanner.deck_scraper = Scraper()
    urls, valid = scanner._select_recent_decks(
        None, "https://example.com/commander/Test?timePeriod=ALL_TIME&minEventSize=0&standing=top"
    )

    assert searches == [
        {"standing": "top", "timePeriod": "THREE_MONTHS", "minEventSize": "50"},
        {"standing": "top", "timePeriod": "THREE_MONTHS", "minEventSize": "30"},
    ]
    assert len(urls) == 35
    assert len(valid) == 33
    assert len(calls) == len(set(calls)) == 35
    output = capsys.readouterr().out
    assert "THREE_MONTHS, 50+ players: 29/31 usable decklists" in output
    assert "Processing deck" not in output
    assert "expanding search criteria" not in output


def test_recent_search_keeps_identical_decks_from_separate_entries():
    scanner = module.MTGDeckScanner.__new__(module.MTGDeckScanner)
    scanner.debug = False
    searches = []
    scanner.get_topdeck_urls = lambda page, url: searches.append(url) or [str(i) for i in range(30)]

    class Scraper:
        def scrape_deck_data(self, page, url):
            return ["shared", "other"], {"swamp": 2}

    scanner.deck_scraper = Scraper()
    urls, valid = scanner._select_recent_decks(None, "https://example.com/commander/Test")

    assert len(searches) == 1
    assert len(urls) == len(valid) == 30


def test_recent_search_stops_at_first_sufficient_window():
    scanner = module.MTGDeckScanner.__new__(module.MTGDeckScanner)
    scanner.debug = False
    searches = []
    scanner.get_topdeck_urls = lambda page, url: searches.append(url) or [str(i) for i in range(30)]

    class Scraper:
        def scrape_deck_data(self, page, url):
            return [url], {}

    scanner.deck_scraper = Scraper()
    urls, valid = scanner._select_recent_decks(
        None, "https://example.com/commander/Test?timePeriod=ONE_YEAR"
    )

    assert len(searches) == 1
    assert "timePeriod=THREE_MONTHS" in searches[0]
    assert len(urls) == len(valid) == 30


def test_recent_search_warns_when_maximum_range_is_insufficient(capsys):
    scanner = module.MTGDeckScanner.__new__(module.MTGDeckScanner)
    scanner.debug = False
    searches = []
    scanner.get_topdeck_urls = lambda page, url: searches.append(url) or ["deck"]

    class Scraper:
        def scrape_deck_data(self, page, url):
            return ["card"], {}

    scanner.deck_scraper = Scraper()
    urls, valid = scanner._select_recent_decks(None, "https://example.com/commander/Test")

    assert len(searches) == 10
    assert "timePeriod=ALL_TIME" in searches[-1]
    assert len(urls) == len(valid) == 1
    assert "Warning: Only 1 usable decklists" in capsys.readouterr().out


def test_recent_search_debug_reports_individual_decks(capsys):
    scanner = module.MTGDeckScanner.__new__(module.MTGDeckScanner)
    scanner.debug = True
    scanner.get_topdeck_urls = lambda page, url: [str(i) for i in range(30)]

    class Scraper:
        def scrape_deck_data(self, page, url):
            return [url], {}

    scanner.deck_scraper = Scraper()
    scanner._select_recent_decks(None, "https://example.com/commander/Test")

    assert "[DEBUG] Processing deck 30/30." in capsys.readouterr().out


@pytest.mark.parametrize(
    ("source_lands", "target_lands"),
    [(24, 27), (26, 27), (27, 27), (28, 28), (31, 31), (32, 31), (36, 31)],
)
def test_analyze_commander_bounds_data_driven_land_target(capsys, tmp_path, source_lands, target_lands):
    scanner = module.MTGDeckScanner.__new__(module.MTGDeckScanner)
    scanner.type_order = ["Commander", "Creature", "Artifact", "Enchantment",
                          "Instant", "Sorcery", "Planeswalker", "Land"]
    scanner.type_hierarchy = ("Land", "Creature", "Artifact", "Enchantment",
                              "Instant", "Sorcery", "Planeswalker")
    scanner.ui_noise = list(scanner.type_hierarchy)
    scanner.excluded_cards = {}
    scanner.b2_exclusions = {}
    scanner.min_percentage = 0.2
    scanner.debug = False
    scanner.snapshot_file = str(tmp_path / "snapshots.json")
    cards = ([f"Land {i}" for i in range(source_lands)] + [f"Instant {i}" for i in range(14)]
             + [f"Creature {i}" for i in range(99 - source_lands - 14)])
    alternative = cards[:-10] + [f"Creature Alternative {i}" for i in range(10)]
    scanner._select_recent_decks = lambda page, url: (
        [str(i) for i in range(30)], [(cards, {}) for _ in range(29)] + [(alternative, {})]
    )
    scanner.get_scryfall_data = lambda card: (card, card.split(" ")[0], True, False)

    scanner.analyze_commander(None, "https://example.com/commander/Test")

    output = capsys.readouterr().out
    assert "Using 30 of 30 decklists" in output
    assert f"Targeting: {target_lands} Lands (source median: {source_lands}) | 14-16 Instants" in output
    assert "### Instant (14)" in output
    assert f"### Land ({target_lands})" in output
    assert sum(map(int, module.re.findall(r"^### \w+ \((\d+)\)$", output, module.re.MULTILINE))) == 100


def test_analyze_commander_warns_instead_of_overfilling_lands(capsys, tmp_path):
    scanner = module.MTGDeckScanner.__new__(module.MTGDeckScanner)
    scanner.type_order = ["Commander", "Creature", "Artifact", "Enchantment",
                          "Instant", "Sorcery", "Planeswalker", "Land"]
    scanner.type_hierarchy = ("Land", "Creature", "Artifact", "Enchantment",
                              "Instant", "Sorcery", "Planeswalker")
    scanner.ui_noise = list(scanner.type_hierarchy)
    scanner.excluded_cards = {}
    scanner.b2_exclusions = {}
    scanner.min_percentage = 0.2
    scanner.debug = False
    scanner.snapshot_file = str(tmp_path / "snapshots.json")
    cards = [f"Land {i}" for i in range(28)] + ["Creature 1"]
    scanner._select_recent_decks = lambda page, url: (
        ["deck"], [(cards, {})]
    )
    scanner.get_scryfall_data = lambda card: (card, card.split(" ")[0], True, False)

    scanner.analyze_commander(None, "https://example.com/commander/Test")

    output = capsys.readouterr().out
    assert "### Land (28)" in output
    assert "Warning: Could not build a complete deck within the land range: 30 cards, 28 lands." in output


def test_auto_scroll_stops_after_maximum_attempts(capsys):
    class GrowingPage:
        def __init__(self):
            self.scrolls = 0

        @property
        def first(self):
            return self

        def locator(self, selector):
            return self

        def count(self):
            return 0

        def evaluate(self, script):
            if script.startswith("window.scrollTo"):
                self.scrolls += 1
                return None
            return self.scrolls

        def wait_for_timeout(self, timeout):
            pass

    scanner = module.MTGDeckScanner.__new__(module.MTGDeckScanner)
    page = GrowingPage()

    scanner.auto_scroll_to_bottom(page, max_scrolls=3)

    assert page.scrolls == 3
    assert "reached its 3-step limit" in capsys.readouterr().out


def test_find_new_cards_reports_additions_after_first_run(monkeypatch, tmp_path):
    monkeypatch.setenv("CACHE_ROOT", str(tmp_path))
    scanner = module.MTGDeckScanner.__new__(module.MTGDeckScanner)
    scanner.snapshot_file = str(tmp_path / "snapshots.json")

    first = {"Creature": ["1 Old Card"], "Land": ["1 Forest"]}
    assert scanner._find_new_cards("Cmdr|None", first) is None

    second = {"Creature": ["1 Old Card", "1 New Card"], "Land": ["1 Forest"]}
    assert scanner._find_new_cards("Cmdr|None", second) == [("New Card", "Creature")]
    assert scanner._find_new_cards("Cmdr|None", second) == []
