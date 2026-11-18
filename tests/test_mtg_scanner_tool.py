import importlib.util, pathlib, sys

repo_root = pathlib.Path(__file__).resolve().parents[1]
if str(repo_root) not in sys.path:
    sys.path.insert(0, str(repo_root))

spec = importlib.util.spec_from_file_location(
    "mtg_scanner_tool", repo_root / "mtg_scanner_tool.py"
)
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)

def test_imports():
    assert module is not None


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
    assert len(scraper.deck_cache["deck-url"]) == 2


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


def test_auto_scroll_stops_after_maximum_attempts(capsys):
    class GrowingPage:
        def __init__(self):
            self.scrolls = 0

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
