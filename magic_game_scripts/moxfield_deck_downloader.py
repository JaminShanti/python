import argparse
import os
import re
from pathlib import Path

import requests
import yaml

DECK_ID_PATTERN = re.compile(r"moxfield\.com/decks/([\w-]+)")
API_URL = "https://api2.moxfield.com/v3/decks/all/{deck_id}"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Accept": "application/json",
}
BOARD_ORDER = ("commanders", "companions", "mainboard", "sideboard", "maybeboard")


class MoxfieldDeckDownloader:
    def __init__(self, cache_dir=None, config_filename="moxfield_decks.yaml", include_maybeboard=False):
        base_cache = os.getenv("CACHE_ROOT", os.path.abspath(os.path.dirname(__file__)))
        self.cache_dir = Path(cache_dir or os.path.join(base_cache, "moxfield_cache"))
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.config_file = self.cache_dir / config_filename
        self.output_dir = self.cache_dir / "decks"
        self.output_dir.mkdir(exist_ok=True)
        self.boards = tuple(b for b in BOARD_ORDER if include_maybeboard or b != "maybeboard")

    def load_config(self):
        if not self.config_file.exists():
            self.config_file.write_text("decks: []\n")
            return []
        data = yaml.safe_load(self.config_file.read_text()) or {}
        decks = []
        for entry in data.get("decks", []):
            url, name = (entry.get("url"), entry.get("name")) if isinstance(entry, dict) else (entry, None)
            match = DECK_ID_PATTERN.search(url or "")
            if match:
                decks.append((match.group(1), name))
            else:
                print(f"Warning: skipping unrecognized Moxfield URL: {entry}")
        return decks

    @staticmethod
    def slugify(name):
        return re.sub(r"[^a-z0-9]+", "_", name.lower()).strip("_") or "deck"

    def format_decklist(self, deck):
        lines = []
        for board in self.boards:
            cards = (deck.get("boards", {}).get(board) or {}).get("cards", {})
            entries = sorted(
                (e["card"]["name"], e["card"].get("set", ""), e["card"].get("cn", ""), e.get("finish", ""), e["quantity"])
                for e in cards.values()
            )
            if not entries:
                continue
            if board in ("sideboard", "maybeboard"):
                lines.append("")
                lines.append(board.upper())
            lines.extend(self.format_line(*entry) for entry in entries)
        return "\n".join(lines) + "\n"

    @staticmethod
    def format_line(name, set_code, collector_number, finish, quantity):
        line = f"{quantity} {name}"
        if set_code:
            line += f" ({set_code.upper()}) {collector_number}".rstrip()
        if finish == "foil":
            line += " *F*"
        elif finish == "etched":
            line += " *E*"
        return line

    def fetch_deck(self, deck_id):
        response = requests.get(API_URL.format(deck_id=deck_id), headers=HEADERS, timeout=30)
        response.raise_for_status()
        return response.json()

    def run(self):
        decks = self.load_config()
        if not decks:
            print(f"No decks configured. Add Moxfield URLs under 'decks:' in {self.config_file}")
            return []

        saved = []
        for deck_id, name in decks:
            try:
                deck = self.fetch_deck(deck_id)
            except Exception as e:
                print(f"Warning: could not download {deck_id}: {e}")
                continue
            path = self.output_dir / f"{self.slugify(name or deck['name'])}.txt"
            path.write_text(self.format_decklist(deck), encoding="utf-8")
            count = sum(
                e["quantity"]
                for b in self.boards
                for e in (deck.get("boards", {}).get(b) or {}).get("cards", {}).values()
            )
            print(f"Saved {deck['name']} ({count} cards) -> {path}")
            saved.append(path)
        return saved

def main(argv=None):
    parser = argparse.ArgumentParser(description="Download Moxfield decks listed in moxfield_decks.yaml.")
    parser.add_argument("--maybeboard", action="store_true", help="Include the maybeboard in saved lists.")
    args = parser.parse_args(argv)
    MoxfieldDeckDownloader(include_maybeboard=args.maybeboard).run()


if __name__ == "__main__":
    main()
