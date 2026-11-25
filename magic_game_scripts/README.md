# Magic: The Gathering tools

Run these commands from the repository root. Install the project dependencies
from the main README, including Playwright's Chromium browser for the deck
scanner. The tools access external websites and can take time to populate their
caches; review prices and availability before making purchasing decisions.

## Commander deck scanner

Create `magic_game_scripts\mtg_scanner_cache\mtg_scanner_config.yaml` with
commander URLs to analyze, for example:

```yaml
commander_urls:
  - "https://edhtop16.com/commander/Example"
  - url: "https://edhtop16.com/commander/Another"
    bracket: 2
```

Replace the example URLs with actual commander pages. Run:

```powershell
python .\magic_game_scripts\mtg_scanner_tool.py
python .\magic_game_scripts\mtg_scanner_tool.py --debug
```

The scanner uses Scryfall and deck data, and caches results in
`mtg_scanner_cache`. Add excluded card names, one per line, to
`mtg_scanner_cache\excluded_cards.txt`; `old name -> replacement` entries
specify substitutions. Optional `bracket_2_exclusions.yaml` can group lists of
card names under `bracket_2_exclusions` by reason. Setting `CACHE_ROOT` moves
the scanner's `mtg_scanner_cache` under that directory. Each
run saves the final list per commander and bracket in
`mtg_scanner_cache\decklist_snapshots.json`; later runs print a
"New Since Last Run" section at the top of the list with cards that were not in
the previous run (nothing is shown on the first run). The companion
`mtg_deck_scraper.py` is used by the scanner and is not a standalone CLI.

## Price dip detector

```powershell
python .\magic_game_scripts\mtg_dip_detector.py
```

The detector downloads MTGJSON data and checks staples and historical prices.
It stores data in `mtg_cache` and writes reports and TCGplayer import lists to
`mtg_dip_output`, both relative to this directory regardless of the current
working directory. It can use substantial disk space and network bandwidth.

## Mighty Meeple inventory

```powershell
python .\magic_game_scripts\review_mighty_meeply.py "C:\Decks\my_deck.txt"
```

Replace the example with a text decklist path, or run without an argument after
adding targets to `meeple_cache\meeple_config.yaml`. The script creates an empty
configuration on first run. Imported names are added as targets with a default
maximum price; the interactive review produces cart links for selected matches.
Inventory and review state are stored in `meeple_cache`.

The cache directories and newly generated output are Git-ignored; back them up
independently if needed. The dated sample reports in `mtg_dip_output` are tracked.
Run the Magic tests with `python -m pytest magic_game_scripts\tests`.
