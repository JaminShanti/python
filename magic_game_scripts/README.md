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
`mtg_scanner_cache`. Each run starts with the latest three-month window and
widens the event-size and time filters only when fewer than 30 usable decklists
are available. Separate tournament entries count separately, even for identical
decklists; player identity is not verified. Each search window reports a
summary; use `--debug` for individual deck diagnostics. TopDeck listing URLs are cached for
seven days. It warns if the maximum range still has fewer than 30. The land
target is the source median limited to 27-31, counting spell/land cards as lands;
the instant target is calculated from the usable source decks. When too few
eligible spells are available to fill a 100-card deck without exceeding the
land target, the scanner warns and leaves the deck incomplete. Older deck caches
are refreshed to correct previously misparsed card names, so the first run after
upgrading may take longer. Add excluded card names, one per line, to
`mtg_scanner_cache\excluded_cards.txt`; `old name -> replacement` entries
specify substitutions. Optional `bracket_2_exclusions.yaml` can group lists of
card names under `bracket_2_exclusions` by reason. Setting `CACHE_ROOT` moves
the scanner's `mtg_scanner_cache` under that directory. Each
run saves the final list per commander and bracket in
`mtg_scanner_cache\decklist_snapshots.json`; later runs print a
"New Since Last Run" section at the top of the list with cards that were not in
the previous run (nothing is shown on the first run). The companion
`mtg_deck_scraper.py` is used by the scanner and is not a standalone CLI.

## Moxfield deck downloader

List your decks in `magic_game_scripts\moxfield_cache\moxfield_decks.yaml`
(created empty on first run); `name` is optional and sets the output filename:

```yaml
decks:
  - url: "https://moxfield.com/decks/lDLqRbLGGEO9qK4Pd6CWsw"
    name: winota
  - "https://moxfield.com/decks/another_id"
```

```powershell
python .\magic_game_scripts\moxfield_deck_downloader.py
python .\magic_game_scripts\moxfield_deck_downloader.py --maybeboard
```

Each deck is saved as a plain decklist in `moxfield_cache\decks\<name>.txt`
(commander first) in Moxfield's export format, keeping the printing and finish:
`1 Command Tower (SLD) 7030 *F*` (`*F*` foil, `*E*` etched, no marker for
non-foil). `review_mighty_meeply.py` can import these files; it currently reads
only the card name. The script calls
Moxfield's JSON API with a browser User-Agent; no browser is needed.

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
