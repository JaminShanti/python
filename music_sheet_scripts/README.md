# Music sheet tools

These tools help prepare and recover big-band parts. They do not determine
whether a chart, arrangement, or part is musically correct: review the source,
page order, completeness, song database and existing Dropbox parts yourself.

Run the examples from the repository root in PowerShell. Install the repository
dependencies, Tesseract and Poppler as described in the main README. Dropbox
recovery also requires `rclone` configured with access to the band's Dropbox.

## Split a source PDF

1. Generate an OCR draft:
   ```powershell
   python .\music_sheet_scripts\music_part_splitter.py "C:\Music\My Chart.pdf" --dump
   ```
   This creates `My Chart_page_map.yaml` beside the PDF. OCR labels are
   suggestions only; they do not assign pages.
2. Open the PDF and edit the map. Leave `source_file`, `source_sha256` and
   `page_count` unchanged. Set the quoted song number and title, assign every
   complete part its one-based pages, and explain unrelated or incomplete pages
   under `excluded_pages`. For example:
   ```yaml
   song_number: "30b"
   song_title: Satin Doll
   arrangement: Wilfred Roberts
   parts:
     Alto Sax I: [1, 2]
     Alto Sax II: [3, 4]
   excluded_pages:
     Unrelated pages: [5, 6]
   ```
   The page numbers are illustrative. Examine all pages for continuity, mixed
   arrangements and incorrect OCR. Leave `ocr_suggestions` as reference only.
3. Extract the reviewed parts:
   ```powershell
   python .\music_sheet_scripts\music_part_splitter.py "C:\Music\My Chart.pdf" --page-map "C:\Music\My Chart_page_map.yaml" --output-dir "C:\Music\processed"
   ```
   The splitter checks the source hash, complete page coverage and duplicate
   pages, then creates `30b - Satin Doll_parts` with band-standard names and a
   copy of the page map. It refuses to overwrite an existing folder. Omit
   `--output-dir` to create the folder beside the source. It does not move the
   source or update inventories.

## Recover missing parts

First compare the arrangement to existing Dropbox parts and verify each output
PDF. Record the split parts in the preparation inventory; the recovery script
requires exactly one matching row per part. It does not create those records.
Use the **exact** song entry from the active YAML inventory and the standard
instrument name. To preview, for example:

```powershell
python .\music_sheet_scripts\music_part_recovery.py "C:\Music\processed\534 - You Made Me Love You_parts" --instrument "Alto Sax II" --missing-entry "534 - ( I Didn't Want To Do It) You Made Me Love You" --preparation-inventory "C:\Music\processed\upload_preparation_inventory.csv"
```

Repeat `--instrument` to select more reviewed parts. Without `--apply`, the
script only checks the inventories and lists planned uploads or potential
conflicts. After reviewing the output, repeat the command with `--apply` to
upload missing files, download-verify their bytes, and remove verified entries
from both the active YAML and CSV inventories. Existing potential matches are
skipped, including zero-padded names; they are never intentionally overwritten.
The default Dropbox root is `dropbox:Reflection Big Band`; `--remote` overrides
it, and `--active-yaml` selects another YAML and its same-named CSV.

If a previous upload succeeded but inventory updates failed, inspect the error
and inventories. Use `--resume-existing --apply` only when the exact destination
is the sole match: the script verifies its bytes before updating inventories.
If inventories have diverged or a file differs, stop and resolve the discrepancy
manually. This script does not identify alternate arrangements or upload them to
New Tunes.

## Chart downloader and files

Copy `music_cache\chart_downloader.yaml.sample` to
`music_cache\chart_downloader.yaml`, then set `target_domains`, `input_file` and
`download_folder`. Paths in the configuration are relative to
`music_sheet_scripts`, unless absolute. The example uses a placeholder domain;
replace it with a real hostname. Uncomment and adjust the optional `host_aliases`,
`force_https`, and `plus_as_space` only when needed by your sites. URL options
apply to all configured sites. The local config is ignored by Git.

Run `python .\music_sheet_scripts\chart_downloader.py --check-only` to list
matches without downloading, then omit `--check-only` to download the best match
per query and configured domain. Use `--config` to select another YAML file.
Each domain search includes its subdomains; search result ranking is heuristic.
This requires Playwright Chromium and network access. Search results are
candidates, not verified arrangements; review them and respect applicable
permissions before using them.

`music_cache\instruments.yaml` contains OCR aliases, and `music_output` holds
local inventories and query data.
The output directory is ignored by Git; back up its files independently. Run the
music tests with `python -m pytest music_sheet_scripts\tests`.
