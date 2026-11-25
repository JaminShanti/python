# Music sheet tools

These tools help prepare and recover big-band parts. They do not determine
whether a chart, arrangement, or part is musically correct: review the source,
page order, completeness, song database and existing Dropbox parts yourself.

Run the examples from the repository root in PowerShell. Install the repository
dependencies, Tesseract and Poppler as described in the main README. Dropbox
recovery also requires `rclone` configured with access to the band's Dropbox.

## Chart-by-chart workflow

The current workflow uses
`C:\Users\Radjammin\Desktop\Reflections_Source_Music_charts` as the source
folder, its `processed` subfolder for completed splits, and `not_processing`
for charts set aside. Substitute your own paths as needed.

1. Generate OCR drafts for source PDFs that do not already have drafts:
   ```powershell
   cd D:\apps\repositories\github\JaminShanti\python
   Get-ChildItem "C:\Users\Radjammin\Desktop\Reflections_Source_Music_charts\*.pdf" -File |
     ForEach-Object {
       python ".\music_sheet_scripts\music_part_splitter.py" $_.FullName --dump
     }
   ```
   Each PDF gets a `_page_map.yaml` and an `_OCR_DUMP.txt` beside it. The
   splitter refuses to overwrite an existing draft or dump, so check the log
   for failures rather than assuming a rerun regenerated everything.
2. Select one chart for review. Check its title, song number and arranger
   against the song library and existing Dropbox parts; don't guess a number
   or assume that two recordings of the same title use the same arrangement.
   Review every source page visually, including continuation pages and
   alternate keys. Correct the map as described below; OCR suggestions and
   complete page coverage alone are not proof that assignments are correct.
   Set aside charts that are unreadable, incomplete, unneeded or uncertain
   instead of forcing them through the split.
3. Run the reviewed split for that chart using the command in **Split a source
   PDF**. With `--output-dir` set to the existing `processed` folder, the
   splitter creates the parts folder and moves the original PDF there after a
   successful split.
4. Review the resulting parts against the archived source for page order,
   completeness and legibility. Compare them with Dropbox by title,
   arrangement, instrument and completeness. Do not overwrite an existing
   part or assume a different scan is a different arrangement. Upload only
   the needed, reviewed parts to the appropriate instrument folders, and
   download-verify each upload. Resolve unclear destinations, alternate keys,
   conflicting files or questionable scan quality before uploading.
5. After review and any upload, remove that chart's source-directory OCR dump
   and page map if they are no longer needed. Keep the resolved `page_map.yaml`
   inside the processed parts folder. If setting a chart aside, move its PDF
   to `not_processing` without overwriting an existing file; remove its draft
   and dump only after the move succeeds and they are no longer needed.

The manual review and upload workflow does not update the missing-parts
inventories. Use the recovery workflow below when those inventories need to be
updated, or reconcile them separately after verifying the Dropbox files.

## Split a source PDF

1. Generate an OCR draft:
   ```powershell
   python .\music_sheet_scripts\music_part_splitter.py "C:\Music\My Chart.pdf" --dump
   ```
   This creates `My Chart_page_map.yaml` beside the PDF. OCR labels are
   suggestions only; they do not assign pages. The log names the source PDF
   before processing begins and reports OCR progress for each page.
2. Open the PDF and review the map. Leave `source_file`, `source_sha256` and
   `page_count` unchanged and set the quoted song number and title. If all
   `suggested_parts` are correct, leave `parts: {}`; the splitter will use the
   suggestions directly. Otherwise edit `parts` to assign the correct one-based
   pages. Explain unrelated or incomplete pages under `excluded_pages`. For
   example:
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
   arrangements and incorrect OCR. `ocr_suggestions` is reference only;
   unidentified pages in `suggested_exclusions` are not excluded automatically.
   Assign or explicitly exclude every page before splitting.
3. Extract the reviewed parts:
   ```powershell
   python .\music_sheet_scripts\music_part_splitter.py "C:\Music\My Chart.pdf" --page-map "C:\Music\My Chart_page_map.yaml" --output-dir "C:\Music\processed"
   ```
   The splitter checks the source hash, complete page coverage and duplicate
   pages, then creates `30b - Satin Doll_parts` with band-standard names and a
   copy of the page map. When using `suggested_parts`, the copied map records
   them as `parts` for downstream recovery; the source map remains unchanged.
   It refuses to overwrite an existing folder or source PDF. After a successful
   split, it moves the original PDF to the output parent; the source map and OCR
   dump remain where they are. Omit `--output-dir` to create the folder beside
   the source without moving it. It does not update inventories.

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
