"""Upload explicitly reviewed, missing chart parts and update the inventories."""

import argparse
import csv
import io
import json
import os
from pathlib import Path, PurePosixPath
import re
import subprocess
import tempfile

import yaml

if __package__:
    from .music_part_splitter import BigBandChartSplitter, UniqueKeyLoader
else:
    from music_part_splitter import BigBandChartSplitter, UniqueKeyLoader


ROOT = Path(__file__).resolve().parent
DEFAULT_YAML = ROOT / "music_output" / "reflection_big_band_missing_parts_active.yaml"
DEFAULT_REMOTE = "dropbox:Reflection Big Band"


def rclone(*args):
    result = subprocess.run(["rclone", *args], check=True, stdout=subprocess.PIPE)
    return result.stdout


def inventory_updates(yaml_bytes, csv_bytes, report_bytes, instrument, song, folder, filename, remote_path):
    active = yaml.load(yaml_bytes.decode("utf-8-sig"), Loader=UniqueKeyLoader)
    if not isinstance(active, dict):
        raise ValueError("Active YAML must contain a mapping.")
    csv_rows = list(csv.reader(io.StringIO(csv_bytes.decode("utf-8-sig"), newline="")))
    if not csv_rows or csv_rows[0][:2] != ["Part", "Missing Songs"]:
        raise ValueError("Unexpected active CSV header.")
    parsed = {}
    for row in csv_rows[1:]:
        if not row or not any(row):
            continue
        if len(row) != 2 or row[0] in parsed:
            raise ValueError("Invalid or duplicate active CSV row.")
        parsed[row[0]] = [item.strip() for item in row[1].split(";") if item.strip()]
    if active != parsed:
        raise ValueError("Active YAML and CSV inventories do not match.")
    if instrument not in active or active[instrument].count(song) != 1:
        raise ValueError(f"Expected exactly one active entry for {instrument}: {song}")

    report_rows = list(csv.DictReader(io.StringIO(report_bytes.decode("utf-8-sig"), newline="")))
    if not report_rows:
        raise ValueError("Preparation inventory is empty.")
    required = {"Source folder", "Source part", "Dropbox status", "Dropbox paths"}
    if not required.issubset(report_rows[0]):
        raise ValueError("Unexpected preparation inventory header.")
    matches = [row for row in report_rows if row["Source folder"] == folder and row["Source part"] == filename]
    if len(matches) != 1:
        raise ValueError("Expected exactly one matching preparation inventory record.")

    lines, section, removed = [], None, 0
    for line in yaml_bytes.decode("utf-8-sig").splitlines(keepends=True):
        if not line.startswith((" ", "\t")) and line.rstrip().endswith(":"):
            section = line.rstrip()[:-1]
            if section == instrument and len(active[instrument]) == 1:
                ending = "\r\n" if line.endswith("\r\n") else "\n" if line.endswith("\n") else ""
                line = f"{section}: []{ending}"
        if section == instrument and line.strip() == f"- {json.dumps(song, ensure_ascii=False)}":
            removed += 1
        else:
            lines.append(line)
    if removed != 1:
        raise ValueError("Cannot safely locate the exact YAML entry in its original formatting.")
    parsed[instrument].remove(song)
    for row in csv_rows:
        if len(row) == 2 and row[0] == instrument:
            row[1] = "; ".join(parsed[instrument])
    csv_output = io.StringIO(newline="")
    csv.writer(csv_output, lineterminator="\r\n").writerows(csv_rows)
    matches[0]["Dropbox status"] = "Recovered version uploaded and verified"
    matches[0]["Dropbox paths"] = remote_path
    report_output = io.StringIO(newline="")
    writer = csv.DictWriter(report_output, fieldnames=report_rows[0].keys())
    writer.writeheader()
    writer.writerows(report_rows)
    return ("".join(lines).encode("utf-8"), csv_output.getvalue().encode("utf-8-sig"),
            report_output.getvalue().encode("utf-8-sig"))


def replace_inventories(paths, old, new):
    staged = []
    replaced = []
    try:
        for path, content in zip(paths, new):
            with tempfile.NamedTemporaryFile(dir=path.parent, delete=False) as file:
                staged.append(Path(file.name))
                file.write(content)
        if any(path.read_bytes() != content for path, content in zip(paths, old)):
            raise RuntimeError("Inventories changed concurrently; no inventory files were updated.")
        for path, tmp, before, after in zip(paths, staged, old, new):
            os.replace(tmp, path)
            replaced.append((path, before, after))
    except Exception:
        for path, before, after in reversed(replaced):
            if path.read_bytes() != after:
                raise RuntimeError(f"Inventory update failed and {path} changed; restore it manually.")
            path.write_bytes(before)
        raise
    finally:
        for tmp in staged:
            tmp.unlink(missing_ok=True)


def possible_conflicts(paths, instrument, number, title):
    number_pattern = re.compile(r"(?:^|[^a-z0-9])0*" + re.escape(number) + r"(?![a-z0-9-])", re.I)
    normalized_title = re.sub(r"[^a-z0-9]", "", title.casefold())
    found = []
    for path in paths:
        name = PurePosixPath(path).name
        parent = PurePosixPath(path).parent.name
        normalized_name = re.sub(r"[^a-z0-9]", "", name.casefold())
        instrument_pattern = r"(?<![a-z0-9])" + re.escape(instrument) + r"(?![a-z0-9])"
        same_instrument = parent.casefold() == instrument.casefold() or re.search(instrument_pattern, name, re.I)
        if same_instrument and (number_pattern.search(name) or normalized_title in normalized_name):
            found.append(path)
    return found


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("parts_folder", type=Path, help="Reviewed parts folder with page_map.yaml")
    parser.add_argument("--instrument", action="append", required=True, help="Reviewed missing instrument; repeat as needed")
    parser.add_argument("--missing-entry", required=True, help="Exact song entry in the active inventory")
    parser.add_argument("--preparation-inventory", type=Path, required=True)
    parser.add_argument("--active-yaml", type=Path, default=DEFAULT_YAML)
    parser.add_argument("--remote", default=DEFAULT_REMOTE)
    parser.add_argument("--apply", action="store_true", help="Upload and update inventories after manual musical review")
    parser.add_argument("--resume-existing", action="store_true", help="Accept only an identical existing destination after interrupted upload")
    args = parser.parse_args(argv)
    with (args.parts_folder / "page_map.yaml").open(encoding="utf-8") as file:
        mapping = yaml.load(file, Loader=UniqueKeyLoader)
    number = BigBandChartSplitter._safe_component(mapping["song_number"], "Song number")
    title = BigBandChartSplitter._safe_component(mapping["song_title"], "Song title")
    if not re.fullmatch(r"\d+(?:-?[A-Za-z]|-\d+)?", number):
        raise ValueError("Invalid song number in page map.")
    if not re.match(r"^" + re.escape(number) + r" - ", args.missing_entry):
        raise ValueError("Missing entry does not match the reviewed song number.")
    if len(set(args.instrument)) != len(args.instrument):
        raise ValueError("Duplicate requested instrument.")
    paths = (args.active_yaml, args.active_yaml.with_suffix(".csv"), args.preparation_inventory)
    listing = rclone("lsf", args.remote, "--recursive", "--files-only").decode("utf-8-sig").splitlines()
    for instrument in args.instrument:
        BigBandChartSplitter._safe_component(instrument, "Instrument")
        if instrument not in mapping["parts"]:
            raise ValueError(f"{instrument} is not in the reviewed page map.")
        local = args.parts_folder / f"{instrument} - {number} {title}.pdf"
        data = local.read_bytes()
        destination = f"{instrument}/{local.name}"
        old = tuple(path.read_bytes() for path in paths)
        new = inventory_updates(*old, instrument, args.missing_entry, args.parts_folder.name, local.name, destination)
        conflicts = possible_conflicts(listing, instrument, number, title)
        resumable = args.resume_existing and conflicts == [destination]
        if conflicts and not resumable:
            print(f"SKIP {instrument}: possible Dropbox conflict: {conflicts}")
            continue
        if not args.apply:
            print(f"PLAN {instrument}: {'verify existing' if resumable else 'upload'} {destination}")
            continue
        remote_file = args.remote.rstrip("/\\") + "/" + destination
        if not resumable:
            rclone("copyto", str(local), remote_file, "--ignore-existing")
        if rclone("cat", remote_file) != data or local.read_bytes() != data:
            raise RuntimeError(f"Byte verification failed for {remote_file}; inventory unchanged.")
        replace_inventories(paths, old, new)
        listing.append(destination)
        print(f"RECOVERED {instrument}: {destination}")


if __name__ == "__main__":
    main()
