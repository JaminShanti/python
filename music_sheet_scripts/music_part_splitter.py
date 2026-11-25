import re
import difflib
import logging
import argparse
import hashlib
import shutil
import tempfile
import yaml
import sys
from pathlib import Path
try:
    from pypdf import PdfWriter, PdfReader
except ImportError:
    import warnings
    warnings.filterwarnings("ignore", category=DeprecationWarning, module="PyPDF2")
    from PyPDF2 import PdfWriter, PdfReader
from pdf2image import convert_from_path
import pytesseract


class UniqueKeyLoader(yaml.SafeLoader):
    def construct_mapping(self, node, deep=False):
        mapping = {}
        for key_node, value_node in node.value:
            key = self.construct_object(key_node, deep=deep)
            if key in mapping:
                raise ValueError(f"Duplicate key in page map: {key!r}")
            mapping[key] = self.construct_object(value_node, deep=deep)
        return mapping


class BigBandChartSplitter:
    def __init__(self, config_path, log_level=logging.INFO):
        self.logger = logging.getLogger("BigBandSplitter")
        self.logger.setLevel(log_level)
        self.config_path = config_path

        if not self.logger.handlers:
            h = logging.StreamHandler()
            h.setFormatter(logging.Formatter('%(asctime)s | %(levelname)s | %(message)s'))
            self.logger.addHandler(h)

        self.conductor_aliases = []
        self.conductor_regex = None
        self.instruments = []

        self._load_config(config_path)

    def _load_config(self, config_path):
        """Loads instrument definitions from a YAML file."""
        try:
            with open(config_path, 'r', encoding='utf-8') as f:
                config = yaml.safe_load(f)

            self.conductor_aliases = config.get('conductor_aliases', ['conductor', 'score'])
            self._build_conductor_regex()

            self.instruments = [(inst['name'], inst['aliases']) for inst in config.get('instruments', [])]
        except Exception as e:
            self.logger.error(f"Failed to load configuration from {config_path}: {e}")
            sys.exit(1)

    def _build_conductor_regex(self):
        """Dynamically builds the regex used to identify Conductor Scores."""
        patterns = []
        for a in self.conductor_aliases:
            if re.match(r'^\w+$', a):
                patterns.append(r'\b' + a + r'\b')
            else:
                patterns.append(re.escape(a))
        self.conductor_regex = re.compile('|'.join(patterns))

    def _add_alias_to_config(self, instrument_name, new_alias):
        """Adds a new alias to the YAML config and in-memory list."""
        new_alias = new_alias.lower().strip()

        try:
            with open(self.config_path, 'r', encoding='utf-8') as f:
                config_data = yaml.safe_load(f)

            if instrument_name == 'Conductor Score':
                if new_alias not in self.conductor_aliases:
                    self.conductor_aliases.append(new_alias)
                    self._build_conductor_regex()

                if 'conductor_aliases' not in config_data:
                    config_data['conductor_aliases'] = []
                if new_alias not in config_data['conductor_aliases']:
                    config_data['conductor_aliases'].append(new_alias)
            else:
                for i, (name, aliases) in enumerate(self.instruments):
                    if name == instrument_name:
                        if new_alias not in aliases:
                            aliases.append(new_alias)
                        break

                for inst in config_data.get('instruments', []):
                    if inst['name'] == instrument_name:
                        if new_alias not in inst['aliases']:
                            inst['aliases'].append(new_alias)
                        break

            with open(self.config_path, 'w', encoding='utf-8') as f:
                yaml.dump(config_data, f, default_flow_style=False, sort_keys=False)

            self.logger.info(f"Learned new alias: '{new_alias}' saved to {instrument_name}.")
        except Exception as e:
            self.logger.error(f"Failed to save new alias to config: {e}")

    def _get_best_match(self, text):
        text = text.lower()
        lines = [line for line in text.splitlines() if line.strip()]
        meaningful_lines = [l for l in lines if len(l.strip()) > 2 and re.search(r'[a-z0-9]', l)]
        header_text = "\n".join(meaningful_lines[:20])

        best_inst = None
        highest_score = 0

        for name, aliases in self.instruments:
            score = 0
            for a in aliases:
                score += len(re.findall(r"\b" + re.escape(a) + r"\b", header_text))
            if score > highest_score:
                highest_score = score
                best_inst = name

        if highest_score == 0:
            words = [w for w in re.findall(r"[a-z0-9]+", header_text)]
            for name, aliases in self.instruments:
                fscore = 0
                for a in aliases:
                    a_words = re.findall(r"[a-z0-9]+", a)
                    n = len(a_words)
                    if n == 0: continue
                    for i in range(0, max(1, len(words) - n + 1)):
                        candidate = " ".join(words[i:i + n])
                        if a == "bass" and candidate == "brass":
                            continue
                        ratio = difflib.SequenceMatcher(None, candidate, a).ratio()
                        if ratio >= 0.88:
                            fscore += 1
                if fscore > highest_score:
                    highest_score = fscore
                    best_inst = name

        return best_inst if highest_score > 0 else None

    def _get_page_instrument(self, image, text):
        lower_text = text.lower()
        normalized = re.sub(r"[^a-z0-9\n]+", " ", lower_text)
        normalized = re.sub(r"\b(one|first|1st|ist|let|lot|lst)\b", " 1 ", normalized)
        normalized = re.sub(r"\b(two|second|2nd|too|tno)\b", " 2 ", normalized)
        normalized = re.sub(r"\b(three|third|3rd)\b", " 3 ", normalized)
        normalized = re.sub(r"\b(four|fourth|4th)\b", " 4 ", normalized)

        instrument_names = (
            "alto sax", "tenor sax", "baritone sax", "bari sax",
            "trumpet", "trombone", "piano", "drums", "bass", "guitar",
        )
        instrument_hits = sum(
            1 for instrument in instrument_names
            if re.search(r"\b" + re.escape(instrument) + r"\b", lower_text)
        )
        if instrument_hits >= 3:
            return "Conductor Score"

        direct_families = (
            (r"\balto\s+sax(?:ophone)?\b", "Alto Sax", ("I", "II", "III", "IV")),
            (r"\btenor\s+sax(?:ophone)?\b", "Tenor Sax", ("I", "II", "III", "IV")),
            (r"\b(?:baritone|bari)\s+sax(?:ophone)?\b", "Baritone Sax", (None,)),
            (r"\btrumpet\b", "Trumpet", ("I", "II", "III", "IV")),
            (r"\btrombone\b", "Trombone", ("I", "II", "III", "IV")),
        )
        for family_pattern, family, suffixes in direct_families:
            for line in normalized.splitlines():
                match = re.search(
                    r"\b([1-4])\d*[ \t]+" + family_pattern
                    + r"|" + family_pattern + r"[ \t]+([1-4])\b", line
                )
                if match:
                    if family == "Baritone Sax":
                        return family
                    number = int(match.group(1) or match.group(2))
                    return f"{family} {suffixes[number - 1]}"
                if family == "Baritone Sax" and re.search(family_pattern, line):
                    return family

        simple_names = (
            (r"\bpiano\b", "Piano"),
            (r"\b(?:drums|drum set|percussion)\b", "Drums"),
            (r"\bbass\b", "Bass"),
            (r"\bguitar\b", "Guitar"),
            (r"\bvibes\b", "Vibes"),
            (r"\b(?:vocal|vocals|voice|voce)\b", "Vocal"),
        )
        for pattern, name in simple_names:
            if re.search(pattern, normalized):
                return name

        generic_labels = (
            r"\btrombone\b",
            r"\btrumpet\b",
            r"\balto sax(?:ophone)?\b",
            r"\btenor sax(?:ophone)?\b",
        )
        numbered_or_ordinal = re.compile(
            r"(?:1st|2nd|3rd|4th|\b[1-4]\b|\bi{1,4}\b)"
        )
        for generic in generic_labels:
            if re.search(generic, lower_text) and not numbered_or_ordinal.search(lower_text):
                return None
        detected = self._get_best_match(text)
        if detected is None:
            grayscale = image.convert('L')
            for psm in (11, 12):
                alternate_text = pytesseract.image_to_string(
                    grayscale,
                    config=f'--psm {psm}',
                ).lower()
                detected = self._get_best_match(alternate_text)
                if detected:
                    self.logger.debug(
                        f"Alternate OCR PSM {psm} detected part: {detected}"
                    )
                    break
        return detected

    def _run_wizard(self, page_index, text):
        print(f"\n{'=' * 60}")
        print(f"⚠️ WIZARD: Unrecognized Part on Page {page_index + 1} ⚠️")
        print(f"{'=' * 60}")

        lines = [line for line in text.splitlines() if line.strip()]
        meaningful_lines = [l for l in lines if len(l.strip()) > 2 and re.search(r'[a-z0-9]', l)]
        snippet = "\n".join(meaningful_lines[:6])

        print("--- Top OCR Text Snippet ---")
        print(snippet if snippet else "[No legible text found]")
        print("----------------------------\n")

        print("Please select the correct instrument for this page:")
        print("0. Skip / I don't know")

        for idx, (name, _) in enumerate(self.instruments, 1):
            print(f"{idx:2d}. {name}")

        print(f"{len(self.instruments) + 1:2d}. Conductor Score")

        while True:
            try:
                choice = input("\nEnter number (or hit Enter to skip): ").strip()
                if not choice:
                    return None

                choice = int(choice)
                if choice == 0:
                    return None
                elif 1 <= choice <= len(self.instruments):
                    chosen_inst = self.instruments[choice - 1][0]
                elif choice == len(self.instruments) + 1:
                    chosen_inst = 'Conductor Score'
                else:
                    print("Invalid selection. Please choose a number from the list.")
                    continue

                print(f"\nSelected: {chosen_inst}")
                alias_input = input(
                    f"Type a snippet from the text above to identify this part in the future (or hit Enter to skip): ").strip()
                if alias_input:
                    self._add_alias_to_config(chosen_inst, alias_input)

                return chosen_inst

            except ValueError:
                print("Please enter a valid number.")

    @staticmethod
    def _standard_part_name(name):
        if not name:
            return None
        aliases = {
            "Conductor Score": "Conductor",
            "1st Alto Sax": "Alto Sax I",
            "2nd Alto Sax": "Alto Sax II",
            "1st Tenor Sax": "Tenor Sax I",
            "2nd Tenor Sax": "Tenor Sax II",
            "Bari Sax": "Baritone Sax",
            "Trumpet 1": "Trumpet I",
            "Trumpet 2": "Trumpet II",
            "Trumpet 3": "Trumpet III",
            "Trumpet 4": "Trumpet IV",
            "Trombone 1": "Trombone I",
            "Trombone 2": "Trombone II",
            "Trombone 3": "Trombone III",
            "Trombone 4": "Trombone IV",
        }
        return aliases.get(name, name)

    @classmethod
    def _improve_suggestions(cls, suggestions):
        """Fill only locally unambiguous OCR gaps before creating a draft."""
        improved = [
            {
                "page": suggestion["page"],
                "suggested_instrument": suggestion.get("suggested_instrument"),
            }
            for suggestion in suggestions
        ]

        names = [item["suggested_instrument"] for item in improved]
        solo_indexes = [
            index for index, name in enumerate(names)
            if name == "Solo Tenor Sax"
        ]
        if len(solo_indexes) > 8:
            start = solo_indexes[0]
            end = solo_indexes[-1]
            if end - start + 1 == len(solo_indexes):
                for index in solo_indexes:
                    names[index] = None

        if names and names[0] == "Conductor Score":
            for index in range(1, len(names)):
                if names[index] is None:
                    names[index] = "Conductor"
                else:
                    break

        leading_end = 0
        while leading_end < len(names) and names[leading_end] is None:
            leading_end += 1
        if leading_end > 0 and leading_end < len(names):
            first_known = names[leading_end]
            if first_known in {
                "Alto Sax I", "1st Alto Sax",
                "Alto Sax II", "2nd Alto Sax",
            }:
                for index in range(leading_end):
                    names[index] = "Alto Sax I"

        for index, name in enumerate(names):
            if name is not None:
                continue
            left = names[index - 1] if index > 0 else None
            right = names[index + 1] if index + 1 < len(names) else None

            if left is not None and right == left:
                names[index] = left
            elif left is not None and right is not None:
                left_family = re.sub(r"\b(?:I|II|III|IV|1|2|3|4)$", "", str(left)).strip()
                right_family = re.sub(r"\b(?:I|II|III|IV|1|2|3|4)$", "", str(right)).strip()
                if left_family and left_family == right_family:
                    names[index] = left
                elif right in {"Drums", "Percussion"}:
                    names[index] = right
                elif left in {"Piano", "Guitar", "Bass", "Drums", "Vibes", "Vocal"}:
                    names[index] = left
            elif left is None and right is not None:
                if right in {"Alto Sax II", "2nd Alto Sax"}:
                    names[index] = "Alto Sax I"
            elif left is not None:
                if len(names) >= 3:
                    names[index] = left

        for item, name in zip(improved, names):
            item["suggested_instrument"] = name
        return improved

    @classmethod
    def _auto_parts(cls, suggestions):
        parts = {}
        excluded = {}
        for suggestion in suggestions:
            name = cls._standard_part_name(suggestion.get("suggested_instrument"))
            page = suggestion["page"]
            if name:
                parts.setdefault(name, []).append(page)
            else:
                excluded.setdefault("OCR not identified", []).append(page)
        return parts, excluded

    def process_file(self, file_path, dump=False):
        """Create an OCR draft. Suggestions never become page assignments."""
        file_path = Path(file_path)
        self.logger.info("Starting OCR draft for: %s", file_path)
        draft_path = file_path.with_name(f"{file_path.stem}_page_map.yaml")
        dump_path = file_path.with_name(f"{file_path.stem}_OCR_DUMP.txt")
        if draft_path.exists() or (dump and dump_path.exists()):
            raise FileExistsError("Draft or OCR dump already exists; move it before creating another.")
        page_count = len(PdfReader(file_path).pages)
        source_hash = self._file_hash(file_path)
        suggestions, raw_ocr = [], []
        for page in range(1, page_count + 1):
            image = convert_from_path(file_path, dpi=300, first_page=page, last_page=page)[0]
            header = image.crop((0, 0, image.width, image.height * 2 // 5)).convert('L')
            text = pytesseract.image_to_string(header, config='--psm 3')
            detected = self._get_page_instrument(header, text)
            if self.conductor_regex.search(text.lower()):
                detected = "Conductor Score" if detected is None else detected
            suggestions.append({"page": page, "suggested_instrument": detected})
            raw_ocr.append(f"PAGE {page}:\n{text}")
            self.logger.info("Page %s/%s | OCR suggestion: %s", page, page_count, detected)
        if self._file_hash(file_path) != source_hash:
            raise RuntimeError("Source PDF changed during OCR; no draft was written.")
        suggestions = self._improve_suggestions(suggestions)
        draft = {
            "source_file": file_path.name, "source_sha256": source_hash,
            "page_count": page_count, "song_number": "", "song_title": "",
            "arrangement": "", "parts": {}, "excluded_pages": {},
            "ocr_suggestions": suggestions,
        }
        suggested_parts, suggested_exclusions = self._auto_parts(suggestions)
        if suggested_parts:
            draft["suggested_parts"] = suggested_parts
        if suggested_exclusions:
            draft["suggested_exclusions"] = suggested_exclusions
        with draft_path.open("x", encoding="utf-8") as f:
            yaml.safe_dump(draft, f, sort_keys=False, allow_unicode=True)
        if dump:
            with dump_path.open("x", encoding="utf-8") as f:
                f.write("\n\n".join(raw_ocr))
        self.logger.info("Review and edit page map: %s", draft_path)
        return draft_path

    @staticmethod
    def _file_hash(path):
        digest = hashlib.sha256()
        with path.open("rb") as f:
            for block in iter(lambda: f.read(1024 * 1024), b""):
                digest.update(block)
        return digest.hexdigest()

    @staticmethod
    def _safe_component(value, label):
        if (not isinstance(value, str) or not value.strip() or value != value.strip()
                or re.search(r'[<>:"/\\|?*\x00-\x1f]', value) or value.endswith('.')
                or value in {".", ".."}):
            raise ValueError(f"{label} must be a nonempty, safe Windows filename component.")
        if re.fullmatch(r'(con|prn|aux|nul|com[1-9]|lpt[1-9])(?:\..*)?', value, re.I):
            raise ValueError(f"{label} is a reserved Windows filename.")
        return value

    def split_from_map(self, source, map_path, output_parent=None):
        source, map_path = Path(source), Path(map_path)
        self.logger.info("Starting reviewed split for: %s (page map: %s)", source, map_path)
        with map_path.open(encoding="utf-8") as f:
            mapping = yaml.load(f, Loader=UniqueKeyLoader)
        if not isinstance(mapping, dict):
            raise ValueError("Page map must be a YAML mapping.")
        source_hash = self._file_hash(source)
        reader = PdfReader(source)
        if (mapping.get("source_file") != source.name or mapping.get("source_sha256") != source_hash
                or mapping.get("page_count") != len(reader.pages)):
            raise ValueError("Page map does not match this source PDF and page count.")
        number = self._safe_component(mapping.get("song_number"), "Song number")
        if not re.fullmatch(r'\d+(?:-?[A-Za-z]|-\d+)?', number):
            raise ValueError("Song number must be a number, optionally followed by a letter or -number.")
        title = self._safe_component(mapping.get("song_title"), "Song title")
        parts, excluded = mapping.get("parts"), mapping.get("excluded_pages")
        using_suggestions = parts == {} and bool(mapping.get("suggested_parts"))
        if using_suggestions:
            parts = mapping["suggested_parts"]
            self.logger.warning("Using suggested parts; verify them against the source PDF before using the output.")
        if not isinstance(parts, dict) or not parts or not isinstance(excluded, dict):
            raise ValueError("Provide parts or suggested_parts and an excluded_pages mapping, even if exclusions are empty.")
        seen, names = set(), set()
        for group, entries in (("parts", parts), ("excluded_pages", excluded)):
            for label, pages in entries.items():
                if group == "parts":
                    self._safe_component(label, "Instrument")
                    if label.casefold() in names:
                        raise ValueError(f"Duplicate instrument name: {label}")
                    names.add(label.casefold())
                elif not isinstance(label, str) or not label.strip():
                    raise ValueError("Every excluded page group needs a reason.")
                if not isinstance(pages, list) or not pages:
                    raise ValueError(f"{label}: pages must be a nonempty list.")
                for page in pages:
                    if type(page) is not int or page < 1 or page > len(reader.pages) or page in seen:
                        raise ValueError(f"{label}: duplicate or invalid source page {page!r}.")
                    seen.add(page)
        missing = sorted(set(range(1, len(reader.pages) + 1)) - seen)
        if missing:
            raise ValueError(f"Every page must be assigned or excluded. Unaccounted pages: {missing}")
        parent = Path(output_parent) if output_parent else source.parent
        if not parent.is_dir():
            raise FileNotFoundError(f"Output directory does not exist: {parent}")
        archive = parent / source.name
        move_source = source.parent.resolve() != parent.resolve()
        if move_source and archive.exists():
            raise FileExistsError(f"Source PDF destination already exists: {archive}")
        out_dir = parent / f"{number} - {title}_parts"
        if out_dir.exists():
            raise FileExistsError(f"Output already exists: {out_dir}")
        temporary = Path(tempfile.mkdtemp(prefix=".split-", dir=parent))
        try:
            for name, pages in parts.items():
                writer = PdfWriter()
                for page in pages:
                    writer.add_page(reader.pages[page - 1])
                with (temporary / f"{name} - {number} {title}.pdf").open("xb") as f:
                    writer.write(f)
            if using_suggestions:
                output_mapping = dict(mapping)
                output_mapping["parts"] = parts
                with (temporary / "page_map.yaml").open("x", encoding="utf-8") as f:
                    yaml.safe_dump(output_mapping, f, sort_keys=False, allow_unicode=True)
            else:
                shutil.copyfile(map_path, temporary / "page_map.yaml")
            if self._file_hash(source) != source_hash:
                raise RuntimeError("Source PDF changed during splitting.")
            temporary.rename(out_dir)
        except Exception:
            shutil.rmtree(temporary)
            raise
        self.logger.info("Saved %s reviewed parts to %s", len(parts), out_dir)
        if move_source:
            created = False
            try:
                with source.open("rb") as src:
                    with archive.open("xb") as dst:
                        created = True
                        shutil.copyfileobj(src, dst)
                if self._file_hash(archive) != source_hash or self._file_hash(source) != source_hash:
                    raise RuntimeError("Source PDF changed or archive copy did not match.")
            except Exception as exc:
                if created:
                    archive.unlink()
                raise RuntimeError(
                    f"Parts saved to {out_dir}, but the original was not moved from {source}: {exc}"
                ) from exc
            try:
                source.unlink()
            except OSError as exc:
                raise RuntimeError(
                    f"Parts saved to {out_dir} and original copied to {archive}, "
                    f"but the source could not be removed from {source}: {exc}"
                ) from exc
            self.logger.info("Moved source PDF to: %s", archive)
        return out_dir

    def _sanitize_filename(self, name):
        return re.sub(r'[^a-zA-Z0-9]', '_', name)

    def _save(self, source, out_dir, parts):
        reader = PdfReader(source)
        pdf_title = self._sanitize_filename(source.stem)

        for name, pages in parts.items():
            writer = PdfWriter()
            for p in pages:
                writer.add_page(reader.pages[p])

            clean_part_name = self._sanitize_filename(name)
            out_filename = f"{clean_part_name}_{pdf_title}.pdf"
            out_file = out_dir / out_filename

            with open(out_file, "wb") as f:
                writer.write(f)
            self.logger.info(f"Saved {out_filename} ({len(pages)} pages)")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('path', help='Path to the PDF file')
    default_config = Path(__file__).resolve().parent / "music_cache" / "instruments.yaml"
    parser.add_argument('-c', '--config', default=str(default_config), help='Path to the YAML config file')
    parser.add_argument('-d', '--debug', action='store_true', help='Enable debug logging')
    parser.add_argument('--dump', action='store_true', help='Dump raw OCR text to a file')
    parser.add_argument('--page-map', help='Reviewed YAML page map; without this option, create an OCR draft only')
    parser.add_argument('--output-dir', help='Existing parent directory for the reviewed parts folder')
    args = parser.parse_args()

    level = logging.DEBUG if args.debug else logging.INFO
    splitter = BigBandChartSplitter(config_path=args.config, log_level=level)
    if args.page_map:
        if args.dump:
            parser.error("--dump is only available when creating an OCR draft")
        splitter.split_from_map(Path(args.path), Path(args.page_map), args.output_dir)
    else:
        if args.output_dir:
            parser.error("--output-dir requires --page-map")
        splitter.process_file(Path(args.path), dump=args.dump)