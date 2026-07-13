import re
import difflib
import logging
import argparse
import yaml
import sys
from pathlib import Path
from PyPDF2 import PdfWriter, PdfReader
from pdf2image import convert_from_path
import pytesseract


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

    def _get_next_instrument(self, current_name):
        """Returns the next instrument in the sequence based on YAML order."""
        for i, (name, _) in enumerate(self.instruments):
            if name == current_name and i + 1 < len(self.instruments):
                return self.instruments[i + 1][0]
        return current_name

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
                        ratio = difflib.SequenceMatcher(None, candidate, a).ratio()
                        if ratio >= 0.88:
                            fscore += 1
                if fscore > highest_score:
                    highest_score = fscore
                    best_inst = name

        return best_inst if highest_score > 0 else None

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

    def process_file(self, file_path, dump=False):
        self.logger.info(f"Starting processing for: {file_path.name}")
        self.logger.info(
            f"Converting PDF to images at 300 DPI (This will take a moment for {file_path.stat().st_size / (1024 * 1024):.2f} MB)...")

        images = convert_from_path(file_path, dpi=300)
        self.logger.info(f"Successfully converted PDF into {len(images)} images. Starting OCR...")

        out_dir = file_path.parent / f"{file_path.stem}_parts"
        out_dir.mkdir(exist_ok=True)

        parts = {}
        last_inst = None
        last_detected_page = -1
        last_title_page = -10
        raw_ocr_dump = []

        part_instance_count = {}

        for i, img in enumerate(images):
            # Only downcase the string now; no regex normalization
            text = pytesseract.image_to_string(img.convert('L'), config='--psm 3').lower()
            raw_ocr_dump.append(f"PAGE {i + 1}:\n{text}")

            detected = self._get_best_match(text)
            is_first_page_of_part = bool(re.search(r'\b(arranged by|music by|words by|composed by)\b', text))

            self.logger.debug(
                f"Page {i + 1} OCR length={len(text)}; detected={detected}; is_first_page={is_first_page_of_part}")

            is_new_detection = False
            current_inst = None

            if last_inst == 'Conductor Score' and not (is_first_page_of_part or detected):
                current_inst = 'Conductor Score'

            elif self.conductor_regex.search(text):
                current_inst = 'Conductor Score'
                is_new_detection = True
                last_title_page = i

            elif is_first_page_of_part:
                if detected:
                    current_inst = detected
                    if (i - last_title_page) > 1:
                        part_instance_count[detected] = part_instance_count.get(detected, 0) + 1

                    count = part_instance_count.get(detected, 1)
                    if count == 2:
                        current_inst = self._get_next_instrument(detected)
                else:
                    current_inst = self._get_next_instrument(last_inst)

                is_new_detection = True
                last_title_page = i

            else:
                if detected and detected != last_inst:
                    current_inst = detected
                    is_new_detection = True
                else:
                    distance = i - last_detected_page
                    if last_inst is not None and distance <= 3:
                        current_inst = last_inst
                    elif detected:
                        current_inst = detected
                        is_new_detection = True

            if is_new_detection and current_inst == 'Trombone 4' and last_inst == 'Trombone 2':
                current_inst = 'Trombone 3'

            if current_inst is None:
                current_inst = self._run_wizard(i, text)
                if current_inst:
                    is_new_detection = True

            if current_inst:
                parts.setdefault(current_inst, []).append(i)
                if is_new_detection:
                    last_detected_page = i
                last_inst = current_inst
            else:
                last_inst = None

            self.logger.info(f"Page {i + 1} | Assigned: {current_inst}")

        if dump:
            dump_path = out_dir / f"{file_path.stem}_OCR_DUMP.txt"
            with open(dump_path, "w", encoding="utf-8") as f:
                f.write("\n\n".join(raw_ocr_dump))
            self.logger.info(f"Dump saved to {dump_path}")

        self._save(file_path, out_dir, parts)

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
    parser.add_argument('-c', '--config', default='instruments.yaml', help='Path to the YAML config file')
    parser.add_argument('-d', '--debug', action='store_true', help='Enable debug logging')
    parser.add_argument('--dump', action='store_true', help='Dump raw OCR text to a file')
    args = parser.parse_args()

    level = logging.DEBUG if args.debug else logging.INFO
    splitter = BigBandChartSplitter(config_path=args.config, log_level=level)
    splitter.process_file(Path(args.path), dump=args.dump)