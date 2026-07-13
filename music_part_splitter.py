import re
import difflib
import logging
import argparse
from pathlib import Path
from PyPDF2 import PdfWriter, PdfReader
from pdf2image import convert_from_path
import pytesseract


class BigBandChartSplitter:
    INSTRUMENTS = [
        ('1st Alto Sax', ['alto sax 1', 'alto 1', '1st alto', 'alto sax i', 'auto sax', '1st eb alto saxophone', 'tst eb alto saxophone', '1st alto saxophone']),
        ('2nd Alto Sax', ['alto sax 2', 'alto 2', '2nd alto', 'alto sax ii', '2nd eb alto saxophone', '2nd ep? alto saxophone', '2nd alto saxophone', '2nd eb alto']),
        ('1st Tenor Sax', ['tenor sax 1', 'tenor 1', '1st tenor', 'tenor sax i', '1st bb tenor saxophone', '1st tenor saxophone', '1st bb tenor']),
        ('2nd Tenor Sax', ['tenor sax 2', 'tenor 2', '2nd tenor', 'tenor sax ii', 'bb tenor saxophone', '2nd bb tenor', '2nd bb tenor saxophone', '2nd tenor saxophone']),
        ('Bari Sax', ['bari sax', 'baritone sax', 'baritone', 'bari saxophone', 'bari', 'eb baritone saxophone', 'e? baritone saxophone', 'baritone saxophone']),
        ('Trumpet 1', ['trumpet 1', '1st trumpet', 'trumpet i', 'solo bb trumpet', '1st bb trumpet', '1st b trumpet', 'ist b trumpet', 'ist bb trumpet']),
        ('Trumpet 2', ['trumpet 2', '2nd trumpet', 'trumpet ii', '2nd bb trumpet', '2nd b trumpet']),
        ('Trumpet 3', ['trumpet 3', '3rd trumpet', 'trumpet iii', '3rd bb trumpet', '3rd b trumpet']),
        ('Trumpet 4', ['trumpet 4', '4th trumpet', 'trumpet iv', '4th bb trumpet']),
        ('Trombone 1', ['trombone 1', '1st trombone', 'trombone i', 'ist trombone']),
        ('Trombone 2', ['trombone 2', '2nd trombone', 'trombone ii']),
        ('Trombone 3', ['trombone 3', '3rd trombone', 'trombone iii', 'dd, trombone']),
        ('Trombone 4', ['trombone 4', '4th trombone', 'bass trombone', 'trombone iv']),
        ('Piano', ['piano', 'pno', 'pno.', 'piano 1', 'piano 2', 'piano 3']),
        ('Drums', ['drum', 'drums', 'drumset', 'drum set', 'percussion']),
        ('Guitar', ['guitar', 'gtr', 'guitarist', "guitarist's guide", "guitarist's"]),
        ('Bass', ['string bass', 'upright bass', 'electric bass', 'bass guitar', 'bass (string bass)', 'bass aad', 'bass']),
        ('Vibes', ['vibes', 'vibraphone'])
    ]

    def __init__(self, log_level=logging.INFO, psm=3):
        self.logger = logging.getLogger("BigBandSplitter")
        self.logger.setLevel(log_level)
        self.psm = psm
        if not self.logger.handlers:
            h = logging.StreamHandler()
            h.setFormatter(logging.Formatter('%(asctime)s | %(levelname)s | %(message)s'))
            self.logger.addHandler(h)

    def _normalize(self, text):
        text = text.lower()

        # Global Typos
        text = text.replace('1st.', '1st')
        text = text.replace('2nd.', '2nd')
        text = text.replace('3rd.', '3rd')
        text = text.replace('4th.', '4th')
        text = text.replace('tst ', '1st ')
        text = text.replace('ep?', 'eb')
        text = text.replace('dd, trombone', '3rd trombone')

        # Saxophones
        text = re.sub(r'\balto sax 4\b', 'alto sax 1', text)
        text = re.sub(r'\bauto sax\b', 'alto sax 1', text)
        text = re.sub(r'\balto sax\s*7\b', 'alto sax 1', text)
        text = re.sub(r'\bbacitone\b', 'baritone', text)
        text = re.sub(r'\bbacitone sax\b', 'baritone sax', text)
        text = re.sub(r'\btenoe\b', 'tenor', text)

        # Trombones
        text = re.sub(r'\b(toombone|t2ombone|teomsone|qombone|trombome)\b', 'trombone', text)
        text = re.sub(r'\btrombone\s*(\d)\b', r'trombone \1', text)

        # Trumpets
        text = re.sub(r'\b(teumper|teumrer|eumeer)\b', 'trumpet', text)

        # Rhythm Section
        text = re.sub(r'\bp no\b', 'pno', text)
        text = re.sub(r'\bp\s*no\b', 'pno', text)
        text = re.sub(r'\boeums\b', 'drums', text)
        text = re.sub(r'\b(guirae|uitae)\b', 'guitar', text)

        return text

    def _get_best_match(self, text):
        lines = [line for line in text.splitlines() if line.strip()]
        # Filter to meaningful lines (not single-char OCR garbage like '>', '=', 'uu')
        meaningful_lines = [l for l in lines if len(l.strip()) > 2 and re.search(r'[a-z]', l)]
        header_text = "\n".join(meaningful_lines[:20])

        best_inst = None
        highest_score = 0

        for name, aliases in self.INSTRUMENTS:
            score = 0
            for a in aliases:
                score += len(re.findall(r"\b" + re.escape(a) + r"\b", header_text))
            if score > highest_score:
                highest_score = score
                best_inst = name

        if highest_score == 0:
            words = [w for w in re.findall(r"[a-z0-9]+", header_text)]
            for name, aliases in self.INSTRUMENTS:
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
            text = self._normalize(pytesseract.image_to_string(img.convert('L'), config=f'--psm {self.psm}'))
            raw_ocr_dump.append(f"PAGE {i + 1}:\n{text}")

            detected = self._get_best_match(text)

            # Check if this page is the START of a new part
            is_first_page_of_part = bool(re.search(r'\b(arranged by|music by|words by|composed by)\b', text))

            self.logger.debug(
                f"Page {i + 1} OCR length={len(text)}; detected={detected}; is_first_page={is_first_page_of_part}")

            is_new_detection = False

            # 1. SCORE LOCK-IN: The score runs uninterrupted until a definitive new title page is found.
            # This completely ignores margin cues like "Guitar" on page 28.
            if last_inst == 'Conductor Score' and not (is_first_page_of_part or detected):
                current_inst = 'Conductor Score'

            # 2. DETECT SCORE START
            elif re.search(r'\b(conductor|score|ssanuvsn|nolonihsvaa|yotavl)\b|40\}9nnpuo|jojonpvon|jo,onpuot,|holininod|jojonpuad', text):
                current_inst = 'Conductor Score'
                is_new_detection = True
                last_title_page = i

            # 3. DETECT NEW TITLE PAGE
            elif is_first_page_of_part:
                if detected:
                    current_inst = detected

                    # BUMP LOGIC: Handle misprinted or chopped-off part names
                    if (i - last_title_page) > 1:
                        part_instance_count[detected] = part_instance_count.get(detected, 0) + 1

                    count = part_instance_count.get(detected, 1)
                    if count == 2:
                        bump_map = {
                            '1st Alto Sax': '2nd Alto Sax',
                            '1st Tenor Sax': '2nd Tenor Sax',
                            'Trumpet 1': 'Trumpet 2',
                            'Trumpet 2': 'Trumpet 3',
                            'Trumpet 3': 'Trumpet 4',
                            'Trombone 1': 'Trombone 2',
                            'Trombone 2': 'Trombone 3',
                            'Trombone 3': 'Trombone 4',
                            'Guitar': 'Bass',
                            'Bass': 'Drums',
                            'Drums': 'Piano',
                        }
                        current_inst = bump_map.get(detected, detected)
                else:
                    # BLIND BUMP LOGIC: Found a title page, but OCR failed to read the header entirely.
                    # Assume it's the next logical part in the sequence (Fixes Trumpet 3!).
                    blind_bump_map = {
                        '1st Alto Sax': '2nd Alto Sax',
                        '1st Tenor Sax': '2nd Tenor Sax',
                        'Trumpet 1': 'Trumpet 2',
                        'Trumpet 2': 'Trumpet 3',
                        'Trumpet 3': 'Trumpet 4',
                        'Trombone 1': 'Trombone 2',
                        'Trombone 2': 'Trombone 3',
                        'Trombone 3': 'Trombone 4',
                        'Guitar': 'Bass',
                        'Bass': 'Drums',
                        'Drums': 'Piano',
                    }
                    current_inst = blind_bump_map.get(last_inst, last_inst)

                is_new_detection = True
                last_title_page = i

            # 4. CONTINUATION PAGES
            else:
                if detected and detected != last_inst:
                    current_inst = detected
                    is_new_detection = True
                else:
                    distance = i - last_detected_page

                    # ENFORCE CONTINUATION: If within a safe multi-page window (4 pages max for standard parts)
                    # FORCE continuation. This ignores false-positive cues like "Bass" on page 2 of Guitar.
                    if last_inst is not None and distance <= 3:
                        current_inst = last_inst
                    elif detected:
                        # Outside window, but found something? Trust it.
                        current_inst = detected
                        is_new_detection = True
                    else:
                        current_inst = None

            # OCR CONTEXT HACK: In PSM 3, "Trombone 3" occasionally scans literally as "Trombone 4"
            if is_new_detection and current_inst == 'Trombone 4' and last_inst == 'Trombone 2':
                current_inst = 'Trombone 3'

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
        # Replace non-alphanumeric characters with underscores
        return re.sub(r'[^a-zA-Z0-9]', '_', name)

    def _save(self, source, out_dir, parts):
        reader = PdfReader(source)
        # Get the clean title of the PDF
        pdf_title = self._sanitize_filename(source.stem)

        for name, pages in parts.items():
            writer = PdfWriter()
            for p in pages:
                writer.add_page(reader.pages[p])

            # Format: PartName_PDFTitle.pdf
            clean_part_name = self._sanitize_filename(name)
            out_filename = f"{clean_part_name}_{pdf_title}.pdf"
            out_file = out_dir / out_filename

            with open(out_file, "wb") as f:
                writer.write(f)
            self.logger.info(f"Saved {out_filename} ({len(pages)} pages)")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('path')
    parser.add_argument('-d', '--debug', action='store_true')
    parser.add_argument('--dump', action='store_true')
    parser.add_argument('--psm', type=int, default=3, help='Tesseract PSM mode (0-13, default 3)')
    args = parser.parse_args()

    level = logging.DEBUG if args.debug else logging.INFO
    splitter = BigBandChartSplitter(log_level=level, psm=args.psm)
    splitter.process_file(Path(args.path), dump=args.dump)