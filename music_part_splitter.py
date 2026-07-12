import os
import sys
import logging
import pytesseract
from pathlib import Path
from pdf2image import convert_from_path
from PyPDF2 import PdfWriter, PdfReader


class BigBandChartSplitter:
    STANDARD_INSTRUMENTS = [
        ['1st Alto Sax', '1st Eb Alto', 'Ist Eb Alto', 'ALTO SAX 1', '1st Eb Alto Saxophone', 'Alto Saxophone 1'],
        ['2nd Alto Sax', '2nd Eb Alto', 'ALTO SAX 2', '2st Eb Alto Saxophone', '2nd Eb Alto Saxophone',
         'Alto Saxophone 2'],
        ['1st Tenor Sax', 'TENOR SAX 1', '1st Bb Tenor', '1st Bb Tenor Saxophone', 'Tenor Saxophone 1'],
        ['2nd Tenor Sax', 'TENOR SAX ll', 'TENOR SAX 2', '2nd Bb Tenor Saxophone', 'Tenor Saxophone 2'],
        ['Bari Sax', 'Eb Baritone', 'BARITONE SAX', 'Eb Baritone Saxophone', 'Baritone Saxophone'],
        ['Trumpet 1', '1st Bb Trumpet', '1st Trumpet', 'Trumpet 1'],
        ['Trumpet 2', '2nd Bb Trumpet', '2nd Trumpet', 'Trumpet 2'],
        ['Trumpet 3', '3rd Bb Trumpet', '3rd Trumpet', 'Trumpet 3'],
        ['Trumpet 4', '4th Bb Trumpet', '4rd Bb Trumpet', '4th Trumpet', 'Ard gb TRUMPET'],
        ['Trombone 1', 'tet Trombone', '1st Trombone', 'Trombone 1'],
        ['Trombone 2', '2nd Trombone', 'XOMBONE 2'],
        ['Trombone 3', '3rd Trombone', 'TROMBONE3'],
        ['Trombone 4', '4th Trombone', 'Bass Trombone'],
        ['Piano'],
        ['Drums', 'Drum Set'],
        ['Guitar'],
        ['Bass', 'String Bass', 'Electric Bass'],
        ['Conductor Score', 'Conductor', 'Full Score']
    ]

    def __init__(self, log_level=logging.INFO):
        self._setup_logger(log_level)
        self._normalize_instruments()

    def _setup_logger(self, log_level):
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(log_level)
        if not self.logger.handlers:
            ch = logging.StreamHandler()
            formatter = logging.Formatter('%(asctime)s | %(levelname)s | %(message)s')
            ch.setFormatter(formatter)
            self.logger.addHandler(ch)

    def _normalize_instruments(self):
        self.instrument_rules = []
        for aliases in self.STANDARD_INSTRUMENTS:
            primary_name = aliases[0]
            search_terms = [alias.lower() for alias in aliases]
            self.instrument_rules.append((primary_name, search_terms))

    def _ocr_scan_pdf(self, pdf_file):
        self.logger.info("Extracting Text from PDF Images per page via OCR...")
        part_list = []
        doc = convert_from_path(pdf_file)
        for page_number, page_data in enumerate(doc):
            self.logger.info(f"OCR Scan Page {page_number + 1}")
            txt = str(pytesseract.image_to_string(page_data, lang="eng").encode("utf-8"))
            part_list.append(txt)
        return part_list

    def process_pdf(self, pdf_file):
        my_file = Path(pdf_file)
        # Fix: Now derived from my_file.parent
        file_base_name = my_file.stem
        part_folder = my_file.parent / f"{file_base_name}_parts"

        self.logger.info(f"Processing: {my_file.name}")
        self.logger.info(f"Creating output directory at: {part_folder}")
        part_folder.mkdir(parents=True, exist_ok=True)

        parts = {}
        part_list = self._ocr_scan_pdf(pdf_file)

        for primary_name, aliases_lower in self.instrument_rules:
            matched_pages = [
                i for i, page_text in enumerate(part_list)
                if any(alias in page_text[:250].lower() for alias in aliases_lower)
            ]
            if matched_pages:
                self.logger.info(f"Found {primary_name} on pages: {[p + 1 for p in matched_pages]}")
                parts[primary_name] = matched_pages

        self._extract_and_save_parts(pdf_file, part_folder, parts)

    def _extract_and_save_parts(self, pdf_file, output_folder, parts_map):
        if not parts_map:
            self.logger.warning("No parts matched to extract.")
            return

        input_pdf = PdfReader(open(pdf_file, "rb"))
        file_name = Path(pdf_file).name

        for inst_name, page_numbers in parts_map.items():
            self.logger.info(f"Writing PDF part for {inst_name}")
            output = PdfWriter()
            for page_num in page_numbers:
                if page_num < len(input_pdf.pages):
                    output.add_page(input_pdf.pages[page_num])

            output_filename = output_folder / f"{inst_name}_{file_name}"
            with open(output_filename, "wb") as output_stream:
                output.write(output_stream)
        self.logger.info(f"Extraction Completed. Files saved to: {output_folder}")


if __name__ == "__main__":
    splitter = BigBandChartSplitter(log_level=logging.INFO)
    try:
        raw_input_path = input("Enter the full path to the PDF chart: ").strip().replace('"', '').replace("'", "")
        pdf_target = Path(raw_input_path)
        if not pdf_target.is_file():
            print(f"\n[Error] Could not locate file at: {pdf_target}")
            sys.exit(1)
        splitter.process_pdf(pdf_target)
    except KeyboardInterrupt:
        sys.exit(0)