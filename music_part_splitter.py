import os
import re
import yaml
import logging
import pytesseract
from pathlib import Path
from pdf2image import convert_from_path
from PyPDF2 import PdfWriter, PdfReader


# Note: PyPDF2 deprecated PdfFileWriter/PdfFileReader in newer versions.
# Updated to the modern PdfWriter/PdfReader syntax.

class BigBandChartSplitter:
    """
    A utility to split full big band PDF scores into individual instrument parts
    using a YAML configuration and Tesseract OCR.
    """

    def __init__(self, config_path='config.yaml', log_level=logging.INFO):
        self._setup_logger(log_level)
        self.config_path = config_path
        self.config = self._load_config()

    def _setup_logger(self, log_level):
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(log_level)
        if not self.logger.handlers:
            ch = logging.StreamHandler()
            formatter = logging.Formatter('%(asctime)s | %(levelname)s | %(message)s')
            ch.setFormatter(formatter)
            self.logger.addHandler(ch)

    def _load_config(self):
        """Loads the YAML config and normalizes the instruments list."""
        if not os.path.exists(self.config_path):
            self.logger.error(f"Configuration file {self.config_path} not found.")
            return {'instruments': [], 'overrides': {}}

        with open(self.config_path, 'r') as file:
            config = yaml.load(file, Loader=yaml.FullLoader)

        self.logger.debug("Normalizing single instrument names to lists")
        # Ensure every instrument entry is a list of strings for matching
        for i, instrument in enumerate(config.get('instruments', [])):
            if isinstance(instrument, str):
                config['instruments'][i] = [instrument]
            config['instruments'][i] = [str(x) for x in config['instruments'][i]]

        return config

    def _parse_page_ranges(self, page_rule, total_pages):
        """Helper to parse exact pages or ranges like '1-2' from the config."""
        if isinstance(page_rule, int):
            return [page_rule - 1]

        page_rule = str(page_rule)
        if page_rule.isdigit():
            return [int(page_rule) - 1]

        if re.match(r"\d+-\d+", page_rule):
            start, end = [int(x) for x in page_rule.split("-")]
            return list(range(start - 1, end))

        return []

    def _ocr_scan_pdf(self, pdf_file):
        """Converts PDF to images and extracts text per page."""
        self.logger.info("Extracting Text from PDF Images per page via OCR...")
        part_list = []
        doc = convert_from_path(pdf_file)

        for page_number, page_data in enumerate(doc):
            self.logger.info(f"OCR Scan Page {page_number + 1}")
            txt = str(pytesseract.image_to_string(page_data, lang="eng").encode("utf-8"))
            part_list.append(txt)
            self.logger.debug(f"Page OCR Information: {txt}")

        return part_list

    def process_pdf(self, pdf_file):
        """Main pipeline to process a single PDF chart."""
        my_file = Path(pdf_file)
        if not my_file.is_file():
            self.logger.error(f"File not found: {pdf_file}")
            return

        file_base_name = my_file.stem
        part_folder = my_file.parent / f"{file_base_name}_parts"

        self.logger.info(f"Processing: {my_file.name}")
        self.logger.info(f"Creating output directory: {part_folder}")
        part_folder.mkdir(parents=True, exist_ok=True)

        parts = {}  # Dictionary mapping instrument name to a list of 0-indexed page numbers

        # Check for overrides (Fixing the notebook bug here)
        if my_file.name in self.config.get('overrides', {}):
            self.logger.info(f"Applying overrides for {my_file.name}")
            override_rules = self.config['overrides'][my_file.name]

            # override_rules looks like: [['1st Eb Alto Saxophone', '1-2'], ['2nd Bb Tenor Saxophone', '5-6']]
            for instrument_rule in override_rules:
                if len(instrument_rule) == 2:
                    inst_name, page_rule = instrument_rule
                    parts[inst_name] = self._parse_page_ranges(page_rule, 0)
        else:
            # No override, fallback to OCR
            part_list = self._ocr_scan_pdf(pdf_file)

            for instrument_aliases in self.config.get('instruments', []):
                primary_name = instrument_aliases[0]
                aliases_lower = [x.lower() for x in instrument_aliases]

                self.logger.debug(f"Scanning for: {', '.join(aliases_lower)}")

                # Check if the instrument rule includes a hardcoded page rule (e.g., ['1st Alto Sax', '1-2'])
                if len(instrument_aliases) == 2 and (
                        str(instrument_aliases[1]).isdigit() or re.match(r"\d+-\d+", str(instrument_aliases[1]))):
                    self.logger.info(f"{primary_name}: Extracting hardcoded pages {instrument_aliases[1]}")
                    parts[primary_name] = self._parse_page_ranges(instrument_aliases[1], len(part_list))
                else:
                    # OCR Text Matching (Checking the first 200 chars of the page)
                    matched_pages = [
                        i for i, page_text in enumerate(part_list)
                        if any(alias in page_text[:200].lower() for alias in aliases_lower)
                    ]
                    if matched_pages:
                        parts[primary_name] = matched_pages

        self._extract_and_save_parts(pdf_file, part_folder, parts)

    def _extract_and_save_parts(self, pdf_file, output_folder, parts_map):
        """Splits the physical PDF and writes the individual instrument files."""
        if not parts_map:
            self.logger.warning("No parts matched to extract.")
            return

        input_pdf = PdfReader(open(pdf_file, "rb"))
        file_name = Path(pdf_file).name

        for inst_name, page_numbers in parts_map.items():
            if not page_numbers:
                continue

            self.logger.info(f"Writing PDF part for {inst_name}")
            output = PdfWriter()

            for page_num in page_numbers:
                if page_num < len(input_pdf.pages):
                    output.add_page(input_pdf.pages[page_num])
                else:
                    self.logger.warning(f"Page index {page_num} out of range for {inst_name}")

            output_filename = output_folder / f"{inst_name}_{file_name}"
            with open(output_filename, "wb") as output_stream:
                output.write(output_stream)

        self.logger.info("Extraction Completed successfully.")

    def process_directory(self, directory='.'):
        """Processes all PDFs in a given directory."""
        pdf_list = [f for f in os.listdir(directory) if f.lower().endswith('.pdf')]
        self.logger.info(f"Found {len(pdf_list)} PDFs in {directory}")

        for pdf_file in pdf_list:
            self.process_pdf(os.path.join(directory, pdf_file))


# --- CLI Execution ---
if __name__ == "__main__":
    # You can instantiate this in another script, or run it directly here.
    splitter = BigBandChartSplitter(config_path='config.yaml', log_level=logging.DEBUG)

    # Process a specific file just like cell 70 in your notebook:
    # splitter.process_pdf('Feelings - Full Big Band (Lowden).pdf')

    # Or process the whole directory:
    # splitter.process_directory('.')