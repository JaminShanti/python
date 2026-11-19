import importlib.util, pathlib, sys
import yaml
from unittest.mock import patch

repo_root = pathlib.Path(__file__).resolve().parents[1]
if str(repo_root) not in sys.path:
    sys.path.insert(0, str(repo_root))

spec = importlib.util.spec_from_file_location(
    "music_part_splitter", repo_root / "music_part_splitter.py"
)
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)

def test_imports():
    assert module is not None


def test_recognizes_vocal_part_label(tmp_path):
    config_path = tmp_path / "instruments.yaml"
    config_path.write_text(
        yaml.safe_dump(
            {
                "conductor_aliases": ["conductor", "score"],
                "instruments": [
                    {"name": "Bass", "aliases": ["bass"]},
                    {"name": "Vocal", "aliases": ["vocal", "vocals", "voice", "voce", "singer"]},
                ],
            }
        ),
        encoding="utf-8",
    )
    splitter = module.BigBandChartSplitter(config_path)

    assert splitter._get_best_match("my reverie\nvoce\nmelody based on") == "Vocal"
    assert splitter._get_best_match("my reverie\nbrass") is None


def test_retries_sparse_page_ocr_to_recognize_vocal(tmp_path):
    config_path = tmp_path / "instruments.yaml"
    config_path.write_text(
        yaml.safe_dump(
            {
                "conductor_aliases": ["conductor", "score"],
                "instruments": [
                    {"name": "Bass", "aliases": ["bass"]},
                    {"name": "Vocal", "aliases": ["vocal", "vocals", "voice", "voce", "singer"]},
                ],
            }
        ),
        encoding="utf-8",
    )
    splitter = module.BigBandChartSplitter(config_path)
    image = type("Image", (), {"convert": lambda self, mode: self})()

    with patch.object(module.pytesseract, "image_to_string", return_value="my reverie\nvoce\nbrass"):
        assert splitter._get_page_instrument(image, "my reverie\nmelody based on") == "Vocal"


def test_unrecognized_part_title_uses_wizard_instead_of_next_instrument(tmp_path):
    config_path = tmp_path / "instruments.yaml"
    config_path.write_text(
        yaml.safe_dump(
            {
                "conductor_aliases": ["conductor", "score"],
                "instruments": [
                    {"name": "Trumpet 3", "aliases": ["trumpet 3"]},
                    {"name": "Trumpet 4", "aliases": ["trumpet 4"]},
                    {"name": "Trombone 1", "aliases": ["trombone 1"]},
                ],
            }
        ),
        encoding="utf-8",
    )
    splitter = module.BigBandChartSplitter(config_path)
    source = tmp_path / "chart.pdf"
    source.write_bytes(b"placeholder")
    image = type("Image", (), {"convert": lambda self, mode: self})()
    images = [image, image]

    with (
        patch.object(module, "convert_from_path", return_value=images),
        patch.object(
            module.pytesseract,
            "image_to_string",
            side_effect=["trumpet 3\narranged by", "title\narranged by"],
        ),
        patch.object(
            splitter,
            "_get_page_instrument",
            side_effect=["Trumpet 3", None],
        ),
        patch.object(splitter, "_run_wizard", return_value="Trombone 1") as wizard,
        patch.object(splitter, "_save") as save,
    ):
        splitter.process_file(source)

    wizard.assert_called_once_with(1, "title\narranged by")
    assert save.call_args.args[2] == {"Trumpet 3": [0], "Trombone 1": [1]}


def test_repeated_recognized_title_does_not_guess_next_instrument(tmp_path):
    config_path = tmp_path / "instruments.yaml"
    config_path.write_text(
        yaml.safe_dump(
            {
                "conductor_aliases": ["conductor", "score"],
                "instruments": [
                    {"name": "Trumpet 3", "aliases": ["trumpet 3"]},
                    {"name": "Trumpet 4", "aliases": ["trumpet 4"]},
                ],
            }
        ),
        encoding="utf-8",
    )
    splitter = module.BigBandChartSplitter(config_path)
    source = tmp_path / "chart.pdf"
    source.write_bytes(b"placeholder")
    image = type("Image", (), {"convert": lambda self, mode: self})()

    with (
        patch.object(module, "convert_from_path", return_value=[image, image, image]),
        patch.object(
            module.pytesseract,
            "image_to_string",
            side_effect=[
                "trumpet 3\narranged by",
                "page 2 music",
                "trumpet 3\narranged by",
            ],
        ),
        patch.object(
            splitter,
            "_get_page_instrument",
            side_effect=["Trumpet 3", None, "Trumpet 3"],
        ),
        patch.object(splitter, "_run_wizard") as wizard,
        patch.object(splitter, "_save") as save,
    ):
        splitter.process_file(source)

    wizard.assert_not_called()
    assert save.call_args.args[2] == {"Trumpet 3": [0, 1, 2]}


def test_detected_trombone_four_is_not_remapped(tmp_path):
    config_path = tmp_path / "instruments.yaml"
    config_path.write_text(
        yaml.safe_dump(
            {
                "conductor_aliases": ["conductor", "score"],
                "instruments": [
                    {"name": "Trombone 2", "aliases": ["trombone 2"]},
                    {"name": "Trombone 3", "aliases": ["trombone 3"]},
                    {"name": "Trombone 4", "aliases": ["trombone 4"]},
                ],
            }
        ),
        encoding="utf-8",
    )
    splitter = module.BigBandChartSplitter(config_path)
    source = tmp_path / "chart.pdf"
    source.write_bytes(b"placeholder")
    image = type("Image", (), {"convert": lambda self, mode: self})()

    with (
        patch.object(module, "convert_from_path", return_value=[image, image]),
        patch.object(
            module.pytesseract,
            "image_to_string",
            side_effect=[
                "trombone 2\narranged by",
                "trombone 4\narranged by",
            ],
        ),
        patch.object(
            splitter,
            "_get_page_instrument",
            side_effect=["Trombone 2", "Trombone 4"],
        ),
        patch.object(splitter, "_save") as save,
    ):
        splitter.process_file(source)

    assert save.call_args.args[2] == {"Trombone 2": [0], "Trombone 4": [1]}


def test_recognizes_legacy_saxophone_labels():
    splitter = module.BigBandChartSplitter(repo_root / "music_cache" / "instruments.yaml")

    assert splitter._get_best_match("2nd Eb Alto Saxophone") == "2nd Alto Sax"
    assert splitter._get_best_match("3rd Sax. Eb Alto") == "2nd Alto Sax"
    assert splitter._get_best_match("2nd Saxophone Bb Tenor") == "1st Tenor Sax"
    assert splitter._get_best_match("4th Saxophone Bb Tenor") == "2nd Tenor Sax"
    assert splitter._get_best_match("Violins") == "Violins"
