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
