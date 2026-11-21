import importlib.util, pathlib, sys
import yaml
import pytest
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


def test_ocr_draft_does_not_assign_cues_or_continuation_pages(tmp_path):
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
    writer = module.PdfWriter()
    writer.add_blank_page(width=100, height=100)
    writer.add_blank_page(width=100, height=100)
    with source.open("wb") as f:
        writer.write(f)
    image = type("Image", (), {"width": 100, "height": 100,
                                "crop": lambda self, box: self,
                                "convert": lambda self, mode: self})()

    with (
        patch.object(module, "convert_from_path", return_value=[image]),
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
    ):
        draft = yaml.safe_load(splitter.process_file(source).read_text(encoding="utf-8"))

    assert draft["parts"] == {}
    assert draft["excluded_pages"] == {}
    assert draft["ocr_suggestions"] == [
        {"page": 1, "suggested_instrument": "Trumpet 3"},
        {"page": 2, "suggested_instrument": None},
    ]
    assert not list(tmp_path.glob("*_parts"))


def make_reviewed_map(tmp_path):
    splitter = module.BigBandChartSplitter(repo_root / "music_cache" / "instruments.yaml")
    source = tmp_path / "chart.pdf"
    writer = module.PdfWriter()
    for width in [100, 200, 300]:
        writer.add_blank_page(width=width, height=100)
    with source.open("wb") as f:
        writer.write(f)
    data = {"source_file": source.name, "source_sha256": splitter._file_hash(source),
            "page_count": 3, "song_number": "30b", "song_title": "Satin Doll",
            "parts": {"Alto Sax I": [2, 1]}, "excluded_pages": {"unrelated song": [3]}}
    path = tmp_path / "map.yaml"
    path.write_text(yaml.safe_dump(data), encoding="utf-8")
    return splitter, source, path, data


def test_reviewed_map_splits_exact_pages_with_band_names(tmp_path):
    splitter, source, path, _ = make_reviewed_map(tmp_path)
    out = splitter.split_from_map(source, path)
    pdf = out / "Alto Sax I - 30b Satin Doll.pdf"
    pages = module.PdfReader(pdf).pages
    assert [float(p.mediabox.width) for p in pages] == [200, 100]
    assert (out / "page_map.yaml").read_bytes() == path.read_bytes()
    with pytest.raises(FileExistsError):
        splitter.split_from_map(source, path)


def test_reviewed_map_accepts_hyphenated_alternate_number(tmp_path):
    splitter, source, path, data = make_reviewed_map(tmp_path)
    data["song_number"] = "172-A"
    path.write_text(yaml.safe_dump(data), encoding="utf-8")
    assert splitter.split_from_map(source, path).name == "172-A - Satin Doll_parts"


def test_reviewed_map_rejects_duplicate_yaml_keys(tmp_path):
    splitter, source, path, _ = make_reviewed_map(tmp_path)
    with path.open("a", encoding="utf-8") as f:
        f.write("parts:\n  Trumpet I: [1, 2, 3]\n")
    with pytest.raises(ValueError, match="Duplicate key"):
        splitter.split_from_map(source, path)


@pytest.mark.parametrize("change", ["missing", "duplicate", "wrong_source", "unsafe", "invalid_page"])
def test_reviewed_map_rejects_unsafe_or_incomplete_assignments(tmp_path, change):
    splitter, source, path, data = make_reviewed_map(tmp_path)
    if change == "missing":
        data["excluded_pages"] = {}
    elif change == "duplicate":
        data["excluded_pages"] = {"unrelated": [1, 3]}
    elif change == "wrong_source":
        data["source_sha256"] = "wrong"
    elif change == "unsafe":
        data["song_title"] = "..\\outside"
    else:
        data["parts"] = {"Alto Sax I": [True, 2]}
    path.write_text(yaml.safe_dump(data), encoding="utf-8")
    with pytest.raises(ValueError):
        splitter.split_from_map(source, path)
    assert not list(tmp_path.glob("*_parts"))


def test_recognizes_legacy_saxophone_labels():
    splitter = module.BigBandChartSplitter(repo_root / "music_cache" / "instruments.yaml")

    assert splitter._get_best_match("2nd Eb Alto Saxophone") == "2nd Alto Sax"
    assert splitter._get_best_match("3rd Sax. Eb Alto") == "2nd Alto Sax"
    assert splitter._get_best_match("2nd Saxophone Bb Tenor") == "1st Tenor Sax"
    assert splitter._get_best_match("4th Saxophone Bb Tenor") == "2nd Tenor Sax"
    assert splitter._get_best_match("Violins") == "Violins"
