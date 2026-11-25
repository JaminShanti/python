import importlib.util, logging, pathlib, sys
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


def test_logs_source_before_processing(tmp_path, caplog):
    splitter = module.BigBandChartSplitter(repo_root / "music_cache" / "instruments.yaml")
    source = tmp_path / "missing chart.pdf"
    caplog.set_level(logging.INFO, logger="BigBandSplitter")

    with pytest.raises(FileNotFoundError):
        splitter.process_file(source)
    assert f"Starting OCR draft for: {source}" in caplog.messages

    with pytest.raises(FileNotFoundError):
        splitter.split_from_map(source, tmp_path / "missing map.yaml")
    assert f"Starting reviewed split for: {source} (page map: {tmp_path / 'missing map.yaml'})" in caplog.messages


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


def test_moves_original_to_output_parent_after_success(tmp_path):
    splitter, source, path, _ = make_reviewed_map(tmp_path)
    original = source.read_bytes()
    parent = tmp_path / "processed"
    parent.mkdir()

    out = splitter.split_from_map(source, path, parent)

    assert (out / "Alto Sax I - 30b Satin Doll.pdf").exists()
    assert (parent / source.name).read_bytes() == original
    assert not source.exists()
    assert path.exists()


def test_does_not_overwrite_archived_original(tmp_path):
    splitter, source, path, _ = make_reviewed_map(tmp_path)
    original = source.read_bytes()
    parent = tmp_path / "processed"
    parent.mkdir()
    (parent / source.name).write_bytes(b"existing file")

    with pytest.raises(FileExistsError, match="destination already exists"):
        splitter.split_from_map(source, path, parent)

    assert source.read_bytes() == original
    assert (parent / source.name).read_bytes() == b"existing file"
    assert not list(parent.glob("*_parts"))


def test_keeps_original_if_archiving_fails_after_split(tmp_path):
    splitter, source, path, _ = make_reviewed_map(tmp_path)
    original = source.read_bytes()
    parent = tmp_path / "processed"
    parent.mkdir()

    with patch.object(module.shutil, "copyfileobj", side_effect=OSError("disk full")):
        with pytest.raises(RuntimeError, match="Parts saved.*original was not moved"):
            splitter.split_from_map(source, path, parent)

    assert source.read_bytes() == original
    assert not (parent / source.name).exists()
    assert (parent / "30b - Satin Doll_parts").is_dir()


def test_splits_suggested_parts_and_saves_resolved_map(tmp_path, caplog):
    splitter, source, path, data = make_reviewed_map(tmp_path)
    data["suggested_parts"] = data["parts"]
    data["parts"] = {}
    path.write_text(yaml.safe_dump(data), encoding="utf-8")
    caplog.set_level(logging.WARNING, logger="BigBandSplitter")

    out = splitter.split_from_map(source, path)

    pages = module.PdfReader(out / "Alto Sax I - 30b Satin Doll.pdf").pages
    assert [float(p.mediabox.width) for p in pages] == [200, 100]
    saved = yaml.safe_load((out / "page_map.yaml").read_text(encoding="utf-8"))
    assert saved["parts"] == {"Alto Sax I": [2, 1]}
    assert yaml.safe_load(path.read_text(encoding="utf-8"))["parts"] == {}
    assert "Using suggested parts" in caplog.text


def test_suggested_unidentified_pages_are_not_silently_excluded(tmp_path):
    splitter, source, path, data = make_reviewed_map(tmp_path)
    data["suggested_parts"] = data["parts"]
    data["parts"] = {}
    data["excluded_pages"] = {}
    data["suggested_exclusions"] = {"OCR not identified": [3]}
    path.write_text(yaml.safe_dump(data), encoding="utf-8")

    with pytest.raises(ValueError, match=r"Unaccounted pages: \[3\]"):
        splitter.split_from_map(source, path)
    assert not list(tmp_path.glob("*_parts"))


def test_explicit_parts_take_precedence_over_suggestions(tmp_path):
    splitter, source, path, data = make_reviewed_map(tmp_path)
    data["suggested_parts"] = {"Trumpet I": [1, 2]}
    path.write_text(yaml.safe_dump(data), encoding="utf-8")

    out = splitter.split_from_map(source, path)
    assert (out / "Alto Sax I - 30b Satin Doll.pdf").exists()
    assert not (out / "Trumpet I - 30b Satin Doll.pdf").exists()


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


def test_improves_ocr_suggestions_with_continuity(tmp_path):
    splitter = module.BigBandChartSplitter(repo_root / "music_cache" / "instruments.yaml")
    suggestions = [
        {"page": 1, "suggested_instrument": "Piano"},
        {"page": 2, "suggested_instrument": "Piano"},
        {"page": 3, "suggested_instrument": None},
        {"page": 4, "suggested_instrument": "Drums"},
    ]

    improved = splitter._improve_suggestions(suggestions)

    assert improved[2]["suggested_instrument"] == "Drums"
    assert splitter._standard_part_name("1st Alto Sax") == "Alto Sax I"
    assert splitter._standard_part_name("Conductor Score") == "Conductor"


def test_does_not_reorder_pages_based_on_common_band_order():
    splitter = module.BigBandChartSplitter(repo_root / "music_cache" / "instruments.yaml")
    suggestions = [
        {"page": 1, "suggested_instrument": "1st Alto Sax"},
        {"page": 2, "suggested_instrument": "1st Tenor Sax"},
        {"page": 3, "suggested_instrument": "2nd Alto Sax"},
        {"page": 4, "suggested_instrument": "2nd Tenor Sax"},
    ]

    improved = splitter._improve_suggestions(suggestions)

    assert [item["suggested_instrument"] for item in improved] == [
        "1st Alto Sax",
        "1st Tenor Sax",
        "2nd Alto Sax",
        "2nd Tenor Sax",
    ]


def test_label_number_takes_precedence_over_song_number():
    splitter = module.BigBandChartSplitter(repo_root / "music_cache" / "instruments.yaml")
    image = type("Image", (), {"convert": lambda self, mode: self})()
    assert splitter._get_page_instrument(image, "When I Fall In Love 210\nVocal\n3rd Trombone") == "Trombone III"
    assert splitter._get_page_instrument(image, "When I Fall In Love 210\nVocal\n4th Trombone") == "Trombone IV"


def test_numbered_parts_can_appear_in_reverse_order():
    suggestions = [
        {"page": 1, "suggested_instrument": "Trumpet IV"},
        {"page": 2, "suggested_instrument": "Trumpet III"},
    ]
    assert module.BigBandChartSplitter._improve_suggestions(suggestions) == suggestions


def test_generic_trombone_label_is_not_assigned_to_trombone_i():
    splitter = module.BigBandChartSplitter(repo_root / "music_cache" / "instruments.yaml")
    image = type("Image", (), {})()

    with patch.object(module.pytesseract, "image_to_string", return_value=""):
        assert splitter._get_page_instrument(image, "Trombone") is None


def test_multistaff_score_is_classified_as_conductor():
    splitter = module.BigBandChartSplitter(repo_root / "music_cache" / "instruments.yaml")
    image = type("Image", (), {})()
    text = "Alto Sax 1\nTenor Sax 1\nTrumpet 1\nTrombone 1\nPiano\nDrums"

    with patch.object(module.pytesseract, "image_to_string", return_value=""):
        assert splitter._get_page_instrument(image, text) == "Conductor Score"


def test_repeated_solo_tenant_is_not_assumed_as_a_real_part():
    splitter = module.BigBandChartSplitter(repo_root / "music_cache" / "instruments.yaml")
    suggestions = [
        {"page": page, "suggested_instrument": "Solo Tenor Sax"}
        for page in range(18, 44)
    ]

    improved = splitter._improve_suggestions(suggestions)

    assert all(item["suggested_instrument"] is None for item in improved)


def test_readable_noisy_ordinals_are_detected_directly():
    splitter = module.BigBandChartSplitter(repo_root / "music_cache" / "instruments.yaml")
    image = type("Image", (), {"convert": lambda self, mode: self})()

    cases = [
        ("167 ALTO SAX\nAMERICAN MEDLEY", "Alto Sax I"),
        ("two ALTO SAX\nAMERICAN MEDLEY", "Alto Sax II"),
        ("lot TENOR SAX\nAMERICAN MEDLEY", "Tenor Sax I"),
        ("two TENOR SAX\nAMERICAN MEDLEY", "Tenor Sax II"),
        ("3rd TRUMPET\nAMERICAN MEDLEY", "Trumpet III"),
        ("4th TROMBONE\nAMERICAN MEDLEY", "Trombone IV"),
        ("DRUMS\nAMERICAN MEDLEY", "Drums"),
    ]
    for text, expected in cases:
        with patch.object(module.pytesseract, "image_to_string", return_value=""):
            assert splitter._get_page_instrument(image, text) == expected
