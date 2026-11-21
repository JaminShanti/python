import csv
import io

import pytest
import yaml

from music_sheet_scripts import music_part_recovery as recovery


@pytest.fixture
def chart(tmp_path):
    folder = tmp_path / "534 - You Made Me Love You_parts"
    folder.mkdir()
    (folder / "page_map.yaml").write_text(yaml.safe_dump({
        "song_number": "534", "song_title": "You Made Me Love You",
        "parts": {"Alto Sax II": [1]},
    }), encoding="utf-8")
    (folder / "Alto Sax II - 534 You Made Me Love You.pdf").write_bytes(b"pdf data")
    active = tmp_path / "active.yaml"
    active.write_text('Alto Sax II:\n  - "534 - You Made Me Love You"\n  - "22 - Another Song"\n', encoding="utf-8")
    active.with_suffix(".csv").write_text(
        "Part,Missing Songs\n\nAlto Sax II,534 - You Made Me Love You; 22 - Another Song\n",
        encoding="utf-8-sig",
    )
    report = tmp_path / "report.csv"
    report.write_text(
        "Source folder,Source part,Dropbox status,Dropbox paths\n"
        "534 - You Made Me Love You_parts,Alto Sax II - 534 You Made Me Love You.pdf,Local,\n",
        encoding="utf-8-sig",
    )
    args = [str(folder), "--instrument", "Alto Sax II", "--missing-entry", "534 - You Made Me Love You",
            "--preparation-inventory", str(report), "--active-yaml", str(active)]
    return args, active, report


def test_dry_run_does_not_change_anything(chart, monkeypatch, capsys):
    args, active, report = chart
    before = [p.read_bytes() for p in (active, active.with_suffix(".csv"), report)]
    monkeypatch.setattr(recovery, "rclone", lambda *args: b"")
    recovery.main(args)
    assert "PLAN Alto Sax II" in capsys.readouterr().out
    assert before == [p.read_bytes() for p in (active, active.with_suffix(".csv"), report)]


def test_upload_requires_verified_bytes_before_inventory_update(chart, monkeypatch):
    args, active, report = chart
    commands = []

    def fake(*command):
        commands.append(command)
        return b"different" if command[0] == "cat" else b""

    monkeypatch.setattr(recovery, "rclone", fake)
    before = [p.read_bytes() for p in (active, active.with_suffix(".csv"), report)]
    with pytest.raises(RuntimeError, match="Byte verification"):
        recovery.main(args + ["--apply"])
    assert any(command[0] == "copyto" and "--ignore-existing" in command for command in commands)
    assert before == [p.read_bytes() for p in (active, active.with_suffix(".csv"), report)]


def test_verified_upload_updates_both_inventories_and_report(chart, monkeypatch):
    args, active, report = chart
    monkeypatch.setattr(recovery, "rclone", lambda *command: b"pdf data" if command[0] == "cat" else b"")
    recovery.main(args + ["--apply"])
    assert yaml.safe_load(active.read_text(encoding="utf-8")) == {"Alto Sax II": ["22 - Another Song"]}
    assert "534 -" not in active.with_suffix(".csv").read_text(encoding="utf-8-sig")
    record = next(csv.DictReader(io.StringIO(report.read_text(encoding="utf-8-sig"))))
    assert record["Dropbox status"] == "Recovered version uploaded and verified"
    assert record["Dropbox paths"] == "Alto Sax II/Alto Sax II - 534 You Made Me Love You.pdf"


def test_conflicting_existing_part_is_not_uploaded(chart, monkeypatch, capsys):
    args, active, _ = chart
    before = active.read_bytes()
    monkeypatch.setattr(recovery, "rclone", lambda *command: (
        b"Alto Sax II/Alto Sax II - 00534 You Made Me Love You.pdf\n"
        if command[0] == "lsf" else pytest.fail("Unexpected remote operation")
    ))
    recovery.main(args + ["--apply"])
    assert "SKIP" in capsys.readouterr().out
    assert active.read_bytes() == before


def test_resume_identical_existing_file_without_copy(chart, monkeypatch):
    args, active, _ = chart
    name = "Alto Sax II/Alto Sax II - 534 You Made Me Love You.pdf"
    def fake(*command):
        if command[0] == "lsf":
            return (name + "\n").encode()
        if command[0] == "cat":
            return b"pdf data"
        pytest.fail("Existing destination must not be copied over")
    monkeypatch.setattr(recovery, "rclone", fake)
    recovery.main(args + ["--apply", "--resume-existing"])
    assert yaml.safe_load(active.read_text()) == {"Alto Sax II": ["22 - Another Song"]}


def test_inconsistent_inventory_is_rejected_before_upload(chart, monkeypatch):
    args, active, _ = chart
    active.with_suffix(".csv").write_text("Part,Missing Songs\nAlto Sax II,22 - Another Song\n", encoding="utf-8-sig")
    monkeypatch.setattr(recovery, "rclone", lambda *command: b"" if command[0] == "lsf" else pytest.fail("Unexpected upload"))
    with pytest.raises(ValueError, match="do not match"):
        recovery.main(args + ["--apply"])


def test_unrelated_alt_number_does_not_conflict():
    assert recovery.possible_conflicts(
        ["Alto Sax II/Alto Sax II - 534a Other Song.pdf"], "Alto Sax II", "534", "You Made Me Love You"
    ) == []


def test_trombone_two_does_not_block_trombone_one():
    assert recovery.possible_conflicts(
        ["Trombone II/Trombone II - 534 You Made Me Love You.pdf"],
        "Trombone I", "534", "You Made Me Love You",
    ) == []


def test_removing_last_entry_keeps_an_empty_list(chart, monkeypatch):
    args, active, _ = chart
    active.write_text('Alto Sax II:\n  - "534 - You Made Me Love You"\n', encoding="utf-8")
    active.with_suffix(".csv").write_text(
        "Part,Missing Songs\nAlto Sax II,534 - You Made Me Love You\n", encoding="utf-8-sig"
    )
    monkeypatch.setattr(recovery, "rclone", lambda *command: b"pdf data" if command[0] == "cat" else b"")
    recovery.main(args + ["--apply"])
    assert yaml.safe_load(active.read_text()) == {"Alto Sax II": []}
