from magic_game_scripts import mtg_dip_detector as module

def test_imports():
    assert module is not None


def test_default_directories_are_script_relative(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(module.os, "makedirs", lambda *args, **kwargs: None)
    detector = module.MTGDipDetector()
    base = module.Path(module.__file__).resolve().parent
    assert module.Path(detector.cache_dir) == base / "mtg_cache"
    assert module.Path(detector.output_dir) == base / "mtg_dip_output"


def test_explicit_directories_remain_cwd_relative(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    detector = module.MTGDipDetector(cache_dir="custom-cache", output_dir="custom-output")
    assert module.Path(detector.cache_dir) == tmp_path / "custom-cache"
    assert module.Path(detector.output_dir) == tmp_path / "custom-output"
