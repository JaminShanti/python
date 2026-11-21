from types import SimpleNamespace

import pytest
import yaml

from music_sheet_scripts import chart_downloader as downloader


def config(tmp_path, **overrides):
    data = {"target_domains": ["example.com"], "input_file": str(tmp_path / "queries.txt"),
            "download_folder": str(tmp_path / "downloads")}
    data.update(overrides)
    path = tmp_path / "config.yaml"
    path.write_text(yaml.safe_dump(data), encoding="utf-8")
    return downloader.load_config(path)


def test_config_accepts_multiple_domains_and_relative_paths(tmp_path):
    cfg = config(tmp_path, target_domains=["EXAMPLE.COM", "another.org"], input_file="queries.txt")
    assert cfg.target_domains == ("example.com", "another.org")
    assert cfg.input_file == downloader.BASE_DIR / "queries.txt"


def test_sample_config_is_valid_and_generic():
    sample = downloader.BASE_DIR / "music_cache" / "chart_downloader.yaml.sample"
    cfg = downloader.load_config(sample)
    assert cfg.target_domains == ("example.com",)
    assert not cfg.force_https and not cfg.plus_as_space


@pytest.mark.parametrize("domains", [[], "example.com", ["https://example.com"], ["example.com/path"],
                                      ["example.com", "EXAMPLE.COM"], ["example.com:443"]])
def test_rejects_invalid_domains(tmp_path, domains):
    with pytest.raises(ValueError):
        config(tmp_path, target_domains=domains)


def test_aliases_must_stay_on_configured_domain(tmp_path):
    with pytest.raises(ValueError, match="within a configured target domain"):
        config(tmp_path, host_aliases={"example.com": "other.org"})


def test_generic_url_preserves_query_and_plus(tmp_path):
    cfg = config(tmp_path)
    url = "http://files.example.com/song+title.pdf?token=a%2Bb&version=2"
    assert downloader.normalize_url(url, cfg) == url


def test_optional_legacy_url_normalization(tmp_path):
    cfg = config(tmp_path, target_domains=["uhsband.com"],
                 host_aliases={"mail.uhsband.com": "uhsband.com"},
                 force_https=True, plus_as_space=True)
    assert downloader.normalize_url("http://mail.uhsband.com/A+B.pdf?x=1", cfg) == \
        "https://uhsband.com/A%20B.pdf?x=1"


class Link:
    def __init__(self, href):
        self.href = href

    def get_attribute(self, name):
        assert name == "href"
        return self.href


class FakePage:
    def __init__(self, urls):
        self.urls = urls

    def goto(self, url, **kwargs):
        self.search_url = url

    def wait_for_selector(self, *args, **kwargs):
        pass

    def locator(self, selector):
        assert selector == "a[href]"
        return SimpleNamespace(all=lambda: [Link(url) for url in self.urls])


def test_search_filters_hostname_and_unwraps_duckduckgo_links(tmp_path, monkeypatch):
    monkeypatch.setattr(downloader.time, "sleep", lambda _: None)
    cfg = config(tmp_path)
    page = FakePage([
        "https://example.com/Chart.pdf?x=1",
        "https://files.example.com/Other.pdf",
        "https://example.com.evil.org/Wrong.pdf",
        "https://notexample.com/Wrong.pdf",
        "https://duckduckgo.com/l/?uddg=https%3A%2F%2Fexample.com%2FChart.pdf%3Fx%3D1",
    ])
    results = downloader.search_ddg_playwright(page, "Song Title", "example.com", cfg)
    assert results == ["https://example.com/Chart.pdf?x=1", "https://files.example.com/Other.pdf"]
    assert "site%3Aexample.com" in page.search_url


def test_check_only_searches_all_domains_without_download(tmp_path, monkeypatch, capsys):
    cfg = config(tmp_path, target_domains=["example.com", "another.org"])
    cfg.input_file.write_text("Blue Midnight\n", encoding="utf-8")
    config_path = tmp_path / "config.yaml"
    seen = []
    monkeypatch.setattr(downloader, "search_ddg_playwright",
                        lambda page, query, domain, settings: seen.append(domain) or
                        [f"https://{domain}/Blue%20Midnight.pdf"])
    monkeypatch.setattr(downloader, "download_file", lambda *args: pytest.fail("unexpected download"))
    monkeypatch.setattr(downloader.time, "sleep", lambda _: None)
    browser = SimpleNamespace(new_context=lambda **kwargs: SimpleNamespace(new_page=lambda: object()),
                              close=lambda: None)
    playwright = SimpleNamespace(chromium=SimpleNamespace(launch=lambda **kwargs: browser))
    class Context:
        def __enter__(self):
            return playwright
        def __exit__(self, *args):
            return False
    monkeypatch.setattr(downloader, "sync_playwright", lambda: Context())
    downloader.main(["--config", str(config_path), "--check-only"])
    assert seen == ["example.com", "another.org"]
    assert not cfg.download_folder.exists()
    assert "another.org" in capsys.readouterr().out


def test_missing_config_explains_how_to_create_it(tmp_path):
    with pytest.raises(ValueError, match="Copy music_cache"):
        downloader.load_config(tmp_path / "missing.yaml")
