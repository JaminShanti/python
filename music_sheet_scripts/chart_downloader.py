import argparse
import difflib
import os
import re
import time
import urllib.parse
from dataclasses import dataclass
from pathlib import Path

import requests
import yaml
from playwright.sync_api import TimeoutError as PlaywrightTimeoutError, sync_playwright

BASE_DIR = Path(__file__).resolve().parent
DEFAULT_CONFIG = BASE_DIR / "music_cache" / "chart_downloader.yaml"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
}

STOPWORDS = {"a", "an", "the", "in", "on", "of", "for", "and", "to", "pdf", "full", "big", "band", "complete"}


@dataclass(frozen=True)
class DownloaderConfig:
    target_domains: tuple[str, ...]
    input_file: Path
    download_folder: Path
    host_aliases: dict[str, str]
    force_https: bool
    plus_as_space: bool


def validate_domain(value):
    if not isinstance(value, str) or not re.fullmatch(
        r"(?=.{1,253}$)(?:[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?\.)+"
        r"[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?", value, re.I
    ):
        raise ValueError(f"Invalid domain: {value!r}. Use a hostname without a scheme, port or path.")
    return value.lower()


def load_config(path: Path) -> DownloaderConfig:
    try:
        with path.open(encoding="utf-8") as file:
            data = yaml.safe_load(file)
    except FileNotFoundError as error:
        raise ValueError(f"Configuration not found: {path}. Copy music_cache\\chart_downloader.yaml.sample first.") from error
    if not isinstance(data, dict):
        raise ValueError("Configuration must be a YAML mapping.")
    unknown = set(data) - {"target_domains", "input_file", "download_folder", "host_aliases", "force_https", "plus_as_space"}
    if unknown:
        raise ValueError(f"Unknown configuration keys: {', '.join(sorted(unknown))}")
    domains = data.get("target_domains")
    if not isinstance(domains, list) or not domains:
        raise ValueError("target_domains must be a nonempty list of hostnames.")
    domains = tuple(validate_domain(domain) for domain in domains)
    if len(set(domains)) != len(domains):
        raise ValueError("target_domains contains duplicate domains.")
    aliases = data.get("host_aliases", {})
    if not isinstance(aliases, dict):
        raise ValueError("host_aliases must be a mapping of hostnames.")
    aliases = {validate_domain(k): validate_domain(v) for k, v in aliases.items()}
    for source, target in aliases.items():
        if not any(domain_matches(source, domain) and domain_matches(target, domain) for domain in domains):
            raise ValueError(f"Alias {source} -> {target} must remain within a configured target domain.")
    for key in ("force_https", "plus_as_space"):
        if not isinstance(data.get(key, False), bool):
            raise ValueError(f"{key} must be true or false.")
    def config_path(key):
        value = data.get(key)
        if not isinstance(value, str) or not value.strip():
            raise ValueError(f"{key} must be a nonempty path.")
        candidate = Path(value)
        return candidate if candidate.is_absolute() else BASE_DIR / candidate
    return DownloaderConfig(domains, config_path("input_file"), config_path("download_folder"),
                            aliases, data.get("force_https", False), data.get("plus_as_space", False))


def domain_matches(hostname: str, domain: str) -> bool:
    hostname = hostname.lower().rstrip(".")
    return hostname == domain or hostname.endswith("." + domain)


def sanitize_filename(name: str) -> str:
    """Removes invalid characters for Windows/Mac/Linux filenames."""
    return re.sub(r'[\\/*?:"<>|]', "_", name).strip()

def normalize_url(url: str, config: DownloaderConfig) -> str:
    """Apply explicitly configured site-specific URL adjustments."""
    parsed = urllib.parse.urlparse(url)
    host = parsed.hostname
    if parsed.scheme not in ("http", "https") or not host or parsed.username or parsed.password:
        raise ValueError(f"Invalid search result URL: {url}")
    netloc = config.host_aliases.get(host.lower(), host.lower())
    if parsed.port:
        netloc += f":{parsed.port}"
    path = parsed.path.replace("+", "%20") if config.plus_as_space else parsed.path
    path = urllib.parse.quote(path, safe="/%-_.~!$&'()*+,;=:@")
    return urllib.parse.urlunparse(("https" if config.force_https else parsed.scheme,
                                    netloc, path, parsed.params, parsed.query, parsed.fragment))

def extract_keywords(text: str) -> list[str]:
    """Extracts lowercase alphanumeric words (len >= 3) excluding common stopwords."""
    words = re.findall(r"[a-z0-9]+", text.lower())
    return [w for w in words if len(w) >= 3 and w not in STOPWORDS]

def word_match_score(q_word: str, file_words: list[str]) -> float:
    """
    Returns 1.0 for an exact word match, or 0.85 for a close spelling match
    (e.g., 'chattanooga' vs 'chatanooga').
    """
    if q_word in file_words:
        return 1.0
    for f_word in file_words:
        if difflib.SequenceMatcher(None, q_word, f_word).ratio() >= 0.85:
            return 0.85
    return 0.0

def pick_best_match(query: str, urls: list[str]) -> tuple[str | None, list[tuple[float, str]]]:
    """
    Scores candidate URLs by fuzzy keyword overlap, arranger match bonus,
    and score completeness, returning the single highest-scoring URL.
    """
    query_words = extract_keywords(query)
    if not query_words:
        return (urls[0], [(1.0, u) for u in urls]) if urls else (None, [])

    # Deduplicate query words while preserving order; the last word is usually the arranger's surname
    unique_q_words = list(dict.fromkeys(query_words))
    arranger_word = unique_q_words[-1] if len(unique_q_words) >= 2 else None

    scored: list[tuple[float, str]] = []

    for url in urls:
        filename = os.path.basename(urllib.parse.unquote(urllib.parse.urlparse(url).path))
        stem = os.path.splitext(filename)[0]
        file_words = extract_keywords(stem)
        lower_stem = stem.lower()

        # 1. Base score: sum of exact (1.0) and fuzzy (0.85) keyword matches
        base_score = sum(word_match_score(qw, file_words) for qw in unique_q_words)

        # Require at least 2 matching keywords (or 1 if the query only had 1 keyword)
        min_required = 1.7 if len(unique_q_words) >= 2 else 0.85
        if base_score < min_required:
            continue

        bonus = 0.0

        # 2. Arranger surname bonus (heavily favors matching the specific arranger)
        if arranger_word and word_match_score(arranger_word, file_words) > 0:
            bonus += 1.5

        # 3. Prefer fuller/complete arrangements over bare lead sheets or alternate page sizes
        if any(tag in lower_stem for tag in ("full", "complete", "score")):
            bonus += 0.3
        if "vocal" in lower_stem:
            bonus += 0.1
        if "8 x 11" in lower_stem or "8x11" in lower_stem:
            bonus -= 0.2

        # 4. Slight preference for cleaner/shorter filenames when everything else is equal
        length_penalty = len(stem) * 0.001

        total_score = round(base_score + bonus - length_penalty, 4)
        scored.append((total_score, url))

    if not scored:
        return None, []

    scored.sort(key=lambda item: item[0], reverse=True)
    return scored[0][1], scored

def download_file(url: str, fallback_name: str, folder: Path, config: DownloaderConfig):
    """Downloads the matched URL into the target folder."""
    fixed_url = normalize_url(url, config)
    urls_to_try = [fixed_url] if fixed_url == url else [fixed_url, url]

    response = None
    final_url = url

    for candidate_url in urls_to_try:
        try:
            r = requests.get(candidate_url, headers=HEADERS, stream=True, timeout=15)
            r.raise_for_status()
            response = r
            final_url = candidate_url
            break
        except requests.exceptions.HTTPError as e:
            if candidate_url == urls_to_try[-1]:
                print(f"  [ERROR] Failed to download {candidate_url}: {e}")
                return
        except Exception as e:
            print(f"  [ERROR] Failed to download {candidate_url}: {e}")
            return

    parsed_path = urllib.parse.urlparse(final_url).path
    filename = os.path.basename(urllib.parse.unquote(parsed_path))

    if not filename or "." not in filename:
        content_type = response.headers.get("Content-Type", "")
        ext = ".html" if "text/html" in content_type else ""
        filename = sanitize_filename(fallback_name) + ext
    else:
        filename = sanitize_filename(filename)

    filepath = os.path.join(folder, filename)

    base, extension = os.path.splitext(filepath)
    counter = 1
    while os.path.exists(filepath):
        filepath = f"{base}_{counter}{extension}"
        counter += 1

    with open(filepath, "wb") as f:
        for chunk in response.iter_content(chunk_size=8192):
            f.write(chunk)

    print(f"  [SAVED] -> {filepath}")

def search_ddg_playwright(page, query: str, domain: str, config: DownloaderConfig) -> list[str]:
    """Search DuckDuckGo for links on the configured domain or its subdomains."""
    encoded_q = urllib.parse.quote_plus(f"{query} site:{domain}")
    search_url = f"https://duckduckgo.com/?q={encoded_q}&ia=web"

    page.goto(search_url, wait_until="domcontentloaded", timeout=30000)

    try:
        page.wait_for_selector("a[href]", timeout=5000)
    except PlaywrightTimeoutError:
        pass
    time.sleep(1.0)

    links = page.locator("a[href]").all()
    results = []

    for link in links:
        href = link.get_attribute("href") or ""
        parsed = urllib.parse.urlparse(urllib.parse.urljoin("https://duckduckgo.com", href))
        if domain_matches(parsed.hostname or "", "duckduckgo.com") and parsed.path == "/l/":
            href = urllib.parse.parse_qs(parsed.query).get("uddg", [""])[0]
            parsed = urllib.parse.urlparse(href)
        if (parsed.scheme in ("http", "https") and parsed.hostname
                and not parsed.username and not parsed.password and domain_matches(parsed.hostname, domain)):
            clean_url = normalize_url(href, config)
            if clean_url not in results:
                results.append(clean_url)

    return results

def main(argv=None):
    parser = argparse.ArgumentParser(description="Find candidate charts on configured domains.")
    parser.add_argument("--config", type=Path, default=DEFAULT_CONFIG, help="Downloader YAML configuration")
    parser.add_argument("--check-only", action="store_true", help="List matches without downloading")
    args = parser.parse_args(argv)
    try:
        config = load_config(args.config)
    except (ValueError, yaml.YAMLError) as error:
        parser.error(str(error))
    with config.input_file.open("r", encoding="utf-8-sig") as f:
        queries = [line.strip() for line in f if line.strip()]
    if not args.check_only:
        config.download_folder.mkdir(parents=True, exist_ok=True)

    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=False,
            args=["--disable-blink-features=AutomationControlled"]
        )
        context = browser.new_context(user_agent=HEADERS["User-Agent"])
        page = context.new_page()

        for query in queries:
            for domain in config.target_domains:
                print(f"Searching {domain}: {query}")

                try:
                    raw_matches = search_ddg_playwright(page, query, domain, config)
                    best_url, ranked_matches = pick_best_match(query, raw_matches)
                except Exception as e:
                    print(f"  [SEARCH ERROR] {e}")
                    time.sleep(3)
                    continue

                if best_url:
                    if len(ranked_matches) > 1:
                        print(f"  [BEST OF {len(ranked_matches)}] Selected top match: {best_url}")
                        for score, url in ranked_matches[1:]:
                            print(f"         (runner-up, score={score}): {url}")
                    else:
                        print(f"  [MATCH] Found 1 result: {best_url}")

                    if not args.check_only:
                        download_file(best_url, query, config.download_folder, config)
                else:
                    if raw_matches:
                        print(f"  [SKIP] {len(raw_matches)} result(s) found on {domain}, but none matched the title/arranger.")
                    else:
                        print(f"  [SKIP] 0 matches on {domain} found.")

                time.sleep(2.5)

        browser.close()

if __name__ == "__main__":
    main()