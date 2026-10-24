import re
from youtube_transcript_api import YouTubeTranscriptApi
from youtube_transcript_api._errors import TranscriptsDisabled, NoTranscriptFound


class HypeQuoteScraper:
    """Scrapes YouTube transcripts for specific hype keywords."""

    def __init__(self, keywords):
        self.keywords = [kw.lower() for kw in keywords]

    @staticmethod
    def _extract_video_id(url_or_id):
        match = re.search(r"(?:v=|\/)([a-zA-Z0-9_-]{11})", url_or_id)
        return match.group(1) if match else url_or_id

    @staticmethod
    def _format_timestamp(seconds):
        m, s = divmod(int(seconds), 60)
        h, m = divmod(m, 60)
        return f"{h:02d}:{m:02d}:{s:02d}"

    def scrape_youtube_videos(self, video_urls, output_filename="hype_quotes.txt"):
        extracted_quotes = []

        # v1.2+ Syntax: Instantiate the API object here
        ytt_api = YouTubeTranscriptApi()

        for vid in video_urls:
            video_id = self._extract_video_id(vid)
            print(f"Processing Video ID: {video_id}...")

            try:
                # Fetch the transcript and convert it to raw dictionary data
                fetched = ytt_api.fetch(video_id)
                transcript = fetched.to_raw_data()

                for entry in transcript:
                    text = entry['text'].replace('\n', ' ')
                    text_lower = text.lower()

                    matched_words = [kw for kw in self.keywords if kw in text_lower]
                    if matched_words:
                        timestamp = self._format_timestamp(entry['start'])
                        quote_entry = f"[{timestamp}] (Matches: {', '.join(matched_words)}) \"{text.strip()}\""
                        extracted_quotes.append(quote_entry)

            except TranscriptsDisabled:
                print(f"Skipping {video_id}: Transcripts are disabled for this video.")
            except NoTranscriptFound:
                print(f"Skipping {video_id}: No English transcript found.")
            except Exception as e:
                print(f"Error processing {video_id}: {e}")

        self._save_results(extracted_quotes, output_filename)
        return extracted_quotes

    def _save_results(self, quotes, filename):
        with open(filename, "w", encoding="utf-8") as f:
            f.write("\n".join(quotes))
        print(f"\nDone! Extracted {len(quotes)} hype quotes to '{filename}'.")


if __name__ == "__main__":
    URLS_TO_SCRAPE = [
        "https://www.youtube.com/watch?v=Y54jNWQN8sg",
        "https://www.youtube.com/watch?v=pfcwU9YGTpY",
        "https://www.youtube.com/watch?v=Jeu27vBvWDE",
        "https://www.youtube.com/watch?v=35bbUpwVuQk",
        "https://www.youtube.com/watch?v=kfLLfZ2WDXg",
        "https://www.youtube.com/watch?v=rRGwwbK3Tlo",
        "https://www.youtube.com/watch?v=CPbT3pWmgMM",
        "https://www.youtube.com/watch?v=NZDvbkjDpqA",
        "https://www.youtube.com/watch?v=FBwOoMKVHmE",
        "https://www.youtube.com/watch?v=5bL7T2DZu00"
    ]

    AMBASSADOR_BUZZWORDS = [
        "busted", "insane", "staple", "format defining", "game changer",
        "strictly better", "unreal", "value", "home run", "flavor win",
        "broken", "must play", "power level", "absolute powerhouse",
        "chef's kiss", "incredible", "overpowered", "instant slam"
    ]

    scraper = HypeQuoteScraper(keywords=AMBASSADOR_BUZZWORDS)
    scraper.scrape_youtube_videos(video_urls=URLS_TO_SCRAPE, output_filename="mtg_influencer_hype_quotes.txt")