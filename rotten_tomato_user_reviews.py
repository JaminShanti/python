import logging
from pathlib import Path
from typing import List

from playwright.sync_api import sync_playwright, Page
from wordcloud import WordCloud
import matplotlib.pyplot as plt


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class RottenTomatoesReviewScraper:
    BASE_URL = "https://www.rottentomatoes.com/m"
    REVIEWS_ENDPOINT = "reviews/all-audience"
    MIN_REVIEW_LENGTH = 15
    MAX_REVIEW_LENGTH = 5000
    LOAD_MORE_SELECTOR = 'rt-button[data-pagemediareviewsmanager="loadMoreBtn:click"]'
    REVIEW_CARD_SELECTOR = 'review-card'
    REVIEW_TEXT_SELECTOR = 'drawer-more span[slot="content"]'

    def __init__(self, headless: bool = True, debug: bool = False):
        self.headless = headless
        if debug:
            logger.setLevel(logging.DEBUG)
        logger.info(f"Initialized scraper (headless={headless}, debug={debug})")

    def fetch_reviews(self, movie_slug: str, max_reviews: int = 200) -> List[str]:
        logger.info(f"Starting review fetch for: {movie_slug} (target: {max_reviews} reviews)")
        reviews = set()
        url = f"{self.BASE_URL}/{movie_slug}/{self.REVIEWS_ENDPOINT}"

        try:
            with sync_playwright() as p:
                browser = p.chromium.launch(headless=self.headless)
                page = browser.new_page()
                page.goto(url)
                page.wait_for_load_state('networkidle')
                page.wait_for_timeout(2000)
                logger.info("Page loaded successfully")

                load_count = 0
                max_loads = 30
                no_new_reviews_count = 0

                while len(reviews) < max_reviews and load_count < max_loads:
                    reviews_before = len(reviews)
                    reviews.update(self._extract_reviews_from_page(page))
                    reviews_after = len(reviews)

                    logger.info(f"Load iteration {load_count + 1}: Found {reviews_after} total reviews (+{reviews_after - reviews_before} new)")

                    if reviews_after >= max_reviews:
                        logger.info(f"Reached target review count ({reviews_after} >= {max_reviews})")
                        break

                    if reviews_after == reviews_before:
                        no_new_reviews_count += 1
                        if no_new_reviews_count >= 3:
                            logger.info("No new reviews found for 3 consecutive iterations. Stopping.")
                            break
                    else:
                        no_new_reviews_count = 0

                    if not self._click_load_more(page):
                        logger.info("No more reviews available or 'Load More' button not found")
                        break
                    load_count += 1

                browser.close()
                logger.info(f"Browser closed. Total reviews collected: {len(reviews)}")

        except Exception as e:
            logger.error(f"Error during review fetching: {e}", exc_info=True)

        review_list = list(reviews)[:max_reviews]
        logger.info(f"Returning {len(review_list)} reviews")
        return review_list

    def _extract_reviews_from_page(self, page: Page) -> List[str]:
        reviews = []
        review_cards = page.query_selector_all(self.REVIEW_CARD_SELECTOR)
        logger.debug(f"Found {len(review_cards)} review card elements")

        for idx, card in enumerate(review_cards):
            try:
                span = card.query_selector(self.REVIEW_TEXT_SELECTOR)
                if span:
                    text = span.inner_text().strip()
                    if self._is_valid_review(text):
                        reviews.append(text)
                        logger.debug(f"Card {idx}: Extracted review ({len(text)} chars)")
                    else:
                        logger.debug(f"Card {idx}: Skipped (invalid length: {len(text)} chars)")
            except Exception as e:
                logger.debug(f"Card {idx}: Error extracting text - {e}")
        return reviews

    def _is_valid_review(self, text: str) -> bool:
        return (text and self.MIN_REVIEW_LENGTH < len(text) < self.MAX_REVIEW_LENGTH and 'Load More' not in text)

    def _click_load_more(self, page: Page) -> bool:
        try:
            load_more_btn = page.query_selector(self.LOAD_MORE_SELECTOR)
            if not load_more_btn:
                logger.debug("'Load More' button not found")
                return False

            try:
                logger.debug("Attempting JavaScript click...")
                page.evaluate(f"document.querySelector('{self.LOAD_MORE_SELECTOR}').scrollIntoView(true)")
                page.wait_for_timeout(300)
                page.evaluate(f"document.querySelector('{self.LOAD_MORE_SELECTOR}').click()")
                page.wait_for_load_state('networkidle')
                page.wait_for_timeout(1500)
                logger.debug("Successfully clicked 'Load More' button via JavaScript")
                return True
            except Exception as js_err:
                logger.debug(f"JavaScript click failed: {js_err}")
                try:
                    logger.debug("Attempting Playwright click...")
                    load_more_btn.click(timeout=2000)
                    page.wait_for_load_state('networkidle')
                    page.wait_for_timeout(1500)
                    logger.debug("Successfully clicked 'Load More' button via Playwright")
                    return True
                except Exception as click_err:
                    logger.debug(f"Playwright click also failed: {click_err}")
                    return False
        except Exception as e:
            logger.error(f"Error in _click_load_more: {e}")
            return False

    def generate_wordcloud(self, reviews: List[str], output_file: str = 'rotten_wordcloud.png', show: bool = False) -> bool:
        if not reviews:
            logger.warning("No reviews provided. Cannot generate word cloud.")
            return False

        try:
            logger.info(f"Generating word cloud from {len(reviews)} reviews")
            combined = ' '.join(reviews)
            logger.debug(f"Combined text length: {len(combined)} characters")

            # Create wordclouds directory if it doesn't exist
            wordclouds_dir = Path('wordclouds')
            wordclouds_dir.mkdir(exist_ok=True)

            # Ensure output_file is in the wordclouds directory
            if not str(output_file).startswith('wordclouds/'):
                output_file = wordclouds_dir / output_file

            wordcloud = WordCloud(width=800, height=400, background_color='white', colormap='viridis', max_words=100).generate(combined)
            plt.figure(figsize=(10, 5))
            plt.imshow(wordcloud, interpolation='bilinear')
            plt.axis('off')
            plt.title('Rotten Tomatoes User Reviews Word Cloud', fontsize=16)

            if show:
                plt.show()

            wordcloud.to_file(str(output_file))
            logger.info(f"Word cloud saved to: {output_file}")
            plt.close()
            return True
        except Exception as e:
            logger.error(f"Error generating word cloud: {e}", exc_info=True)
            return False


def main():
    movie_slug = 'sonic_the_hedgehog_2020'
    scraper = RottenTomatoesReviewScraper(headless=True, debug=False)
    logger.info(f"Scraping Rotten Tomatoes reviews for: {movie_slug}")
    reviews = scraper.fetch_reviews(movie_slug, max_reviews=150)
    logger.info(f"Successfully retrieved {len(reviews)} reviews")

    if reviews:
        output_file = f'{movie_slug}_wordcloud.png'
        scraper.generate_wordcloud(reviews, output_file=output_file)
        logger.info("Word cloud generation completed")
    else:
        logger.warning("No reviews were extracted. The website structure may have changed.")


if __name__ == '__main__':
    main()
