import requests
import json
import pandas as pd
import time
import re
import os
import logging
from wordcloud import WordCloud, STOPWORDS
import matplotlib.pyplot as plt
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class RottenTomatoesReviews:
    """
    A class to scrape and analyze user reviews from Rotten Tomatoes.
    """
    
    def __init__(self, show_name, is_movie=True):
        self.show_name = show_name
        self.is_movie = is_movie
        self.base_url = "https://www.rottentomatoes.com"
        self.review_name = self.show_name.replace('/', '_')
        
        # Create output directories if they don't exist
        os.makedirs('reviews_csv', exist_ok=True)
        os.makedirs('rt_review_img', exist_ok=True)

    def get_initial_metadata(self):
        """
        Fetches the initial page to extract movie/show ID and pagination cursors.
        """
        url = f"{self.base_url}/{self.show_name}/reviews?type=user"
        logger.info(f"Fetching initial metadata from: {url}")
        
        try:
            response = requests.get(url)
            response.raise_for_status()
            page_content = response.text
            
            if self.is_movie:
                context_key = 'root.RottenTomatoes.context.movieReview'
                match = re.search(r'%s = (.*?);' % context_key, page_content)
                if match:
                    data = json.loads(match.group(1))
                    self.show_id = data['movieId']
                    self.napi_type = 'movie'
                    self.initial_cursor = data['pageInfo']['endCursor']
                    return True
            else:
                # TV Show logic (season reviews)
                context_key = 'root.RottenTomatoes.context.seasonReviews'
                match = re.search(r'%s = (.*?);' % context_key, page_content)
                if match:
                    data = json.loads(match.group(1))
                    self.show_id = data['emsId']
                    self.napi_type = f'tv/{self.show_id}/season' # Note: URL structure might vary for TV
                    self.initial_cursor = data['pageInfo']['endCursor']
                    return True
                    
            logger.error("Could not find metadata in page content.")
            return False
            
        except Exception as e:
            logger.error(f"Error fetching metadata: {e}")
            return False

    def fetch_reviews(self):
        """
        Iterates through paginated API to fetch all reviews.
        """
        if not hasattr(self, 'show_id'):
            if not self.get_initial_metadata():
                return pd.DataFrame()

        reviews_list = []
        cursor = self.initial_cursor
        has_next_page = True
        
        logger.info(f"Starting review fetch for ID: {self.show_id}")

        while has_next_page:
            # Construct API URL
            # Note: The API structure seems to rely on endCursor for the next page
            api_url = f"{self.base_url}/napi/{self.napi_type}/{self.show_id}/reviews/user"
            params = {
                'direction': 'next',
                'endCursor': cursor
            }
            
            try:
                response = requests.get(api_url, params=params)
                if response.status_code != 200:
                    logger.warning(f"Failed to fetch page with cursor {cursor}. Status: {response.status_code}")
                    break
                    
                data = response.json()
                page_info = data.get('pageInfo', {})
                reviews = data.get('reviews', [])
                
                if not reviews:
                    break
                
                for review in reviews:
                    review['source_url'] = response.url
                    reviews_list.extend(reviews)
                
                logger.info(f"Fetched {len(reviews)} reviews. Next cursor: {page_info.get('endCursor')}")
                
                has_next_page = page_info.get('hasNextPage', False)
                cursor = page_info.get('endCursor')
                
                # Be nice to the server
                time.sleep(0.5)
                
            except Exception as e:
                logger.error(f"Error fetching reviews: {e}")
                break
                
        self.reviews_df = pd.DataFrame(reviews_list)
        return self.reviews_df

    def save_reviews_csv(self):
        """
        Saves the fetched reviews to a CSV file.
        """
        if self.reviews_df.empty:
            logger.warning("No reviews to save.")
            return

        timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
        filename = f"reviews_csv/{self.review_name}_{timestamp}.csv"
        
        try:
            self.reviews_df.to_csv(filename, index=False, encoding='utf-8')
            logger.info(f"Reviews saved to {filename}")
        except Exception as e:
            logger.error(f"Failed to save CSV: {e}")

    def analyze_reviews(self):
        """
        Performs basic analysis: average rating and word cloud.
        """
        if self.reviews_df.empty:
            logger.warning("No reviews to analyze.")
            return

        # Clean and convert ratings
        # Assuming rating comes in format like 'STAR_5_0' or similar, or just numbers
        # The original script had: x.lstrip('STAR_').replace('_','.')
        try:
            # Check if 'rating' column exists and is string
            if 'rating' in self.reviews_df.columns:
                 # Handle different potential formats safely
                def clean_rating(x):
                    if isinstance(x, str):
                        return float(x.replace('STAR_', '').replace('_', '.'))
                    return float(x)

                self.reviews_df['rating_val'] = self.reviews_df['rating'].apply(clean_rating)
                average_review = round(self.reviews_df['rating_val'].mean(), 1)
                logger.info(f"The Average review for {self.show_name} is: {average_review}")
            else:
                logger.warning("Rating column not found.")

        except Exception as e:
            logger.error(f"Error calculating average rating: {e}")

        # Generate Word Cloud
        try:
            if 'review' in self.reviews_df.columns:
                text = " ".join(str(review) for review in self.reviews_df['review'].dropna())
                stopwords = set(STOPWORDS)
                
                wordcloud = WordCloud(width=800, height=400, stopwords=stopwords, background_color='white').generate(text)
                
                plt.figure(figsize=(10, 5), facecolor='k')
                plt.imshow(wordcloud, interpolation='bilinear')
                plt.axis("off")
                plt.tight_layout(pad=0)
                
                date_str = datetime.now().strftime("%Y-%m-%d")
                img_filename = f"rt_review_img/{self.review_name}_{date_str}.png"
                plt.savefig(img_filename)
                plt.close()
                
                logger.info(f"Word cloud saved to {img_filename}")
            else:
                logger.warning("Review text column not found for word cloud.")
                
        except Exception as e:
            logger.error(f"Error generating word cloud: {e}")

    def run(self):
        """
        Executes the full scraping and analysis pipeline.
        """
        self.fetch_reviews()
        self.save_reviews_csv()
        self.analyze_reviews()

if __name__ == "__main__":
    # Example usage
    # For a movie: 'm/sonic_the_hedgehog_2020'
    # For a TV show, you might need to adjust the is_movie flag and URL logic
    
    target_show = 'm/sonic_the_hedgehog_2020'
    scraper = RottenTomatoesReviews(target_show, is_movie=True)
    scraper.run()
