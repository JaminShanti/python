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
from concurrent.futures import ThreadPoolExecutor

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
        self.reviews_df = pd.DataFrame()
        
        # Create output directories
        os.makedirs('reviews_csv', exist_ok=True)
        os.makedirs('rt_review_img', exist_ok=True)

    def get_initial_metadata(self):
        url = f"{self.base_url}/{self.show_name}/reviews?type=user"
        logger.info(f"Fetching initial metadata from: {url}")
        
        try:
            response = requests.get(url, timeout=15)
            response.raise_for_status()
            page_content = response.text
            
            context_key = 'root.RottenTomatoes.context.movieReview' if self.is_movie else 'root.RottenTomatoes.context.seasonReviews'
            match = re.search(r'%s = (.*?);' % context_key, page_content)
            
            if match:
                data = json.loads(match.group(1))
                self.show_id = data['movieId'] if self.is_movie else data['emsId']
                self.napi_type = 'movie' if self.is_movie else f'tv/{self.show_id}/season'
                self.initial_cursor = data['pageInfo']['endCursor']
                return True
                    
            logger.error("Could not find metadata in page content.")
            return False
        except Exception as e:
            logger.error(f"Error fetching metadata: {e}")
            return False

    def fetch_reviews(self):
        if not hasattr(self, 'show_id') and not self.get_initial_metadata():
            return pd.DataFrame()

        reviews_list = []
        cursor = self.initial_cursor
        has_next_page = True
        
        logger.info(f"Starting review fetch for ID: {self.show_id}")

        # To increase network usage, you'd typically use a pool, but RT's pagination 
        # is sequential (requires the cursor from the previous page).
        # We'll use a larger session and clear data to manage RAM.
        session = requests.Session()

        while has_next_page:
            api_url = f"{self.base_url}/napi/{self.napi_type}/{self.show_id}/reviews/user"
            params = {'direction': 'next', 'endCursor': cursor}
            
            try:
                response = session.get(api_url, params=params, timeout=15)
                if response.status_code != 200:
                    break
                    
                data = response.json()
                page_info = data.get('pageInfo', {})
                batch = data.get('reviews', [])
                
                if not batch:
                    break
                
                # FIXED: Correctly appending batch to list without duplication
                for r in batch:
                    r['source_url'] = response.url
                reviews_list.extend(batch)
                
                logger.info(f"Fetched {len(batch)} reviews (Total: {len(reviews_list)}). Next cursor: {page_info.get('endCursor')}")
                
                has_next_page = page_info.get('hasNextPage', False)
                cursor = page_info.get('endCursor')
                
                # Brief sleep to avoid rate limiting
                time.sleep(0.1)
                
            except Exception as e:
                logger.error(f"Error fetching reviews: {e}")
                break
                
        self.reviews_df = pd.DataFrame(reviews_list)
        return self.reviews_df

    def analyze_reviews(self):
        if self.reviews_df.empty:
            return

        # Optimization: Process text in chunks or downsample if the dataset is massive
        try:
            if 'rating' in self.reviews_df.columns:
                def clean_rating(x):
                    if isinstance(x, str):
                        return float(x.replace('STAR_', '').replace('_', '.'))
                    return float(x)
                self.reviews_df['rating_val'] = self.reviews_df['rating'].apply(clean_rating)
                logger.info(f"Average Rating: {round(self.reviews_df['rating_val'].mean(), 1)}")
        except Exception as e:
            logger.error(f"Error in analysis: {e}")

        # Wordcloud can be RAM intensive for huge text blocks
        if 'review' in self.reviews_df.columns:
            logger.info("Generating word cloud...")
            text = " ".join(str(r) for r in self.reviews_df['review'].dropna())
            wordcloud = WordCloud(width=800, height=400, background_color='white').generate(text)
            
            plt.figure(figsize=(10, 5))
            plt.imshow(wordcloud, interpolation='bilinear')
            plt.axis("off")
            
            img_filename = f"rt_review_img/{self.review_name}_{datetime.now().strftime('%Y-%m-%d')}.png"
            plt.savefig(img_filename)
            plt.close()
            logger.info(f"Word cloud saved to {img_filename}")

    def run(self):
        self.fetch_reviews()
        if not self.reviews_df.empty:
            timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
            self.reviews_df.to_csv(f"reviews_csv/{self.review_name}_{timestamp}.csv", index=False)
            self.analyze_reviews()

if __name__ == "__main__":
    scraper = RottenTomatoesReviews('m/sonic_the_hedgehog_2020')
    scraper.run()
