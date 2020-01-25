import requests as rq
import json
import pandas as pd
import time
import re
from wordcloud import WordCloud, STOPWORDS
import matplotlib.pyplot as plt

show_name = 'm/dolittle'
review_types = ['root.RottenTomatoes.context.movieReview', 'root.RottenTomatoes.context.seasonReviews']
this_review_type = review_types[0]
rotten_url = "https://www.rottentomatoes.com/%s/reviews?type=user" % show_name

review_name = '%s_reviews' % show_name.replace('/','_')

date = time.strftime("%Y-%m-%d")
page = rq.get(rotten_url).text
movie_review_base = json.loads(re.findall(r"%s = (.*);" % this_review_type, page)[0])
if this_review_type == 'root.RottenTomatoes.context.movieReview':
    show_id = movie_review_base['movieId']
    napi_type = 'movie'
else:
    show_id = movie_review_base['emsId']
    napi_type = 'tv/%s/season' % show_name

csv_full_name = "%s_%s.csv" % (review_name, time.strftime("%Y%m%d-%H%M%S"))
reviews_base_url = "https://www.rottentomatoes.com/napi/%s/%s/reviews/user?direction=next&endCursor=%s&startCursor=%s" % (
    napi_type, show_id, movie_review_base['pageInfo']['endCursor'], movie_review_base['pageInfo']['startCursor'])

page_info_and_reviews = json.loads(rq.get(reviews_base_url).text)
page_info = page_info_and_reviews['pageInfo']
review_page = page_info_and_reviews['reviews']
review_page_df = pd.DataFrame(review_page)
review_page_df['review_url'] = reviews_base_url
reviews = pd.DataFrame()
reviews = reviews.append(review_page_df, ignore_index=True)

while page_info_and_reviews['pageInfo']['hasNextPage']:
    page_url = "https://www.rottentomatoes.com/napi/%s/%s/reviews/user?direction=next&endCursor=%s&startCursor=%s" % (
        napi_type, show_id, page_info['endCursor'], page_info['startCursor'])
    print("reviewing page: %s" % page_info['endCursor'])
    page_info_and_reviews = json.loads(rq.get(page_url).text)
    page_info = page_info_and_reviews['pageInfo']
    review_page = page_info_and_reviews['reviews']
    review_page_df = pd.DataFrame(review_page)
    review_page_df['review_url'] = page_url
    reviews = reviews.append(review_page_df, ignore_index=True)

reviews.to_csv(csv_full_name, encoding='utf-8')

stopwords = set(STOPWORDS)
text = " ".join(review for review in reviews.review)
wordcloud = WordCloud(width=800, height=400, stopwords=stopwords).generate(text)

plt.figure(figsize=(10, 5), facecolor='k')
plt.imshow(wordcloud)
plt.axis("off")
plt.tight_layout(pad=0)
plt.savefig("rt_review_img\%s_%s.png" % (review_name, date))
