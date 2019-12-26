from bs4 import BeautifulSoup
import requests as rq
import json
import pandas as pd
import time

rotten_url = 'https://www.rottentomatoes.com/m/star_wars_the_rise_of_skywalker/reviews?type=user'

page = rq.get(rotten_url).text
soup = BeautifulSoup(page, 'lxml')

page_scripts = soup.find('script',{'type':'text/javascript'} )
#finds:
#root.RottenTomatoes.context.movieReview = {"title":"Star Wars: The Rise of Skywalker","movieId":"d7083795-3ab7-3b17-9717-bbe6401ffd79","type":"user","reviewsCount":10,"pageInfo":{"hasNextPage":true,"hasPreviousPage":false,"endCursor":"eyJyZWFsbV91c2VySWQiOiJGYW5kYW5nb184OUQzQkYzNC05OUU5LTQ3MkYtOTg3OS01MDhGMUY0NzQwMzQiLCJlbXNJZCI6ImQ3MDgzNzk1LTNhYjctM2IxNy05NzE3LWJiZTY0MDFmZmQ3OSIsImVtc0lkX2hhc1Jldmlld0lzVmlzaWJsZSI6ImQ3MDgzNzk1LTNhYjctM2IxNy05NzE3LWJiZTY0MDFmZmQ3OV9UIiwiY3JlYXRlRGF0ZSI6IjIwMTktMTItMjFUMjI6NDU6NDIuOTI1WiJ9","startCursor":null},"reviewerDefaultImg":"https:\u002F\u002Fwww.rottentomatoes.com\u002Fstatic\u002Fimages\u002Fredesign\u002Factor.default.tmb.gif","reviewerDefaultImgWidth":100};
#convert to :
json_data = '{"title":"Star Wars: The Rise of Skywalker","movieId":"d7083795-3ab7-3b17-9717-bbe6401ffd79","type":"user","reviewsCount":10,"pageInfo":{"hasNextPage":true,"hasPreviousPage":false,"endCursor":"eyJyZWFsbV91c2VySWQiOiJGYW5kYW5nb184OUQzQkYzNC05OUU5LTQ3MkYtOTg3OS01MDhGMUY0NzQwMzQiLCJlbXNJZCI6ImQ3MDgzNzk1LTNhYjctM2IxNy05NzE3LWJiZTY0MDFmZmQ3OSIsImVtc0lkX2hhc1Jldmlld0lzVmlzaWJsZSI6ImQ3MDgzNzk1LTNhYjctM2IxNy05NzE3LWJiZTY0MDFmZmQ3OV9UIiwiY3JlYXRlRGF0ZSI6IjIwMTktMTItMjFUMjI6NDU6NDIuOTI1WiJ9","startCursor":null},"reviewerDefaultImg":"https:\u002F\u002Fwww.rottentomatoes.com\u002Fstatic\u002Fimages\u002Fredesign\u002Factor.default.tmb.gif","reviewerDefaultImgWidth":100}'
movie_review_base =json.loads(json_data)
#https://www.rottentomatoes.com/napi/movie/d7083795-3ab7-3b17-9717-bbe6401ffd79/reviews/user?direction=next&endCursor=eyJyZWFsbV91c2VySWQiOiJGYW5kYW5nb19iNDlhMjI3Yi05NGZiLTRhYWQtODk5MS01OWQwMDE1ZjA4MjgiLCJlbXNJZCI6ImQ3MDgzNzk1LTNhYjctM2IxNy05NzE3LWJiZTY0MDFmZmQ3OSIsImVtc0lkX2hhc1Jldmlld0lzVmlzaWJsZSI6ImQ3MDgzNzk1LTNhYjctM2IxNy05NzE3LWJiZTY0MDFmZmQ3OV9UIiwiY3JlYXRlRGF0ZSI6IjIwMTktMTItMjFUMDU6NDI6MDAuNTU1WiJ9&startCursor=

file_name = "ros_reviews_%s.csv" % time.strftime("%Y%m%d-%H%M%S")
reviews_base_url = "https://www.rottentomatoes.com/napi/movie/%s/reviews/user?direction=next&endCursor=%s&startCursor=%s" % (movie_review_base['movieId'],movie_review_base['pageInfo']['endCursor'],movie_review_base['pageInfo']['startCursor'])

page_info_and_reviews = json.loads(rq.get(reviews_base_url).text)
page_info = page_info_and_reviews['pageInfo']
review_page = page_info_and_reviews['reviews']
review_page_df = pd.DataFrame(review_page)
review_page_df['review_url']= reviews_base_url
reviews = pd.DataFrame()
reviews = reviews.append(review_page_df, ignore_index = True)


while page_info_and_reviews['pageInfo']['hasNextPage']:
    page_url = "https://www.rottentomatoes.com/napi/movie/%s/reviews/user?direction=next&endCursor=%s&startCursor=%s" % (movie_review_base['movieId'], page_info['endCursor'], page_info['startCursor'])
    page_info_and_reviews = json.loads(rq.get(page_url).text)
    page_info = page_info_and_reviews['pageInfo']
    review_page = page_info_and_reviews['reviews']
    review_page_df = pd.DataFrame(review_page)
    review_page_df['review_url']= page_url
    reviews = reviews.append(review_page_df, ignore_index = True)


reviews.to_csv(file_name, sep='\t', encoding='utf-8')
