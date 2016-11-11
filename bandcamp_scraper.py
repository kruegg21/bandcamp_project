import pymongo
import requests
import os
import time
import random
import re
from bs4 import BeautifulSoup
from selenium import webdriver

def random_sleep():
    sleep_time = random.uniform(10,15)
    print "Going to sleep for {} seconds".format(sleep_time)
    time.sleep(sleep_time)

def get_mongo_database(db_name, collection_name):
    # Open MongoDB client
    client = pymongo.MongoClient()

    # Open database
    db = client[db_name]

    # Choose collection
    return db[collection_name]

def get_user_collection(url, driver):
    driver.get(url)

    # Counters to keep track of number of titles we have gathered
    n_after = 0
    n_previous = -999
    counter = 1
    while n_after != n_previous:
        # Scroll down chunk of page
        page_height = counter * 1500
        driver.execute_script("window.scrollTo(0, {});".format(page_height))
        time.sleep(0.2)

        # Get raw HTML
        html = driver.page_source

        # Make soup
        soup = BeautifulSoup(html)

        # Get album and artist titles
        album_dict = dict()

        tag = 'collection-item-container track_play_hilite lazy'
        classes = [tag, tag + ' ']
        for t in classes:
            for user in soup.find_all('li', {'class': t}):
                user_dict = dict()
                album_tag = user.find('div', {'class' : 'collection-item-title'})
                artist_tag = user.find('div', {'class' : 'collection-item-artist'})
                track_tag = user.find('a', {'class' : 'fav-track-link'})
                collection_tag = user.find('a', {'class' : 'item-link also-link'})
                if artist_tag:
                    user_dict['artist'] = artist_tag.text[3:]
                if track_tag:
                    user_dict['track'] = track_tag.text
                    user_dict['album_url'] = track_tag['href']
                if collection_tag:
                    t = collection_tag.text
                    user_dict['collection_count'] = ''.join(c for c in t if c.isdigit())
                if album_tag:
                    t = album_tag.text
                    if t[-13:] == '(gift given)\n':
                        t = t[:-13].strip()
                        user_dict['wishlist'] = '0'
                    else:
                        user_dict['wishlist'] = '1'
                    t = mongo_preprocess(t)
                    user_dict['album'] = t
                    album_dict[t] = user_dict

        # Update counters
        n_previous = n_after
        n_after = len(album_dict)
        counter += 1

    final_dict = {url.split('/')[-1] : album_dict}
    return final_dict

def scrape_album_metadata(html):
    # Get MongoDB collection instance
    collection = get_mongo_database('bandcamp_data', 'album_support')

    # Make soup
    soup = BeautifulSoup(html)

    album_title = soup.find('h2', {'class' : 'trackTitle'}).contents[0].strip()
    artist_name = soup.find('span', {'itemprop' : 'byArtist'}).find('a').contents[0].strip()

    print album_title, artist_name

    for user in soup.find_all('a', {'class': 'fan pic'}):
         print user['href']
    for user in soup.find_all('a', {'class' : 'pic'}):
         print user

    # # Traverse HTML
    # for entry in soup.find_all("tr"):
    #     post = {}
    #     artist = entry.find('a', {'class': 'artist'})
    #     if artist:
    #         post['artist_id'] = artist['title'].strip('[]')
    #         post['artist_name'] = artist.contents[0]
    #     album = entry.find("a", {"class": "album"})
    #     if album:
    #         post['album_id'] = album['title'].strip('[]')
    #         post['album_name'] = album.contents[0]
    #         post['year'] = payload['year']
    #         post['genre'] = payload['genres']
    #     statistics = entry.find_all('b')
    #     if statistics:
    #         post['average_rating'] = statistics[0].contents[0]
    #         post['number_of_ratings'] = statistics[1].contents[0]
    #         post['number_of_reviews'] = statistics[2].contents[0]
    #     if post:
    #         # Insert into MongoDB database
    #         result = collection.insert_one(post)
    # return soup


# def get_user_collection_request(url):
#
#     r = requests.get(url)
#     random_sleep()
#     soup = BeautifulSoup(r.content, 'html.parser')
#     print soup
#     raw_input()
#
#     album_title = soup.find('h2', {'class' : 'trackTitle'}).contents[0].strip()
#     artist_name = soup.find('span', {'itemprop' : 'byArtist'}).find('a').contents[0].strip()
#
#     print album_title, artist_name
#
#     for user in soup.find_all('a', {'class': 'fan pic'}):
#          print user['href']
#     for user in soup.find_all('a', {'class' : 'pic'}):
#          print user

def mongo_preprocess(s):
    """
    MongoDB requires keys to not have periods in them. This function replace
    '.' with '_' to make MongoDB happy
    """
    return s.replace('.', ',')

if __name__ == "__main__":
    # Root URL
    url = 'https://jeffrosenstock.bandcamp.com/'

    # Web driver
    driver = webdriver.Chrome(os.getcwd() + '/chromedriver')

    # Get Mongo collection to dump things into
    mongo_collection = get_mongo_database('bandcamp', 'user_collections')

    # Get collectino for user
    collection_dictionary = get_user_collection('https://bandcamp.com/devonpotrie', driver)

    # Dump user collection into Mongo
    mongo_collection.insert(collection_dictionary)
