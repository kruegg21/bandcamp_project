#!/bin/sh
import pymongo
import requests
import os
import time
import pprint
import random
import json
import demjson
import re
import pandas as pd
import simplejson
from bs4 import BeautifulSoup
from HTMLParser import HTMLParser
from selenium import webdriver
from multiprocessing import Pool
from helper import mongo_key_formatting

def random_sleep():
    sleep_time = random.uniform(10,15)
    print "Going to sleep for {} seconds".format(sleep_time)
    time.sleep(sleep_time)

def get_mongo_database(db_name):
    # Open MongoDB client
    client = pymongo.MongoClient()

    # Open database
    return client[db_name]

def clean_html(s):
    return [c for c in s if c not in "\"<>\\/"]

def click_through_more_button(driver, class_):
    # Make soup


    # Click 'more' button until we reach bottom of user list
    while True:
        # Check if 'more' button exists
        try:
            button = driver.find_element_by_class_name(class_)
        except:
            break

        # Click 'more' button
        if button.is_displayed():
            button.click()
        else:
            break

        # Sleep to allow time for data to load
        time.sleep(5)

    return driver

def add_entry_to_user_collection(user, album_dict, album_list):
    # Get all tags
    album_tag = user.find('div', {'class' : 'collection-item-title'})
    album_url_tag = user.find('a', {'class': 'item-link'})
    artist_tag = user.find('div', {'class' : 'collection-item-artist'})
    track_tag = user.find('a', {'class' : 'fav-track-link'})
    text_tag = user.find('span', {'class' : 'text'})
    collection_tag = user.find('a', {'class' : 'item-link also-link'})

    # Extract what we want
    if track_tag:
        track = track_tag.text
    else:
        track = 'None'

    if collection_tag:
        t = collection_tag.text
        collection_count = ''.join(c for c in t if c.isdigit())
    else:
        collection_count = 'None'
    if text_tag:
        text = text_tag.text
    else:
        text = 'None'
    album = album_tag.text
    if album[-13:] == '(gift given)\n':
        album = album[:-13].strip()
        wishlist = '0'
    else:
        wishlist = '1'
    album_url = album_url_tag['href']
    album_list.append(album_url)
    artist = artist_tag.text[3:]

    user_dict = dict()
    user_dict['album_url'] = album_url
    user_dict['artist'] = artist
    user_dict['track'] = track
    user_dict['collection_count'] = collection_count
    user_dict['text'] = text
    user_dict['wishlist'] = wishlist
    user_dict['album'] = album

    album_dict[mongo_key_formatting(album_url)] = user_dict
    return album_dict, album_list

def get_user_collection(url, driver, db):
    # Search URL
    r = requests.get(url)
    html = r.text

    # Get album and artist titles
    album_dict = dict()
    album_list = list()

    # Make soup
    soup = BeautifulSoup(html, 'lxml')

    JSON = re.compile('var CollectionData = ({.*?});', re.DOTALL)
    matches = JSON.search(html)

    if matches:
        try:
            for key, value in demjson.decode(matches.group(1))['item_details'].iteritems():
                album_url = value['item_url']
                album_list.append(album_url)
                album_dict[mongo_key_formatting(album_url)] = value

            # Dump to MongoDB
            final_dict = {'_id': mongo_key_formatting(url),
                          'data': simplejson.dumps(album_dict)}
            if not db.user_collections_new.find_one({"_id": mongo_key_formatting(url)}):
                db.user_collections_new.insert(final_dict)

            # Error Check
            print "User name: {}".format(url)
            print "Number of albums in list: {}".format(len(album_dict))
        except:
            pass
    return album_list

def get_album_data(url, driver, db, click_through = True):
    # Search URL
    driver.get(url)

    if click_through:
        # Click to bottom of writing button
        driver = click_through_more_button(driver, 'more-writing')

        # Click to bottom of 'more' button
        driver = click_through_more_button(driver, 'more-thumbs')

    # Get raw HTML
    html = driver.page_source

    # Make soup
    soup = BeautifulSoup(html, 'lxml')


    if click_through:
        # Get list of user URLs
        user_urls = list()
        for user in soup.find_all('a', {'class': 'pic'}):
            user_urls.append(user['href'][:-15])

    # Get link to album artwork
    album_artwork_tag = soup.find('a', {'class': 'popupImage'})
    if album_artwork_tag:
        album_artwork_url = album_artwork_tag['href']
    else:
        album_artwork_url = None

    # Get album tags
    album_tags = [tag['href'] for tag in soup.find_all('a', {'class': 'tag'})]

    # Get album description
    album_description_tag = soup.find('div', {'class': 'tralbumData tralbum-about'})
    if album_description_tag:
        album_description = album_description_tag.text
    else:
        album_description = None

    # Get album credits
    album_credits_tag = soup.find('div', {'class': 'tralbumData tralbum-credits'})
    if album_credits_tag:
        album_credits = album_credits_tag.text
    else:
        album_credits = None

    # Get purchasing information
    purchasing_info = dict()
    price_tag = soup.find('span', {'class': 'base-text-color'})
    currency_tag = soup.find('span', {'class': 'buyItemExtra secondaryText'})
    if price_tag:
        price = ''.join([c for c in price_tag.text if c.isdigit() or c in '.'])
    else:
        price = 'Name your price'
    if currency_tag:
        currency = currency_tag.text
    else:
        currency = 'None'

    # Album data
    album_data = dict()
    album_data['user_urls'] = user_urls
    album_data['name'] = url
    album_data['album_artwork_url'] = album_artwork_url
    album_data['album_tags'] = album_tags
    album_data['album_description'] = album_description
    album_data['album_credits'] = album_credits
    album_data['price'] = price
    album_data['currency'] = currency

    # Dump to MongoDB
    final_dict = {'_id': mongo_key_formatting(url),
                  'album_data' : simplejson.dumps(album_data)}
    if not db.albums.find_one({"_id": mongo_key_formatting(url)}) and user_urls:
        db.albums.insert(final_dict)

        # Error checking
        print "Number of users supporting: {}".format(len(user_urls))
        print "Link to album artwork: {}".format(album_artwork_url)
        print "Album tags: {}".format([translate_url_to_tag(url) for url in album_tags])
        print "Price: {}".format(price)
        print "Currency: {}\n\n\n".format(currency)

    return user_urls

def check_for_key(collection, key):
    """
    Input:
        db -- MongoDB database
        collection -- collection with db
        key -- string of key we are checking for
    Output:
        Bool indicating if key exists in collection
    """
    return collection.find({key : {'$exists': True}}).limit(1)

def reverse_mongo_key_formatting(s):
    return s.replace('_', '.')

def translate_url_to_tag(url):
    return url.split('/')[-1]

def crawler(roots):
    while True:
        try:
            # Unpack tuple
            root_album = roots[0]
            root_user = roots[1]

            # Web driver
            driver = webdriver.Chrome(os.getcwd() + '/drivers/chromedriver_mac64')

            # Get Mongo database to dump things into
            db = get_mongo_database('bandcamp')

            # Global set of user and album URLs to iterate through
            user_urls = set()
            album_urls = set()

            # Get user URLs from root
            new_album_urls = get_user_collection(root_user, driver, db)
            new_user_urls = get_album_data(root_album, driver, db)

            user_urls.update(new_user_urls)
            album_urls.update(new_album_urls)

            # Crawl through website
            while album_urls:
                while user_urls:
                    # Get user, gather data and add albums to list
                    user_url = user_urls.pop()
                    if not db.user_collections_new.find_one({"_id": mongo_key_formatting(user_url)}):
                        new_album_urls = get_user_collection(user_url, driver, db)
                        album_urls.update(new_album_urls)
                    else:
                        print "Skipping key: {}".format(user_url)

                album_url = album_urls.pop()
                if not db.albums.find_one({"_id": mongo_key_formatting(album_url)}):
                    new_user_urls = get_album_data(album_url, driver, db)
                    user_urls.update(new_user_urls)
                else:
                    print "Skipping key: {}".format(album_url)
        except:
            driver.close()

def main_scraper():
    params = [('https://preoccupations.bandcamp.com/',
               'https://bandcamp.com/charlesquinn'),
              ('https://inquisitionbm.bandcamp.com/album/obscure-verses-for-the-multiverse',
               'https://bandcamp.com/carcharoth'),
              ('https://deathspellomega.bandcamp.com/album/paracletus',
               'https://bandcamp.com/bryankerndrummer'),
              ('https://miloraps.bandcamp.com/album/i-wish-my-brother-rob-was-here',
               'https://bandcamp.com/Totallygnarly'),
              ('https://graveface.bandcamp.com/album/dandelion-gum',
               'https://bandcamp.com/rajjawa'),
              ('https://toucheamore.bandcamp.com/album/is-survived-by',
               'https://bandcamp.com/Rhokeheartburkett'),
              ('https://nonameraps.bandcamp.com/album/telefone',
               'https://bandcamp.com/duvaldiykid'),
              ('https://girlslivingoutsidesocietysshit.bandcamp.com/album/demo',
               'https://bandcamp.com/danielashton')]
    p = Pool(8)
    p.map(crawler, params)

def album_metadata_scraper():
    df = pd.read_csv('data/user_to_album_sf.csv')
    album_list = df['album_id'].unique()

    n = len(album_list) / 8
    album_url_chunks = [album_list[i:i + n] for i in range(0, len(album_list), n)]

    p = Pool(8)
    p.map(album_scraper_worker, album_url_chunks)

def album_scraper_worker(album_urls):
    while True:
        try:
            # Web driver
            driver = webdriver.Chrome(os.getcwd() + '/drivers/chromedriver_mac64')

            # Get Mongo database to dump things into
            db = get_mongo_database('bandcamp')

            for album_url in album_urls:
                if not db.albums.find_one({"_id": mongo_key_formatting(album_url)}):
                    _ = get_album_data(reverse_mongo_key_formatting(album_url), driver, db)
        except:
            driver.close()


if __name__ == "__main__":
    album_metadata_scraper()
