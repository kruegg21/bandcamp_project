import pymongo
import requests
import time
import random
from bs4 import BeautifulSoup

"""
Potential to speed up
1. Concurrent requests using grequests
2. lxml to speed up HTML parsing

RYM is extremely strict with botting. Add a pause between
asking for tickets.
"""

def random_sleep():
    time.sleep(random.uniform(10,15))

def scrape_album_metadata(r):
    # Get MongoDB collection instance
    collection = get_mongo_database('rym_data', 'album_metadata')

    # Make soup
    soup = BeautifulSoup(r.content, 'html.parser')
    print soup

    # Traverse HTML
    for entry in soup.find_all("tr"):
        post = {}
        artist = entry.find('a', {'class': 'artist'})
        if artist:
            post['artist_id'] = artist['title'].strip('[]')
            post['artist_name'] = artist.contents[0]
        album = entry.find("a", {"class": "album"})
        if album:
            post['album_id'] = album['title'].strip('[]')
            post['album_name'] = album.contents[0]
            post['year'] = payload['year']
            post['genre'] = payload['genres']
        statistics = entry.find_all('b')
        if statistics:
            post['average_rating'] = statistics[0].contents[0]
            post['number_of_ratings'] = statistics[1].contents[0]
            post['number_of_reviews'] = statistics[2].contents[0]
        if post:
            # Insert into MongoDB database
            result = collection.insert_one(post)
    return soup

def get_mongo_database(db_name, collection_name):
    # Open MongoDB client
    client = pymongo.MongoClient()

    # Open database
    db = client[db_name]

    # Choose collection
    return db[collection_name]


if __name__ == '__main__':
    payload = {'page': '1',
               'type': 'album',
               'year' : '1985',
               'genre_include': '1',
               'include_child_genres': '1',
               'genres': 'punk',
               'include_child_genres_chk': '1',
               'include': 'both',
               'origin_countries': '',
               'limit': 'none',
               'countries': ''}

    r = requests.get('https://rateyourmusic.com/customchart', params=payload)
    soup = scrape_album_metadata(r)
    random_sleep()

    # Get number of pages
    link_tags = soup.find_all('a', {'class': 'navlinknum'})
    latest_page = max([int(x.contents[0]) for x in link_tags])

    for i in xrange(latest_page):
        payload['page'] = str(i+1)
        r = requests.get('https://rateyourmusic.com/customchart', params=payload)
        soup = scrape_album_metadata(r)
        random_sleep()
        print "On page {} of {} search".format(payload['page'], payload['genres'])
