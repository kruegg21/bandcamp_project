import pymongo
import requests
import time
import random
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
    accumulator = []
    n_titles = 0
    previous_length = -999
    counter = 1
    while n_titles != previous_length:
        previous_length = n_titles

        page_height = counter * 1500
        counter += 1
        driver.execute_script("window.scrollTo(0, {});".format(page_height))
        time.sleep(0.2)
        html = driver.page_source

        # Make soup
        soup = BeautifulSoup(html)

        album_titles = list()
        for user in soup.find_all('a', {'class': 'item-link'}):
            if user.find('div', {'class' : 'collection-item-title'}):
                album_titles.append(user.find('div', {'class' : 'collection-item-title'}).text)
        print album_titles
        accumulator = album_titles
        n_titles = len(accumulator)



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

def get_user_collection_request(url):

    r = requests.get(url)
    random_sleep()
    soup = BeautifulSoup(r.content, 'html.parser')
    print soup
    raw_input()

    album_title = soup.find('h2', {'class' : 'trackTitle'}).contents[0].strip()
    artist_name = soup.find('span', {'itemprop' : 'byArtist'}).find('a').contents[0].strip()

    print album_title, artist_name

    for user in soup.find_all('a', {'class': 'fan pic'}):
         print user['href']
    for user in soup.find_all('a', {'class' : 'pic'}):
         print user

if __name__ == "__main__":
    url = 'https://jeffrosenstock.bandcamp.com/'

    driver = webdriver.Chrome('/Users/kruegg/Desktop/RYM/chromedriver')
    # driver.implicitly_wait(10) # seconds
    # driver.get(url)
    # driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
    get_user_collection('https://bandcamp.com/devonpotrie', driver)


    # # get_user_collection('https://bandcamp.com/devonpotrie', driver)
    # # random_sleep()
