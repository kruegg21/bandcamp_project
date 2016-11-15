import graphlab
import json
import numpy as np
import os
import pandas as pd
import pymongo
import time # timeit
from bandcamp_scraper import get_mongo_database

# Contains column names for DataFrames we are working with
column_names_dict = {
    'user_to_album_list' : ['_id', 'albums', 'n_albums']
}

# Timing function
def timeit(method):
    """
    Timing wrapper
    """
    def timed(*args, **kw):
        ts = time.time()
        result = method(*args, **kw)
        te = time.time()
        print 'Running %r took %2.4f sec\n' % \
              (method.__name__, te-ts)
        return result
    return timed

def update_dataframe(name = None, feature_building_method = None,
                     database = None, dump = False):
    # Read in old DataFrame if we have already built it
    if os.path.isfile('data/{}.csv'.format(name)):
        old_data_df = pd.read_csv('data/{}.csv'.format(name))
    else:
        old_data_df = pd.DataFrame(columns = column_names_dict[name])

    # List of '_id's we already have
    _id_list = old_data_df._id.tolist()

    # Specify collection
    collection = 'user_collections_new'

    # Print number of new points
    count = database[collection].find(filter = {'_id': {'$nin': _id_list}}).count()
    print "Number of rows in old DataFrame: {}".format(len(old_data_df))
    print "Number of new data points: {}".format(count)

    # Get cursor
    cursor = database[collection].find(filter = {'_id': {'$nin': _id_list}})

    # Create DataFrame to hold new data
    new_data_df = pd.DataFrame(index = range(count),
                               columns = column_names_dict[name])

    # Run through each row
    i = 0
    for row in cursor:
        feature_building_method(new_data_df, row, i)

        # Progress counter
        if i % 100 == 0:
            print "{} complete".format(round(float(i) / count, 2))
        i += 1

    # Stack DataFrames
    full_data_df = pd.concat([old_data_df, new_data_df]).reset_index(drop = True).dropna()
    print "Number of rows in full DataFrame: {}".format(len(full_data_df))

    # Dump
    if dump:
        full_data_df.to_csv('data/{}.csv'.format(name), index = False)

    return full_data_df

def add_row(r, d):
    for item in r[c]:
        d[r._id] = item
    return d

@timeit
def convert_to_sframe_format(df, list_like_column = None, count_column = None,
                             name = None, dump = True):
    """
    Inputs:
        df -- DataFrame with '_id' column, list-like column, and count column
        dump -- Bool if should dump to CSV
    Output:
        graphlab SFrame
    """
    _id_list = list()
    album_list = list()
    rating_list = list()

    i = 0
    count = len(df)
    for index, row in df.iterrows():
        print row[list_like_column][:10]
        raw_input()
        n_albums = len(row[list_like_column])
        _id_list += [row._id] * n_albums
        rating_list += [1] * n_albums
        album_list += row[list_like_column]

        # Progress counter
        if i % 100 == 0:
            print "{} complete".format(round(float(i) / count, 2))
        i += 1


    print album_list[:100]
    print len(set(album_list))
    print len(set(_id_list))
    print len(set(rating_list))

    raw_input()

    # Remove DataFrame
    del df

    # Create SFrame
    sf = graphlab.SFrame({'_id': _id_list,
                          'album_id': album_list,
                          'rating': rating_list})

    print "Created SFrame"

    # Dump
    if dump:
        sf.save('data/{}.csv'.format(name), format='csv')

    return sf








# Feature building methods
def album_list(df, row, i):
    _id = row['_id']
    albums = json.loads(row['data']).keys()
    df.loc[i, '_id'] = _id
    df.loc[i, 'albums'] = albums
    df.loc[i, 'n_albums'] = len(albums)



# Pipelines
def main_pipeline():
    # Get databasea to read from
    db = get_mongo_database('bandcamp')

    df = update_dataframe(name = 'user_to_album_list',
                          feature_building_method = album_list,
                          database = db,
                          dump = False)

    sf = convert_to_sframe_format(df,
                                  list_like_column = 'albums',
                                  count_column = 'n_albums',
                                  name = 'user_to_album_sf',
                                  dump = True)


def test():
    # Read subsetted data (only 200 rows)
    subsetted_data = pd.read_csv('subsetted_data.csv')

    #
    gl.SFrame()

if __name__ == "__main__":
    main_pipeline()
