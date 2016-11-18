import graphlab
graphlab.set_runtime_config('GRAPHLAB_DEFAULT_NUM_PYLAMBDA_WORKERS', 64)
import graphlab.aggregate as agg
from graphlab.toolkits.feature_engineering import OneHotEncoder
from graphlab import recommender
import json
import numpy as np
import os
import pandas as pd
import numpy as np
import pickle
import pymongo
import time

# Contains column names for DataFrames we are working with
column_names_dict = {
    'user_to_album_list' : ['_id', 'albums', 'n_albums'],
    'user_to_album_art' : ['_id', 'album_art_code']
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

@timeit
def dump_sf(sf, name):
    sf.save(name, format = 'csv')

def split_into_artist_album(sf):
    """
    Splits 'album_id' column into artist and album
    """
    sf['artist'] = sf['album_id'].apply(lambda x: x.split('_bandcamp_')[0])
    sf['album'] = sf['album_id'].apply(lambda x: x.split('_bandcamp_')[-1])

    return sf

def show_sframe_sparcity(sf):
    n_albums = len(sf['album_id'].unique())
    n_users = len(sf['_id'].unique())

    print "Number of unique albums: {}".format(n_albums)
    print "Number of unique users: {}".format(n_users)
    print "Number of filled cells: {}".format(len(sf))
    print "Matrix sparcity: {}\n\n".format(float(len(sf)) / (n_albums * n_users))



@timeit
def low_pass_filter_on_counts(sf, column = None, min_cutoff = 0,
                              max_cutoff = np.inf, name = None, dump = True):
    # Show initial sparcity
    print "\nInitial SFrame sparcity"
    show_sframe_sparcity(sf)

    # Albums counts
    counts_sf = sf.groupby(key_columns = column,
                           operations = {'count': agg.COUNT()})

    # Make SArray of albums with high rating counts
    high_album_counts = counts_sf[(counts_sf['count'] > min_cutoff) & \
                        (counts_sf['count'] < max_cutoff)][column]

    # Filter
    filtered_sf = sf.filter_by(high_album_counts, column, exclude = False)

    # Dump
    if dump:
        filtered_sf.save('data/{}{}_filtered.csv'.format(name, column), format = 'csv')

    # Show sparcity
    show_sframe_sparcity(filtered_sf)

    return filtered_sf


def update_dataframe(name = None, feature_building_method = None,
                     database = None, dump = False, test = False):
    """
    Input:
        name -- string of the name of the DataFrame
        feature_building_method -- function indicating how to build each row
        database -- MongoDB client to draw data from
        dump -- Bool indicating whether or not to dump resulting DataFrame
    Output:
        None

    Checks to see if a DataFrame with 'name' already exists and updates it with
    any new data found in our database.
    """

    # Read in old DataFrame if we have already built it
    if os.path.isfile('data/{}.csv'.format(name)):
        old_data_df = pd.read_csv('data/{}.csv'.format(name))
    else:
        old_data_df = pd.DataFrame(columns = column_names_dict[name])

    # List of '_id's we already have
    _id_list = list(set(old_data_df._id.tolist()))

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

        if test:
            if i > 200:
                print new_data_df
                break

    # Stack DataFrames
    full_data_df = pd.concat([old_data_df, new_data_df]).reset_index(drop = True).dropna()
    print "Number of rows in full DataFrame: {}".format(len(full_data_df))

    # Dump
    if dump:
        full_data_df.to_csv('data/{}.csv'.format(name), index = False)

    return full_data_df

@timeit
def convert_to_sframe_format(df, list_like_columns = None,
                             resulting_column_names = None, count_column = None,
                             name = None, dump = True, verbose = True,
                             delimiters = None, get_album_counts = True):
    """
    Inputs:
        df -- DataFrame with '_id' column, list-like column, and count column
        list_like_column -- string with name of list-like column
        count_column -- column with counts for each albums
        delimiter -- string with characters sequence to split on
        dump -- Bool if should dump to CSV
    Output:
        graphlab SFrame


    """
    _id_list = list()
    list_columns = {column: list() for column in list_like_columns}
    rating_list = list()

    i = 0
    count = len(df)
    for index, row in df.iterrows():
        for column, delimiter in zip(list_like_columns, delimiters):
            list_string = row[column]

            # Strip list symbols
            if list_string[:3] == '[u\'':
                list_string = list_string[3:]
            if list_string[-2:] == '\']':
                list_string = list_string[:-2]
            if list_string[0] == '[':
                list_string = list_string[1:]
            if list_string[-1] == ']':
                list_string = list_string[:-1]

            # Split list string into list
            list_ = list_string.split(delimiter)
            n_albums = len(list_)

            #
            _id_list += [row._id] * n_albums
            rating_list += [1] * n_albums
            list_columns[column] += list_

        if verbose:
            # Progress counter
            if i % 100 == 0:
                print "{} complete".format(round(float(i) / count, 2))
            i += 1

    # Sanity checks
    n_filled = len(rating_list)
    n_users = len(set(_id_list))
    n_albums = len(set(list_columns[list_like_columns[0]]))
    for column, resulting_name in zip(list_like_columns, resulting_column_names):
        print "Number of unique {}: {}".format(resulting_name,
                                               len(set(list_columns[column])))
    print "Number of unique users: {}".format(n_users)
    print "Number of filled cells: {}".format(n_filled)
    print "Matrix sparcity: {}".format(float(n_filled) / (n_albums * n_users))
    print "Rows have correct length: {}".format(n_albums == n_users)
    if len(list_like_columns) > 1:
        n_albums_secondary = len(list_columns[list_like_columns[1]])
        print "List like column have same length: {}".format(n_albums == n_albums_secondary)

    # Create SFrame
    sframe_dict = {'_id': _id_list,
                  'rating': rating_list}
    for column, resulting_name in zip(list_like_columns, resulting_column_names):
        sframe_dict[resulting_name] = list_columns[column]
    sf = graphlab.SFrame(sframe_dict)

    # Albums counts
    if get_album_counts:
        album_counts = sf.groupby(key_columns = resulting_column_names[0],
                                  operations = {'count': agg.COUNT()})
        album_counts.save('data/{}_album_counts.csv'.format(name),
                          format = 'csv')

    # Dump
    if dump:
        dump_sf(sf, 'data/{}.csv'.format(name))

    return sf

def convert_sframe_to_integer_ids(sf, name = None, columns = None, dump = True):
    """
    Inputs:
        sf -- SFrame with string entries
        columns -- list of column with string entries to convert
        dump -- Bool indicating whether or not to dump dictionary and SFrame
    Output:
        sf -- SFrame with string entries converted to integers
        translation_dictionaries -- tuple of dictionaries that translate integers
                                   back to string

    """
    encoded_sf = sf
    for column in columns:
        encoder = OneHotEncoder(features = [column],
                                output_column_name = column)
        encoded_sf = encoder.fit_transform(encoded_sf)
        encoded_sf[column] = encoded_sf[column].apply(lambda x: x.keys()[0])

        print encoder.list_fields()

        if dump:
            encoder.save('data/{}_one_hot_encoder.obj'.format(name))

    print encoded_sf
    print encoder['feature_encoding']

    if dump:
        dump_sf(encoded_sf, 'data/{}_integerified.csv'.format(name))
    return encoded_sf

def mongo_key_formatting(s):
    """
    MongoDB requires keys to not have periods in them. This function replace
    '.' with '_' to make MongoDB happy
    """
    return s.replace('.', '_')


# Feature building methods
def album_list(df, row, i):
    _id = row['_id']
    albums = json.loads(row['data']).keys()
    df.loc[i, '_id'] = _id
    df.loc[i, 'albums'] = albums
    df.loc[i, 'n_albums'] = len(albums)

def album_art(df, row, i):
    _id = row['_id']
    json_dict = json.loads(row['data'])
    df.loc[i, '_id'] = _id
    df.loc[i, 'album_art_code'] = [(key, value['item_art_id']) for key, value in json_dict.iteritems()]
