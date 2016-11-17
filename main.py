import graphlab
from graphlab.toolkits.feature_engineering import OneHotEncoder
import json
import numpy as np
import os
import pandas as pd
import pickle
import pymongo
from bandcamp_scraper import get_mongo_database
from helper import *


def update_dataframe(name = None, feature_building_method = None,
                     database = None, dump = False):
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

@timeit
def convert_to_sframe_format(df, list_like_column = None, count_column = None,
                             name = None, dump = True, verbose = True,
                             delimiter = None):
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
    album_list = list()
    rating_list = list()

    i = 0
    count = len(df)
    for index, row in df.iterrows():
        albums_string = row[list_like_column]

        # Strip list symbols
        if albums_string[:3] == '[u\'':
            albums_string = albums_string[3:]
        if albums_string[-2:] == '\']':
            albums_string = albums_string[:-2]

        albums = albums_string.split(delimiter)
        n_albums = len(albums)
        _id_list += [row._id] * n_albums
        rating_list += [1] * n_albums
        album_list += albums

        if verbose:
            # Progress counter
            if i % 100 == 0:
                print "{} complete".format(round(float(i) / count, 2))
            i += 1

    # Status checks
    print "Number of unique albums: {}".format(len(set(album_list)))
    print "Number of unique users: {}".format(len(set(_id_list)))
    print "Number of filled cells: {}".format(len(rating_list))
    print "Matrix sparcity: {}".format(float(len(rating_list)) / \
                                    (len(set(album_list)) * len(set(_id_list))))
    print "Rows have correct length: {}".format(len(album_list) == len(_id_list))

    # Create SFrame
    sf = graphlab.SFrame({'_id': _id_list,
                          'album_id': album_list,
                          'rating': rating_list})

    # Albums counts
    album_counts = sf.groupby(key_columns='album_id',
                              operations={'count': agg.COUNT()})
    if dump:
        album_counts.save('data/{}_album_counts.csv'.format(name), format = 'csv')

    # Dump
    if dump:
        sf.save('data/{}.csv'.format(name), format = 'csv')

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

    encoder = OneHotEncoder(features = columns)
    encoded_sf = encoder.fit_transform(sf)

    print encoded_sf
    print encoder['feature_encoding']
    if dump:
        encoded_sf.save('data/{}_integerified.csv'.format(name), format = 'csv')
        pickle.dump(encoder, '{}_one_hot_encoder.obj'.format(name))
    return encoded_sf


@timeit
def convert_to_gephi_format(sf, node_column = None, link_column = None):
    """
    Inputs:
        sf -- SFrame object with rows of '_id' and 'album_id'
        node_column -- string of column that we want to use as our graph nodes

    Converts to list of tuples of 'album_id' to 'album_id' representing all
    single user connections between albums.
    """
    joined_sf = sf.join(sf, on = '_id', how = 'inner')

    print "Joined successfully"

    subsetted_joined_sf = joined_sf.sample(0.3, seed = 5)

    print "Subsetted successfully"

    joined_sf.save('data/gephi_graph.csv', format = 'csv')

    # # Create SFrames with node to link and link to node data
    # node_to_link_sf = sf.groupby(key_columns = node_column,
    #                              operations = agg.CONCAT(link_column))
    #
    # link_to_node_sf = sf.groupby(key_columns = link_column,
    #                              operations = agg.CONCAT(node_column))
    #
    #
    #
    # # Conver to Gephi format
    # outer_list = list()
    # counter = 0
    # for row in node_to_link_sf:
    #     inner_list = list()
    #     for i in row['List of {}'.format(link_column)]:
    #         inner_list += link_to_node_sf[link_to_node_sf[link_column] == i]['List of {}'.format(node_column)][0]
    #     outer_list.append(inner_list)
    #     if counter % 100 == 0:
    #         print counter
    #     counter += 1
    # node_to_link_sf['neighbors'] = outer_list
    #
    # print node_to_link_sf
    #
    # node_to_link_sf = node_to_link_sf.remove_column('List of {}'.format(link_column))
    #
    # print node_to_link_sf.unpack('neighbors')


    # with open('data/album_node_gephi.csv', 'w+') as f:
    #     counter = 0
    #     print "Number of rows to search: {}".format(len(node_to_link_sf))
    #     for row in node_to_link_sf:
    #         for i in row['List of {}'.format(link_column)]:
    #             for item in link_to_node_sf[link_to_node_sf[link_column] == i]['List of {}'.format(node_column)][0]:
    #                 f.write('{},{}\n'.format(row['album_id'], item))
    #         if counter % 100 == 0:
    #             print counter
    #         counter += 1



# Feature building methods
def album_list(df, row, i):
    _id = row['_id']
    albums = json.loads(row['data']).keys()
    df.loc[i, '_id'] = _id
    df.loc[i, 'albums'] = albums
    df.loc[i, 'n_albums'] = len(albums)

def tags(df, row, i):
    _id = row['_id']
    tags = json.loads(row['data']).values()
    df.loc[i, '_id'] = _id
    df.loc[i, 'tags'] = tags






# Pipelines
def build_from_database():
    # Get databasea to read from
    db = get_mongo_database('bandcamp')

    df = update_dataframe(name = 'user_to_album_list',
                          feature_building_method = album_list,
                          database = db,
                          dump = False)


def build_from_album_list():
    df = pd.read_csv('data/user_to_album_list.csv')

    # Convert to SFrame
    sf = convert_to_sframe_format(df,
                                  list_like_column = 'albums',
                                  count_column = 'n_albums',
                                  name = 'user_to_album_sf',
                                  dump = False,
                                  verbose = False,
                                  delimiter = '\', u\'')

    sf = convert_sframe_to_integer_ids(sf,
                                       columns = ['album_id', '_id'],
                                       name = 'user_to_album_sf',
                                       dump = True)


    sf = low_pass_filter_on_counts(sf,
                                   column = 'album_id',
                                   cutoff = 10,
                                   name = 'user_to_album_sf_',
                                   dump = True)

    sf = low_pass_filter_on_counts(sf,
                                   column = '_id',
                                   cutoff = 10,
                                   name = 'user_to_album_sf_album',
                                   dump = True)



def gephi_test():
    sf = graphlab.SFrame.read_csv('data/user_to_album_sf_album_id_filtered.csv')
    convert_to_gephi_format(sf,
                            node_column = 'album_id',
                            link_column = '_id')

def test_code():
    sf = graphlab.SFrame.read_csv('data/user_to_album_sf_album_id_filtered.csv')
    sf = sf.remove_column('rating')

if __name__ == "__main__":
    build_from_album_list()
