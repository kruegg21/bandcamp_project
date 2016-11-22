import graphlab
graphlab.set_runtime_config('GRAPHLAB_DEFAULT_NUM_PYLAMBDA_WORKERS', 64)
from graphlab.toolkits.feature_engineering import OneHotEncoder
from graphlab import recommender
import json
import numpy as np
import os
import pandas as pd
import pickle
import pymongo
import random
from model import graphlab_factorization_recommender, graphlab_grid_search
from bandcamp_scraper import get_mongo_database
from helper import *

"""
Notes:
Album art URLs are of form 'https://f4.bcbits.com/img/a<'item_art_id'>_9.jpg'
"""


@timeit
def convert_to_gephi_format(sf, node_column = None, link_column = None,
                            edge_subset_proportion = 0.3,
                            dump_full_graph = False, edge_weight_cutoff = 10):
    """
    Inputs:
        sf -- SFrame object with rows of '_id' and 'album_id'
        node_column -- string of column that we want to use as our graph nodes

    Converts to list of tuples of 'album_id' to 'album_id' representing all
    single user connections between albums.
    """
    correct_columns_sf = sf[[node_column, link_column]]
    joined_sf = correct_columns_sf.join(correct_columns_sf,
                                        on = link_column,
                                        how = 'inner')
    joined_sf.remove_column(link_column)

    print "Joined successfully"

    # Make set of nodes
    node_set = set()
    for column in joined_sf.column_names():
        node_set.update(joined_sf[column])
        print len(node_list)

    # Get proportion of nodes
    node_subset_proportion = 0.3
    node_list = random.sample(node_set, len(node_set * node_subset_proportion))

    print "Number of elements before filter: {}".format(len(joined_sf))
    for column in joined_sf.column_names():
        joined_sf = joined_sf.filter_by(node_list, column)
        print "Number of elements after filter: {}".format(len(joined_sf))

    joined_sf.rename({node_column: 'source',
                                node_column + '.1': 'target'})

    # Get edge weights
    joined_sf = joined_sf.groupby(key_columns = ['source', 'target'],
                                  operations = {'weight': agg.COUNT()})

    # Cutoff based on edge weights
    joined_sf = joined_sf[joined_sf['weight'] > edge_weight_cutoff]

    # Add column specifying undirected
    joined_sf['type'] = 'undirected'

    # Dump
    dump_sf(joined_sf, 'data/gephi_graph_subsetted.csv')
    print "Subsetted successfully"

    if dump_full_graph:
        dump_sf(joined_sf, 'data/gephi_graph_full.csv')






# Pipelines
# DO THIS ON LOCAL
def build_user_to_album_list_from_database():
    # Get databasea to read from
    db = get_mongo_database('bandcamp')

    df = update_dataframe(name = 'user_to_album_list',
                          feature_building_method = album_list,
                          database = db,
                          dump = False)


# DO THIS ON LOCAL
def build_user_to_album_art_from_database():
    # Get databasea to read from
    db = get_mongo_database('bandcamp')

    df = update_dataframe(name = 'user_to_album_art',
                          feature_building_method = album_art,
                          database = db,
                          dump = True,
                          test = False)

# DO THIS ON EC2
def build_from_album_art_list(verbose = True):
    """
    Input:
        None
    Output:
        None

    Reads from 'user_to_album_art.csv', which contains a column of user IDs and
    a column of a list of tuples of album url and art id. It then parses this
    list of album urls and art ids into an SFrame of columns 'album_url' and
    'art_id' and dumps to file 'album_url_to_art_id.csv'
    """
    df = pd.read_csv('data/user_to_album_art.csv')

    album_url_dict = dict()

    i = 0
    count = len(df)
    for index, row in df.iterrows():
        list_string = row['album_art_code']
        album_art_list = eval(list_string)
        album_url_dict.update(dict(album_art_list))

        if verbose:
            # Progress counter
            if i % 100 == 0:
                print "{} complete".format(round(float(i) / count, 2))
            i += 1

    sf = graphlab.SFrame({'album_url': album_url_dict.keys(),
                          'art_id': album_url_dict.values()})

    # Sanity checks
    n_albums = len(sf)
    n_unique_albums = len(set(sf['album_url']))
    print "Number of albums: {}".format(n_albums)
    print "Album URLs are unique: {}".format(n_albums == n_unique_albums)

    # Dump
    dump_sf(sf, 'data/album_url_to_art_id.csv')

    return sf

# DO THIS ON EC2
def build_from_album_list():
    df = pd.read_csv('data/user_to_album_list.csv')

    # Convert to SFrame
    sf = convert_to_sframe_format(df,
                                  list_like_columns = ['albums'],
                                  resulting_column_names = ['album_id'],
                                  delimiters = ['\', u\''],
                                  count_column = 'n_albums',
                                  name = 'user_to_album_sf',
                                  dump = False,
                                  verbose = False,
                                  get_album_counts = True)

    sf = low_pass_filter_on_counts(sf,
                                   column = 'album_id',
                                   min_cutoff = 10,
                                   max_cutoff = 1000,
                                   name = 'user_to_album_sf_',
                                   dump = True)

    sf = low_pass_filter_on_counts(sf,
                                   column = '_id',
                                   min_cutoff = 10,
                                   name = 'user_to_album_sf_album',
                                   dump = True)


def build_gephi_data():
    sf = graphlab.SFrame.read_csv('data/user_to_album_sf.csv')

    sf = low_pass_filter_on_counts(sf,
                                   column = 'album_id',
                                   min_cutoff = 20,
                                   name = 'user_to_album_sf',
                                   dump = True)

    sf = low_pass_filter_on_counts(sf,
                                   column = '_id',
                                   min_cutoff = 100,
                                   name = 'user_to_album_sf_album',
                                   dump = True)

    sf = convert_to_truncated_string_ids(sf)

    convert_to_gephi_format(sf,
                            node_column = 'album_id',
                            link_column = '_id',
                            edge_subset_proportion = 0.50,
                            edge_weight_cutoff = 10,
                            dump_full_graph = False)

# # DO THIS ON EC2
# def graphlab_recommender_test(should_filter = True):
#     sf = graphlab.SFrame.read_csv('data/user_to_album_sf.csv')
#
#     if should_filter:
#         # Filter to make data more dense
#         sf = low_pass_filter_on_counts(sf,
#                                        column = 'album_id',
#                                        min_cutoff = 40,
#                                        max_cutoff = 1300,
#                                        name = 'user_to_album_sf',
#                                        dump = True)
#
#         sf = low_pass_filter_on_counts(sf,
#                                        column = '_id',
#                                        min_cutoff = 150,
#                                        name = 'user_to_album_sf_album',
#                                        dump = True)
#     else:
#         sf = graphlab.SFrame.read_csv('data/user_to_album_sf_album_id_filtered.csv')
#
#     # Convert ids from URLs to more readable format
#     sf = convert_to_truncated_string_ids(sf)
#
#     # Make model
#     model = graphlab_factorization_recommender(sf,
#                                                dump = True,
#                                                train = True)
#     return model
#
#     # Make predictions
#     album_list = ['https://openmikeeagle360.bandcamp.com/album/dark-comedy',
#                   'https://miloraps.bandcamp.com/album/too-much-of-life-is-mood',
#                   'https://miloraps.bandcamp.com/album/so-the-flies-dont-come',
#                   'https://miloraps.bandcamp.com/album/plain-speaking',
#                   'https://openmikeeagle360.bandcamp.com/album/hella-personal-film-festival',
#                   'https://openmikeeagle360.bandcamp.com/album/time-materials',
#                   'https://openmikeeagle360.bandcamp.com/album/a-special-episode-of-ep']
#
#     # album_list = ['https://toucheamore.bandcamp.com/album/is-survived-by',
#     #               'http://toucheamore.bandcamp.com/album/parting-the-sea-between-brightness-and-me',
#     #               'https://deafheavens.bandcamp.com/album/sunbather',
#     #               'https://deafheavens.bandcamp.com/track/from-the-kettle-onto-the-coil',
#     #               'https://deafheavens.bandcamp.com/album/new-bermuda']
#
#     album_list = album_list
#     rating_list = [1] * len(album_list)
#     _id_list = ['https://bandcamp.com/kruegg'] * len(album_list)
#
#     # Get keys in correct format
#     album_list = [mongo_key_formatting(x) for x in album_list]
#     _id_list = [mongo_key_formatting(x) for x in _id_list]
#
#     # Create SFrame
#     prediction_sf = graphlab.SFrame({'_id': _id_list,
#                                      'album_id': album_list,
#                                      'rating': rating_list})
#     prediction_sf = convert_to_truncated_string_ids(prediction_sf)
#
#     # Make recommendations
#     recommendations_sf = model.recommend(users = ['https://bandcamp.com/kruegg'],
#                                          k = 20,
#                                          new_user_data = prediction_sf)
#
#     # Split into logical columns
#     recommendations_sf = split_into_artist_album(recommendations_sf)
#
#     print recommendations_sf
#
#     # Sample
#     recommendations_sf = graphlab.SFrame(recommendations_sf.to_dataframe().
#                             drop_duplicates(subset = ['artist']))
#
#     print recommendations_sf
#
#     # Dump recommendations to CSV
#     dump_sf(recommendations_sf, 'data/recommendations.csv')


if __name__ == "__main__":
    # recommendations = graphlab_recommender_test(should_filter = False)
    build_gephi_data()
