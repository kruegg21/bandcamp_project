import graphlab
import numpy as np
import pandas as pd
import random
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
        node_array = joined_sf[column].unique().to_numpy()
        print node_array.shape[0]
        break


    print "Number of nodes before subsetting: {}".format(node_array.shape[0])

    # Get proportion of nodes
    node_subset_proportion = 0.3
    node_subset_array = np.random.choice(node_array,
                                         size = node_array.shape[0] * node_subset_proportion,
                                         replace = False)

    print "Number of nodes after subsetting: {}".format(node_subset_array.shape[0])
    print "Subsetted nodes"

    # Filter SFrame keeping only subset of nodes
    print "Number of elements before filter: {}".format(len(joined_sf))
    for column in joined_sf.column_names():
        joined_sf = joined_sf.filter_by(node_subset_array, column)
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
def build_from_album_list():
    df = pd.read_csv('data/user_to_album_list.csv')

    # Convert to SFrame
    sf = convert_to_sframe_format(df,
                                  list_like_columns = ['albums'],
                                  resulting_column_names = ['album_id'],
                                  delimiters = ['\', u\''],
                                  count_column = 'n_albums',
                                  name = 'user_to_album_sf',
                                  dump = True,
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

if __name__ == "__main__":
    build_from_album_list()
