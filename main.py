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
from bandcamp_scraper import get_mongo_database
from helper import *

"""
Notes:
Album art URLs are of form 'https://f4.bcbits.com/img/a<'item_art_id'>_9.jpg'
"""




@timeit
def convert_to_gephi_format(sf, node_column = None, link_column = None,
                            edge_subset_proportion = 0.3):
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

    subsetted_joined_sf = joined_sf.sample(edge_subset_proportion, seed = 5)

    subsetted_joined_sf.rename({node_column: 'source',
                                node_column + '.1': 'target'})

    # Get edge weights
    subsetted_joined_sf = subsetted_joined_sf.groupby(key_columns = ['source', 'target'],
                                                      operations = {'weight': agg.COUNT()})

    dump_sf(subsetted_joined_sf, 'data/gephi_graph_subsetted.csv')

    print "Subsetted successfully"

    dump_sf(joined_sf, 'data/gephi_graph_full.csv')



def graphlab_factorization_recommender(sf):
    # Test train split
    (train_set, test_set) = sf.random_split(0.8, seed=1)

    # # Collaborative filtering item similarity model
    # # https://turi.com/products/create/docs/generated/graphlab.recommender.item_similarity_recommender.ItemSimilarityRecommender.html#graphlab.recommender.item_similarity_recommender.ItemSimilarityRecommender
    # collaborative_filtering = recommender.create(sf,
    #                                              user_id = '_id',
    #                                              item_id = 'album_id')

    # Factorization recommender
    # https://turi.com/products/create/docs/generated/graphlab.recommender.factorization_recommender.FactorizationRecommender.html#graphlab.recommender.factorization_recommender.FactorizationRecommender
    factorization_recommender = recommender.create(sf,
                                                   user_id = '_id',
                                                   item_id = 'album_id')

    print factorization_recommender.predict(sf).shape
    print factorization_recommender.evaluate_precision_recall(sf, cutoffs = [100,200,1000])
    print factorization_recommender.get_similar_items()




# Pipelines
def build_user_to_album_list_from_database():
    # Get databasea to read from
    db = get_mongo_database('bandcamp')

    df = update_dataframe(name = 'user_to_album_list',
                          feature_building_method = album_list,
                          database = db,
                          dump = False)


def build_user_to_album_art_from_database():
    # Get databasea to read from
    db = get_mongo_database('bandcamp')

    df = update_dataframe(name = 'user_to_album_art',
                          feature_building_method = album_art,
                          database = db,
                          dump = True,
                          test = False)


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


def build_gephi_data():
    sf = graphlab.SFrame.read_csv('data/user_to_album_sf.csv')

    # sf = convert_sframe_to_integer_ids(sf,
    #                                    columns = ['album_id', '_id'],
    #                                    name = 'user_to_album_sf',
    #                                    dump = True)

    sf = low_pass_filter_on_counts(sf,
                                   column = 'album_id',
                                   cutoff = 100,
                                   name = 'user_to_album_sf',
                                   dump = True)

    sf = low_pass_filter_on_counts(sf,
                                   column = '_id',
                                   cutoff = 100,
                                   name = 'user_to_album_sf_album',
                                   dump = True)

    convert_to_gephi_format(sf,
                            node_column = 'album_id',
                            link_column = '_id',
                            edge_subset_proportion = 0.01)

def graphlab_recommender_test():
    sf = graphlab.SFrame.read_csv('data/user_to_album_sf_album_id_filtered.csv')

    # Make model
    graphlab_factorization_recommender(sf)

if __name__ == "__main__":
    build_gephi_data()
