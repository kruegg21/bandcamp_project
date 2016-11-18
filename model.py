import graphlab
import numpy as np
from helper import *

class model_specifications(object):
    """
    Object to contain information about model building to make things easy
    to change and log.
    """
    def __init__(self, **kwargs):
        self.user_count_max_cutoff = kwargs.get('user_count_max_cutoff')
        self.user_count_min_cutoff = kwargs.get('user_count_min_cutoff')
        self.album_count_max_cutoff = kwargs.get('album_count_max_cutoff')
        self.album_count_min_cutoff = kwargs.get('album_count_min_cutoff')
        self.n_albums = kwargs.get('n_albums')
        self.n_users = kwargs.get('n_users')
        self.model = kwargs.get('model')
        self.param_grid = kwargs.get('param_grid')
        self.params = kwargs.get('params')
        self.folds = kwargs.get('folds')

def build_model(should_grid_search = True, should_filter = True,
                specs = None):
    """
    Input:
        should_grid_search -- Bool indicating if we should grid search or not
        specs -- model_specifications Object with info about data
    """
    # Filter
    if should_filter:
        # Starting data
        sf = graphlab.SFrame.read_csv('data/user_to_album_sf.csv')

        # Filter to make data more dense
        sf = low_pass_filter_on_counts(sf,
                                       column = 'album_id',
                                       min_cutoff = specs.album_count_min_cutoff,
                                       max_cutoff = specs.album_count_max_cutoff,
                                       name = 'user_to_album_sf',
                                       dump = True)

        sf = low_pass_filter_on_counts(sf,
                                       column = '_id',
                                       min_cutoff = specs.user_count_min_cutoff,
                                       max_cutoff = specs.user_count_max_cutoff,
                                       name = 'user_to_album_sf_album',
                                       dump = True)
    else:
        sf = graphlab.SFrame.read_csv('data/user_to_album_sf_album_id_filtered.csv')

    # Grid Search
    if should_grid_search:
        graphlab_grid_search(sf, specs)

    # Train
    graphlab_factorization_recommender(sf, specs, dump = True)


def graphlab_grid_search(sf, specs = None):
    # Create K-Folds splits
    folds = graphlab.cross_validation.KFold(sf, specs.folds)

    # Define parameters to grid search
    # params = dict([('target', 'rating'),
    #                ('user_id', '_id'),
    #                ('item_id', 'album_id'),
    #                ('binary_target', True),
    #                ('max_iterations', 200),
    #                ('ranking_regularization', [0.1, 0.2])
    #               ])
    params = specs.param_grid
    job = graphlab.grid_search.create(data = folds,
                                      model_factory = graphlab.ranking_factorization_recommender.create,
                                      model_parameters = params,
                                      evaluator = custom_evaluation)
    print job.get_results()

    # Put optimal parameters in specifications


def custom_evaluation(model, train, test):
    recommendations = evaluate_precision_recall(test,
                                                cutoffs = [100,200,1000],
                                                exclude_known = False)
    score = recommendations['precision_recall_overall']['recall'][0]
    return {'recall_100': score}

def graphlab_factorization_recommender(sf, specs, dump = True, train = True):
    if train:
        # Test train split
        (train_set, test_set) = sf.random_split(0.8, seed=1)

        # # Collaborative filtering item similarity model
        # # https://turi.com/products/create/docs/generated/graphlab.recommender.item_similarity_recommender.ItemSimilarityRecommender.html#graphlab.recommender.item_similarity_recommender.ItemSimilarityRecommender
        # collaborative_filtering = recommender.create(sf,
        #                                              user_id = '_id',
        #                                              item_id = 'album_id')

        # Factorization recommender
        # https://turi.com/products/create/docs/generated/graphlab.recommender.factorization_recommender.FactorizationRecommender.html#graphlab.recommender.factorization_recommender.FactorizationRecommender
        rec_model = graphlab.ranking_factorization_recommender.create(train_sf,
                                                                      target='rating',
                                                                      user_id = '_id',
                                                                      item_id = 'album_id',
                                                                      binary_target = True,
                                                                      max_iterations = 200,
                                                                      ranking_regularization = 0.1)

        # Data print out
        print rec_model.evaluate_precision_recall(test_set, cutoffs = [100,200,1000], exclude_known = False)
        print rec_model.get_similar_items()
    else:
        rec_model = graphlab.load_model('factorization_recommender')

    # Dump
    if dump:
        rec_model.save('factorization_recommender')

    return rec_model

if __name__ == "__main__":
    specs = model_specifications(param_grid = dict([('target', 'rating'),
                                                    ('user_id', '_id'),
                                                    ('item_id', 'album_id'),
                                                    ('binary_target', True),
                                                    ('max_iterations', 200),
                                                    ('regularization', [0.1, 0.2]),
                                                    ('linear_regularization', [0.1, 0.2])
                                                   ]),
                                 user_count_min_cutoff = 100,
                                 user_count_max_cutoff = np.inf,
                                 album_count_max_cutoff = 1200,
                                 album_count_min_cutoff = 40,
                                 folds = 10)

    build_model(should_grid_search = True,
                should_filter = True,
                specs = specs)
