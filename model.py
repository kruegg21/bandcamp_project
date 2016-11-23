import graphlab
graphlab.set_runtime_config('GRAPHLAB_DEFAULT_NUM_PYLAMBDA_WORKERS', 64)
import numpy as np
from sklearn.feature_extraction.text import TfidfTransformer
from scipy.sparse import coo_matrix
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
        self.rank_factorization_param_grid = kwargs.get('rank_factorization_param_grid')
        self.item_similarity_param_grid = kwargs.get('item_similarity_param_grid')
        self.rank_factorization_params = kwargs.get('rank_factorization_params')
        self.item_similarity_params = kwargs.get('item_similarity_params')
        self.params = kwargs.get('params')
        self.folds = kwargs.get('folds')
        self.should_tfidf = kwargs.get('should_tfidf')
        self.should_shuffle_folds = kwargs.get('should_shuffle_folds')

def build_model(should_grid_search = True, should_filter = True,
                specs = None, should_make_test_predictions = True):
    """
    Input:
        should_grid_search -- Bool indicating if we should grid search or not
        specs -- model_specifications Object with info about data
    """
    sf = graphlab.SFrame.read_csv('data/user_to_album_sf_album_id_filtered.csv')

    if specs.should_tfidf:
        sf = sparse_matrix_tfidf(sf)
        dump_sf(sf, 'data/tfidf_sf.csv')

    sf = graphlab.SFrame.read_csv('data/tfidf_sf.csv')
    # Grid Search
    if should_grid_search:
        graphlab_grid_search(sf, specs)

    # Train
    model = graphlab_factorization_recommender(sf, specs, dump = True)

    if should_make_test_predictions:
        make_test_predictions(model)

@timeit
def sparse_matrix_tfidf(sf):
    print "Starting TF-IFD"
    print sf

    album_to_count_dict = read_dictionary_model('album_url_to_count')

    popularity_weighted_sf = None

    # # Transform to DataFrame
    # df = sf.to_dataframe()
    #
    # print "Translated to DF"
    #
    # # Make translation dictionaries for '_id' and 'album_id'
    # _id_translation_dict = reverse_dict(dict(enumerate(df['_id'].unique())))
    # album_translation_dict = reverse_dict(dict(enumerate(df['album_id'].unique())))
    #
    # print "Translation dictionaries made"
    #
    # # Create scipy sparse matrix
    # df['_id'].replace(_id_translation_dict, inplace = True)
    # df['album_id'].replace(album_translation_dict, inplace = True)
    # row = df['_id'].values
    # col = df['album_id'].values
    # data = df['rating'].values
    # print "Replaced with translated values"
    # sparse_mat = coo_matrix((data, (row, col)), shape = (row.shape[0], col.shape[0]))
    # print "Converted to scipy matrix"
    # save_sparse_coo('sparse_mat_dump.coo', sparse_mat)
    #
    # # TF-IDF scores
    # transformer = TfidfTransformer()
    # tfidf_sparse_mat = transformer.fit_transform(sparse_mat).tocoo()
    # print "Transformed to TF-IDF scores"
    # print sparse_mat.data
    # print tfidf_sparse_mat.data
    #
    # print "Created TFIDF ratings"
    #
    # tfidf_df = pd.DataFrame()
    # tfidf_df['rating'] = tfidf_sparse_mat.data
    # tfidf_df['_id'] = tfidf_sparse_mat.row
    # tfidf_df['album_id'] = tfidf_sparse_mat.col
    # tfidf_df['_id'].replace(reverse_dict(_id_translation_dict), inplace = True)
    # tfidf_df['album_id'].replace(reverse_dict(album_translation_dict), inplace = True)
    #
    # print "Make secondary dataframe"

    return graphlab.SFrame(tfidf_df)

def graphlab_grid_search(sf, specs = None):
    # Create K-Folds splits
    if specs.should_shuffle_folds:
        shuffled_sf = graphlab.toolkits.cross_validation.shuffle(sf)
        folds = graphlab.cross_validation.KFold(shuffled_sf, specs.folds)
    else:
        folds = graphlab.cross_validation.KFold(sf, specs.folds)

    # Run Grid Search
    job = graphlab.grid_search.create(folds,
                                      graphlab.ranking_factorization_recommender.create,
                                      specs.rank_factorization_param_grid,
                                      evaluator = custom_evaluation)
    print job.get_results()
    log_grid_search_results(grid_search_job)

    # Put optimal parameters in specifications


def log_grid_search_results(grid_search_job, specs):
    """
    Input:
        grid_search_job -- GraphLab ModelSearchJob object
    Ouptu:
        None

    Dumps the result of grid search to proper text file
    """


def custom_evaluation(model, train, test):
    recommendations = model.evaluate_precision_recall(test,
                                                      cutoffs = [10,200,1000],
                                                      exclude_known = False)
    score = recommendations['precision_recall_overall']['precision'][0]
    return {'precision_10': score}

def make_test_predictions(model):
    # Make predictions
    album_list = metal_album_list
    rating_list = [1] * len(album_list)
    _id_list = ['https://bandcamp.com/kruegg'] * len(album_list)

    # Get keys in correct format
    album_list = [convert_to_mongo_key_formatting(x) for x in album_list]
    _id_list = [convert_to_mongo_key_formatting(x) for x in _id_list]

    # Create SFrame
    prediction_sf = graphlab.SFrame({'_id': _id_list,
                                     'album_id': album_list,
                                     'rating': rating_list})
    prediction_sf = convert_to_truncated_string_ids(prediction_sf)

    # Make recommendations
    recommendations_sf = model.recommend(users = ['https://bandcamp.com/kruegg'],
                                         k = 150,
                                         new_user_data = prediction_sf)

    # Split into logical columns
    recommendations_sf = split_into_artist_album(recommendations_sf)

    print recommendations_sf

    # Sample
    recommendations_sf = graphlab.SFrame(recommendations_sf.to_dataframe().
                            drop_duplicates(subset = ['artist']))

    print recommendations_sf

    # Dump recommendations to CSV
    dump_sf(recommendations_sf, 'data/recommendations.csv')

def graphlab_factorization_recommender(sf, specs, dump = True, train = True):
    if train:
        # Test train split
        (train_set, test_set) = sf.random_split(0.8, seed=1)

        # Create model
        if specs.should_tfidf:
            binary_target = False
        else:
            binary_target = True

        rec_model = graphlab.ranking_factorization_recommender.create(
                      train_set,
                      target = specs.rank_factorization_params.target,
                      user_id = specs.rank_factorization_params.target.user_id,
                      item_id = spec.rank_factorization_params.item_id,
                      binary_target = specs.rank_factorization_params.binary_target,
                      max_iterations = specs.rank_factorization_params.max_iterations,
                      ranking_regularization = specs.rank_factorization_params.ranking_regularization,
                      linear_regularization = specs.rank_factorization_params.linear_regularization,
                      regularization = specs.rank_factorization_params.regularization)

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
    # Grid Search Parameters
    rank_factorization_param_grid = dict([('target', 'rating'),
                                          ('user_id', '_id'),
                                          ('item_id', 'album_id'),
                                          ('binary_target', True),
                                          ('max_iterations', 500),
                                          ('regularization', [1e-5, 1e-3, 1e-1]),
                                          ('linear_regularization', [1e-3, 1e-1]),
                                          ('ranking_regularization', [0.5, 0.4, 0.3]),
                                          ('num_sampled_negative_examples', [4])
                                         ])

    item_similarity_param_grid = dict()

    rank_factorization_params = dict([('target', 'rating'),
                                      ('user_id', '_id'),
                                      ('item_id', 'album_id'),
                                      ('binary_target', True),
                                      ('max_iterations', 500),
                                      ('ranking_regularization', 0.4),
                                      ('linear_regularization', 0.001),
                                      ('regularization', .001)])

    item_similarity_params = dict()


    # Specifications for building
    specs = model_specifications(rank_factorization_param_grid = rank_factorization_param_grid,
                                 item_similarity_param_grid = item_similarity_param_grid,
                                 rank_factorization_params = rank_factorization_params,
                                 item_similarity_params = item_similarity_params,
                                 user_count_min_cutoff = 100,
                                 user_count_max_cutoff = np.inf,
                                 album_count_max_cutoff = 1200,
                                 album_count_min_cutoff = 40,
                                 folds = 10,
                                 should_tfidf = False,
                                 should_shuffle_folds = True)

    build_model(should_grid_search = False,
                should_make_test_predictions = True,
                specs = specs)
