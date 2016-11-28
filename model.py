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
        self.params = kwargs.get('params')
        self.param_grid = kwargs.get('param_grid')
        self.folds = kwargs.get('folds')
        self.should_tfidf = kwargs.get('should_tfidf')
        self.should_shuffle_folds = kwargs.get('should_shuffle_folds')
        self.serendipity_coefficient = kwargs.get('serendipity_coefficient')

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
#
# def graphlab_grid_search(sf, specs = None):
#     # Create K-Folds splits
#     if specs.should_shuffle_folds:
#         shuffled_sf = graphlab.toolkits.cross_validation.shuffle(sf)
#         folds = graphlab.cross_validation.KFold(shuffled_sf, specs.folds)
#     else:
#         folds = graphlab.cross_validation.KFold(sf, specs.folds)
#
#     # Run Grid Search
#     job = graphlab.grid_search.create(folds,
#                                       graphlab.ranking_factorization_recommender.create,
#                                       specs.rank_factorization_param_grid,
#                                       evaluator = custom_evaluation)
#     print job.get_results()



def make_test_predictions(model):
    # Make predictions
    album_list = rap_album_list
    rating_list = [1] * len(album_list)
    _id_list = ['http://bandcamp.com/kruegg'] * len(album_list)

    # Get keys in correct format
    album_list = [convert_to_mongo_key_formatting(x) for x in album_list]
    _id_list = [convert_to_mongo_key_formatting(x) for x in _id_list]

    # Create SFrame
    prediction_sf = graphlab.SFrame({'_id': _id_list,
                                     'album_id': album_list,
                                     'rating': rating_list})
    # prediction_sf = convert_to_truncated_string_ids(prediction_sf)

    print prediction_sf

    # Make recommendations
    recommendations_sf = model.recommend(users = [convert_to_mongo_key_formatting('http://bandcamp.com/kruegg')],
                                         k = 150,
                                         new_observation_data = prediction_sf)

    # Split into logical columns
    recommendations_sf = split_into_artist_album(recommendations_sf)

    print recommendations_sf

    # Sample
    recommendations_sf = graphlab.SFrame(recommendations_sf.to_dataframe().
                            drop_duplicates(subset = ['artist']))

    print recommendations_sf

    # Dump recommendations to CSV
    dump_sf(recommendations_sf, 'data/recommendations.csv')



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
