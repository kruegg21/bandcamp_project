from helper import *
from grid_search import grid_search
from model import make_test_predictions, model_specifications, custom_evaluation

def build_serendipity_model(data = None, should_grid_search = True,
                            should_filter = True, specs = None,
                            should_make_test_predictions = True):
    """
    Input:
        should_grid_search -- Bool indicating if we should grid search or not
        specs -- model_specifications Object with info about data
    """
    # Read data
    sf = graphlab.SFrame.read_csv('data/{}.csv'.format(data))

    # Add serendipity
    sf = add_serendipity(sf, specs = specs)
    print sf

    # Train
    model = serendipity_recommender(sf,
                                    specs,
                                    dump = True,
                                    should_grid_search = should_grid_search)

    # Make Test Predictions
    if should_make_test_predictions:
        make_test_predictions(model)

def add_serendipity(sf, specs = None):
    album_url_to_count_dict = get_album_url_to_count_dict()
    sf['album_counts'] = sf.apply(lambda x: album_url_to_count_dict[x['album_id']]).astype(float)
    sf['log_inversealbum_counts'] = np.log(sf['album_counts'])
    sf['serendipity_score'] = sf['log_album_counts'] * specs.serendipity_coefficient
    return sf

def serendipity_recommender(sf, specs, dump = True, train = True,
                            should_grid_search = False):
    # Test Train Split
    if specs.should_shuffle_folds:
        shuffled_sf = graphlab.toolkits.cross_validation.shuffle(sf)
        (train_set, test_set) = shuffled_sf.random_split(0.8, seed = 1)
    else:
        (train_set, test_set) = sf.random_split(0.8, seed = 1)

    # Grid Search
    if should_grid_search:
        grid_search(train_set,
                    model_factory = graphlab.ranking_factorization_recommender.create,
                    specs = specs)

    # Create model
    rec_model = graphlab.ranking_factorization_recommender.create(
                  train_set,
                  target = specs.params['target'],
                  user_id = specs.params['user_id'],
                  item_id = specs.params['item_id'],
                  binary_target = specs.params['binary_target'],
                  max_iterations = specs.params['max_iterations'],
                  num_factors = specs.params['num_factors'],
                  ranking_regularization = specs.params['ranking_regularization'],
                  linear_regularization = specs.params['linear_regularization'],
                  unobserved_rating_value = specs.params['unobserved_rating_value'],
                  regularization = specs.params['regularization'])

    # Data print out
    print rec_model.evaluate_precision_recall(test_set, cutoffs = [100,200,1000], exclude_known = False)
    print rec_model.get_similar_items()

    # Dump
    if dump:
        rec_model.save('factorization_recommender')

    return rec_model

if __name__ == "__main__":
    # Grid Search Parameters
    param_grid = dict([('target', 'serendipity_score'),
                       ('user_id', '_id'),
                       ('item_id', 'album_id'),
                       ('binary_target', False),
                       ('max_iterations', 500),
                       ('regularization', [1e-5, 1e-3, 1e-1]),
                       ('linear_regularization', [1e-3, 1e-1]),
                       ('ranking_regularization', [0.5, 0.4, 0.3]),
                       ('num_sampled_negative_examples', [4])
                      ])

    params = dict([('target', 'serendipity_score'),
                   ('user_id', '_id'),
                   ('item_id', 'album_id'),
                   ('binary_target', False),
                   ('max_iterations', 500),
                   ('ranking_regularization', 0.4),
                   ('linear_regularization', 0.001),
                   ('regularization', .001),
                   ('unobserved_rating_value', 0),
                   ('num_factors', 200)])

    # Specifications for building
    specs = model_specifications(param_grid = param_grid,
                                 params = params,
                                 user_count_min_cutoff = 100,
                                 user_count_max_cutoff = np.inf,
                                 album_count_max_cutoff = 1200,
                                 album_count_min_cutoff = 40,
                                 serendipity_coefficient = 1,
                                 folds = 10,
                                 should_shuffle_folds = True)

    build_serendipity_model(data = 'user_to_album_sf_album_id_filtered',
                            should_grid_search = False,
                            should_make_test_predictions = True,
                            specs = specs)
