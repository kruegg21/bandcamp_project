from helper import *
from grid_search import grid_search
from model import make_test_predictions, model_specifications, custom_evaluation

def build_rank_factorization_model(data = None, should_grid_search = True,
                                   should_filter = True, specs = None,
                                   should_make_test_predictions = True,
                                   should_add_side_data = True):
    """
    Input:
        should_grid_search -- Bool indicating if we should grid search or not
        specs -- model_specifications Object with info about data
    """
    # Read data
    sf = graphlab.SFrame.read_csv('data/{}.csv'.format(data))

    # Train
    model = rank_factorization_recommender(sf,
                                           specs,
                                           dump = True,
                                           should_grid_search = should_grid_search,
                                           should_add_side_data = should_add_side_data)

    # Make Test Predictions
    if should_make_test_predictions:
        make_test_predictions(model)

def rank_factorization_recommender(sf, specs, dump = True, train = True,
                                   should_grid_search = False,
                                   should_add_side_data = True):
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

    # Get side data
    if should_add_side_data:
        side_data_sf = graphlab.SFrame.read_csv('data/album_side_data_sf.csv')

        # Convert price to to int
        side_data_sf['album_id'] = side_data_sf['_id']
        temp_df = side_data_sf.to_dataframe().replace({'Name your price': 0})
        temp_df['price'] = pd.to_numeric(temp_df['price'])
        side_data_sf = graphlab.SFrame(temp_df)
        item_data_sf = side_data_sf['album_id', 'price']
        print item_data_sf['price'].dtype()

        album_to_tag_sf = graphlab.SFrame.read_csv('data/album_url_to_album_tag.csv')
        for tag in album_to_tag_sf.to_dataframe()['album_tag'].value_counts().index[:10]:
            album_with_tag_list = list(album_to_tag_sf[album_to_tag_sf['album_tag'] == tag]['album_id'])
            item_data_sf['album_has_{}'.format(tag)] = side_data_sf['_id'].is_in(album_with_tag_list)
    else:
        item_data_sf = None

    # Create model
    rec_model = graphlab.ranking_factorization_recommender.create(
                  train_set,
                  target = specs.params['target'],
                  user_id = specs.params['user_id'],
                  item_id = specs.params['item_id'],
                  item_data = item_data_sf,
                  binary_target = specs.params['binary_target'],
                  max_iterations = specs.params['max_iterations'],
                  num_factors = specs.params['num_factors'],
                  ranking_regularization = specs.params['ranking_regularization'],
                  linear_regularization = specs.params['linear_regularization'],
                  unobserved_rating_value = specs.params['unobserved_rating_value'],
                  regularization = specs.params['regularization'])

    # Display Precision on test set
    print rec_model.evaluate_precision_recall(test_set, cutoffs = [10,20,100], exclude_known = True)
    print rec_model.get_similar_items()

    # Retrain on full data set
    #####

    #####

    # Dump
    if dump:
        rec_model.save('factorization_recommender')

    return rec_model

if __name__ == "__main__":
    # Grid Search Parameters
    param_grid = dict([('target', 'rating'),
                       ('user_id', '_id'),
                       ('item_id', 'album_id'),
                       ('binary_target', True),
                       ('max_iterations', 100),
                       ('regularization', [1e-6, 1e-7, 1e-5]),
                       ('linear_regularization', [1e-4, 1e-5, 1e-3]),
                       ('ranking_regularization', [0.5]),
                       ('num_sampled_negative_examples', [4]),
                       ('unobserved_rating_value', [0]),
                       ('num_factors', [200, 100])
                      ])

    # Model Parameters
    params = dict([('target', 'rating'),
                   ('user_id', '_id'),
                   ('item_id', 'album_id'),
                   ('binary_target', True),
                   ('max_iterations', 100),
                   ('regularization', [1e-6, 1e-7, 1e-5]),
                   ('linear_regularization', [1e-4, 1e-5, 1e-3]),
                   ('ranking_regularization', 0.5),
                   ('unobserved_rating_value', 0),
                   ('num_factors', [50, 100])
                  ])

    # Specifications for building
    specs = model_specifications(param_grid = param_grid,
                                 params = params,
                                 user_count_min_cutoff = 100,
                                 user_count_max_cutoff = np.inf,
                                 album_count_max_cutoff = 1200,
                                 album_count_min_cutoff = 40,
                                 folds = 5,
                                 should_shuffle_folds = True)

    # Build Model
    build_rank_factorization_model(data = 'user_to_album_sf_album_id_filtered',
                                   should_grid_search = True,
                                   should_make_test_predictions = True,
                                   should_add_side_data = True,
                                   specs = specs)
