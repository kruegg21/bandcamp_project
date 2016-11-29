from helper import *
from grid_search import grid_search
from model import make_test_predictions, model_specifications

def build_item_similarity_model(data = None, should_grid_search = True,
                                should_filter = True, specs = None,
                                should_make_test_predictions = True):
    """
    Input:
        should_grid_search -- Bool indicating if we should grid search or not
        specs -- model_specifications object with info about parameters and data
    """
    # Read data
    sf = graphlab.SFrame.read_csv('data/{}.csv'.format(data))

    # Train
    model = item_similarity_recommender(sf,
                                        specs,
                                        dump = True,
                                        should_grid_search = should_grid_search)

    # Make Test Predictions
    if should_make_test_predictions:
        make_test_predictions(model)

    return model

def item_similarity_recommender(sf, specs, dump = True, train = True,
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
                    model_factory = graphlab.item_similarity_recommender.create,
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

    # Create model without test data
    rec_model = graphlab.item_similarity_recommender.create(
                          train_set,
                          target = specs.params['target'],
                          user_id = specs.params['user_id'],
                          item_id = specs.params['item_id'],
                          item_data = item_data_sf,
                          similarity_type = specs.params['similarity_type'],
                          threshold = specs.params['threshold'],
                          only_top_k = specs.params['only_top_k'],
                          target_memory_usage = specs.params['target_memory_usage'])
    # Data print out
    print rec_model.evaluate_precision_recall(test_set, cutoffs = [10,20,100], exclude_known = True)

    # Create full model
    rec_model = graphlab.item_similarity_recommender.create(
                          sf,
                          target = specs.params['target'],
                          user_id = specs.params['user_id'],
                          item_id = specs.params['item_id'],
                          similarity_type = specs.params['similarity_type'],
                          threshold = specs.params['threshold'],
                          only_top_k = specs.params['only_top_k'],
                          target_memory_usage = specs.params['target_memory_usage'])

    # Dump
    if dump:
        rec_model.save('models/item_similarity_recommender')

    return rec_model


if __name__ == "__main__":
    # Grid Search Parameters
    param_grid = dict([('target', ['rating']),
                       ('user_id', ['_id']),
                       ('item_id', ['album_id']),
                       ('similarity_type', ['jaccard']),
                       ('threshold', [0.0001]),
                       ('only_top_k', [1000]),
                       ('target_memory_usage', [8589934592 * 8])
                      ])

    # Model Parameters
    params = dict([('target', 'rating'),
                   ('user_id', '_id'),
                   ('item_id', 'album_id'),
                   ('similarity_type', 'jaccard'),
                   ('threshold', 0.001),
                   ('only_top_k', 100),
                   ('target_memory_usage', 8589934592 * 8)
                  ])

    # Specifications for building
    specs = model_specifications(param_grid = param_grid,
                                 params = params,
                                 user_count_min_cutoff = 100,
                                 user_count_max_cutoff = np.inf,
                                 album_count_max_cutoff = 1200,
                                 album_count_min_cutoff = 40,
                                 folds = 10,
                                 should_shuffle_folds = True)

    # Build Model
    m = build_item_similarity_model(data = 'user_to_album_sf_album_id_filtered',
                                    should_grid_search = False,
                                    should_make_test_predictions = True,
                                    specs = specs)
