import graphlab
from helper import custom_evaluation

def grid_search(sf, model_factory = None, specs = None):
    # Create K-Folds splits
    if specs.should_shuffle_folds:
        shuffled_sf = graphlab.toolkits.cross_validation.shuffle(sf)
        folds = graphlab.cross_validation.KFold(shuffled_sf, specs.folds)
    else:
        folds = graphlab.cross_validation.KFold(sf, specs.folds)

    # Run Grid Search
    job = graphlab.grid_search.create(folds,
                                      model_factory,
                                      specs.param_grid,
                                      evaluator = custom_evaluation)
    print job.get_results()
