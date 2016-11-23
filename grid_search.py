import graphlab
from helper import custom_evaluation
import time

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
    while True:
        time.sleep(10)
        print job.get_status()


    print job.get_results()
