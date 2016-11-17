import graphlab
graphlab.set_runtime_config('GRAPHLAB_DEFAULT_NUM_PYLAMBDA_WORKERS', 64)
import graphlab.aggregate as agg
import time

# Contains column names for DataFrames we are working with
column_names_dict = {
    'user_to_album_list' : ['_id', 'albums', 'n_albums'],
    'user_to_tags' : ['_id', 'tags']
}

# Timing function
def timeit(method):
    """
    Timing wrapper
    """
    def timed(*args, **kw):
        ts = time.time()
        result = method(*args, **kw)
        te = time.time()
        print 'Running %r took %2.4f sec\n' % \
              (method.__name__, te-ts)
        return result
    return timed

def show_sframe_sparcity(sf):
    n_albums = len(sf['album_id'].unique())
    n_users = len(sf['_id'].unique())

    print "Number of unique albums: {}".format(n_albums)
    print "Number of unique users: {}".format(n_users)
    print "Number of filled cells: {}".format(len(sf))
    print "Matrix sparcity: {}\n\n".format(float(len(sf)) / (n_albums * n_users))



@timeit
def low_pass_filter_on_counts(sf, column = None, cutoff = None, name = None, dump = True):
    # Show initial sparcity
    print "\nInitial SFrame sparcity"
    show_sframe_sparcity(sf)

    # Albums counts
    counts_sf = sf.groupby(key_columns = column,
                           operations = {'count': agg.COUNT()})

    # Make SArray of albums with high rating counts
    high_album_counts = counts_sf[counts_sf['count'] > cutoff][column]

    # Filter
    filtered_sf = sf.filter_by(high_album_counts, column, exclude = False)

    # Dump
    if dump:
        filtered_sf.save('data/{}{}_filtered.csv'.format(name, column), format = 'csv')

    # Show sparcity
    show_sframe_sparcity(filtered_sf)

    return filtered_sf
