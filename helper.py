import graphlab
import time

# Contains column names for DataFrames we are working with
column_names_dict = {
    'user_to_album_list' : ['_id', 'albums', 'n_albums']
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
