from helper import *

# DO THIS ON LOCAL
def build_user_to_album_art_from_database():
    # Get databasea to read from
    db = get_mongo_database('bandcamp')

    df = update_dataframe(name = 'user_to_album_art',
                          feature_building_method = album_art,
                          database = db,
                          dump = True,
                          test = False)

# DO THIS ON EC2
def build_from_album_art_list(verbose = True):
    """
    Input:
        None
    Output:
        None

    Reads from 'user_to_album_art.csv', which contains a column of user IDs and
    a column of a list of tuples of album url and art id. It then parses this
    list of album urls and art ids into an SFrame of columns 'album_url' and
    'art_id' and dumps to file 'album_url_to_art_id.csv'
    """
    df = pd.read_csv('data/user_to_album_art.csv')

    # Read through each row
    album_url_dict = dict()
    i = 0
    count = len(df)
    for index, row in df.iterrows():
        list_string = row['album_art_code']
        album_art_list = eval(list_string)
        album_url_dict.update(dict(album_art_list))

        if verbose:
            # Progress counter
            if i % 100 == 0:
                print "{} complete".format(round(float(i) / count, 2))
            i += 1

    # Create SFrame
    sf = graphlab.SFrame({'album_url': album_url_dict.keys(),
                          'art_id': album_url_dict.values()})

    # Sanity checks
    n_albums = len(sf)
    n_unique_albums = len(set(sf['album_url']))
    print "Number of albums: {}".format(n_albums)
    print "Album URLs are unique: {}".format(n_albums == n_unique_albums)

    # Dump
    dump_sf(sf, 'data/album_url_to_art_id.csv')

    # Dump pickled dictionary
    d = get_album_url_to_art_dict()
    dump_dictionary_model(d, 'album_url_to_art_id')

if __name__ == "__main__":
    build_from_album_art_list(verbose = True)
