from helper import *

# DO THIS ON EC2
def build_album_side_data_from_database():
    # Get database to read from
    db = get_mongo_database('bandcamp', 'localhost')

    sf = update_sframe(name = 'album_side_data',
                       collection = 'albums',
                       database = db)

# DO THIS ON EC2
def build_from_album_tag_list(verbose = True):
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
    df = pd.read_csv('data/album_side_data_sf.csv')

    # Read through each row
    album_url_list = list()
    album_tag_list = list()
    value = list()
    i = 0
    count = len(df)
    for index, row in df.iterrows():
        album_url = row['_id']
        list_string = row['album_tags']
        album_tag_list += eval(list_string)
        album_url_list += [album_url] * len(eval(list_string))
        value += [1] * len(eval(list_string))


        if verbose:
            # Progress counter
            if i % 100 == 0:
                print "{} complete".format(round(float(i) / count, 2))
            i += 1

    # Create SFrame
    sf = graphlab.SFrame({'album_id': album_url_list,
                          'album_tag': album_tag_list,
                          'value': value})
    print sf


    # Sanity checks
    n_unique_albums = len(set(sf['album_id']))
    n_unique_tags = len(set(sf['album_tag']))
    print "Number of albums: {}".format(n_unique_albums)
    print "Number of tags: {}".format(n_unique_tags)

    # Dump
    dump_sf(sf, 'data/album_url_to_album_tag.csv')

    # Dump pickled dictionary
    d = get_album_url_to_art_dict()
    dump_dictionary_model(d, 'album_url_to_album_tag')

if __name__ == "__main__":
    build_from_album_tag_list(verbose = True)
