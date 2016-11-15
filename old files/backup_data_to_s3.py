from boto.s3.connection import S3Connection
from boto.s3.key import Key
import os
ACCESS_KEY = os.environ['AWS_ACCESS_KEY_ID']
SECRET_KEY = os.environ['AWS_SECRET_ACCESS_KEY']
BUCKET_NAME = 'kruegg'
paths = ['user_collections_new.bson', 'user_collections_new.metadata.json',
         'albums.bson', 'albums.metadata.json']

def save_file_in_s3(filename):
    conn = S3Connection(ACCESS_KEY,
                        SECRET_KEY,
                        host="s3-website-us-west-2.amazonaws.com")
    bucket = conn.get_bucket(BUCKET_NAME)
    k = Key(bucket)
    k.key = filename
    k.set_contents_from_filename(filename)

if __name__ == "__main__":
    for path in paths:
        full_path = os.getcwd() + '/dump/bandcamp/' + path
        save_file_in_s3(full_path)
