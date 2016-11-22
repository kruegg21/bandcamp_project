from flask import Flask, request, render_template, url_for
from flask_bootstrap import Bootstrap
from helper import *


def create_app():
  app = Flask(__name__)
  Bootstrap(app)

  return app

app = create_app()

# Home page
@app.route('/')
def index():
    return "hello"

@app.route('/dashboard')
def dashboard():
    rendered_template = render_template('index.html')
    return rendered_template

@app.route('/results', methods=['POST', 'GET'])
def results():
    data = request.form['exampleTextarea']
    url_list = data.split('\n')

    """
    Predict should take a list of bandcamp album URLs and return a list of
    predictions
    """
    # pred_url_list = predict(url_list)
    pred_url_list = metal_album_list
    pred_url_list = [s.replace('https', 'http') for s in pred_url_list]

    # Get URLs to album art for each predicted album
    url_to_art_dict = get_album_art_to_url_dict()
    art_id_list = [url_to_art_dict[convert_to_mongo_key_formatting(x)] for x in pred_url_list]

    albums_list = list()
    for album_url, art_id in zip(pred_url_list, art_id_list):
        albums_list.append({'album_url': album_url, 'art_id': art_id})
    return render_template('results.html', items = albums_list)


if __name__ == '__main__':
  app.run(host = "0.0.0.0", port = int("8000"), debug = True)
