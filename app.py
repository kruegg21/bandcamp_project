from flask import Flask, request, render_template, url_for
from flask_bootstrap import Bootstrap


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
    pred_url_list = 

    return url_list

if __name__ == '__main__':
  app.run(host = "0.0.0.0", port = int("8000"), debug = True)
