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

@app.route('/results', methods=['POST'])
def results():
    data = request.form['exampleTextarea']
    return data

if __name__ == '__main__':
  app.run(host = "0.0.0.0", port = int("8000"), debug = True))
