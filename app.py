from flask import Flask, request

app = Flask(__name__)


# home page
@app.route('/')
def index():
    return "hello"

# if __name__ == '__main__':
#     app.run(host='0.0.0.0', port=8080, debug=True)
