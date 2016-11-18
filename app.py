from flask import Flask, request

app = Flask(__name__)

# Home page
@app.route('/')
def index():
    return "hello"

# 
