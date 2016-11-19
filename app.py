from flask import Flask, request, render_template

app = Flask(__name__)

# Home page
@app.route('/')
def index():
    return "hello"

@app.route('/dashboard')
def dashboard():
    rendered_template = render_template('dashboard_template.html')
    return rendered_template
