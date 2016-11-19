from flask import Flask, request, render_template

app = Flask(__name__)

# Home page
@app.route('/')
def index():
    return "hello"

@app.route('/dashboard')
def html_greeting_via_template(name):
    rendered_template = render_template('templates/dashboard_template.html')
    return rendered_template
