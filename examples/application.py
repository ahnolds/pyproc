#!/usr/bin/env python

from flask import Flask, request, render_template, redirect
import pyproc

def dummy_handler(filename):
    """A dummy file handler"""
    import time
    import uuid
    time.sleep(1)
    return str(uuid.uuid4())
# Add the dummy file handler
pyproc.add_file_handler('dummy', dummy_handler)
# Watch the sample directory
pyproc.watch_dir('/tmp/watched')

# Set up the flask app
app = Flask(__name__)

@app.route('/')
def home():
    """Render the homepage"""
    return render_template("home.html")

@app.route('/select', methods=['GET', 'POST'])
def select():
    """Select the items to run on"""
    if request.method == 'POST':
        # Explicitly request all the relevant files to move them up the queue
        for filename in request.form.getlist('files'):
            pyproc.enqueue_file(filename)
        return redirect('/select')
    else: # GET
        filenames = pyproc.all_available()
        return render_template("select.html", filenames=filenames)

@app.route('/status/<filename>')
def status():
    """Get the status of the given item"""
    return {'dummy': pyproc.get_handler_output(filename, 'dummy')}

@app.route('/results')
def results():
    """Render the results"""
    return render_template("results.html")

if __name__ == '__main__':
    #app.run(host='0.0.0.0', port=80)
    app.run(debug=True)