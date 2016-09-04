#!/usr/bin/env python

from flask import Flask, jsonify, request, render_template, redirect

import pyproc

def dummy_handler(filename):
    """A dummy file handler"""
    import time
    import uuid
    time.sleep(10)
    return str(uuid.uuid4())
watcher = pyproc.Watcher('/tmp/watched', dummy_handler)

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
            watcher.enqueue_file(filename)
        return redirect('/select')
    else: # GET
        filenames = sorted(watcher.all_available())
        return render_template("select.html", filenames=filenames)

@app.route('/status')
def status():
    """Get the status of the given item

    Note that this does not enqueue the queried files, and is meant to check the
    status of enqueued files after a call to select
    """
    # Get the filename and make sure it was given
    filename = request.args.get('filename')
    if filename is None:
        return jsonify(message='Please specify a filename'), 400
    # Check if the file is being watched
    if filename not in watcher.all_available():
        return jsonify(message='The requested file is not being watched'), 404
    # See if the file is processed yet
    try:
        result = watcher[filename]
    except KeyError:
        # Not yet done processing
        return jsonify(message='The requested file is being processed'), 202
    else:
        # The file is available, return it
        return jsonify(value=watcher[filename])

@app.route('/results')
def results():
    """Render the results"""
    return render_template("results.html")

if __name__ == '__main__':
    #app.run(host='0.0.0.0', port=80)
    app.run(debug=True)
