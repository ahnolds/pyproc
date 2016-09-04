#!/usr/bin/env python

# Import Flask for the web component
from flask import Flask, jsonify, request, render_template, redirect
# Import pyproc.Watcher to watch the specified directory and handle files in it
from pyproc import Watcher
# Import the (dummy) processor to call on all watched files
import dummy_processor

# Create a watcher to call dummy_processor.handler on all files in /tmp/watched
watcher = Watcher('/tmp/watched', dummy_processor.handler)

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

    Always returns a json blob with one text field named "msg". The status code
    identifies the status of the file:
        * 200: done processing
        * 202: not yet done processing
        * 404: not being watched/processed
        * 400: no file specified

    Note that this does not enqueue the queried files, and is meant to check the
    status of enqueued files after a call to select
    """
    # Get the filename and make sure it was given
    filename = request.args.get('filename')
    if filename is None:
        return jsonify(msg='Please specify a filename'), 400
    # Check if the file is being watched
    if filename not in watcher.all_available():
        return jsonify(msg='The requested file is not being watched'), 404
    # See if the file is processed yet
    try:
        result = watcher[filename]
    except KeyError:
        # Not yet done processing
        return jsonify(msg='The requested file has not yet been processed'), 202
    else:
        # The file is available, return it
        return jsonify(msg='The result is available')

@app.route('/results')
def results():
    """Render the results"""
    return render_template("results.html")

if __name__ == '__main__':
    #app.run(host='0.0.0.0', port=80)
    app.run(debug=True)
