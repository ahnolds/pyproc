import collections
import inotify.adapters
import inotify.constants
import os
import pyhash
import Queue
import threading

# Priorities for the priority queue
PRIORITY_REQUEST = 0
PRIORITY_ORIGINAL = 1
PRIORITY_INOTIFY = 2

# Get a queue to store the files in
_queue = Queue.PriorityQueue()

# File handler callbacks
_handler_funcs = collections.OrderedDict()

def add_file_handler(name, func):
    """Add a handler that will be called once for each unique file

    Handlers must be added BEFORE watching a directory or enqueuing a file,
    since they are NOT called on already seen files.

    Handlers are called in the order they are added
    """
    _handler_funcs[name] = func

# Track what files are being watched
_watched_files = set([])

def enqueue_file(filename):
    """Request that the specified file be processed ASAP

    The specified file is enqueued with PRIORITY_REQUEST
    """
    _watched_files.add(filename)
    _queue.put((PRIORITY_REQUEST, filename))

# Track what directories are being watched
_watched_dirs = set([])

def watch_dir(dirname):
    """Add all files in the directory to the queue and watch for future changes

    Any files already in the directory are enqueued with PRIORITY_ORIGINAL, and
    any files for which changes are subsequently detected are enqueued with
    PRIORITY_INOTIFY

    CAUTION: This may not work properly if the watched directory is a Docker
             mounted volume
    """
    # Add the directory to the list of watched directories
    _watched_dirs.add(dirname)

    # Add all files initially below the directory to the queue
    for (dirpath, dirnames, filenames) in os.walk(dirname):
        for name in filenames:
            _queue.put((PRIORITY_ORIGINAL, os.path.join(dirpath, name)))

    def watcher():
        """Watch for changes in the watched directory and put them on the queue

        CAUTION: This may not work properly if the watched directory is a Docker
                 mounted volume
        """
        # Watch for files being closed after being written
        mask  = inotify.constants.IN_CLOSE_WRITE
        # Watch for attribute changes (like permission changes)
        mask |= inotify.constants.IN_ATTRIB
        # Watch for file moves into the directory
        mask |= inotify.constants.IN_MOVED_TO

        # Watch for changes in the directory
        i = inotify.adapters.InotifyTree(dirname, mask=mask)
        for event in i.event_gen():
            if event is None:
                continue
            (header, type_names, watch_path, filename) = event
            if header.mask & inotify.constants.IN_ISDIR:
                # We aren't interested in directory changes here
                continue
            full_filename = os.path.join(watch_path.decode('utf-8'), filename.decode('utf-8'))
            if header.mask & inotify.constants.IN_MOVED_TO:
                # File was moved into the directory we're watching
                pass
            elif header.mask & inotify.constants.IN_CLOSE_WRITE:
                # File was closed after being written to
                pass
            elif header.mask & inotify.constants.IN_ATTRIB:
                # File metadata (e.g. permissions) was changed
                pass
            else:
                # Some other event type we aren't interested in
                continue
            # Put this file in the queue
            _queue.put((PRIORITY_INOTIFY, full_filename))

    # Run the watcher in a seperate thread
    inotify_thread = threading.Thread(target=watcher, name='inotify')
    inotify_thread.daemon = True
    inotify_thread.start()

def all_available():
    """Get the list of all available files

    Note that available just means that it exists in the watched directory,
    not necessarily that it has already been processed"""
    available = set([])
    # Add any readable files below watched directories
    for watched_dir in _watched_dirs:
        for (dirpath, dirnames, filenames) in os.walk(watched_dir):
            for name in filenames:
                fullname = os.path.join(dirpath, name)
                if os.access(fullname, os.R_OK):
                    available.add(fullname)
    # Add any readable files that were explicitly enqueued
    for watched_file in _watched_files:
        if os.access(watched_file, os.R_OK):
            available.add(watched_file)
    return available

def _get_hash(filename):
    """Get a hash of this file (cryptographic hash not necessary here)"""
    # Set up the hash function
    hasher = pyhash.city_128()
    value = 0L
    # Read up to 1MB at a time
    BLOCK_SIZE = 2**20
    with open(filename, 'rb') as handle:
        # Read the data one block at a time
        buf = handle.read(BLOCK_SIZE)
        while len(buf) > 0:
            # Hash each block, using the previous output as the seed
            value = hasher(buf, seed=value)
            buf = handle.read(BLOCK_SIZE)
    # Return the final output
    return value

# Processed items
_outputs = {}

def get_handler_output(filename, handler_name):
    """Get the output from running the specified handler on the given file

    If the handler has not yet been run on the file, raises KeyError
    """
    if filename in _outputs:
        if handler_name in _outputs[filename]:
            return _outputs[filename][handler_name]
    raise KeyError('{} has not been run on {}'.format(handler_name, filename))

# Tracking for files we've already seen
_seen = {}

def _reader():
    """Get files from the queue forever"""
    while True:
        priority, filename = _queue.get()
        # First, make sure the file is readable
        if not os.access(filename, os.R_OK):
            continue
        # Next, check if we've seen this file before
        digest = _get_hash(filename)
        if digest in _seen:
            # We've seen a file with this hash before, use the cached result
            _outputs[filename] = _seen[digest]
        else:
            # New or modified file, handle it
            _outputs[filename] = {}
            for handler_name, handler_func in _handler_funcs.iteritems():
                _outputs[filename][handler_name] = handler_func(filename)
            # Cache the result
            _seen[digest] = _outputs[filename]

# Run the reader in a seperate thread
_reader_thread = threading.Thread(target=_reader, name='reader')
_reader_thread.daemon = True
_reader_thread.start()
