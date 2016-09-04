import inotify.adapters
import inotify.constants
import logging
import os
import Queue
import threading
import time

from .digest import get_hash

# Priorities for the priority queue
PRIORITY_REQUEST = 0
PRIORITY_ORIGINAL = 1
PRIORITY_UPDATED = 2
PRIORITY_REQUEUE = 3

_LOGGER = logging.getLogger(__name__)

class Watcher(object):
    """A Watcher monitors a directory and handles all files below it"""

    def __init__(self, dirname, handler, num_reader_threads=4,
                 requeue_sleep_secs=1):
        """Construct a new Watcher

        Arguments
            dirname: The name of the directory to watch
            handler: The handler function to call on each unique file
            num_reader_threads: The number of threads to use to read from the
                                file queue and call the handler from
            requeue_sleep_secs: The number of seconds to sleeep when requeuing
                                a file that is being processed in another thread
        """
        self._watched_dir = dirname
        self._handler_func = handler
        self._requeue_secs = requeue_sleep_secs
        self._outputs = {}
        self._outputs_lock = threading.Lock()

        # Get a queue to store the files in
        self._queue = Queue.PriorityQueue()

        # Start watching
        self._watch_dir()

        # Start reading
        self._read_queue(num_reader_threads)

    def enqueue_file(self, filename):
        """Request that the specified file be processed ASAP"""
        # Verify that this file is in the watched directory
        real_name = os.path.realpath(filename)
        real_dir = os.path.realpath(self._watched_dir) + os.sep
        if not real_name.startswith(real_dir):
            raise ValueError("Can only enqueue files in the watched directory")
        # Enqueue the file with top priority
        self._queue.put((PRIORITY_REQUEST, filename))

    def _watch_dir(self, use_inotify):
        """Watch for changes in the watched directory

        Any files already in the directory are enqueued with PRIORITY_ORIGINAL,
        and any files for which changes are subsequently detected are enqueued
        with PRIORITY_UPDATED.
        """

        def walk_directory(priority):
            """Add all files below the directory to the queue"""
            for (dirpath, dirnames, filenames) in os.walk(self._watched_dir):
                for name in filenames:
                    self._queue.put((priority, os.path.join(dirpath, name)))

        # Add any files initially in the directory
        walk_directory(PRIORITY_ORIGINAL)

        def inotify_watcher():
            """Use inotify to watch for changes in the directory

            In certain cases, the inotify-based watcher will not work correctly
            (for example, network filesystems, pseudo-filesystems like /proc,
            and volumes shared into a virtual machine). In this case, users
            would have to fall back to the periodic-directory-walk approach.
            """
            # Watch for files being closed after being written
            mask  = inotify.constants.IN_CLOSE_WRITE
            # Watch for attribute changes (like permission changes)
            mask |= inotify.constants.IN_ATTRIB
            # Watch for file moves into the directory
            mask |= inotify.constants.IN_MOVED_TO

            # Watch for changes in the directory
            i = inotify.adapters.InotifyTree(self._watched_dir, mask=mask)
            for event in i.event_gen():
                if event is None:
                    continue
                (header, type_names, watch_path, filename) = event
                if header.mask & inotify.constants.IN_ISDIR:
                    # We aren't interested in directory changes here
                    continue
                watch_path = watch_path.decode('utf-8')
                filename = filename.decode('utf-8')
                full_filename = os.path.join(watch_path, filename)
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
                self._queue.put((PRIORITY_UPDATED, full_filename))

        def walk_and_sleep_watcher(delay_seconds=60):
            """Periodically walk the directory to watch for changes

            This is generally inferior to the inotify-based watcher, since it
            lacks an easy way to tell if a file has been changed. To ensure
            it doesn't miss a file change, it simply requeues all the files on
            each pass. If the file hasn't changed, then the hash will be the
            same and the copy will be dropped relatively quickly, but the cost
            of hashing each file frequently can rapidly become non-trivial.

            However, in certain cases, the inotify-based watcher will not work
            correctly (for example, network filesystems, pseudo-filesystems like
            /proc, and volumes shared into a virtual machine). In this case,
            users would have to fall back to this approach.
            """
            while True:
                walk_directory(PRIORITY_UPDATED)
                time.sleep(delay_seconds)

        # Run the watcher in a seperate thread
        target = inotify_watcher if use_inotify else walk_and_sleep_watcher
        watch_thread = threading.Thread(target=target, name='watcher')
        watch_thread.daemon = True
        watch_thread.start()

    def all_available(self):
        """Get the list of all available files

        Note that available just means that it exists in the watched directory
        and is readable, not necessarily that it has already been processed.
        """
        available = set([])
        # Add any readable files below the watched directory
        for (dirpath, dirnames, filenames) in os.walk(self._watched_dir):
            for name in filenames:
                fullname = os.path.join(dirpath, name)
                if os.access(fullname, os.R_OK):
                    available.add(fullname)
        return available

    def __getitem__(self, filename):
        """Get the output from running the handler on the given file

        If the handler has not yet been run on the file, raises KeyError
        """
        with self._outputs_lock:
            if filename in self._outputs:
                return self._outputs[filename][0]
        raise KeyError('Handler has not been run on {}'.format(filename))

    def _read_queue(self, num_threads):
        """Get files from the queue forever"""
        digest_lock = threading.Lock()
        results = {}
        seen = set()
        seen_lock = threading.Lock()

        def set_output(filename, result, digest_time):
            """Set the result for the given file

            If there is already an output for this file, only update the output
            if the digest associated with this result is more recent than the
            digest associated with the previous result. If there isn't yet an
            output for this file, just set it and store the given time.
            """

            # TODO Is this implicitly assuming that the digest and output were
            #      computed in the same (i.e. interleaved) order between threads?
            with self._outputs_lock:
                if filename in self._outputs:
                    if digest_time > self._outputs[filename][1]:
                        self._outputs[fileanme] = (results[digest], digest_time)
                else:
                    self._outputs[filename] = (results[digest], digest_time)

        def wait_and_requeue(filename):
            """Launch a background thread to requeue the specified file later"""
            def requeuer(filename):
                """Wait the specified amount of time and the requeue the file"""
                time.sleep(self._requeue_secs)
                self._queue.put((PRIORITY_REQUEUE, filename))
            # Run the requeuer in a seperate thread
            requeuer_thread = threading.Thread(target=requeuer, args=filename)
            requeuer_thread.daemon = True
            requeuer_thread.start()

        def reader():
            """Read from the queue forever using the given number of threads"""
            while True:
                # Grab an item off the queue
                priority, filename = self._queue.get()
                # First, make sure the file is readable
                if not os.access(filename, os.R_OK):
                    continue
                # Next, get a hash of the file to check if we've seen it before
                with digest_lock:
                    digest_time = time.time()
                    digest = get_hash(filename)
                # Check if we've already processed this digest
                if digest in results:
                    set_output(filename, results[digest], digest_time)
                    continue
                # Check if the digest is currently being processed
                currently_being_processed = True
                with seen_lock:
                    if digest not in seen:
                        # Not already being processed so do it here
                        seen.add(digest)
                        currently_being_processed = False
                # It's currently being processed, so deffer processing this copy
                # Note: technically the processing could have completed between
                #       when we saw that it wasn't in results and when we saw
                #       that it was in seen, but at worst this adds one cycle.
                if currently_being_processed:
                    wait_and_requeue(filename)
                    continue
                # At this point, this is the first time seeing this digest
                try:
                    result = self._handler_func(filename)
                except:
                    # Something failed, allow retries for other files
                    with seen_lock:
                        seen.remove(digest)
                    # Log the failure
                    _LOGGER.exception('Caught exception for {}, skipping'.format(filename))
                else:
                    # The handler succeeded, set the result
                    set_output(filename, result, digest_time)
                    results[digest] = result

        # Start threads to run the reader
        for num in range(num_threads):
            thread_name = 'reader-{}'.format(num)
            reader_thread = threading.Thread(target=reader, name=thread_name)
            reader_thread.daemon = True
            reader_thread.start()
