import time
import uuid

def handler(filename):
    """A dummy file handler (waits ten seconds then returns a uuid)"""
    time.sleep(10)
    return str(uuid.uuid4())
